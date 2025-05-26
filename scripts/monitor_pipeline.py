#!/usr/bin/env python3

import snowflake.connector
import pandas as pd
from datetime import datetime, timedelta
import os
import json
import logging
import psutil
import requests
from typing import Dict, List, Any
import time
from prometheus_client import start_http_server, Gauge, Counter, Histogram
import threading
from airflow.models import DagRun
from airflow.settings import Session

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline_monitoring.log'),
        logging.StreamHandler()
    ]
)

class PipelineMonitor:
    """Monitoring system for the bike share ELT pipeline."""
    
    def __init__(self):
        """Initialize the pipeline monitor."""
        # Create monitoring directory
        os.makedirs('pipeline_monitoring', exist_ok=True)
        
        # Initialize Snowflake connection parameters
        self.conn_params = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'role': os.getenv('SNOWFLAKE_ROLE')
        }
        
        # Initialize Prometheus metrics
        self.metrics = {
            'pipeline_duration': Histogram('pipeline_duration_seconds', 'Pipeline execution duration'),
            'records_processed': Counter('records_processed_total', 'Total records processed'),
            'pipeline_success': Counter('pipeline_success_total', 'Successful pipeline runs'),
            'pipeline_failure': Counter('pipeline_failure_total', 'Failed pipeline runs'),
            'data_freshness': Gauge('data_freshness_hours', 'Hours since last data update'),
            'cpu_usage': Gauge('cpu_usage_percent', 'CPU usage percentage'),
            'memory_usage': Gauge('memory_usage_percent', 'Memory usage percentage'),
            'disk_usage': Gauge('disk_usage_percent', 'Disk usage percentage')
        }
        
        # Initialize alert settings
        self.alert_settings = {
            'slack_webhook': os.getenv('SLACK_WEBHOOK_URL', ''),
            'email_recipients': os.getenv('ALERT_EMAIL_RECIPIENTS', '').split(','),
            'thresholds': {
                'data_freshness': 24,  # Alert if data is older than 24 hours
                'cpu_usage': 80,       # Alert if CPU usage > 80%
                'memory_usage': 80,    # Alert if memory usage > 80%
                'disk_usage': 80       # Alert if disk usage > 80%
            }
        }
    
    def start_prometheus_server(self, port: int = 8000):
        """Start Prometheus metrics server."""
        start_http_server(port)
        logging.info(f"✅ Prometheus metrics server started on port {port}")
    
    def monitor_system_resources(self):
        """Monitor system resource usage."""
        while True:
            try:
                # CPU usage
                cpu_percent = psutil.cpu_percent(interval=1)
                self.metrics['cpu_usage'].set(cpu_percent)
                
                # Memory usage
                memory = psutil.virtual_memory()
                self.metrics['memory_usage'].set(memory.percent)
                
                # Disk usage
                disk = psutil.disk_usage('/')
                self.metrics['disk_usage'].set(disk.percent)
                
                # Check thresholds and alert if needed
                self._check_resource_thresholds({
                    'cpu': cpu_percent,
                    'memory': memory.percent,
                    'disk': disk.percent
                })
                
                time.sleep(60)  # Update every minute
                
            except Exception as e:
                logging.error(f"❌ Error monitoring system resources: {e}")
                time.sleep(60)  # Wait before retrying
    
    def monitor_airflow_dags(self):
        """Monitor Airflow DAG runs."""
        session = Session()
        
        while True:
            try:
                # Get recent DAG runs
                recent_runs = session.query(DagRun).filter(
                    DagRun.start_date >= datetime.now() - timedelta(hours=24)
                ).all()
                
                for run in recent_runs:
                    if run.state == 'success':
                        self.metrics['pipeline_success'].inc()
                    elif run.state == 'failed':
                        self.metrics['pipeline_failure'].inc()
                        self._send_alert(f"❌ DAG {run.dag_id} failed at {run.start_date}")
                
                time.sleep(300)  # Check every 5 minutes
                
            except Exception as e:
                logging.error(f"❌ Error monitoring Airflow DAGs: {e}")
                time.sleep(300)
    
    def monitor_data_freshness(self):
        """Monitor data freshness in Snowflake."""
        while True:
            try:
                conn = snowflake.connector.connect(**self.conn_params)
                
                # Get latest data timestamp
                latest_data = pd.read_sql("""
                    SELECT MAX(date) as latest_date
                    FROM fct_hourly_rentals
                """, conn)
                
                if not latest_data.empty:
                    latest_date = latest_data['latest_date'].iloc[0]
                    hours_since_update = (datetime.now() - latest_date).total_seconds() / 3600
                    
                    self.metrics['data_freshness'].set(hours_since_update)
                    
                    if hours_since_update > self.alert_settings['thresholds']['data_freshness']:
                        self._send_alert(f"⚠️ Data is {hours_since_update:.1f} hours old")
                
                conn.close()
                time.sleep(3600)  # Check every hour
                
            except Exception as e:
                logging.error(f"❌ Error monitoring data freshness: {e}")
                time.sleep(3600)
    
    def monitor_pipeline_metrics(self):
        """Monitor pipeline execution metrics."""
        while True:
            try:
                conn = snowflake.connector.connect(**self.conn_params)
                
                # Get pipeline execution metrics
                metrics = pd.read_sql("""
                    SELECT 
                        COUNT(*) as total_records,
                        MIN(date) as earliest_date,
                        MAX(date) as latest_date
                    FROM fct_hourly_rentals
                """, conn)
                
                if not metrics.empty:
                    self.metrics['records_processed'].inc(metrics['total_records'].iloc[0])
                    
                    # Calculate pipeline duration
                    duration = (metrics['latest_date'].iloc[0] - metrics['earliest_date'].iloc[0]).total_seconds()
                    self.metrics['pipeline_duration'].observe(duration)
                
                conn.close()
                time.sleep(300)  # Update every 5 minutes
                
            except Exception as e:
                logging.error(f"❌ Error monitoring pipeline metrics: {e}")
                time.sleep(300)
    
    def _check_resource_thresholds(self, resources: Dict[str, float]):
        """Check if resource usage exceeds thresholds."""
        for resource, usage in resources.items():
            threshold = self.alert_settings['thresholds'][f'{resource}_usage']
            if usage > threshold:
                self._send_alert(f"⚠️ {resource.upper()} usage is {usage}% (threshold: {threshold}%)")
    
    def _send_alert(self, message: str):
        """Send alert through configured channels."""
        if self.alert_settings['slack_webhook']:
            try:
                payload = {'text': message}
                response = requests.post(self.alert_settings['slack_webhook'], json=payload)
                response.raise_for_status()
                logging.info("✅ Slack alert sent successfully")
            except Exception as e:
                logging.error(f"❌ Failed to send Slack alert: {e}")
        
        if self.alert_settings['email_recipients']:
            try:
                import smtplib
                from email.mime.text import MIMEText
                from email.mime.multipart import MIMEMultipart
                
                msg = MIMEMultipart()
                msg['Subject'] = 'Pipeline Monitoring Alert'
                msg['From'] = os.getenv('ALERT_EMAIL_SENDER', 'alerts@bikeshare.com')
                msg['To'] = ', '.join(self.alert_settings['email_recipients'])
                
                msg.attach(MIMEText(message, 'plain'))
                
                with smtplib.SMTP('smtp.gmail.com', 587) as server:
                    server.starttls()
                    server.login(os.getenv('ALERT_EMAIL_USER'), os.getenv('ALERT_EMAIL_PASSWORD'))
                    server.send_message(msg)
                
                logging.info("✅ Email alert sent successfully")
            except Exception as e:
                logging.error(f"❌ Failed to send email alert: {e}")
    
    def generate_monitoring_report(self):
        """Generate monitoring report."""
        try:
            report = {
                'timestamp': datetime.now().isoformat(),
                'system_metrics': {
                    'cpu_usage': psutil.cpu_percent(),
                    'memory_usage': psutil.virtual_memory().percent,
                    'disk_usage': psutil.disk_usage('/').percent
                },
                'pipeline_metrics': {
                    'records_processed': self.metrics['records_processed']._value.get(),
                    'pipeline_success': self.metrics['pipeline_success']._value.get(),
                    'pipeline_failure': self.metrics['pipeline_failure']._value.get(),
                    'data_freshness': self.metrics['data_freshness']._value.get()
                }
            }
            
            # Save report
            report_file = f'pipeline_monitoring/monitoring_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            with open(report_file, 'w') as f:
                json.dump(report, f, indent=2)
            
            logging.info(f"✅ Monitoring report generated: {report_file}")
            return report
            
        except Exception as e:
            logging.error(f"❌ Failed to generate monitoring report: {e}")
            raise
    
    def start_monitoring(self):
        """Start all monitoring processes."""
        logging.info("🚀 Starting pipeline monitoring...")
        
        # Start Prometheus server
        self.start_prometheus_server()
        
        # Start monitoring threads
        threads = [
            threading.Thread(target=self.monitor_system_resources, daemon=True),
            threading.Thread(target=self.monitor_airflow_dags, daemon=True),
            threading.Thread(target=self.monitor_data_freshness, daemon=True),
            threading.Thread(target=self.monitor_pipeline_metrics, daemon=True)
        ]
        
        for thread in threads:
            thread.start()
        
        logging.info("✅ All monitoring processes started")
        
        try:
            # Keep main thread alive
            while True:
                time.sleep(3600)  # Generate report every hour
                self.generate_monitoring_report()
        except KeyboardInterrupt:
            logging.info("👋 Stopping pipeline monitoring...")

if __name__ == "__main__":
    monitor = PipelineMonitor()
    monitor.start_monitoring() 