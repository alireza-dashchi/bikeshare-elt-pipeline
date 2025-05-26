#!/usr/bin/env python3

import snowflake.connector
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import json
import logging
from typing import Dict, List, Any
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_quality.log'),
        logging.StreamHandler()
    ]
)

class DataQualityMonitor:
    """Data quality monitoring system for the bike share dataset."""
    
    def __init__(self):
        """Initialize the data quality monitor with connection parameters."""
        self.conn_params = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'role': os.getenv('SNOWFLAKE_ROLE')
        }
        
        # Create monitoring directory
        os.makedirs('data_quality', exist_ok=True)
        
        # Initialize quality thresholds
        self.thresholds = {
            'completeness': 0.95,  # 95% data completeness required
            'accuracy': 0.90,      # 90% data accuracy required
            'timeliness': 24,      # Data should be no more than 24 hours old
            'consistency': 0.95    # 95% data consistency required
        }
        
        # Initialize alert settings
        self.alert_settings = {
            'email_enabled': True,
            'email_recipients': os.getenv('ALERT_EMAIL_RECIPIENTS', '').split(','),
            'slack_webhook': os.getenv('SLACK_WEBHOOK_URL', ''),
            'alert_threshold': 0.90  # Alert if quality score below 90%
        }
    
    def connect_to_snowflake(self):
        """Establish connection to Snowflake."""
        try:
            conn = snowflake.connector.connect(**self.conn_params)
            logging.info("✅ Successfully connected to Snowflake")
            return conn
        except Exception as e:
            logging.error(f"❌ Failed to connect to Snowflake: {e}")
            raise
    
    def check_completeness(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check data completeness metrics."""
        completeness_metrics = {}
        
        # Check for null values
        null_counts = df.isnull().sum()
        total_rows = len(df)
        
        for column in df.columns:
            null_percentage = (null_counts[column] / total_rows) * 100
            completeness_metrics[column] = {
                'null_count': int(null_counts[column]),
                'null_percentage': round(null_percentage, 2),
                'is_complete': null_percentage <= (1 - self.thresholds['completeness']) * 100
            }
        
        return completeness_metrics
    
    def check_accuracy(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check data accuracy metrics."""
        accuracy_metrics = {}
        
        # Check for negative values in numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            negative_count = len(df[df[col] < 0])
            accuracy_metrics[col] = {
                'negative_count': negative_count,
                'is_accurate': negative_count == 0
            }
        
        # Check for invalid dates
        if 'date' in df.columns:
            invalid_dates = len(df[df['date'] > datetime.now()])
            accuracy_metrics['date'] = {
                'future_dates': invalid_dates,
                'is_accurate': invalid_dates == 0
            }
        
        return accuracy_metrics
    
    def check_timeliness(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check data timeliness metrics."""
        timeliness_metrics = {}
        
        if 'date' in df.columns:
            latest_date = df['date'].max()
            current_date = datetime.now()
            hours_delay = (current_date - latest_date).total_seconds() / 3600
            
            timeliness_metrics = {
                'latest_date': latest_date.strftime('%Y-%m-%d'),
                'hours_delay': round(hours_delay, 2),
                'is_timely': hours_delay <= self.thresholds['timeliness']
            }
        
        return timeliness_metrics
    
    def check_consistency(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check data consistency metrics."""
        consistency_metrics = {}
        
        # Check total_rentals = casual_users + registered_users
        if all(col in df.columns for col in ['total_rentals', 'casual_users', 'registered_users']):
            inconsistent_rows = len(df[df['total_rentals'] != df['casual_users'] + df['registered_users']])
            total_rows = len(df)
            
            consistency_metrics['user_counts'] = {
                'inconsistent_rows': inconsistent_rows,
                'consistency_percentage': round((1 - inconsistent_rows/total_rows) * 100, 2),
                'is_consistent': (1 - inconsistent_rows/total_rows) >= self.thresholds['consistency']
            }
        
        return consistency_metrics
    
    def calculate_quality_score(self, metrics: Dict[str, Any]) -> float:
        """Calculate overall data quality score."""
        weights = {
            'completeness': 0.3,
            'accuracy': 0.3,
            'timeliness': 0.2,
            'consistency': 0.2
        }
        
        scores = {
            'completeness': sum(1 for m in metrics['completeness'].values() if m['is_complete']) / len(metrics['completeness']),
            'accuracy': sum(1 for m in metrics['accuracy'].values() if m['is_accurate']) / len(metrics['accuracy']),
            'timeliness': 1 if metrics['timeliness']['is_timely'] else 0,
            'consistency': sum(1 for m in metrics['consistency'].values() if m['is_consistent']) / len(metrics['consistency'])
        }
        
        quality_score = sum(score * weights[metric] for metric, score in scores.items())
        return round(quality_score * 100, 2)
    
    def send_alert(self, quality_score: float, metrics: Dict[str, Any]):
        """Send alert if quality score is below threshold."""
        if quality_score < self.alert_settings['alert_threshold'] * 100:
            alert_message = f"""
            🚨 Data Quality Alert 🚨
            
            Quality Score: {quality_score}%
            Threshold: {self.alert_settings['alert_threshold'] * 100}%
            
            Issues Found:
            {self._format_issues(metrics)}
            
            Please investigate the data quality issues.
            """
            
            if self.alert_settings['email_enabled'] and self.alert_settings['email_recipients']:
                self._send_email_alert(alert_message)
            
            if self.alert_settings['slack_webhook']:
                self._send_slack_alert(alert_message)
    
    def _format_issues(self, metrics: Dict[str, Any]) -> str:
        """Format issues for alert message."""
        issues = []
        
        # Completeness issues
        for col, metric in metrics['completeness'].items():
            if not metric['is_complete']:
                issues.append(f"• {col}: {metric['null_percentage']}% null values")
        
        # Accuracy issues
        for col, metric in metrics['accuracy'].items():
            if not metric['is_accurate']:
                issues.append(f"• {col}: {metric['negative_count']} invalid values")
        
        # Timeliness issues
        if not metrics['timeliness']['is_timely']:
            issues.append(f"• Data delay: {metrics['timeliness']['hours_delay']} hours")
        
        # Consistency issues
        for col, metric in metrics['consistency'].items():
            if not metric['is_consistent']:
                issues.append(f"• {col}: {metric['inconsistent_rows']} inconsistent rows")
        
        return "\n".join(issues)
    
    def _send_email_alert(self, message: str):
        """Send email alert."""
        try:
            msg = MIMEMultipart()
            msg['Subject'] = 'Data Quality Alert - Bike Share Dataset'
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
    
    def _send_slack_alert(self, message: str):
        """Send Slack alert."""
        try:
            import requests
            payload = {'text': message}
            response = requests.post(self.alert_settings['slack_webhook'], json=payload)
            response.raise_for_status()
            logging.info("✅ Slack alert sent successfully")
        except Exception as e:
            logging.error(f"❌ Failed to send Slack alert: {e}")
    
    def run_quality_check(self):
        """Run complete data quality check."""
        logging.info("🔍 Starting data quality check...")
        
        try:
            conn = self.connect_to_snowflake()
            
            # Load data for quality check
            df = pd.read_sql("""
                SELECT 
                    f.*,
                    w.weather_desc,
                    w.avg_temp_celsius,
                    w.avg_humidity_percent,
                    w.avg_windspeed_kmh
                FROM fct_hourly_rentals f
                JOIN dim_weather w ON f.weather_id = w.weather_id
            """, conn)
            
            logging.info(f"📊 Loaded {len(df):,} records for quality check")
            
            # Run quality checks
            metrics = {
                'completeness': self.check_completeness(df),
                'accuracy': self.check_accuracy(df),
                'timeliness': self.check_timeliness(df),
                'consistency': self.check_consistency(df)
            }
            
            # Calculate quality score
            quality_score = self.calculate_quality_score(metrics)
            
            # Generate quality report
            report = {
                'timestamp': datetime.now().isoformat(),
                'quality_score': quality_score,
                'metrics': metrics,
                'thresholds': self.thresholds
            }
            
            # Save report
            report_file = f'data_quality/quality_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            with open(report_file, 'w') as f:
                json.dump(report, f, indent=2)
            
            # Send alert if needed
            self.send_alert(quality_score, metrics)
            
            logging.info(f"✅ Quality check completed. Score: {quality_score}%")
            logging.info(f"📄 Report saved to {report_file}")
            
            return report
            
        except Exception as e:
            logging.error(f"❌ Quality check failed: {e}")
            raise
        finally:
            conn.close()

if __name__ == "__main__":
    monitor = DataQualityMonitor()
    monitor.run_quality_check() 