#!/usr/bin/env python3

import snowflake.connector
import pandas as pd
import os
import json
from datetime import datetime

def export_dashboard_data():
    """Export data for dashboard creation in Tableau/PowerBI/Looker."""
    
    # Get Snowflake connection parameters
    conn_params = {
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'role': os.getenv('SNOWFLAKE_ROLE')
    }
    
    print("🔗 Connecting to Snowflake for dashboard data export...")
    conn = snowflake.connector.connect(**conn_params)
    
    try:
        # Create dashboard directory
        os.makedirs('dashboard_exports', exist_ok=True)
        
        # 1. Hourly trends for time series visualization
        print("📈 Exporting hourly trends data...")
        hourly_df = pd.read_sql("""
            SELECT 
                date,
                hour,
                day_name,
                season_name,
                weather_id,
                time_of_day,
                total_rentals,
                casual_users,
                registered_users
            FROM fct_hourly_rentals 
            ORDER BY date, hour
        """, conn)
        hourly_df.to_csv('dashboard_exports/hourly_trends.csv', index=False)
        
        # 2. Weather impact summary
        print("🌧️ Exporting weather impact data...")
        weather_df = pd.read_sql("""
            SELECT 
                w.weather_desc,
                w.avg_temp_celsius,
                w.avg_humidity_percent,
                w.avg_windspeed_kmh,
                COUNT(f.record_id) as total_hours,
                AVG(f.total_rentals) as avg_rentals,
                SUM(f.total_rentals) as total_rentals
            FROM fct_hourly_rentals f
            JOIN dim_weather w ON f.weather_id = w.weather_id
            GROUP BY w.weather_id, w.weather_desc, w.avg_temp_celsius, 
                     w.avg_humidity_percent, w.avg_windspeed_kmh
        """, conn)
        weather_df.to_csv('dashboard_exports/weather_impact.csv', index=False)
        
        # 3. Seasonal patterns
        print("🍂 Exporting seasonal patterns...")
        seasonal_df = pd.read_sql("""
            SELECT 
                season_name,
                EXTRACT(MONTH FROM date) as month,
                EXTRACT(YEAR FROM date) as year,
                AVG(total_rentals) as avg_rentals,
                COUNT(*) as total_hours
            FROM fct_hourly_rentals 
            GROUP BY season_name, EXTRACT(MONTH FROM date), EXTRACT(YEAR FROM date)
            ORDER BY year, month
        """, conn)
        seasonal_df.to_csv('dashboard_exports/seasonal_patterns.csv', index=False)
        
        # 4. User behavior analysis
        print("👥 Exporting user behavior data...")
        user_df = pd.read_sql("""
            SELECT 
                date,
                hour,
                day_name,
                time_of_day,
                casual_users,
                registered_users,
                total_rentals,
                ROUND((casual_users::FLOAT / total_rentals) * 100, 2) as casual_percentage
            FROM fct_hourly_rentals 
            WHERE total_rentals > 0
        """, conn)
        user_df.to_csv('dashboard_exports/user_behavior.csv', index=False)
        
        # 5. Create dashboard configuration file
        dashboard_config = {
            "data_sources": {
                "hourly_trends": {
                    "file": "hourly_trends.csv",
                    "description": "Time series data for rental trends",
                    "key_fields": ["date", "hour", "total_rentals"],
                    "visualizations": ["Line chart", "Heatmap", "Calendar view"]
                },
                "weather_impact": {
                    "file": "weather_impact.csv", 
                    "description": "Weather conditions vs rental performance",
                    "key_fields": ["weather_desc", "avg_rentals", "avg_temp_celsius"],
                    "visualizations": ["Bar chart", "Scatter plot", "KPI cards"]
                },
                "seasonal_patterns": {
                    "file": "seasonal_patterns.csv",
                    "description": "Monthly and seasonal rental patterns",
                    "key_fields": ["season_name", "month", "avg_rentals"],
                    "visualizations": ["Seasonal decomposition", "Year-over-year comparison"]
                },
                "user_behavior": {
                    "file": "user_behavior.csv",
                    "description": "Casual vs registered user patterns",
                    "key_fields": ["time_of_day", "casual_percentage", "total_rentals"],
                    "visualizations": ["Stacked bar", "Pie chart", "User segmentation"]
                }
            },
            "suggested_dashboards": [
                {
                    "name": "Executive Summary",
                    "kpis": ["Total Rentals", "Avg Daily Rentals", "Peak Hour", "Weather Impact"],
                    "charts": ["Monthly trends", "Weather comparison", "User mix"]
                },
                {
                    "name": "Operational Dashboard", 
                    "kpis": ["Current Hour Demand", "Weather Alert", "Capacity Utilization"],
                    "charts": ["Hourly heatmap", "Real-time weather", "Fleet distribution"]
                },
                {
                    "name": "Business Intelligence",
                    "kpis": ["Revenue Impact", "Seasonal Growth", "User Acquisition"],
                    "charts": ["Cohort analysis", "Forecasting", "Segment performance"]
                }
            ]
        }
        
        with open('dashboard_exports/dashboard_config.json', 'w') as f:
            json.dump(dashboard_config, f, indent=2)
        
        # 6. Generate Tableau/PowerBI connection strings
        tableau_connection = f"""
        -- Tableau Connection String
        Server: {conn_params['account']}.snowflakecomputing.com
        Database: {conn_params['database']}
        Schema: {conn_params['schema']}
        Warehouse: {conn_params['warehouse']}
        
        -- Sample Calculated Fields for Tableau:
        -- Rental Category: IF [Total Rentals] > 200 THEN "High" ELSEIF [Total Rentals] > 100 THEN "Medium" ELSE "Low" END
        -- Weekend Flag: IF DATEPART('weekday',[Date]) IN (1,7) THEN "Weekend" ELSE "Weekday" END
        -- Peak Hour Flag: IF [Hour] IN (8,17,18) THEN "Peak" ELSE "Off-Peak" END
        """
        
        with open('dashboard_exports/tableau_connection.txt', 'w') as f:
            f.write(tableau_connection)
        
        print(f"\n✅ Dashboard data exported successfully!")
        print(f"📁 Files created in 'dashboard_exports/' directory:")
        print(f"   • hourly_trends.csv ({len(hourly_df):,} records)")
        print(f"   • weather_impact.csv ({len(weather_df):,} records)")  
        print(f"   • seasonal_patterns.csv ({len(seasonal_df):,} records)")
        print(f"   • user_behavior.csv ({len(user_df):,} records)")
        print(f"   • dashboard_config.json (configuration)")
        print(f"   • tableau_connection.txt (connection details)")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    export_dashboard_data() 