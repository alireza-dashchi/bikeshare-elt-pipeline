#!/usr/bin/env python3

import snowflake.connector
import pandas as pd
import os
from datetime import datetime

def show_pipeline_results():
    """Display results from the ELT pipeline."""
    
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
    
    print("🔗 Connecting to Snowflake...")
    conn = snowflake.connector.connect(**conn_params)
    cursor = conn.cursor()
    
    try:
        print("\n🏔️ SNOWFLAKE RESULTS SUMMARY")
        print("=" * 60)
        
        # Check raw data
        cursor.execute("SELECT COUNT(*) FROM BIKESHARE_RAW")
        raw_count = cursor.fetchone()[0]
        print(f"📊 Raw data records: {raw_count:,}")
        
        # Check transformed tables
        cursor.execute("SELECT COUNT(*) FROM dim_weather")
        weather_count = cursor.fetchone()[0]
        print(f"🌤️ Weather dimension records: {weather_count:,}")
        
        cursor.execute("SELECT COUNT(*) FROM fct_hourly_rentals")
        facts_count = cursor.fetchone()[0]
        print(f"🚴 Hourly rentals records: {facts_count:,}")
        
        print("\n🌤️ WEATHER CONDITIONS:")
        print("-" * 50)
        cursor.execute("""
            SELECT weather_id, weather_desc, 
                   ROUND(avg_temp_celsius, 2) as avg_temp,
                   ROUND(avg_humidity_percent, 1) as avg_humidity
            FROM dim_weather 
            ORDER BY weather_id
        """)
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]} (Temp: {row[2]}°C, Humidity: {row[3]}%)")
        
        print("\n📈 TOP 10 BUSIEST HOURS:")
        print("-" * 70)
        cursor.execute("""
            SELECT date, hour, season_name, time_of_day, total_rentals
            FROM fct_hourly_rentals 
            ORDER BY total_rentals DESC 
            LIMIT 10
        """)
        print("  Date       | Hour | Season | Time Period    | Rentals")
        print("  -----------|------|--------|----------------|--------")
        for row in cursor.fetchall():
            print(f"  {row[0]} | {row[1]:4d} | {row[2]:6s} | {row[3]:14s} | {row[4]:7d}")
        
        print("\n📊 RENTAL PATTERNS BY SEASON:")
        print("-" * 50)
        cursor.execute("""
            SELECT season_name, 
                   COUNT(*) as total_hours,
                   AVG(total_rentals) as avg_rentals,
                   SUM(total_rentals) as total_rentals
            FROM fct_hourly_rentals 
            GROUP BY season_name 
            ORDER BY avg_rentals DESC
        """)
        print("  Season | Hours | Avg/Hour | Total Rentals")
        print("  -------|-------|----------|---------------")
        for row in cursor.fetchall():
            print(f"  {row[0]:6s} | {row[1]:5d} | {row[2]:8.1f} | {row[3]:13,d}")
        
        print("\n🌧️ WEATHER IMPACT ON RENTALS:")
        print("-" * 50)
        cursor.execute("""
            SELECT w.weather_desc,
                   COUNT(*) as hours,
                   AVG(f.total_rentals) as avg_rentals
            FROM fct_hourly_rentals f
            JOIN dim_weather w ON f.weather_id = w.weather_id
            GROUP BY w.weather_desc, w.weather_id
            ORDER BY avg_rentals DESC
        """)
        print("  Weather Condition     | Hours | Avg Rentals/Hour")
        print("  ----------------------|-------|------------------")
        for row in cursor.fetchall():
            print(f"  {row[0]:20s} | {row[1]:5d} | {row[2]:16.1f}")
        
        print("\n⏰ PEAK HOURS ANALYSIS:")
        print("-" * 40)
        cursor.execute("""
            SELECT hour, 
                   AVG(total_rentals) as avg_rentals,
                   time_of_day
            FROM fct_hourly_rentals 
            GROUP BY hour, time_of_day
            ORDER BY avg_rentals DESC 
            LIMIT 8
        """)
        print("  Hour | Time Period    | Avg Rentals")
        print("  -----|----------------|------------")
        for row in cursor.fetchall():
            print(f"  {row[0]:4d} | {row[2]:14s} | {row[1]:11.1f}")
        
        print(f"\n✅ Pipeline Results Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    show_pipeline_results() 