#!/usr/bin/env python3

import pandas as pd
import snowflake.connector
import os
from datetime import datetime
from pathlib import Path

def load_data_to_snowflake():
    # Get the project root directory
    project_root = Path(__file__).parent.parent
    data_file = project_root / 'data' / 'raw' / 'hour.csv'
    
    # Get Snowflake connection parameters from environment variables
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
        # Set context
        cursor.execute("USE WAREHOUSE BIKESHARE_WH")
        cursor.execute("USE DATABASE BIKESHARE_DB")
        cursor.execute("USE SCHEMA RAW")
        
        print("📂 Reading CSV data...")
        # Read the CSV file
        df = pd.read_csv(data_file)
        print(f"📊 Loaded {len(df)} records from CSV")
        
        # Clear existing data
        print("🧹 Clearing existing data...")
        cursor.execute("TRUNCATE TABLE BIKESHARE_RAW")
        
        # Insert data in batches
        print("📤 Uploading data to Snowflake...")
        batch_size = 1000
        total_inserted = 0
        
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            
            # Prepare the data for insertion
            values = []
            for _, row in batch.iterrows():
                value = f"({row['instant']}, '{row['dteday']}', {row['season']}, {row['yr']}, {row['mnth']}, {row['hr']}, {row['holiday']}, {row['weekday']}, {row['workingday']}, {row['weathersit']}, {row['temp']}, {row['atemp']}, {row['hum']}, {row['windspeed']}, {row['casual']}, {row['registered']}, {row['cnt']}, CURRENT_TIMESTAMP())"
                values.append(value)
            
            # Execute insert
            insert_sql = f"""
            INSERT INTO BIKESHARE_RAW (instant, dteday, season, yr, mnth, hr, holiday, weekday, workingday, weathersit, temp, atemp, hum, windspeed, casual, registered, cnt, loaded_at)
            VALUES {', '.join(values)}
            """
            
            cursor.execute(insert_sql)
            total_inserted += len(batch)
            print(f"✅ Inserted batch {i//batch_size + 1}: {total_inserted}/{len(df)} records")
        
        # Verify the data
        print("🔍 Verifying data...")
        cursor.execute("SELECT COUNT(*) FROM BIKESHARE_RAW")
        count = cursor.fetchone()[0]
        print(f"✅ Total records in table: {count}")
        
        cursor.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT instant) as unique_records,
            MIN(dteday) as earliest_date,
            MAX(dteday) as latest_date,
            SUM(cnt) as total_rentals
        FROM BIKESHARE_RAW
        """)
        
        result = cursor.fetchone()
        print(f"📈 Data summary:")
        print(f"   - Total records: {result[0]}")
        print(f"   - Unique records: {result[1]}")
        print(f"   - Date range: {result[2]} to {result[3]}")
        print(f"   - Total rentals: {result[4]}")
        
        print("🎉 Data loading completed successfully!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    load_data_to_snowflake() 