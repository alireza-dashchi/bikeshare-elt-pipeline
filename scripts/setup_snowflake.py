#!/usr/bin/env python3

import snowflake.connector
import os

def setup_snowflake():
    # Get Snowflake connection parameters from environment variables
    conn_params = {
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'role': 'ACCOUNTADMIN'  # Use ACCOUNTADMIN to create objects and grant permissions
    }
    
    print("🔗 Connecting to Snowflake as ACCOUNTADMIN...")
    conn = snowflake.connector.connect(**conn_params)
    cursor = conn.cursor()
    
    try:
        print("🏗️  Creating database...")
        cursor.execute("CREATE DATABASE IF NOT EXISTS BIKESHARE_DB")
        
        print("🏗️  Creating schemas...")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS BIKESHARE_DB.RAW")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS BIKESHARE_DB.ANALYTICS")
        
        print("🏗️  Creating warehouse...")
        cursor.execute("""
        CREATE WAREHOUSE IF NOT EXISTS BIKESHARE_WH
            WITH 
            WAREHOUSE_SIZE = 'X-SMALL'
            AUTO_SUSPEND = 60
            AUTO_RESUME = TRUE
            INITIALLY_SUSPENDED = TRUE
        """)
        
        print("🔐 Granting permissions to SYSADMIN...")
        cursor.execute("GRANT USAGE ON WAREHOUSE BIKESHARE_WH TO ROLE SYSADMIN")
        cursor.execute("GRANT ALL ON DATABASE BIKESHARE_DB TO ROLE SYSADMIN")
        cursor.execute("GRANT ALL ON SCHEMA BIKESHARE_DB.RAW TO ROLE SYSADMIN")
        cursor.execute("GRANT ALL ON SCHEMA BIKESHARE_DB.ANALYTICS TO ROLE SYSADMIN")
        
        print("🔄 Switching to SYSADMIN role...")
        cursor.execute("USE ROLE SYSADMIN")
        cursor.execute("USE WAREHOUSE BIKESHARE_WH")
        cursor.execute("USE DATABASE BIKESHARE_DB")
        cursor.execute("USE SCHEMA RAW")
        
        print("📊 Creating raw table...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS RAW.BIKESHARE_RAW (
            instant INT,
            dteday DATE,
            season INT,
            yr INT,
            mnth INT,
            hr INT,
            holiday INT,
            weekday INT,
            workingday INT,
            weathersit INT,
            temp FLOAT,
            atemp FLOAT,
            hum FLOAT,
            windspeed FLOAT,
            casual INT,
            registered INT,
            cnt INT,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        """)
        
        print("📁 Creating file format...")
        cursor.execute("""
        CREATE OR REPLACE FILE FORMAT CSV_FORMAT
            TYPE = 'CSV'
            FIELD_DELIMITER = ','
            RECORD_DELIMITER = '\\n'
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            TRIM_SPACE = TRUE
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
            ESCAPE = 'NONE'
            ESCAPE_UNENCLOSED_FIELD = '\\134'
            DATE_FORMAT = 'YYYY-MM-DD'
            TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
            NULL_IF = ('\\\\N', 'NULL', 'null', '', 'N/A')
        """)
        
        print("🎯 Creating stage...")
        cursor.execute("""
        CREATE OR REPLACE STAGE BIKESHARE_STAGE
            FILE_FORMAT = CSV_FORMAT
        """)
        
        print("✅ Verifying setup...")
        cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE(), CURRENT_ROLE()")
        result = cursor.fetchone()
        print(f"   - Database: {result[0]}")
        print(f"   - Schema: {result[1]}")
        print(f"   - Warehouse: {result[2]}")
        print(f"   - Role: {result[3]}")
        
        print("🎉 Snowflake setup completed successfully!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    setup_snowflake() 