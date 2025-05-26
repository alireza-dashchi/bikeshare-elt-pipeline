-- Snowflake Setup Script for Bike Share ELT Pipeline
-- Run this script in your Snowflake console after creating your account

-- Create database
CREATE DATABASE IF NOT EXISTS BIKESHARE_DB;

-- Create schema for raw data
CREATE SCHEMA IF NOT EXISTS BIKESHARE_DB.RAW;

-- Create schema for analytics/transformed data
CREATE SCHEMA IF NOT EXISTS BIKESHARE_DB.ANALYTICS;

-- Create warehouse for compute
CREATE WAREHOUSE IF NOT EXISTS BIKESHARE_WH
    WITH 
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- Set context
USE WAREHOUSE BIKESHARE_WH;
USE DATABASE BIKESHARE_DB;
USE SCHEMA RAW;

-- Create raw table for bike share data
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
);

-- Create file format for CSV loading
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    ESCAPE = 'NONE'
    ESCAPE_UNENCLOSED_FIELD = '\134'
    DATE_FORMAT = 'YYYY-MM-DD'
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
    NULL_IF = ('\\N', 'NULL', 'null', '', 'N/A');

-- Create internal stage for file uploads
CREATE OR REPLACE STAGE BIKESHARE_STAGE
    FILE_FORMAT = CSV_FORMAT;

-- Grant permissions (adjust role as needed)
GRANT USAGE ON WAREHOUSE BIKESHARE_WH TO ROLE SYSADMIN;
GRANT ALL ON DATABASE BIKESHARE_DB TO ROLE SYSADMIN;
GRANT ALL ON SCHEMA BIKESHARE_DB.RAW TO ROLE SYSADMIN;
GRANT ALL ON SCHEMA BIKESHARE_DB.ANALYTICS TO ROLE SYSADMIN;
GRANT ALL ON TABLE BIKESHARE_DB.RAW.BIKESHARE_RAW TO ROLE SYSADMIN;

-- Display setup confirmation
SELECT 'Snowflake setup completed successfully!' as status;
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE(); 