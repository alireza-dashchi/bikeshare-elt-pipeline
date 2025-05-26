-- Load bike share data into Snowflake
-- Set context
USE WAREHOUSE BIKESHARE_WH;
USE DATABASE BIKESHARE_DB;
USE SCHEMA RAW;

-- First, upload the file to the internal stage
-- Note: This PUT command needs to be run from SnowSQL or Snowflake web interface
-- PUT file://data/raw/hour.csv @BIKESHARE_STAGE AUTO_COMPRESS=TRUE;

-- Copy data from stage to table
COPY INTO BIKESHARE_RAW (
    instant,
    dteday,
    season,
    yr,
    mnth,
    hr,
    holiday,
    weekday,
    workingday,
    weathersit,
    temp,
    atemp,
    hum,
    windspeed,
    casual,
    registered,
    cnt
)
FROM @BIKESHARE_STAGE/hour.csv.gz
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- Verify data loaded correctly
SELECT COUNT(*) as total_records FROM BIKESHARE_RAW;
SELECT * FROM BIKESHARE_RAW LIMIT 5;

-- Check data quality
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT instant) as unique_records,
    MIN(dteday) as earliest_date,
    MAX(dteday) as latest_date,
    SUM(cnt) as total_rentals
FROM BIKESHARE_RAW; 