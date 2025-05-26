from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import pandas as pd
import os
import logging

# Set up logging
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'email': ['your-email@example.com'],  # Replace with your email
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
    'sla': timedelta(hours=4)
}

def validate_raw_data(**context):
    """Validate the raw data before loading to Snowflake."""
    try:
        df = pd.read_csv('/opt/airflow/data/raw/hour.csv')
        
        # Data validation checks
        validation_errors = []
        
        # Check for required columns
        required_columns = ['instant', 'dteday', 'season', 'yr', 'mnth', 'hr', 'holiday', 
                          'weekday', 'workingday', 'weathersit', 'temp', 'atemp', 'hum', 
                          'windspeed', 'casual', 'registered', 'cnt']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            validation_errors.append(f"Missing required columns: {missing_columns}")
        
        # Check for null values
        null_counts = df.isnull().sum()
        columns_with_nulls = null_counts[null_counts > 0]
        if not columns_with_nulls.empty:
            validation_errors.append(f"Found null values in columns: {columns_with_nulls.to_dict()}")
        
        # Check value ranges
        if not df['season'].between(1, 4).all():
            validation_errors.append("Invalid season values found (should be 1-4)")
        if not df['hr'].between(0, 23).all():
            validation_errors.append("Invalid hour values found (should be 0-23)")
        if not df['weathersit'].between(1, 4).all():
            validation_errors.append("Invalid weather situation values found (should be 1-4)")
        
        # Log validation results
        if validation_errors:
            error_msg = "Data validation failed:\n" + "\n".join(validation_errors)
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info("Data validation passed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error validating raw data: {str(e)}")
        raise

def prepare_data_for_snowflake(**context):
    """Prepare and format data for Snowflake loading."""
    try:
        # Read and validate the CSV file
        df = pd.read_csv('/opt/airflow/data/raw/hour.csv')
        
        # Format date column
        df['dteday'] = pd.to_datetime(df['dteday']).dt.strftime('%Y-%m-%d')
        
        # Save to a temporary CSV file
        temp_path = '/opt/airflow/data/raw/temp_hour.csv'
        df.to_csv(temp_path, index=False, header=False, date_format='%Y-%m-%d')
        
        # Log statistics
        logger.info(f"Prepared {len(df)} records for loading")
        logger.info(f"Date range: {df['dteday'].min()} to {df['dteday'].max()}")
        
        # Push the file path to XCom
        context['task_instance'].xcom_push(key='temp_csv_path', value=temp_path)
        context['task_instance'].xcom_push(key='record_count', value=len(df))
        
    except Exception as e:
        logger.error(f"Error preparing data: {str(e)}")
        raise

# SQL commands with better error handling and logging
create_stage = """
CREATE STAGE IF NOT EXISTS BIKESHARE_STAGE
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        SKIP_HEADER = 1
        DATE_FORMAT = 'YYYY-MM-DD'
        NULL_IF = ('NULL', 'null', '')
        EMPTY_FIELD_AS_NULL = TRUE
    );
"""

copy_into_snowflake = """
COPY INTO BIKESHARE_DB.RAW.BIKESHARE_RAW (
    instant, dteday, season, yr, mnth, hr, holiday, weekday,
    workingday, weathersit, temp, atemp, hum, windspeed,
    casual, registered, cnt
)
FROM @BIKESHARE_STAGE/temp_hour.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    DATE_FORMAT = 'YYYY-MM-DD'
)
ON_ERROR = 'ABORT_STATEMENT'
VALIDATION_MODE = 'RETURN_ERRORS';
"""

verify_load = """
SELECT 
    COUNT(*) as record_count,
    MIN(dteday) as min_date,
    MAX(dteday) as max_date,
    COUNT(DISTINCT instant) as unique_records
FROM BIKESHARE_DB.RAW.BIKESHARE_RAW;
"""

# DAG definition
with DAG(
    'bikeshare_pipeline',
    default_args=default_args,
    description='Load and transform bike sharing data with monitoring and validation',
    schedule_interval='0 1 * * *',  # Run at 1 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bikeshare', 'elt'],
    doc_md="""
    # Bike Share Data Pipeline
    
    This DAG orchestrates the ELT pipeline for bike sharing data:
    1. Validates raw data quality
    2. Prepares data for Snowflake
    3. Loads data into Snowflake
    4. Transforms data using dbt
    5. Runs data quality tests
    
    ## Dependencies
    - Snowflake connection: `snowflake_default`
    - Raw data location: `/opt/airflow/data/raw/hour.csv`
    - dbt project: `/opt/airflow/dbt_project`
    """
) as dag:

    validate_raw_data_task = PythonOperator(
        task_id='validate_raw_data',
        python_callable=validate_raw_data,
        provide_context=True,
        doc_md="Validates the raw CSV data before processing"
    )

    prepare_data = PythonOperator(
        task_id='prepare_data',
        python_callable=prepare_data_for_snowflake,
        provide_context=True,
        doc_md="Prepares and formats data for Snowflake loading"
    )

    create_snowflake_stage = SnowflakeOperator(
        task_id='create_snowflake_stage',
        sql=create_stage,
        snowflake_conn_id='snowflake_default',
        doc_md="Creates or verifies Snowflake stage for data loading"
    )

    load_to_snowflake = SnowflakeOperator(
        task_id='load_to_snowflake',
        sql=copy_into_snowflake,
        snowflake_conn_id='snowflake_default',
        doc_md="Loads prepared data into Snowflake raw table"
    )

    verify_snowflake_load = SnowflakeOperator(
        task_id='verify_snowflake_load',
        sql=verify_load,
        snowflake_conn_id='snowflake_default',
        doc_md="Verifies data was loaded correctly into Snowflake"
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt_project && dbt run --profiles-dir .',
        doc_md="Runs dbt models to transform the data"
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt_project && dbt test --profiles-dir .',
        doc_md="Runs dbt tests to verify data quality"
    )

    notify_success = EmailOperator(
        task_id='notify_success',
        to='{{ var.value.notification_email }}',
        subject='Bikeshare Pipeline Completed Successfully',
        html_content="""
        <h3>Pipeline Run Successful</h3>
        <p>The bikeshare data pipeline has completed successfully.</p>
        <p>Execution date: {{ ds }}</p>
        <p>View the pipeline details in Airflow: {{ task_instance.log_url }}</p>
        """,
        trigger_rule='all_success'
    )

    # Set task dependencies
    validate_raw_data_task >> prepare_data >> create_snowflake_stage >> load_to_snowflake >> verify_snowflake_load >> dbt_run >> dbt_test >> notify_success 