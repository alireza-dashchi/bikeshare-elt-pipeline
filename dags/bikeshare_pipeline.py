from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def load_csv_to_snowflake(**context):
    # Read the CSV file
    df = pd.read_csv('/opt/airflow/data/raw/hour.csv')
    
    # Save to a temporary CSV file with proper formatting
    temp_path = '/opt/airflow/data/raw/temp_hour.csv'
    df.to_csv(temp_path, index=False, header=False)
    
    # Push the file path to XCom for next task
    context['task_instance'].xcom_push(key='temp_csv_path', value=temp_path)

# SQL commands
create_stage = """
CREATE STAGE IF NOT EXISTS BIKESHARE_STAGE
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);
"""

copy_into_snowflake = """
COPY INTO BIKESHARE_DB.RAW.BIKESHARE_RAW
FROM @BIKESHARE_STAGE/temp_hour.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';
"""

with DAG(
    'bikeshare_pipeline',
    default_args=default_args,
    description='Load and transform bike sharing data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bikeshare']
) as dag:

    prepare_data = PythonOperator(
        task_id='prepare_data',
        python_callable=load_csv_to_snowflake,
        provide_context=True
    )

    create_snowflake_stage = SnowflakeOperator(
        task_id='create_snowflake_stage',
        sql=create_stage,
        snowflake_conn_id='snowflake_default'
    )

    load_to_snowflake = SnowflakeOperator(
        task_id='load_to_snowflake',
        sql=copy_into_snowflake,
        snowflake_conn_id='snowflake_default'
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt_project && dbt run --profiles-dir .',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt_project && dbt test --profiles-dir .',
    )

    # Set task dependencies
    prepare_data >> create_snowflake_stage >> load_to_snowflake >> dbt_run >> dbt_test 