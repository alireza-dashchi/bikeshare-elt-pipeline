#!/bin/bash

# Set Airflow environment variables
export AIRFLOW_HOME=/opt/airflow
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
export AIRFLOW__WEBSERVER__SECRET_KEY='your-secret-key-here'

# Set Airflow variables
airflow variables set notification_email "your-email@example.com"
airflow variables set snowflake_database "BIKESHARE_DB"
airflow variables set snowflake_warehouse "BIKESHARE_WH"
airflow variables set snowflake_schema "RAW"

# Set up Snowflake connection
airflow connections add 'snowflake_default' \
    --conn-type 'snowflake' \
    --conn-host "$SNOWFLAKE_ACCOUNT.snowflakecomputing.com" \
    --conn-login "$SNOWFLAKE_USER" \
    --conn-password "$SNOWFLAKE_PASSWORD" \
    --conn-schema "$SNOWFLAKE_SCHEMA" \
    --conn-extra "{
        \"database\": \"$SNOWFLAKE_DATABASE\",
        \"warehouse\": \"$SNOWFLAKE_WAREHOUSE\",
        \"role\": \"$SNOWFLAKE_ROLE\",
        \"authenticator\": \"snowflake\",
        \"session_parameters\": {
            \"QUERY_TAG\": \"airflow_bikeshare_pipeline\"
        }
    }"

# Create required directories
mkdir -p $AIRFLOW_HOME/dags
mkdir -p $AIRFLOW_HOME/logs
mkdir -p $AIRFLOW_HOME/plugins

# Set permissions
chmod -R 777 $AIRFLOW_HOME/logs
chmod -R 777 $AIRFLOW_HOME/plugins

echo "✅ Airflow environment setup completed!"
echo "🔑 Don't forget to:"
echo "1. Update notification_email in Airflow variables"
echo "2. Set up proper secret key for Airflow webserver"
echo "3. Verify Snowflake connection using 'airflow connections test snowflake_default'" 