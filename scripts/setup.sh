#!/bin/bash

# Create necessary directories
mkdir -p logs
mkdir -p plugins

# Create Airflow connection for Snowflake
echo "Creating Airflow connection for Snowflake..."
docker-compose -f docker/docker-compose.yml run --rm airflow-webserver airflow connections add 'snowflake_default' \
    --conn-type 'snowflake' \
    --conn-login "$SNOWFLAKE_USER" \
    --conn-password "$SNOWFLAKE_PASSWORD" \
    --conn-schema "$SNOWFLAKE_DATABASE" \
    --conn-extra "{\"account\": \"$SNOWFLAKE_ACCOUNT\", \"warehouse\": \"$SNOWFLAKE_WAREHOUSE\", \"database\": \"$SNOWFLAKE_DATABASE\", \"role\": \"$SNOWFLAKE_ROLE\"}"

# Start Airflow services
echo "Starting Airflow services..."
docker-compose -f docker/docker-compose.yml up -d

echo "Setup complete! Airflow UI will be available at http://localhost:8080"
echo "Username: airflow"
echo "Password: airflow" 