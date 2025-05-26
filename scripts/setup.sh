#!/bin/bash

# Exit on error
set -e

echo "🚲 Setting up Bike Share ELT Pipeline..."

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p data/raw
mkdir -p logs
mkdir -p dbt_project/logs

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required but not installed. Please install Python 3 and try again."
    exit 1
fi

# Create Python virtual environment
echo "🐍 Creating Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Install requirements
echo "📦 Installing Python packages..."
pip install -r requirements.txt

# Source Snowflake environment variables
if [ -f "set_snowflake_env.sh" ]; then
    echo "❄️ Setting up Snowflake environment..."
    source set_snowflake_env.sh
else
    echo "⚠️ Snowflake environment file not found. Please create set_snowflake_env.sh with your credentials."
fi

# Download dataset if not exists
if [ ! -f "data/raw/hour.csv" ]; then
    echo "📥 Downloading bike share dataset..."
    curl -o data/raw/Bike-Sharing-Dataset.zip https://archive.ics.uci.edu/ml/machine-learning-databases/00275/Bike-Sharing-Dataset.zip
    unzip data/raw/Bike-Sharing-Dataset.zip -d data/raw/
fi

# Initialize dbt
echo "🔧 Setting up dbt..."
cd dbt_project
dbt deps
dbt debug

# Start Docker containers
echo "🐳 Starting Docker containers..."
docker-compose -f docker/docker-compose.yml up -d

echo "✅ Setup completed! Next steps:"
echo "1. Visit http://localhost:8080 to access Airflow (user: airflow, pass: airflow)"
echo "2. Enable and trigger the bikeshare_pipeline DAG"
echo "3. Monitor the pipeline execution in Airflow UI" 