# Bike Share ELT Pipeline

An end-to-end data pipeline showcasing modern data engineering practices using **Airflow**, **dbt**, and **Snowflake**.

## Project Overview

This project demonstrates a complete ELT (Extract, Load, Transform) pipeline that:
- Extracts hourly bike share data from the UCI Machine Learning Repository
- Loads raw data into Snowflake
- Transforms data using dbt into analytics-ready models
- Orchestrates the entire pipeline with Apache Airflow
- Includes data quality testing and monitoring

## Architecture

```
[UCI Bike Share Dataset] 
     ↓ (Airflow PythonOperator)
[Snowflake Stage] 
     ↓ (SnowflakeOperator)
[raw_bikeshare table] 
     ↓ (dbt run)
[Staging Models] → [Fact & Dimension Tables] 
     ↓ (dbt test)
[Validated Analytics Models]
```

## Data Source
- **Dataset**: UCI Bike Sharing Dataset (~17,000 records)
- **Source**: [UCI ML Repository](https://archive.ics.uci.edu/ml/datasets/bike+sharing+dataset)
- **Contains**: Hourly bike rental counts with weather and seasonal information

## Tech Stack
- **Orchestration**: Apache Airflow
- **Data Warehouse**: Snowflake
- **Transformation**: dbt (data build tool)
- **Language**: Python, SQL
- **Infrastructure**: Docker (for local Airflow)

## Project Structure
```
bikeshare-elt-pipeline/
├── dags/                           # Airflow DAGs
│   └── bikeshare_pipeline.py
├── dbt_project/                    # dbt project
│   ├── models/
│   │   ├── staging/               # Staging models
│   │   │   ├── stg_bikeshare.sql
│   │   │   └── schema.yml
│   │   └── marts/                 # Mart models
│   │       ├── dim_weather.sql
│   │       ├── fct_hourly_rentals.sql
│   │       └── schema.yml
│   ├── macros/                    # Reusable SQL macros
│   │   ├── get_weather_description.sql
│   │   └── get_season_name.sql
│   ├── tests/                     # Custom data tests
│   ├── dbt_project.yml
│   └── profiles.yml
├── docker/                         # Docker configuration
│   └── docker-compose.yml
├── scripts/                        # Utility scripts
│   └── setup.sh                    # Project setup script
├── sql/                           # SQL scripts for setup
│   └── snowflake_setup.sql
├── set_snowflake_env.sh           # Snowflake credentials
├── requirements.txt
└── README.md
```

## Quick Start

### Prerequisites
1. **Snowflake Account**: [Sign up for free trial](https://trial.snowflake.com)
2. **Docker**: [Install Docker Desktop](https://www.docker.com/products/docker-desktop)
3. **Python 3.8+**: For dbt installation

### Setup Steps

1. **Clone the repository**
   ```bash
   git clone <your-repo-url>
   cd bikeshare-elt-pipeline
   ```

2. **Configure Snowflake Credentials**
   - Copy `set_snowflake_env.template.sh` to `set_snowflake_env.sh`
   - Update with your Snowflake credentials
   ```bash
   cp set_snowflake_env.template.sh set_snowflake_env.sh
   # Edit set_snowflake_env.sh with your credentials
   ```

3. **Run Setup Script**
   ```bash
   ./scripts/setup.sh
   ```
   This will:
   - Create necessary directories
   - Set up Python virtual environment
   - Install dependencies
   - Download the dataset
   - Initialize dbt
   - Start Docker containers

4. **Access Airflow UI**
   - Visit http://localhost:8080
   - Login with:
     - Username: airflow
     - Password: airflow

5. **Run the Pipeline**
   - Enable the `bikeshare_pipeline` DAG
   - Trigger a manual run or wait for scheduled execution

## Data Models

### Staging Layer
- **stg_bikeshare**: Cleaned and typed raw bike share data
  - Data quality tests for all critical fields
  - Type casting and standardization

### Marts Layer
- **dim_weather**: Weather condition dimension
  - Weather descriptions and metrics
  - Comprehensive data quality tests
  - Uses centralized weather description logic

- **fct_hourly_rentals**: Hourly rental facts
  - Enriched with weather and time dimensions
  - Business logic for time periods
  - Extensive data validation tests

## Key Features
- ✅ **Full ELT Pipeline**: Extract → Load → Transform pattern
- ✅ **Data Quality Tests**: Automated testing with dbt
- ✅ **Incremental Loading**: Efficient data processing
- ✅ **Documentation**: Auto-generated dbt docs
- ✅ **Monitoring**: Airflow task monitoring and alerting
- ✅ **Version Control**: All code in Git with proper structure
- ✅ **Reusable Macros**: Centralized business logic
- ✅ **Easy Setup**: One-command project initialization

## Business Value
This pipeline enables analysis of:
- Peak usage patterns by time and weather
- Weather impact on bike share demand
- Seasonal trends and capacity planning
- Operational metrics for bike share optimization

## Interview Talking Points
- "Built end-to-end ELT pipeline processing 17K+ records"
- "Implemented dimensional modeling for bike share analytics"
- "Used dbt for data transformation with automated testing"
- "Orchestrated pipeline with Airflow for reliable daily processing"
- "Applied data engineering best practices: testing, documentation, monitoring"
- "Created reusable SQL macros for maintainable code"

## Next Steps
- Add more data sources (bike station locations, maintenance logs)
- Implement real-time streaming with Apache Kafka
- Add machine learning predictions for demand forecasting
- Create dashboards with Tableau/PowerBI
- Add data quality monitoring with Great Expectations
- Implement CI/CD pipeline with GitHub Actions

## Contributing
Feel free to submit issues and pull requests!

## License
MIT License - see LICENSE file for details
