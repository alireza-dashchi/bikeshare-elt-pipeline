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
│   │   ├── staging/
│   │   │   └── stg_bikeshare.sql
│   │   └── marts/
│   │       ├── dim_weather.sql
│   │       └── fct_hourly_rentals.sql
│   ├── tests/
│   ├── dbt_project.yml
│   └── profiles.yml
├── docker/                         # Docker configuration
│   └── docker-compose.yml
├── sql/                           # SQL scripts for setup
│   └── snowflake_setup.sql
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

2. **Set up Snowflake**
   - Create account and note your account identifier
   - Run the setup SQL script in `sql/snowflake_setup.sql`

3. **Configure dbt**
   ```bash
   pip install dbt-snowflake
   cd dbt_project
   # Update profiles.yml with your Snowflake credentials
   dbt debug  # Test connection
   ```

4. **Start Airflow**
   ```bash
   docker-compose -f docker/docker-compose.yml up -d
   ```
   - Access UI at http://localhost:8080
   - Default credentials: airflow/airflow

5. **Run the pipeline**
   - Enable the `bikeshare_pipeline` DAG in Airflow UI
   - Trigger manual run or wait for scheduled execution

## Data Models

### Staging Layer
- **stg_bikeshare**: Cleaned and typed raw bike share data

### Marts Layer
- **dim_weather**: Weather condition dimension with human-readable descriptions
- **fct_hourly_rentals**: Hourly bike rental facts with weather context

## Key Features
- ✅ **Full ELT Pipeline**: Extract → Load → Transform pattern
- ✅ **Data Quality Tests**: Automated testing with dbt
- ✅ **Incremental Loading**: Efficient data processing
- ✅ **Documentation**: Auto-generated dbt docs
- ✅ **Monitoring**: Airflow task monitoring and alerting
- ✅ **Version Control**: All code in Git with proper structure

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

## Next Steps
- Add more data sources (bike station locations, maintenance logs)
- Implement real-time streaming with Apache Kafka
- Add machine learning predictions for demand forecasting
- Create dashboards with Tableau/PowerBI

## Contributing
Feel free to submit issues and pull requests!

## License
MIT License - see LICENSE file for details
