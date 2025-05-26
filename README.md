# 🚴 Bikeshare ELT Pipeline

A comprehensive **Extract, Load, Transform (ELT)** data pipeline for analyzing bike sharing data using **Airflow**, **dbt**, and **Snowflake**.

## 🏗️ **Architecture Overview**

```
Raw Data (CSV) → Airflow → Snowflake → dbt → Analytics Tables → Data Quality Tests
```

### **Technology Stack:**
- **🔄 Orchestration:** Apache Airflow 2.8.0
- **🏔️ Data Warehouse:** Snowflake
- **🔧 Transformation:** dbt (Data Build Tool)
- **🐳 Containerization:** Docker & Docker Compose
- **🐍 Language:** Python 3.11
- **📊 Data:** UCI Bike Share Dataset (17,379 records, 2011-2012)

---

## 📁 **Project Structure**

```
bikeshare-elt-pipeline/
├── 📂 dags/                      # Airflow DAG definitions
│   └── bikeshare_pipeline.py     # Main ELT pipeline DAG
├── 📂 dbt_project/               # dbt transformation project
│   ├── models/
│   │   ├── staging/              # Data cleaning & standardization
│   │   └── marts/               # Business logic & analytics
│   ├── macros/                  # Reusable dbt functions
│   └── tests/                   # Data quality tests (49 tests)
├── 📂 data/raw/                 # Source data files
│   └── hour.csv                 # Bike share hourly data
├── 📂 docker/                   # Docker configuration
│   └── docker-compose.yml       # Airflow + PostgreSQL setup
├── 📂 scripts/                  # Setup & utility scripts
│   ├── setup_snowflake.py      # Snowflake infrastructure setup
│   ├── load_data_to_snowflake.py # Data loading script
│   └── setup_airflow.sh        # Airflow configuration
└── 📂 sql/                      # SQL scripts & queries
```

---

## 🚀 **Quick Start**

### **Prerequisites**
- Docker & Docker Compose
- Python 3.9+
- Snowflake account with credentials
- Git

### **1. Clone Repository**
```bash
git clone <your-repo-url>
cd bikeshare-elt-pipeline
```

### **2. Set up Snowflake Environment**
```bash
# Copy and edit environment variables
cp set_snowflake_env.sh.example set_snowflake_env.sh
# Edit with your Snowflake credentials
source set_snowflake_env.sh
```

### **3. Initialize Snowflake Infrastructure**
```bash
python scripts/setup_snowflake.py
python scripts/load_data_to_snowflake.py
```

### **4. Start Airflow**
```bash
cd docker
docker compose up -d
```

### **5. Configure Airflow**
```bash
# Wait for containers to start, then:
source ../set_snowflake_env.sh
./scripts/setup_airflow.sh
```

### **6. Access Airflow UI**
- **URL:** http://localhost:8080
- **Username:** `airflow`
- **Password:** `airflow`

### **7. Run the Pipeline**
```bash
# Trigger the DAG
docker exec docker-airflow-webserver-1 airflow dags trigger bikeshare_pipeline
```

---

## 📊 **Data Pipeline Details**

### **Data Flow**

#### **📥 Extract**
- **Source:** UCI Bike Share Dataset
- **Format:** CSV (17,379 hourly records)
- **Timespan:** January 2011 - December 2012
- **Features:** Weather, seasonality, user types, rental counts

#### **📦 Load**
- **Target:** Snowflake Data Warehouse
- **Schema:** `BIKESHARE_DB.RAW.BIKESHARE_RAW`
- **Method:** Batch loading via Snowflake COPY command
- **Validation:** Data quality checks before loading

#### **🔄 Transform (dbt Models)**

1. **Staging Layer** (`stg_bikeshare`)
   - Data type conversions
   - Column renaming & standardization
   - Basic data cleaning

2. **Dimension Tables**
   - `dim_weather` - Weather conditions with aggregated metrics

3. **Fact Tables**
   - `fct_hourly_rentals` - Enriched rental data with business logic

### **🧪 Data Quality Framework**
- **49 comprehensive tests** covering:
  - Source data validation
  - Referential integrity
  - Business rule validation
  - Statistical outlier detection
  - Data freshness checks

---

## 🎯 **Airflow DAG: `bikeshare_pipeline`**

### **Task Flow**
```
validate_raw_data → prepare_data → create_snowflake_stage → 
load_to_snowflake → verify_snowflake_load → dbt_run → 
dbt_test → notify_success
```

### **Task Details**

| Task | Description | Duration |
|------|-------------|----------|
| `validate_raw_data` | Validates CSV data quality & schema | ~5s |
| `prepare_data` | Formats data for Snowflake loading | ~10s |
| `create_snowflake_stage` | Sets up Snowflake file stage | ~5s |
| `load_to_snowflake` | Bulk loads data into raw table | ~30s |
| `verify_snowflake_load` | Validates successful data loading | ~5s |
| `dbt_run` | Executes dbt transformations | ~15s |
| `dbt_test` | Runs all data quality tests | ~30s |
| `notify_success` | Sends completion notification | ~5s |

### **Scheduling**
- **Frequency:** Daily at 1:00 AM UTC
- **Retries:** 2 attempts with 5-minute delays
- **SLA:** 4 hours
- **Monitoring:** Email notifications on success/failure

---

## 🏔️ **Snowflake Configuration**

### **Database Objects**
```sql
DATABASE: BIKESHARE_DB
├── SCHEMA: RAW                    # Source data
│   ├── TABLE: BIKESHARE_RAW      # Raw bike share data
│   ├── STAGE: BIKESHARE_STAGE    # File loading stage
│   └── FILE_FORMAT: CSV_FORMAT   # CSV parsing rules
├── SCHEMA: ANALYTICS             # Transformed data
│   ├── TABLE: dim_weather        # Weather dimension
│   ├── TABLE: fct_hourly_rentals # Rental facts
│   └── VIEW: stg_bikeshare       # Staging view
└── WAREHOUSE: BIKESHARE_WH       # Compute resources
```

### **Security & Access**
- **Roles:** ACCOUNTADMIN (setup), SYSADMIN (runtime)
- **Authentication:** Username/Password
- **Network:** Standard Snowflake connectivity

---

## 🔧 **dbt Configuration**

### **Models Hierarchy**
```
models/
├── staging/
│   └── stg_bikeshare.sql         # Data standardization
└── marts/
    ├── dim_weather.sql           # Weather dimension
    └── fct_hourly_rentals.sql    # Rental facts
```

### **Key Features**
- **Incremental Models:** Optimized for large datasets
- **Macros:** Reusable business logic
- **Tests:** Comprehensive data quality framework
- **Documentation:** Auto-generated lineage & docs

### **Business Logic Examples**
```sql
-- Time-of-day categorization
CASE 
  WHEN hour BETWEEN 0 AND 5 THEN 'Early Morning'
  WHEN hour BETWEEN 6 AND 9 THEN 'Morning Rush'
  WHEN hour BETWEEN 10 AND 15 THEN 'Mid-Day'
  WHEN hour BETWEEN 16 AND 19 THEN 'Evening Rush'
  ELSE 'Night'
END as time_of_day

-- Weather description mapping
CASE weather_id
  WHEN 1 THEN 'Clear/Partly Cloudy'
  WHEN 2 THEN 'Misty/Cloudy'
  WHEN 3 THEN 'Light Rain/Snow'
  WHEN 4 THEN 'Heavy Rain/Snow'
END as weather_desc
```

---

## 📈 **Analytics & Insights**

### **Key Metrics Available**
- **Rental Patterns:** Hourly, daily, seasonal trends
- **User Segmentation:** Casual vs. registered users
- **Weather Impact:** Correlation with rental demand
- **Operational Efficiency:** Peak usage identification

### **Sample Queries**
```sql
-- Peak rental hours by season
SELECT 
    season_name,
    hour,
    AVG(total_rentals) as avg_rentals
FROM fct_hourly_rentals 
GROUP BY season_name, hour 
ORDER BY season_name, avg_rentals DESC;

-- Weather impact analysis
SELECT 
    w.weather_desc,
    AVG(f.total_rentals) as avg_rentals,
    COUNT(*) as total_hours
FROM fct_hourly_rentals f
JOIN dim_weather w ON f.weather_id = w.weather_id
GROUP BY w.weather_desc
ORDER BY avg_rentals DESC;
```

---

## 🔍 **Monitoring & Observability**

### **Data Quality Monitoring**
- **49 automated tests** run with every pipeline execution
- **Test Categories:**
  - Schema validation
  - Referential integrity
  - Business rule compliance
  - Statistical anomaly detection

### **Pipeline Monitoring**
- **Airflow UI:** Real-time task status & logs
- **Email Alerts:** Success/failure notifications
- **SLA Monitoring:** 4-hour completion target
- **Retry Logic:** Automatic failure recovery

### **Performance Metrics**
- **Pipeline Runtime:** ~2 minutes end-to-end
- **Data Volume:** 17,379 records processed
- **Test Coverage:** 100% of critical data paths
- **Uptime Target:** 99.9% availability

---

## 🐛 **Troubleshooting**

### **Common Issues**

#### **Airflow DAG Not Visible**
```bash
# Check DAG syntax
docker exec docker-airflow-webserver-1 airflow dags list | grep bikeshare
# View DAG errors
docker exec docker-airflow-webserver-1 airflow dags show bikeshare_pipeline
```

#### **Snowflake Connection Failed**
```bash
# Test connection
source set_snowflake_env.sh
python scripts/setup_snowflake.py
# Verify credentials in Airflow
docker exec docker-airflow-webserver-1 airflow connections list
```

#### **dbt Tests Failing**
```bash
# Run specific test
cd dbt_project
dbt test --select test_name --profiles-dir .
# Check test results
dbt test --store-failures --profiles-dir .
```

#### **Data Loading Issues**
```bash
# Check data file accessibility
docker exec docker-airflow-webserver-1 ls -la /opt/airflow/data/raw/
# Verify Snowflake permissions
python scripts/setup_snowflake.py
```

---

## 🔄 **Development Workflow**

### **Making Changes**
1. **Modify dbt models** in `dbt_project/models/`
2. **Test changes** locally: `dbt run --profiles-dir .`
3. **Validate quality** with: `dbt test --profiles-dir .`
4. **Commit changes** and push to repository
5. **Monitor pipeline** execution in Airflow UI

### **Adding New Models**
```bash
# Create new model
echo "SELECT * FROM {{ ref('stg_bikeshare') }}" > dbt_project/models/marts/new_model.sql

# Add tests
# Edit dbt_project/models/marts/schema.yml

# Run and test
dbt run --select new_model --profiles-dir .
dbt test --select new_model --profiles-dir .
```

---

## 📚 **Resources & References**

### **Documentation**
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [dbt Documentation](https://docs.getdbt.com/)
- [Snowflake Documentation](https://docs.snowflake.com/)

### **Dataset Information**
- **Source:** [UCI Machine Learning Repository](https://archive.ics.uci.edu/ml/datasets/bike+sharing+dataset)
- **Citation:** Fanaee-T, Hadi, and Gama, Joao. "Event labeling combining ensemble detectors and background knowledge." Progress in Artificial Intelligence (2013): pp. 1-15, Springer Berlin Heidelberg.

### **Architecture Patterns**
- **ELT vs ETL:** Modern data warehouse approach
- **Dimensional Modeling:** Star schema design
- **Data Quality:** Test-driven development

---

## 🤝 **Contributing**

### **Development Setup**
```bash
# Install dependencies
pip install -r requirements.txt

# Set up pre-commit hooks
pre-commit install

# Run tests
pytest tests/
```

### **Code Standards**
- **Python:** PEP 8 compliance
- **SQL:** dbt style guide
- **Documentation:** Comprehensive inline comments
- **Testing:** Minimum 80% coverage

---

## 📄 **License**

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🎯 **Project Status**

### **✅ Completed Features**
- ✅ Complete ELT pipeline implementation
- ✅ Automated data quality testing (49 tests)
- ✅ Comprehensive monitoring & alerting
- ✅ Docker containerization
- ✅ Production-ready configuration

### **🔄 Future Enhancements**
- 🔄 Real-time streaming integration
- 🔄 Advanced analytics & ML models
- 🔄 Data visualization dashboard
- 🔄 CI/CD pipeline integration
- 🔄 Multi-environment deployment

---

## 👨‍💻 **Author**

**Alireza Dashchi**
- 📧 Email: alireza.dashchi@example.com
- 💼 LinkedIn: [Your LinkedIn Profile]
- 🐙 GitHub: [Your GitHub Profile]

---

*Last Updated: May 2024*
