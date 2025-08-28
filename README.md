# Python Airflow Snowflake Data Pipeline

A comprehensive, production-ready data pipeline solution that orchestrates data workflows using Apache Airflow, processes data with PySpark, and stores results in Snowflake with a well-defined data architecture (Bronze, Silver, Gold layers).

## 🚀 Features

- **Apache Airflow 2.9.2**: Modern workflow orchestration with DAG-based scheduling
- **Snowflake Integration**: Native Snowflake provider with optimized data loading
- **Multi-Layer Data Architecture**: Bronze (raw), Silver (cleaned), Gold (curated) data layers
- **PySpark Processing**: Scalable data transformation jobs
- **Local File Processing**: Direct processing of raw data files from local storage
- **Azure Cloud Support**: Integration with Azure services (Blob Storage, Key Vault, Data Lake)
- **Comprehensive Testing**: Unit tests for DAGs, scripts, and Snowflake DDL
- **CI/CD Ready**: GitHub Actions and pre-commit hooks for code quality

## 📁 Project Structure

```
python-airflow-snowflake-data-pipeline/
├── airflow/                          # Airflow configuration and DAGs
│   ├── dags/                        # Airflow DAG definitions
│   ├── config/                      # Airflow configuration files
│   ├── hooks/                       # Custom Airflow hooks
│   ├── operators/                   # Custom Airflow operators
│   ├── sensors/                     # Custom Airflow sensors
│   └── requirements.txt             # Airflow-specific dependencies
├── data/                            # Data storage
│   ├── raw/                         # Raw data files (CSV datasets)
│   └── processed/                   # Processed data output
├── data/                            # Data storage
├── snowflake/                       # Snowflake database objects
│   ├── ddl/                         # Data Definition Language scripts
│   │   ├── bronze/                  # Bronze layer table definitions
│   │   ├── silver/                  # Silver layer table definitions
│   │   ├── gold/                    # Gold layer table definitions
│   │   │   ├── dimensions/          # Dimension tables
│   │   │   ├── facts/               # Fact tables
│   │   │   └── views/               # Curated views
│   │   └── staging/                 # Staging table definitions
│   ├── dml/                         # Data Manipulation Language scripts
│   ├── configuration/                # Snowflake configuration files
│   ├── security/                    # Role and permission management
│   └── reference_table/             # Reference data management
├── utility_scripts/                  # Data processing utilities
│   ├── pyspark_jobs/                # PySpark transformation jobs
│   ├── utils.py                     # Common utility functions
│   └── web_scrapper.py              # Web scraping utilities
├── tests/                           # Test suite
│   ├── airflow_dags_test/           # DAG validation tests
│   ├── scripts_test/                # Utility script tests
│   └── snowflake_ddl_test/          # DDL validation tests
└── requirements.txt                  # Main project dependencies
```

## 🛠️ Prerequisites

- **Python 3.8+**
- **Apache Airflow 2.9.2**
- **Snowflake Account** with appropriate permissions
- **Local File System** (for raw data access)
- **PostgreSQL** (for Airflow metadata)
- **Docker** (optional, for containerized deployment)

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone <repository-url>
cd python-airflow-snowflake-data-pipeline
```

### 2. Set Up Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure Environment Variables

Create a `.env` file in the project root:

```bash
# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema

# Data Directory Configuration
DATA_RAW_PATH=./data/raw
DATA_PROCESSED_PATH=./data/processed

# Azure Configuration (if using)
AZURE_STORAGE_CONNECTION_STRING=your_connection_string
AZURE_KEY_VAULT_URL=your_key_vault_url
```

### 4. Initialize Airflow Database

```bash
cd airflow
airflow db init
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

### 5. Start Airflow Services

```bash
# Terminal 1: Start Airflow Webserver
airflow webserver --port 8080

# Terminal 2: Start Airflow Scheduler
airflow scheduler

# Terminal 3: Verify data files are available
ls -la data/raw/
```

### 6. Access Airflow Web UI

Open your browser and navigate to `http://localhost:8080`
- Username: `admin`
- Password: `admin`

## 📊 Data Architecture

### Bronze Layer (Raw Data)
- Stores raw data as-is from source systems
- Minimal transformation and validation
- Preserves data lineage and audit trail

### Silver Layer (Cleaned Data)
- Applies data quality rules and cleaning
- Standardizes data formats and schemas
- Implements business logic transformations

### Gold Layer (Curated Data)
- Business-ready dimensional models
- Optimized for analytics and reporting
- Pre-aggregated metrics and KPIs

## 🔧 Available DAGs

### 1. `simple_test_dag.py`
- Basic Airflow DAG for testing the environment
- Includes simple tasks for validation

### 2. `upload_local_to_minio_classic_dag.py`
- Processes local files from data/raw directory
- Demonstrates file processing workflows

### 3. `minio_connection_test.py`
- Tests local file system connectivity
- Validates data directory setup

## 🧪 Testing

Run the test suite to validate your setup:

```bash
# Run all tests
python -m pytest tests/

# Run specific test categories
python -m pytest tests/airflow_dags_test/
python -m pytest tests/scripts_test/
python -m pytest tests/snowflake_ddl_test/
```

## 📈 Monitoring and Logging

- **Airflow Logs**: Available in `airflow/logs/`
- **Task Execution**: Monitor via Airflow Web UI
- **Data Quality**: Implement custom sensors for data validation
- **Performance Metrics**: Track DAG execution times and resource usage

## 🔒 Security Features

- **Role-Based Access Control**: Snowflake roles and permissions
- **Secrets Management**: Azure Key Vault integration
- **Data Masking**: Snowflake masking policies for sensitive data
- **Audit Logging**: Comprehensive access and change tracking

## 🚀 Deployment Options

### Local Development
- Use the provided setup scripts
- Local file system for raw data access
- PostgreSQL for Airflow metadata

### Production Deployment
- Kubernetes deployment with Helm charts
- Managed Airflow service (MWAA, Cloud Composer)
- Production-grade Snowflake warehouse
- Monitoring and alerting integration

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Code Quality Standards
- Pre-commit hooks for code formatting
- Type hints and docstrings
- Comprehensive test coverage
- PEP 8 compliance

## 📚 Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Snowflake Documentation](https://docs.snowflake.com/)
- [Python File Operations](https://docs.python.org/3/tutorial/inputoutput.html#reading-and-writing-files)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

For questions and support:
- Create an issue in the GitHub repository
- Check the documentation and examples
- Review the test cases for usage patterns

---

**Note**: This is a production-ready data pipeline solution. Always test thoroughly in a development environment before deploying to production.