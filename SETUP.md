# 🚀 Quick Setup Guide

This guide will help you get the Python Airflow Snowflake Data Pipeline up and running quickly.

## ✅ **All Issues Fixed!**

The following issues have been resolved:

1. ✅ **Requirements.txt cleaned** - Removed duplicates, added PySpark and Spark providers
2. ✅ **Docker Compose consolidated** - Using single infrastructure approach
3. ✅ **Dockerfile enhanced** - Added Spark JARs for Snowflake connectivity
4. ✅ **Environment setup** - Automated .env file creation
5. ✅ **Import paths fixed** - Proper Python path handling for containers

## 🎯 **Quick Start (5 minutes)**

### **Option 1: Automated Setup (Recommended)**

```bash
# 1. Complete setup in one command
make setup-dev

# 2. Edit Snowflake credentials
nano .env

# 3. Start all services
make start

# 4. Initialize Airflow
make init

# 5. Access Airflow UI
open http://localhost:8080
```

### **Option 2: Manual Setup**

```bash
# 1. Create environment file
make setup-env

# 2. Edit .env with your Snowflake credentials
nano .env

# 3. Build Docker images
make build

# 4. Start services
make start

# 5. Initialize Airflow (first time only)
make init
```

## 🔧 **Configuration**

### **Required: Update Snowflake Credentials**

Edit the `.env` file with your actual Snowflake credentials:

```bash
# Update these values in .env
SNOWFLAKE_ACCOUNT=your_account.region.cloud
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_ROLE=your_role
```

## 🌐 **Access Points**

Once started, access these services:

- **Airflow UI**: http://localhost:8080 (admin/admin)
- **Spark UI**: http://localhost:9090
- **PostgreSQL**: localhost:5432 (airflow/airflow)

## 🎮 **Common Commands**

```bash
# View all available commands
make help

# Start services
make start

# Stop services
make stop

# View logs
make logs

# View Airflow logs only
make logs-airflow

# View Spark logs only
make logs-spark

# Check service status
make status

# Run tests
make test

# Run a specific job
make run-job JOB=jobs/bronze/process_raw_data.py

# Format code
make format

# Run linting
make lint
```

## 📊 **Running Your First Pipeline**

1. **Ensure services are running**:
   ```bash
   make status
   ```

2. **Access Airflow UI**: http://localhost:8080

3. **Enable the Bronze Data Pipeline DAG**

4. **Trigger the pipeline manually** or wait for the scheduled run (daily at 2 AM)

5. **Monitor progress** in the Airflow UI

## 🔍 **Troubleshooting**

### **Services won't start**
```bash
# Check Docker resources
docker system df

# Clean up if needed
make clean-docker

# Rebuild and restart
make build
make start
```

### **Airflow connection issues**
```bash
# Check Airflow logs
make logs-airflow

# Restart Airflow services
make restart
```

### **Spark job failures**
```bash
# Check Spark logs
make logs-spark

# Verify Snowflake credentials in .env file
cat .env | grep SNOWFLAKE
```

### **Import errors in jobs**
The import paths have been fixed to work in both Docker and local environments. If you still see import errors:

```bash
# Rebuild Docker images
make build

# Check Python path in container
docker-compose -f infrastructure/docker/docker-compose.yml exec airflow-webserver python -c "import sys; print(sys.path)"
```

## 📁 **Project Structure**

```
python-airflow-snowflake-data-pipeline/
├── 🔧 Makefile                    # Automation commands
├── 🐳 infrastructure/docker/      # Docker configurations
├── 📊 jobs/                       # Data processing jobs
│   ├── bronze/                   # Raw data processing
│   ├── silver/                   # Data cleaning
│   ├── gold/                     # Business logic
│   └── common/                   # Shared utilities
├── 🌬️ airflow/                   # Airflow DAGs and config
├── ❄️ snowflake/                 # Snowflake DDL/DML
├── 📋 config/                     # Configuration management
├── 🧪 tests/                      # Test suite
├── 📚 docs/                       # Documentation
└── 📊 monitoring/                 # Dashboards and alerts
```

## 🎯 **Next Steps**

1. **Customize the pipeline** for your specific data sources
2. **Add new DAGs** in the appropriate airflow/dags/ subdirectories
3. **Implement data quality checks** using the built-in framework
4. **Set up monitoring** using the provided Grafana dashboards
5. **Deploy to production** using the infrastructure configurations

## 🆘 **Getting Help**

- **View available commands**: `make help`
- **Check service status**: `make status`
- **View logs**: `make logs`
- **Documentation**: Check the `docs/` directory
- **Issues**: Create a GitHub issue

---

**🎉 Your production-grade data pipeline is ready to go!**
