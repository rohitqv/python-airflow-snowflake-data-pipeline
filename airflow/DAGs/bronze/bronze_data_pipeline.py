"""
Bronze Layer Data Pipeline DAG

Orchestrates the processing of raw data files into Snowflake Bronze layer.
This DAG handles:
- Data validation
- Raw data processing with Spark
- Loading into Snowflake Bronze tables
- Data quality monitoring
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.email import EmailOperator
from airflow.utils.task_group import TaskGroup
import os

# DAG Configuration
DAG_ID = "bronze_data_pipeline"
SCHEDULE_INTERVAL = "0 2 * * *"  # Daily at 2 AM
START_DATE = datetime(2024, 1, 1)

# Default arguments
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': START_DATE,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['data-team@company.com'],  # Update with actual email
}

# Create DAG
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Bronze layer data processing pipeline',
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
    max_active_runs=1,
    tags=['production', 'bronze', 'spark', 'snowflake'],
)

# Helper functions
def validate_raw_data_files():
    """Validate that all required raw data files are present."""
    import os
    
    raw_data_path = "data/raw"
    required_files = [
        "olist_customers_dataset.csv",
        "olist_orders_dataset.csv",
        "olist_products_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_order_payments_dataset.csv",
        "olist_order_reviews_dataset.csv",
        "olist_sellers_dataset.csv",
        "olist_geolocation_dataset.csv",
        "product_category_name_translation.csv"
    ]
    
    missing_files = []
    file_sizes = {}
    
    for file in required_files:
        file_path = os.path.join(raw_data_path, file)
        if not os.path.exists(file_path):
            missing_files.append(file)
        else:
            file_sizes[file] = os.path.getsize(file_path)
    
    if missing_files:
        raise ValueError(f"Missing required files: {missing_files}")
    
    # Log file sizes for monitoring
    for file, size in file_sizes.items():
        print(f"✅ {file}: {size:,} bytes")
    
    print(f"✅ All {len(required_files)} required files are present")
    return "Raw data validation successful"

def check_snowflake_connection():
    """Test Snowflake connection and required schemas."""
    # This would typically use Snowflake connection
    # For now, we'll use a simple query
    return "SELECT CURRENT_TIMESTAMP() as connection_test"

def send_success_notification(**context):
    """Send success notification with processing summary."""
    dag_run = context['dag_run']
    execution_date = context['execution_date']
    
    message = f"""
    🎉 Bronze Data Pipeline Completed Successfully
    
    Execution Date: {execution_date}
    DAG Run ID: {dag_run.run_id}
    
    All raw data files have been processed and loaded into Snowflake Bronze layer.
    
    Next Steps:
    - Silver layer processing will begin automatically
    - Monitor data quality metrics in the dashboard
    """
    
    print(message)
    return message

# Task Groups
with TaskGroup("data_validation", dag=dag) as validation_group:
    
    validate_files = PythonOperator(
        task_id='validate_raw_files',
        python_callable=validate_raw_data_files,
        doc_md="""
        ## Validate Raw Data Files
        
        Checks that all required CSV files are present in the data/raw directory:
        - Validates file existence
        - Checks file sizes
        - Reports any missing files
        """,
    )
    
    test_snowflake = SnowflakeOperator(
        task_id='test_snowflake_connection',
        sql=check_snowflake_connection(),
        snowflake_conn_id='snowflake_default',
        doc_md="""
        ## Test Snowflake Connection
        
        Validates that Snowflake connection is working and accessible.
        """,
    )
    
    validate_files >> test_snowflake

# Spark Processing Task
process_bronze_data = SparkSubmitOperator(
    task_id='process_bronze_data',
    application='/opt/airflow/jobs/bronze/process_raw_data.py',
    conn_id='spark_default',
    application_args=['--config', '/opt/airflow/config/config.yaml', '--env', '{{ var.value.environment }}'],
    conf={
        'spark.executor.memory': '4g',
        'spark.driver.memory': '2g',
        'spark.executor.cores': '2',
        'spark.executor.instances': '4',
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        'spark.sql.adaptive.skewJoin.enabled': 'true',
    },
    dag=dag,
    doc_md="""
    ## Process Bronze Data
    
    Runs the Spark job to process all raw CSV files:
    - Reads raw data with error handling
    - Applies minimal transformations
    - Adds metadata columns
    - Loads data into Snowflake Bronze layer
    """,
)

# Data Quality Checks
with TaskGroup("data_quality_checks", dag=dag) as quality_group:
    
    check_record_counts = SnowflakeOperator(
        task_id='check_record_counts',
        sql="""
        SELECT 
            'BRONZE.OLIST_CUSTOMERS_DATASET' as table_name,
            COUNT(*) as record_count,
            CURRENT_TIMESTAMP() as check_time
        FROM BRONZE.OLIST_CUSTOMERS_DATASET
        WHERE _processing_date = CURRENT_DATE()
        
        UNION ALL
        
        SELECT 
            'BRONZE.OLIST_ORDERS_DATASET' as table_name,
            COUNT(*) as record_count,
            CURRENT_TIMESTAMP() as check_time
        FROM BRONZE.OLIST_ORDERS_DATASET
        WHERE _processing_date = CURRENT_DATE()
        
        UNION ALL
        
        SELECT 
            'BRONZE.OLIST_PRODUCTS_DATASET' as table_name,
            COUNT(*) as record_count,
            CURRENT_TIMESTAMP() as check_time
        FROM BRONZE.OLIST_PRODUCTS_DATASET
        WHERE _processing_date = CURRENT_DATE()
        """,
        snowflake_conn_id='snowflake_default',
    )
    
    check_data_freshness = SnowflakeOperator(
        task_id='check_data_freshness',
        sql="""
        SELECT 
            table_name,
            MAX(_ingestion_timestamp) as latest_ingestion,
            COUNT(*) as records_today
        FROM (
            SELECT 'CUSTOMERS' as table_name, _ingestion_timestamp FROM BRONZE.OLIST_CUSTOMERS_DATASET WHERE _processing_date = CURRENT_DATE()
            UNION ALL
            SELECT 'ORDERS' as table_name, _ingestion_timestamp FROM BRONZE.OLIST_ORDERS_DATASET WHERE _processing_date = CURRENT_DATE()
            UNION ALL
            SELECT 'PRODUCTS' as table_name, _ingestion_timestamp FROM BRONZE.OLIST_PRODUCTS_DATASET WHERE _processing_date = CURRENT_DATE()
        )
        GROUP BY table_name
        """,
        snowflake_conn_id='snowflake_default',
    )

# Notification Tasks
success_notification = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    dag=dag,
)

failure_notification = EmailOperator(
    task_id='send_failure_notification',
    to=['data-team@company.com'],  # Update with actual email
    subject='🚨 Bronze Data Pipeline Failed - {{ ds }}',
    html_content="""
    <h2>Bronze Data Pipeline Failure</h2>
    <p><strong>DAG:</strong> {{ dag.dag_id }}</p>
    <p><strong>Execution Date:</strong> {{ ds }}</p>
    <p><strong>Run ID:</strong> {{ dag_run.run_id }}</p>
    
    <p>The Bronze data pipeline has failed. Please check the Airflow logs for details.</p>
    
    <p><a href="{{ var.value.airflow_base_url }}/graph?dag_id={{ dag.dag_id }}">View DAG in Airflow</a></p>
    """,
    dag=dag,
    trigger_rule='one_failed',
)

# Task Dependencies
validation_group >> process_bronze_data >> quality_group >> success_notification
validation_group >> failure_notification
process_bronze_data >> failure_notification
quality_group >> failure_notification

# Documentation
dag.doc_md = """
# Bronze Data Pipeline

This DAG orchestrates the processing of raw data files into the Snowflake Bronze layer.

## Pipeline Overview

1. **Data Validation**: Validates raw data files and Snowflake connectivity
2. **Spark Processing**: Processes raw CSV files with comprehensive error handling
3. **Data Quality Checks**: Validates record counts and data freshness
4. **Notifications**: Sends success/failure notifications

## Configuration

- **Schedule**: Daily at 2:00 AM
- **Retries**: 3 attempts with 5-minute delays
- **Environment**: Configurable via Airflow variables

## Monitoring

- Check Airflow logs for detailed processing information
- Monitor Snowflake Bronze tables for data quality
- Review email notifications for pipeline status

## Troubleshooting

Common issues and solutions:
- **Missing files**: Check data/raw directory
- **Snowflake connection**: Verify connection settings
- **Spark errors**: Check resource allocation and job logs
"""
