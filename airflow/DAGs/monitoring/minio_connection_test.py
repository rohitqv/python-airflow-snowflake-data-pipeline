import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago

# --- Configuration ---
MINIO_BUCKET_NAME = "retail-data" # The bucket you expect to connect to
MINIO_CONN_ID = "minio_s3_connection" # Your Airflow Connection ID for MinIO S3

# --- Logging Setup ---
log = logging.getLogger(__name__)

# --- Python Callable for the Test Task ---
def _test_s3_connection_callable():
    """
    Python callable to test connectivity to MinIO S3 using S3Hook.
    It attempts to list objects in the specified bucket.
    """
    log.info(f"Attempting to test connection to MinIO S3 using Conn ID: {MINIO_CONN_ID}")
    
    try:
        # Initialize S3 Hook using the Airflow Connection ID
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        log.info("S3Hook initialized successfully.")

        # Attempt to list objects in the bucket to confirm connectivity and permissions
        # This will also fail if the bucket doesn't exist or is inaccessible
        log.info(f"Attempting to list objects in bucket: {MINIO_BUCKET_NAME}")
        keys = s3_hook.list_keys(bucket_name=MINIO_BUCKET_NAME, prefix="", delimiter="")
        
        if keys is not None:
            log.info(f"Successfully connected to s3://{MINIO_BUCKET_NAME}/.")
            log.info(f"Found {len(keys)} objects (or more, if listing is truncated).")
            # Log first few keys if available
            for i, key in enumerate(keys[:5]):
                log.info(f"  Sample object key {i+1}: {key}")
            if len(keys) > 5:
                log.info("  ... (more objects)")
        else:
            log.info(f"Connected to s3://{MINIO_BUCKET_NAME}/, but no objects found or list_keys returned None.")
        
        log.info("MinIO S3 connection test successful!")

    except Exception as e:
        log.error(f"MinIO S3 connection test FAILED: {e}", exc_info=True)
        # Re-raise the exception to make the task fail in Airflow UI
        raise 

# --- Define the DAG ---
with DAG(
    dag_id="minio_s3_connection_test-2", # Unique DAG ID
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["s3", "minio", "test", "connection"],
    doc_md="""
    ### MinIO S3 Connection Test DAG

    This DAG attempts to connect to a MinIO S3 bucket using the configured
    Airflow connection and list its contents to verify connectivity.
    """
) as dag:
    test_connection_task = PythonOperator(
        task_id='test_minio_s3_connection',
        python_callable=_test_s3_connection_callable,
    )