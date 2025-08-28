import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago

# --- Configuration ---
# Assuming your DAG file is in 'airflow/dags'
# This path points to the project root: python-airflow-snowflake-data-pipeline/
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
LOCAL_RAW_DATA_DIR = os.path.join(BASE_DIR, "data", "raw")

# MinIO S3 Bucket and Airflow Connection ID
MINIO_BUCKET_NAME = "retail-data" # Ensure this bucket exists in your MinIO
MINIO_CONN_ID = "minio_s3_connection" # This MUST match your Airflow Connection ID

# --- Logging Setup ---
log = logging.getLogger(__name__)

# --- Python Callable for the Task ---
def _upload_files_to_minio_callable():
    """
    Python callable function to iterate through files in LOCAL_RAW_DATA_DIR
    and upload them to the specified MinIO S3 bucket using Airflow's S3Hook.
    """
    log.info(f"Starting upload of files from local directory: {LOCAL_RAW_DATA_DIR}")

    # Check if the local directory exists
    if not os.path.isdir(LOCAL_RAW_DATA_DIR):
        log.error(f"Error: Local raw data directory '{LOCAL_RAW_DATA_DIR}' not found.")
        raise FileNotFoundError(f"Directory {LOCAL_RAW_DATA_DIR} does not exist. Please place your data there.")

    # Initialize S3 Hook using the Airflow Connection ID
    try:
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        log.info(f"S3Hook initialized successfully with connection ID: {MINIO_CONN_ID}")
    except Exception as e:
        log.error(f"Failed to initialize S3Hook using connection ID '{MINIO_CONN_ID}': {e}", exc_info=True)
        raise # Fail the task if the hook cannot be initialized (bad connection config)

    uploaded_count = 0
    skipped_count = 0

    # Iterate through all items in the local data directory
    for filename in os.listdir(LOCAL_RAW_DATA_DIR):
        local_file_path = os.path.join(LOCAL_RAW_DATA_DIR, filename)

        # Check if it's a file and ends with .csv
        if os.path.isfile(local_file_path) and filename.lower().endswith(".csv"):
            s3_key = filename # Use the same file name as the object key in S3
            try:
                log.info(f"Attempting to upload '{filename}' to s3://{MINIO_BUCKET_NAME}/{s3_key}")
                
                # Use s3_hook.load_file to upload
                s3_hook.load_file(
                    filename=local_file_path,
                    key=s3_key,
                    bucket_name=MINIO_BUCKET_NAME,
                    replace=True # Overwrite if a file with the same name already exists in the bucket
                )
                log.info(f"Successfully uploaded '{filename}'.")
                uploaded_count += 1
            except Exception as e:
                log.error(f"Failed to upload '{filename}': {e}", exc_info=True)
                # You can choose to raise an exception here to fail the task immediately
                # if any single file upload failure should halt the DAG.
                # For now, it logs the error and continues with other files.
        else:
            log.info(f"Skipping non-CSV file or directory: '{filename}'")
            skipped_count += 1

    if uploaded_count == 0:
        log.warning(f"No CSV files found or uploaded from '{LOCAL_RAW_DATA_DIR}'. Total skipped: {skipped_count}.")
    else:
        log.info(f"Finished uploading. Total {uploaded_count} CSV file(s) uploaded to s3://{MINIO_BUCKET_NAME}/. Total skipped: {skipped_count}.")

# --- Define the DAG ---
with DAG(
    dag_id="upload_local_to_minio_classic", # Unique DAG ID for this version
    start_date=days_ago(1), # Recommended for DAGs that run regularly
    schedule_interval=None, # Set to None for manual runs, or '@daily', etc.
    catchup=False,          # Do not run for past missed schedules
    tags=["s3", "minio", "upload", "data_ingestion", "classic_style"],
    doc_md="""
    ### Upload Local Data to MinIO S3 DAG (Classic Style)

    This DAG uploads all CSV files from the local 'data/raw' directory
    to a specified MinIO S3 bucket.

    **Configuration:**
    - Requires an Airflow Connection with `Conn Id: minio_s3_connection`
      (or custom ID, as defined by `MINIO_CONN_ID` in the DAG).
    - Ensure your MinIO server is running and the target bucket exists.
    - Files are expected in `python-airflow-snowflake-data-pipeline/data/raw/`.
    """
) as dag:
    # Define the PythonOperator task
    upload_task = PythonOperator(
        task_id='upload_files_to_minio_task',
        python_callable=_upload_files_to_minio_callable,
        # No need for provide_context=True if you're not using 'ti' in the callable
    )

    # For a single task, no explicit dependencies are needed,
    # but you would define them here if you had more tasks.
    # Example: upload_task >> next_processing_task