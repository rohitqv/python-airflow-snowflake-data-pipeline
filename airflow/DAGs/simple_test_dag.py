import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import time

log = logging.getLogger(__name__)

# --- Define Python callable functions for each task ---

def _start_process():
    """Logs the start of the overall process."""
    log.info(f"[{datetime.now()}] Starting the overall DAG process.")
    time.sleep(1) # Simulate a small setup time

def _do_parallel_work_A():
    """Simulates parallel work block A."""
    log.info(f"[{datetime.now()}] Doing parallel work A...")
    time.sleep(3) # Simulate a longer work time
    log.info(f"[{datetime.now()}] Parallel work A completed.")

def _do_parallel_work_B():
    """Simulates parallel work block B."""
    log.info(f"[{datetime.now()}] Doing parallel work B...")
    time.sleep(2) # Simulate a shorter work time
    log.info(f"[{datetime.now()}] Parallel work B completed.")

def _end_process():
    """Logs the completion of the overall process."""
    log.info(f"[{datetime.now()}] All parallel work finished. Ending the overall DAG process.")


# --- Define the DAG ---

with DAG(
    dag_id="multiple_tasks_test_dag_classic", # Unique DAG ID
    start_date=datetime(2025, 6, 17),
    schedule_interval=None,
    catchup=False,
    tags=["test", "basic", "multiple_tasks", "classic_style"],
    doc_md="""
    ### Multiple Tasks Test DAG (Classic Style)
    This DAG demonstrates multiple tasks with dependencies:
    - start_task (sequential)
    - parallel_task_A & parallel_task_B (run in parallel after start_task)
    - end_task (runs after both parallel tasks complete)
    """
) as dag:
    # 1. Define the 'start_task'
    start_task = PythonOperator(
        task_id='start_the_process',
        python_callable=_start_process,
    )

    # 2. Define the two parallel tasks
    parallel_task_A = PythonOperator(
        task_id='do_parallel_work_A',
        python_callable=_do_parallel_work_A,
    )

    parallel_task_B = PythonOperator(
        task_id='do_parallel_work_B',
        python_callable=_do_parallel_work_B,
    )

    # 3. Define the 'end_task'
    end_task = PythonOperator(
        task_id='end_the_process',
        python_callable=_end_process,
    )

    # --- Define Task Dependencies ---
    # The '>>' operator means "set downstream"
    # The '<<' operator means "set upstream"

    # 'start_task' must run first
    start_task >> [parallel_task_A, parallel_task_B]

    # Both 'parallel_task_A' and 'parallel_task_B' must complete before 'end_task' runs
    [parallel_task_A, parallel_task_B] >> end_task

    # You could also chain them sequentially like:
    # task1 >> task2 >> task3
    # Or explicitly:
    # start_task.set_downstream(parallel_task_A)
    # start_task.set_downstream(parallel_task_B)
    # parallel_task_A.set_downstream(end_task)
    # parallel_task_B.set_downstream(end_task) # Airflow automatically waits for all upstream tasks