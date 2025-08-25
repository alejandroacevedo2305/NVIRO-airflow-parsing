"""
Example DAG to demonstrate Airflow functionality.
This DAG runs daily and includes various task types.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
import random
import logging

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email": ["admin@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    "example_dag",
    default_args=default_args,
    description="An example DAG with various task types",
    schedule_interval="@daily",
    catchup=False,
    tags=["example", "tutorial"],
)


# Python callable functions
def print_welcome(**context):
    """Print welcome message with execution date."""
    execution_date = context["execution_date"]
    logging.info(f"Welcome to Airflow! Execution date: {execution_date}")
    return "Welcome task completed!"


def generate_random_number(**context):
    """Generate a random number and push to XCom."""
    number = random.randint(1, 100)
    logging.info(f"Generated random number: {number}")
    context["task_instance"].xcom_push(key="random_number", value=number)
    return number


def process_number(**context):
    """Pull number from XCom and process it."""
    ti = context["task_instance"]
    number = ti.xcom_pull(task_ids="generate_number", key="random_number")

    if number is None:
        logging.warning("No number found in XCom")
        return None

    result = number * 2
    logging.info(f"Original number: {number}, Processed result: {result}")
    return result


def check_result(**context):
    """Check the processed result."""
    ti = context["task_instance"]
    result = ti.xcom_pull(task_ids="process_number")

    if result and result > 100:
        logging.info(f"Result {result} is greater than 100!")
    else:
        logging.info(f"Result {result} is less than or equal to 100")

    return "Check completed"


# Define tasks
start_task = DummyOperator(
    task_id="start",
    dag=dag,
)

welcome_task = PythonOperator(
    task_id="print_welcome",
    python_callable=print_welcome,
    provide_context=True,
    dag=dag,
)

# Task group for number processing
with TaskGroup("number_processing", dag=dag) as number_group:
    generate_task = PythonOperator(
        task_id="generate_number",
        python_callable=generate_random_number,
        provide_context=True,
        dag=dag,
    )

    process_task = PythonOperator(
        task_id="process_number",
        python_callable=process_number,
        provide_context=True,
        dag=dag,
    )

    check_task = PythonOperator(
        task_id="check_result",
        python_callable=check_result,
        provide_context=True,
        dag=dag,
    )

    generate_task >> process_task >> check_task

# Bash tasks
system_info_task = BashOperator(
    task_id="print_system_info",
    bash_command='echo "Running on $(hostname) at $(date)"',
    dag=dag,
)

create_file_task = BashOperator(
    task_id="create_temp_file",
    bash_command='echo "Airflow task executed at $(date)" > /tmp/airflow_example_{{ ds }}.txt',
    dag=dag,
)

check_file_task = BashOperator(
    task_id="check_temp_file",
    bash_command='if [ -f /tmp/airflow_example_{{ ds }}.txt ]; then echo "File created successfully"; else echo "File not found"; exit 1; fi',
    dag=dag,
)

end_task = DummyOperator(
    task_id="end",
    dag=dag,
    trigger_rule="none_failed_or_skipped",
)

# Define task dependencies
start_task >> [welcome_task, system_info_task]
welcome_task >> number_group
system_info_task >> create_file_task >> check_file_task
[number_group, check_file_task] >> end_task
