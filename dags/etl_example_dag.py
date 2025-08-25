"""
ETL Example DAG demonstrating a typical Extract-Transform-Load pipeline.
This DAG simulates data processing from multiple sources.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import json
import logging
import random
from typing import Dict, List, Any

# Default arguments
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# Create DAG
dag = DAG(
    "etl_example_pipeline",
    default_args=default_args,
    description="Example ETL pipeline with extraction, transformation, and loading",
    schedule_interval="0 2 * * *",  # Daily at 2 AM
    catchup=False,
    tags=["etl", "example", "data-pipeline"],
    max_active_runs=1,
)


# ETL Functions
def extract_from_source_a(**context) -> Dict[str, Any]:
    """Simulate extracting data from Source A (e.g., API)."""
    logging.info("Extracting data from Source A...")

    # Simulate API data extraction
    data = {
        "source": "source_a",
        "timestamp": datetime.now().isoformat(),
        "records": [
            {"id": i, "value": random.randint(100, 1000), "type": "A"}
            for i in range(1, 11)
        ],
    }

    logging.info(f"Extracted {len(data['records'])} records from Source A")
    return data


def extract_from_source_b(**context) -> Dict[str, Any]:
    """Simulate extracting data from Source B (e.g., Database)."""
    logging.info("Extracting data from Source B...")

    # Simulate database data extraction
    data = {
        "source": "source_b",
        "timestamp": datetime.now().isoformat(),
        "records": [
            {"id": i, "value": random.randint(200, 2000), "type": "B"}
            for i in range(11, 21)
        ],
    }

    logging.info(f"Extracted {len(data['records'])} records from Source B")
    return data


def extract_from_source_c(**context) -> Dict[str, Any]:
    """Simulate extracting data from Source C (e.g., File)."""
    logging.info("Extracting data from Source C...")

    # Simulate file data extraction
    data = {
        "source": "source_c",
        "timestamp": datetime.now().isoformat(),
        "records": [
            {"id": i, "value": random.randint(300, 3000), "type": "C"}
            for i in range(21, 31)
        ],
    }

    logging.info(f"Extracted {len(data['records'])} records from Source C")
    return data


def validate_data(**context) -> bool:
    """Validate extracted data from all sources."""
    ti = context["task_instance"]

    sources = ["extract_source_a", "extract_source_b", "extract_source_c"]
    all_valid = True

    for source in sources:
        data = ti.xcom_pull(task_ids=source)

        if not data or "records" not in data:
            logging.error(f"Invalid data structure from {source}")
            all_valid = False
            continue

        # Validate each record
        for record in data["records"]:
            if not all(key in record for key in ["id", "value", "type"]):
                logging.error(f"Invalid record structure in {source}: {record}")
                all_valid = False

        logging.info(f"Validated {len(data['records'])} records from {source}")

    return all_valid


def transform_data(**context) -> Dict[str, Any]:
    """Transform and combine data from all sources."""
    ti = context["task_instance"]

    logging.info("Starting data transformation...")

    # Retrieve data from all sources
    sources = ["extract_source_a", "extract_source_b", "extract_source_c"]
    all_records = []

    for source in sources:
        data = ti.xcom_pull(task_ids=source)
        if data and "records" in data:
            all_records.extend(data["records"])

    # Apply transformations
    transformed_records = []
    for record in all_records:
        transformed = {
            "id": record["id"],
            "original_value": record["value"],
            "transformed_value": record["value"] * 1.1,  # Apply 10% increase
            "category": f"Category_{record['type']}",
            "processed_at": datetime.now().isoformat(),
        }
        transformed_records.append(transformed)

    # Calculate aggregations
    total_original = sum(r["original_value"] for r in transformed_records)
    total_transformed = sum(r["transformed_value"] for r in transformed_records)

    result = {
        "records": transformed_records,
        "metadata": {
            "total_records": len(transformed_records),
            "total_original_value": total_original,
            "total_transformed_value": total_transformed,
            "transformation_rate": 1.1,
            "processed_at": datetime.now().isoformat(),
        },
    }

    logging.info(f"Transformed {len(transformed_records)} records")
    logging.info(
        f"Aggregations: Original={total_original}, Transformed={total_transformed}"
    )

    return result


def clean_data(**context) -> Dict[str, Any]:
    """Clean transformed data."""
    ti = context["task_instance"]
    transformed_data = ti.xcom_pull(task_ids="transform_data")

    logging.info("Cleaning data...")

    if not transformed_data or "records" not in transformed_data:
        raise ValueError("No transformed data available for cleaning")

    # Remove duplicates and apply cleaning rules
    cleaned_records = []
    seen_ids = set()

    for record in transformed_data["records"]:
        if record["id"] not in seen_ids:
            # Round values to 2 decimal places
            record["transformed_value"] = round(record["transformed_value"], 2)
            cleaned_records.append(record)
            seen_ids.add(record["id"])

    cleaned_data = {
        "records": cleaned_records,
        "metadata": {
            **transformed_data["metadata"],
            "cleaned_records": len(cleaned_records),
            "duplicates_removed": len(transformed_data["records"])
            - len(cleaned_records),
        },
    }

    logging.info(f"Cleaned {len(cleaned_records)} records")
    return cleaned_data


def load_to_warehouse(**context) -> str:
    """Simulate loading data to data warehouse."""
    ti = context["task_instance"]
    cleaned_data = ti.xcom_pull(task_ids="clean_data")

    logging.info("Loading data to warehouse...")

    if not cleaned_data or "records" not in cleaned_data:
        raise ValueError("No cleaned data available for loading")

    # Simulate warehouse loading
    records_loaded = len(cleaned_data["records"])

    # In a real scenario, this would insert into a database
    logging.info(f"Successfully loaded {records_loaded} records to warehouse")
    logging.info(f"Metadata: {json.dumps(cleaned_data['metadata'], indent=2)}")

    return f"Loaded {records_loaded} records"


def load_to_lake(**context) -> str:
    """Simulate loading raw data to data lake."""
    ti = context["task_instance"]

    logging.info("Loading raw data to data lake...")

    # Collect all raw data
    sources = ["extract_source_a", "extract_source_b", "extract_source_c"]
    total_records = 0

    for source in sources:
        data = ti.xcom_pull(task_ids=source)
        if data and "records" in data:
            total_records += len(data["records"])
            # In a real scenario, this would write to S3/HDFS/Azure Storage
            logging.info(
                f"Loaded {len(data['records'])} records from {source} to data lake"
            )

    return f"Loaded {total_records} raw records to data lake"


def generate_report(**context) -> str:
    """Generate summary report of the ETL process."""
    ti = context["task_instance"]

    logging.info("Generating ETL summary report...")

    # Gather all metrics
    cleaned_data = ti.xcom_pull(task_ids="clean_data")
    warehouse_result = ti.xcom_pull(task_ids="load_to_warehouse")
    lake_result = ti.xcom_pull(task_ids="load_to_lake")

    report = {
        "execution_date": context["execution_date"].isoformat(),
        "dag_run_id": context["dag_run"].run_id,
        "warehouse_load": warehouse_result,
        "lake_load": lake_result,
        "metrics": cleaned_data.get("metadata", {}) if cleaned_data else {},
    }

    logging.info("ETL Report Generated:")
    logging.info(json.dumps(report, indent=2))

    return "Report generated successfully"


# Define tasks

# Start
start = BashOperator(
    task_id="start",
    bash_command='echo "Starting ETL pipeline at $(date)"',
    dag=dag,
)

# Extract tasks group
with TaskGroup("extract", dag=dag) as extract_group:
    extract_a = PythonOperator(
        task_id="extract_source_a",
        python_callable=extract_from_source_a,
        provide_context=True,
        dag=dag,
    )

    extract_b = PythonOperator(
        task_id="extract_source_b",
        python_callable=extract_from_source_b,
        provide_context=True,
        dag=dag,
    )

    extract_c = PythonOperator(
        task_id="extract_source_c",
        python_callable=extract_from_source_c,
        provide_context=True,
        dag=dag,
    )

# Validation
validate = PythonOperator(
    task_id="validate_data",
    python_callable=validate_data,
    provide_context=True,
    dag=dag,
)

# Transform tasks group
with TaskGroup("transform", dag=dag) as transform_group:
    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        provide_context=True,
        dag=dag,
    )

    clean = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data,
        provide_context=True,
        dag=dag,
    )

    transform >> clean

# Load tasks group
with TaskGroup("load", dag=dag) as load_group:
    load_warehouse = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=load_to_warehouse,
        provide_context=True,
        dag=dag,
    )

    load_lake = PythonOperator(
        task_id="load_to_lake",
        python_callable=load_to_lake,
        provide_context=True,
        dag=dag,
    )

# Report generation
report = PythonOperator(
    task_id="generate_report",
    python_callable=generate_report,
    provide_context=True,
    dag=dag,
)

# End
end = BashOperator(
    task_id="end",
    bash_command='echo "ETL pipeline completed at $(date)"',
    trigger_rule="none_failed_or_skipped",
    dag=dag,
)

# Define dependencies
start >> extract_group >> validate >> transform_group >> load_group >> report >> end
