from datetime import datetime
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

# from scripts.ingestion_script import ingestion_data
from scripts.csv_to_parquet import format_to_parquet
from scripts.gcs_script import upload_to_gcs

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# Define url for loading data and working directory
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
ZONE_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + "taxi_zone.csv"
ZONE_PARQUET = ZONE_OUTPUT_FILE_TEMPLATE.replace('.csv', '.parquet')

# Default arguments for the DAG
args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    dag_id="nyc_taxi_data_ingestion",
    default_args=args,
    description="Downloading and processing NYC taxi data",
    schedule_interval="0 6 2 * *",  # Run on the 2nd of every month at 6:00 AM
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=3
)

# Tasks
    
download_zone_task = BashOperator(
    task_id="download_taxi_data_parquet",
    bash_command=f"curl -sSL {ZONE_URL} > {ZONE_OUTPUT_FILE_TEMPLATE}",
    dag = dag
)

format_to_parquet_task = PythonOperator(
    task_id="format_to_parquet",
    python_callable=format_to_parquet,
    op_kwargs={
        "src_file": ZONE_OUTPUT_FILE_TEMPLATE
    },
    dag=dag
)

local_to_gcs_task = PythonOperator(
    task_id="zone_data_form_local_to_gcs",
    python_callable=upload_to_gcs,
    op_kwargs={
        "bucket": BUCKET,
        "object_name": f"zone/{ZONE_PARQUET}",
        "local_file": ZONE_OUTPUT_FILE_TEMPLATE
    },
    dag=dag
)

remove_files_task = BashOperator(
    task_id="remove_file_task",
    bash_command=f"rm {ZONE_OUTPUT_FILE_TEMPLATE}"
)

download_zone_task >> format_to_parquet_task >> local_to_gcs_task >> remove_files_task