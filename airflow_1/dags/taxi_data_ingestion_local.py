from datetime import datetime
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

# from scripts.ingestion_script import ingestion_data
from scripts.gcs_script import upload_to_gcs

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# Define url for loading data and working directory
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"

URL_TEMPLATE = URL_PREFIX + "/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + "/output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
TABLE_NAME_TEMPLATE = "yellow_taxi_{{ execution_date.strftime('%Y-%m') }}"
GCS_OBJECT_TEMPLATE = "raw/yellow_taxi_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

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
    
download_taxi_parquet_task = BashOperator(
    task_id="download_taxi_data_parquet",
    bash_command=f"curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}",
    dag = dag
)

local_to_gcs_task = PythonOperator(
    task_id="local_to_gcs_task",
    python_callable=upload_to_gcs,
    op_kwargs=dict(
        bucket=BUCKET,
        object_name=GCS_OBJECT_TEMPLATE,
        local_file=OUTPUT_FILE_TEMPLATE
    ),
    dag = dag
)

remove_files_task = BashOperator(
    task_id="remove_file_task",
    bash_command=f"rm {OUTPUT_FILE_TEMPLATE}"
)

download_taxi_parquet_task >> local_to_gcs_task >> remove_files_task