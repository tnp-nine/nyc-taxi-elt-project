from datetime import datetime
import os
import kaggle
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from scripts.csv_to_parquet import format_to_parquet
from scripts.gcs_script import upload_to_gcs
from scripts.download_kaggle_dataset import download_kaggle_dataset

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

USER_URL = "https://www.kaggle.com/api/v1/datasets/download/arnavsmayan/netflix-userbase-dataset"

# Define url for loading data and working directory
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
DOWNLOAD_FILE = f"{AIRFLOW_HOME}/Netflix Userbase.csv"
RENAMED_FILE = f"{AIRFLOW_HOME}/user_data.csv"
USER_PARQUET = RENAMED_FILE.replace('.csv', '.parquet')

GCS_USER_TEMPLATE = "user_data/user_data.parquet"

kaggle.api.authenticate()
# Default arguments for the DAG
args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Define your DAG
dag = DAG(
    dag_id='user_data_load',
    start_date=datetime(2024, 10, 21),
    schedule_interval=None, 
)

# Tasks
    
download_user_data_task = PythonOperator(
    task_id="download_user_dataset",
    python_callable=download_kaggle_dataset,
    op_kwargs={
        "download_path": AIRFLOW_HOME
    },
    dag=dag
)

rename_file_task = BashOperator(
    task_id="rename_file",
    bash_command=f"mv '{DOWNLOAD_FILE}' '{RENAMED_FILE}'",
    dag=dag
)

format_to_parquet_task = PythonOperator(
    task_id="format_to_parquet",
    python_callable=format_to_parquet,
    op_kwargs={
        "src_file": RENAMED_FILE,
        "dest_file": USER_PARQUET
    },
    dag=dag
)

local_to_gcs_task = PythonOperator(
    task_id="vehicle_data_form_local_to_gcs",
    python_callable=upload_to_gcs,
    op_kwargs={
        "bucket": BUCKET,
        "object_name": GCS_USER_TEMPLATE,
        "local_file": USER_PARQUET
    },
    dag=dag
)

remove_files_task = BashOperator(
    task_id="remove_file_task",
    bash_command=f"rm {RENAMED_FILE} {USER_PARQUET} "
)

download_user_data_task >> rename_file_task >> format_to_parquet_task >> local_to_gcs_task >> remove_files_task