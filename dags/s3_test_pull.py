from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

def download_file_to_local():
    s3_hook = S3Hook(aws_conn_id="s3_datalake")
    # URL : s3://local-bucket/empty-image-2880x1920.jpeg
    local_filename = s3_hook.download_file(key="empty-image-2880x1920.jpeg", bucket_name="local-bucket")
    print(f"The file has been placed locally here : {local_filename}")

with DAG(
    "s3_local_pull", start_date=datetime(2026, 1, 1), schedule_interval=None
) as dag:
    s3_local_pull_task = PythonOperator(task_id="s3_local_pull_task", python_callable=download_file_to_local)