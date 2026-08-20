from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id="taxi_data_pipeline",
    schedule="0 6 1 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["taxi", "pyspark", "portfolio"],
)
def taxi_pipeline():

    @task.bash
    def run_transform():
        return "DATA_BASE_PATH=/opt/airflow S3_ENDPOINT=http://localstack:4566 python /opt/airflow/transformation/transform.py"

    run_transform()

taxi_pipeline()