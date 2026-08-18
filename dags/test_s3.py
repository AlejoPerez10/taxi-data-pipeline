from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

def test_s3_connection():
    hook = S3Hook(aws_conn_id='aws_default')

    # Obtiene el cliente de S3 para listar los buckets
    s3_client = hook.get_conn()
    response = s3_client.list_buckets()

    bucket_names = [b['Name'] for b in response.get('Buckets', [])]
    print(f'Buckets encontrados en LocalStack: {bucket_names}')

with DAG(
    dag_id='test_localstack_s3',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
) as dag:
    
    test_task = PythonOperator(
        task_id='check_s3', python_callable=test_s3_connection
    )