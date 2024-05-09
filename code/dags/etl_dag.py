from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from operators.transform_operator import TransformOperator
from operators.custom_csv_transform_operator import CustomCsvToPostgresOperator
from operators.custom_postgres_operator import CustomPostgresOperator
from airflow.utils.dates import days_ago

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG object
with DAG(
    'etl_dag',
    default_args=default_args,
    description='ETL DAG with custom operators',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False
) as dag:

    # Task to extract data from Postgres
    extract_task = CustomPostgresOperator(
        task_id='extract_data',
        postgres_conn_id='postgres_default',
        sql='SELECT * FROM online_sales;',
    )

    # Task to transform data
    transform_task = TransformOperator(
        task_id='transform_data'
    )

    # Task to load CSV data to Postgres
    load_csv_task = CustomCsvToPostgresOperator(
        task_id='load_csv_to_postgres',
        postgres_conn_id='postgres_conn_id',
        table='online_sales',
        filepath='users/ahmethakan.baglama/Desktop/mlops/airflow_tut/mock_data.csv',
    )

    # Task to load data to Redshift
    load_to_redshift_task = S3ToRedshiftOperator(
        task_id='load_to_redshift',
        schema='redshift_schema',
        table='redshift_table',
        s3_bucket='s3_bucket',
        s3_key='s3_key',
        redshift_conn_id='yredshift_connection_id',
        aws_conn_id='aws_connection_id'
    )

    # Define task dependencies
    extract_task >> transform_task >> [load_csv_task, load_to_redshift_task]
