from airflow.providers.postgres.operators.csv_to_postgres import CsvToPostgresOperator

# Define the task to load CSV data to Postgres
load_csv_task = CsvToPostgresOperator(
    task_id='load_csv_to_postgres',  # Task ID
    postgres_conn_id='postgres_conn_id',  # Connection ID for Postgres
    table='online_sales',  # Target table in Postgres
    filepath='users/ahmethakan.baglama/Desktop/mlops/airflow_tut/mock_data.csv',  # Filepath of CSV data
)
