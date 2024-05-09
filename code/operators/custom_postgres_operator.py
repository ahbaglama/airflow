from airflow.operators.postgres_operator import PostgresOperator

# Define the task to extract data from Postgres
extract_postgres_task = PostgresOperator(
    task_id='extract_from_postgres',  # Task ID
    sql='SELECT * FROM online_sales;',  # SQL query to extract data
    postgres_conn_id='postgres_conn_id',  # Connection ID for Postgres
)
