from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
import psycopg2

class TransformOperator(BaseOperator):

    @apply_defaults
    def __init__(self, postgres_conn_id, sql_query, csv_file_path, *args, **kwargs):
        super(TransformOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql_query = sql_query
        self.csv_file_path = csv_file_path

    def execute(self, context):
        # Fetch and cleanse data from PostgreSQL
        postgres_data = self.fetch_postgres_data()

        # Fetch and cleanse data from CSV
        csv_data = self.fetch_csv_data()

        # Concatenate data from PostgreSQL and CSV
        combined_data = pd.concat([postgres_data, csv_data], ignore_index=True)

        # Group by product_id and sum quantity and sale_amount
        combined_data_grouped = combined_data.groupby('product_id').agg({'quantity': 'sum', 'sale_amount': 'sum'}).reset_index()

        return combined_data_grouped

    def fetch_postgres_data(self):
        conn = psycopg2.connect(
            host='localhost',  # Update with your PostgreSQL host
            port='5432',  # Update with your PostgreSQL port
            database='mock_db',
            user='postgres',
            password='postgres'
        )

        df_sql = pd.read_sql_query(self.sql_query, conn)

        # Delete rows where product_id is null
        df_sql = df_sql.dropna(subset=['product_id'])

        price_list = {
            101: 10.25,
            102: 12.75,
            103: 8,
            104: 7.5,
            105: 9.5
        }

        # Update sale_amount if null
        df_sql['sale_amount'] = df_sql.apply(
            lambda row: row['quantity'] * price_list[row['product_id']] if pd.isnull(row['sale_amount']) else row['sale_amount'],
            axis=1
        )

        # Update quantity if null
        for product_id in price_list.keys():
            df_sql.loc[df_sql['product_id'] == product_id, 'quantity'] = df_sql.loc[df_sql['product_id'] == product_id, 'quantity'].fillna(round(df_sql.loc[df_sql['product_id'] == product_id, 'sale_amount'] / price_list[product_id]))

        # Close connection
        conn.close()

        return df_sql

    def fetch_csv_data(self):
        df_csv = pd.read_csv(self.csv_file_path)

        # Delete rows where product_id is null
        df_csv = df_csv.dropna(subset=['product_id'])

        price_list = {
            101: 10.25,
            102: 12.75,
            103: 8,
            104: 7.5,
            105: 9.5
        }

        # Update sale_amount if null
        df_csv['sale_amount'] = df_csv.apply(
            lambda row: row['quantity'] * price_list[row['product_id']] if pd.isnull(row['sale_amount']) else row['sale_amount'],
            axis=1
        )

        # Update quantity if null
        for product_id in price_list.keys():
            df_csv.loc[df_csv['product_id'] == product_id, 'quantity'] = df_csv.loc[df_csv['product_id'] == product_id, 'quantity'].fillna(round(df_csv.loc[df_csv['product_id'] == product_id, 'sale_amount'] / price_list[product_id]))

        return df_csv
