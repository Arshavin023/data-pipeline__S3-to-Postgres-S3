from datetime import datetime, timedelta
import os
from pathlib import Path
from io import StringIO
import pandas as pd
from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from botocore.client import Config
from botocore import UNSIGNED
import boto3
from sqlalchemy import create_engine
from airflow.utils.dates import days_ago

import logging
import traceback

# 1. Logging Configuration
log_file_path = 'error_log.txt'
logging.basicConfig(filename=log_file_path, level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')


# 2. DAG Definition
@dag(
    'scheduled_data_pipeline',
    default_args={
        'owner': 'uchejudennodim@gmail.com',
        'depends_on_past': False,
        'start_date': days_ago(0),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Download orders, reviews and shipment data from S3 to file directory, load into Postgres, transform and upload to S3',
    schedule_interval='0 1,23 * * *',
    concurrency=1,
)
def daily_extraction_dag():

    # 3. Task Function Signature
    @task(task_id="download_files_task")
    def download_files_from_s3(bucket_name: str, **kwargs) -> str:
        try:
            s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
            local_directory = "/app/data"
            os.makedirs(local_directory, exist_ok=True)
            current_date = datetime.now().strftime('%Y-%m-%d')
            current_date_folder = f'raw_files_{current_date}'
            folder = os.path.join(local_directory, current_date_folder)
            if not os.path.exists(folder):
                os.makedirs(folder)
                s3.download_file(bucket_name, "orders_data/orders.csv", os.path.join(folder, "orders.csv"))
                s3.download_file(bucket_name, "orders_data/reviews.csv", os.path.join(folder, "reviews.csv"))
                s3.download_file(bucket_name, "orders_data/shipment_deliveries.csv",
                                 os.path.join(folder, "shipment_deliveries.csv"))
            else:
                s3.download_file(bucket_name, "orders_data/orders.csv", os.path.join(folder, "orders.csv"))
                s3.download_file(bucket_name, "orders_data/reviews.csv", os.path.join(folder, "reviews.csv"))
                s3.download_file(bucket_name, "orders_data/shipment_deliveries.csv",
                                 os.path.join(folder, "shipment_deliveries.csv"))
            logging.info('Files successfully downloaded')
            return folder

        except Exception as e:
            raise RuntimeError(f"Exception occurred: {str(e)}\n{traceback.format_exc()}")

    @task(task_id="read_csv_task")
    def read_csv_files_into_dictionary(raw_folder_path: str, **kwargs) -> dict:
        try:
            files = os.listdir(raw_folder_path)
            csv_files = [file for file in files if file.endswith('.csv')]
            dataframes_dict = {}
            for csv_file in csv_files:
                file_path = os.path.join(raw_folder_path, csv_file)
                df = pd.read_csv(file_path, index_col=None)
                key = csv_file.split('.')[0]
                dataframes_dict[key] = df
            logging.info('Successfully read CSV files from filepath and saved into dictionary')
            return dataframes_dict

        except Exception as e:
            raise RuntimeError(f"Exception occurred: {str(e)}\n{traceback.format_exc()}")

    @task(task_id="connect_to_postgres")
    def connect_to_postgres_and_get_engine(username: str, password: str,
                                       host: str, database: str):
        try:
            conn_str = f'postgresql://{username}:{password}@{host}:5432/{database}'
            engine = create_engine(conn_str)
            return engine
        
        except Exception as e:
            raise RuntimeError(f"Exception occurred: {str(e)}\n{traceback.format_exc()}")

    @task(task_id="load_files_into_postgres")
    def connect_to_postgres_and_load_data(engine, dataframe_dict: dict) -> str:
        try:
            for name, df in dataframe_dict.items():
                table_name = name
                df.to_sql(f'staging.{table_name}', engine, index=False, if_exists='append')
                logging.info(f'{name} file successfully loaded into PostgreSQL table {name}')
            return 'All files loaded successfully into PostgreSQL'

        except Exception as e:
            raise RuntimeError(f"Exception occurred: {str(e)}\n{traceback.format_exc()}")

    @task(task_id="transformation_in_postgres")
    def perform_transformation_in_postgres(engine) -> str:
        sql_file_path = Path("/app/sql/transformation.sql")
        
        try:
            with open(sql_file_path, 'r') as file:
                sql_query = file.read()
            
            with engine.connect() as conn:
                conn.execute(sql_query)
            logging.info('Successfully performed transformation in PostgreSQL')
            return 'Transformation successfully completed'

        except Exception as e:
            raise RuntimeError(f"Exception occurred: {str(e)}\n{traceback.format_exc()}")

    @task(task_id="postgres_to_S3")
    def download_and_upload_transformed_data_s3(engine: str, bucket_name: str) -> str:
        s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        query_list = ['SELECT * FROM analytics.agg_public_holiday',
                      'SELECT * FROM analytics.agg_shipments',
                      'SELECT * FROM analytics.best_performing_product']
        try:
            with engine.connect() as conn:
                for query in query_list:
                    result = conn.execute(query)
                    column_names = result.keys()
                    transformed_data = result.fetchall()
                    df = pd.DataFrame(transformed_data, columns=column_names)
                    csv_buffer = StringIO()
                    df.to_csv(csv_buffer, index=False)
                    path_in_s3 = '/analytics_export/'
                    table_name = query.split('.')[1]
                    key = f"{path_in_s3}{table_name}.csv"
                    s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=key)

            logging.info('Successfully uploaded transformed files into S3')
            return 'Successfully uploaded transformed data into S3'

        except Exception as e:
            raise RuntimeError(f"Exception occurred: {str(e)}\n{traceback.format_exc()}")
    
    # Postgres database credentials
    current_date = datetime.now().strftime('%Y-%m-%d')
    current_date_folder = Path('raw_files'+f'_{current_date}')
    bucket_name = Variable.get('BUCKET_NAME')
    db_user = Variable.get('POSTGRES_USER')
    db_password = Variable.get('POSTGRES_PASSWORD')
    db_host = Variable.get('POSTGRES_HOST') 
    db_name = Variable.get('POSTGRES_DBNAME')
    download_task = download_files_from_s3(bucket_name)
    read_csv_task = read_csv_files_into_dictionary(download_task)
    # engine = connect_to_postgres_and_load_data(db_user,db_password,db_host,db_name,read_csv_task.output)
    #load_dataframes_into_postgres(result, engine)
    #perform_transformation_in_postgres(engine)
    # download_and_upload_transformed_data_S3(engine, bucket_name, s3)

    # Set dependencies
    download_task >> read_csv_task
    #>> load_dataframes_into_postgres >> perform_transformation_in_postgres >> download_and_upload_transformed_data_S3
     
daily_extraction_dag()



        
