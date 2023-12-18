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
import numpy as np
from sqlalchemy import create_engine
from airflow.utils.dates import days_ago

import logging
import traceback

# 1. Logging Configuration
log_file_path = 'error_log.txt'
logging.basicConfig(filename=log_file_path, level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')


# 2. DAG Definition
@dag(
    'scheduled_elt_pipeline',
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
                for file_name in ["orders.csv", "reviews.csv", "shipment_deliveries.csv"]:
                    s3.download_file(bucket_name, f"orders_data/{file_name}", os.path.join(folder, file_name))
            else:
                for file_name in ["orders.csv", "reviews.csv", "shipment_deliveries.csv"]:
                    s3.download_file(bucket_name, f"orders_data/{file_name}", os.path.join(folder, file_name))
                    
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
    def connect_to_postgres_and_load_data(dataframe_dict, conn_id):
        try:
            for table_name, df in dataframe_dict.items():
                if table_name != 'reviews':
                    # Convert the DataFrame to a list of tuples for bulk insert
                    # data_tuples = [tuple(row) for row in df.to_numpy()]
                    data_tuples = [tuple(map(lambda x: int(x) if isinstance(x, np.int64) else x, row)) for row in df.to_numpy()]

                    # Use PostgresHook to execute the insert query
                    hook = PostgresHook(postgres_conn_id=conn_id)
                    primary_key_column = list(df.columns)[0]
                    # Check for existing records before insert
                    existing_records = hook.get_records(f'''SELECT case when MAX(CAST({primary_key_column} AS INT)) 
                                                        is NULL then 0 else MAX(CAST({primary_key_column} AS INT)) 
                                                        end as max_num
                                                        FROM staging.{table_name}''')
                    # existing_ids = set(record[0] for record in existing_records)
                    max_id = existing_records[0][0]
                    deduplicated_data_tuples = [row for row in data_tuples if int(row[0]) > max_id]
                    if deduplicated_data_tuples is not None:
                        hook.insert_rows(table=f'staging.{table_name}', rows=deduplicated_data_tuples)
                        logging.info(f'Data successfully inserted into staging.{table_name} on PostgreSQL')
                    else:
                        pass
                        logging.info(f'No new data to insert into staging.{table_name}')

                else:
                    data_tuples = [tuple(map(lambda x: int(x) if isinstance(x, np.int64) else x, row)) for row in df.to_numpy()]
                    hook = PostgresHook(postgres_conn_id=conn_id)
                    hook.insert_rows(table=f'staging.{table_name}', rows=data_tuples)
                    logging.info(f'Data successfully inserted into staging.{table_name} on PostgreSQL')

            return "all csv files successfully loaded into staging in Postgres"

        except Exception as e:
            raise RuntimeError(f"Exception occurred: {str(e)}\n{traceback.format_exc()}")

    @task(task_id="transformation_in_postgres")
    def perform_transformation(engine) -> str:
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
    postgres_conn_id = 'postgres_conn_id'  # Specify the Airflow connection ID for PostgreSQL
    download_task = download_files_from_s3(bucket_name)
    dataframe_dict = read_csv_files_into_dictionary(download_task)
    loading_data = connect_to_postgres_and_load_data(dataframe_dict, postgres_conn_id)
    # transformation = perform_transformation(engine)
    # download_and_upload_transformed_data_S3(engine, bucket_name, s3)

    # Set dependencies
    download_task >> dataframe_dict >> loading_data
    #>> load_dataframes_into_postgres >> perform_transformation_in_postgres >> download_and_upload_transformed_data_S3
     
daily_extraction_dag()



        
