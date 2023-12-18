from datetime import datetime, timedelta
import pandas as pd
from xecd_rates_client import XecdClient
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import logging
import traceback
import boto3
import os
from botocore import UNSIGNED
from botocore.client import Config
from pathlib import Path
from io import StringIO
from sqlalchemy import create_engine


log_file_path = 'error_log.txt'
logging.basicConfig(filename=log_file_path, level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

logging.info("Workflow started logged in successfully.")

@dag(
    'S3_to_Postgres_S3',
    default_args={
        'owner': 'uchejudennodim@gmail.com',
        'depends_on_past': False,
        'start_date': datetime(2023, 12, 19),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Download orders, reviews and shipment data from S3 to file directory, load into Postgres, transform and upload to S3',
    schedule_interval='0 1,23 * * *',
)

def daily_extraction_dag():

    @task(task_id="download_files_task")
    def download_files_from_S3(bucket_name,**kwargs):
        try:
            s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
            local_directory = "/opt/airflow/data"
            os.makedirs(local_directory, exist_ok=True)
            current_date = datetime.now().strftime('%Y-%m-%d')
            current_date_folder = 'raw_files'+f'_{current_date}'
            folder = os.path.join(local_directory,current_date_folder)
            if not os.path.exists(folder):
                os.makedirs(folder)
                s3.download_file(bucket_name, "orders_data/orders.csv", os.path.join(folder,"orders.csv"))
                s3.download_file(bucket_name, "orders_data/reviews.csv", os.path.join(folder,"reviews.csv"))
                s3.download_file(bucket_name, "orders_data/shipment_deliveries.csv", Path(os.path.join(folder,"shipment_deliveries.csv")))
            else:  
                s3.download_file(bucket_name, "orders_data/orders.csv", os.path.join(folder,"orders.csv"))
                s3.download_file(bucket_name, "orders_data/reviews.csv", os.path.join(folder,"reviews.csv"))
                s3.download_file(bucket_name, "orders_data/shipment_deliveries.csv", os.path.join(folder,"shipment_deliveries.csv"))
            logging.info('files successfully downloaded')

            return folder
            
        except Exception as e:
            logging.error(f"Exception occurred: {str(e)}")
            logging.error(f"Timestamp: {datetime.now()}")
            logging.error(f"Traceback: {traceback.format_exc()}")
    

           # Read CSV files into a dictionary
    @task(task_id="read_csv_task")
    def read_csv_files_into_dictionary(raw_folder_path, **kwargs):   
        try: 
            # Get a list of all files in the folder
            files = os.listdir(raw_folder_path)
            # Filter the list to only include CSV files
            csv_files = [file for file in files if file.endswith('.csv')]
            # Initialize an empty list to store DataFrames
            dataframes_dict = {}
            # Read each CSV file into a DataFrame and add it to the list
            for csv_file in csv_files:
                file_path = os.path.join(raw_folder_path, csv_file)
                df = pd.read_csv(file_path, index_col=None)
                key = csv_file.split('.')[0]
                dataframes_dict[key] = df
            logging.info('successfully read in csv files from filepath and saved into dictionary')
            
            return dataframes_dict
            
        except Exception as e:
            logging.error(f"Exception occurred: {str(e)}")
            logging.error(f"Timestamp: {datetime.now()}")
            logging.error(f"Traceback: {traceback.format_exc()}")
        
        
    

    @task(task_id="connect_to_postgres")
    def connect_to_postgres_with_alchemy(username: str, password: str, host: str, database: str):
        try:
            conn_str = f'postgresql://{username}:{password}@{host}:5432/{database}'
            engine = create_engine(conn_str)
            logging.info('successfully connected to postgres')
            return engine            

        except Exception as e:
            logging.error(f"Exception occurred: {str(e)}")
            logging.error(f"Timestamp: {datetime.now()}")
            logging.error(f"Traceback: {traceback.format_exc()}")


    # Step 4: Load dataframes into different tables in Postgres database
    @task(task_id="load_data_into_postgres")
    def load_dataframes_into_postgres(dataframe_dict, engine):
        try:
            for name, df in dataframe_dict.items():
                table_name = name
                # print(table_name)
                df.to_sql(f'staging.{table_name}', engine, index=False, if_exists='append')
            logging.info('file successfully loaded into postgres')
            return 'files loaded'
            
            
        except Exception as e:
            logging.error(f"Exception occurred: {str(e)}")
            logging.error(f"Timestamp: {datetime.now()}")
            logging.error(f"Traceback: {traceback.format_exc()}")

    # Step 5: Perform transformation inside Postgres (using SQL queries)
    @task(task_id="transformation_in_postgres")
    def perform_transformation_in_postgres(engine):
        sql_file_path = Path("sql/transformation.sql")
        with open(sql_file_path, 'r') as file:
            sql_query = file.read()
        try:
            with engine.connect() as conn:
                # Add your SQL transformation queries here
                conn.execute(sql_query)
            logging.info('successfully performed transformation in postgres')
            return 'transformation successfully completed'
        
        except Exception as e:
            logging.error(f"Exception occurred: {str(e)}")
            logging.error(f"Timestamp: {datetime.now()}")
            logging.error(f"Traceback: {traceback.format_exc()}")
        
    # Step 6: Download transformed data from Postgres
    @task(task_id="postgres_to_S3")     
    def download_and_upload_transformed_data_S3(engine, bucket_name, s3):
        agg_public_holiday = 'SELECT * FROM analytics.agg_public_holiday'
        agg_shipments = 'SELECT * FROM analytics.agg_shipments'
        best_performing_product = 'SELECT * FROM analytics.best_performing_product'
        query_list = [agg_public_holiday, agg_shipments, best_performing_product]
        # Adjust the query as needed
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
                    s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key = key)

            logging.info('successfully uploaded transformed files into S3')
            return 'successfully uploaded transformed data into S3'

        except Exception as e:
            logging.error(f"Exception occurred: {str(e)}")
            logging.error(f"Timestamp: {datetime.now()}")
            logging.error(f"Traceback: {traceback.format_exc()}")

    
    # Postgres database credentials
    current_date = datetime.now().strftime('%Y-%m-%d')
    current_date_folder = Path('raw_files'+f'_{current_date}')
    local_directory = "/opt/airflow/data"
    bucket_name = Variable.get('BUCKET_NAME')
    db_user = Variable.get('POSTGRES_USER')
    db_password = Variable.get('POSTGRES_PASSWORD')
    db_host = Variable.get('POSTGRES_HOST') 
    db_name = Variable.get('POSTGRES_DBNAME')
    raw_folder_path = os.path.join(local_directory, current_date_folder)
    download_files_from_S3(bucket_name)
    result = read_csv_files_into_dictionary(raw_folder_path)
    engine = connect_to_postgres_with_alchemy(db_user,db_password,db_host,db_name)
    #load_dataframes_into_postgres(result, engine)
    #perform_transformation_in_postgres(engine)
    # download_and_upload_transformed_data_S3(engine, bucket_name, s3)

    # Set dependencies
    download_files_from_S3 >> read_csv_files_into_dictionary >> connect_to_postgres_with_alchemy >> load_dataframes_into_postgres >> perform_transformation_in_postgres >> download_and_upload_transformed_data_S3
     
daily_extraction_dag_instance = daily_extraction_dag()



        
