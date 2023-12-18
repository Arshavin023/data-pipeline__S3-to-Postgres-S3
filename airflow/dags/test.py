from datetime import datetime
from pathlib import Path
import logging
import os
import boto3
import traceback
from botocore import UNSIGNED
from botocore.client import Config


def download_files_from_S3(bucket_name, s3):
    try:
        local_directory = "/usr/local/airflow/data"
        os.makedirs(local_directory, exist_ok=True)
        current_date = datetime.now().strftime('%Y-%m-%d')
        current_date_folder = 'raw_files'+f'_{current_date}'
        folder = os.path.join(local_directory,current_date_folder)
        if not os.path.exists(folder):
            os.makedirs(folder)
            s3.download_file(bucket_name, "orders_data/orders.csv", Path(os.path.join(folder,"orders.csv")))
            s3.download_file(bucket_name, "orders_data/reviews.csv", Path(os.path.join(folder,"reviews.csv")))
            s3.download_file(bucket_name, "orders_data/shipment_deliveries.csv", Path(os.path.join(folder,"shipment_deliveries.csv")))
        else:  
            s3.download_file(bucket_name, "orders_data/orders.csv", Path(os.path.join(folder,"orders.csv")))
            s3.download_file(bucket_name, "orders_data/reviews.csv", Path(os.path.join(folder,"reviews.csv")))
            s3.download_file(bucket_name, "orders_data/shipment_deliveries.csv", Path(os.path.join(folder,"shipment_deliveries.csv")))
        logging.info('files successfully downloaded')
        
    except Exception as e:
        logging.error(f"Exception occurred: {str(e)}")
        logging.error(f"Timestamp: {datetime.now()}")
        logging.error(f"Traceback: {traceback.format_exc()}")

s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
bucket_name = "d2b-internal-assessment-bucket"

download_files_from_S3(bucket_name, s3)
#raw_files
#sql\transformation.sql
#C:\Users\5300\Desktop\S3_to_Postgres_to_S3\sql\transformation.sql

# sql\transformation.sql

