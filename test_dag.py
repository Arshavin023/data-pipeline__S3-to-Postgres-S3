import sqlite3
import pandas as pd
from pandas import DataFrame
import logging
import traceback
from clickhouse_driver import Client

# 1. Logging Configuration
log_file_path = 'error_log.txt'
logging.basicConfig(filename=log_file_path, level=logging.ERROR, 
                    format='%(asctime)s - %(levelname)s - %(message)s')


def connect_to_tripdata_database():
    # Define your ClickHouse connection parameters
    clickhouse_host = 'github.demo.trial.altinity.cloud'
    clickhouse_user = 'demo'
    clickhouse_password = 'demo'
    clickhouse_database = 'default'
    try:
        client = Client(host=clickhouse_host, 
                        # port=clickhouse_port, 
                        user=clickhouse_user, 
                        password=clickhouse_password, 
                        database=clickhouse_database,
                        secure=True)
        print('successfully connected to clickhouse')
        # Your ClickHouse query or operation
        query = '''with 2014_01_01_to_2016_01_01 as 
                    (select pickup_date, pickup_datetime, dropoff_datetime, fare_amount 
                    from tripdata 
                    where pickup_date between '2014-01-01' and '2016-12-31')

                    select DATE_FORMAT(pickup_date, '%Y-%m') AS year_month,
                    round(avg(case when DAYOFWEEK(pickup_date) = 7 THEN 1 ELSE 0 END),2) sat_mean_trip_count,
                    round(avg(case when DAYOFWEEK(pickup_date) = 7 THEN fare_amount else 0 END),2) sat_mean_fare_per_trip,
                    round(avg(case when DAYOFWEEK(pickup_date) = 7 THEN TIMESTAMPDIFF(MINUTE, pickup_datetime, dropoff_datetime) else 0 END),2) sat_mean_duration_per_trip_in_minutes,
                    round(avg(case when DAYOFWEEK(pickup_date) = 1 THEN 1 ELSE 0 END),2) sun_mean_trip_count,
                    round(avg(case when DAYOFWEEK(pickup_date) = 1 THEN fare_amount else 0 END),2) sun_mean_fare_per_trip,
                    round(avg(case when DAYOFWEEK(pickup_date) = 1 THEN TIMESTAMPDIFF(MINUTE, pickup_datetime, dropoff_datetime) else 0 END),2) sun_mean_duration_per_trip_in_minutes
                    from 2014_01_01_to_2016_01_01
                    group by 1
                    order by DATE_FORMAT(pickup_date, '%Y-%m')'''
        
        result = client.execute(query)
        # print(result)
        # Do something with the result, or perform other ClickHouse operations
        columns = ['year_month','sat_mean_trip_count','sat_mean_fare_per_trip','sat_mean_duration_per_trip_in_minutes',
                   'sun_mean_trip_count','sun_mean_fare_per_trip','sun_mean_duration_per_trip_in_minutes']
        df = pd.DataFrame(result, columns=columns)
        client.disconnect()
        # print(df)
        return df

    except Exception as err:
        logging.info(f"Error: {err}")


def write_metrics_to_sqlite(dataframe: DataFrame, **kwargs):
    try:
        # Connect to the SQLite database
        sqlite_conn = sqlite3.connect('default.db')
        
        # Write metrics to SQLite database
        logging.info("Writing metrics to SQLite database.")
        dataframe.to_sql('tripdata_monthly_metrics', con=sqlite_conn, if_exists='replace', index=False)

        # Close the database connection
        sqlite_conn.close()
        logging.info("SQLite connection closed. Task successfully completed.")

        return 'successfully completed'

    except Exception as e:
        error_message = f"Exception occurred: {str(e)}\n{traceback.format_exc()}"
        logging.error(error_message)
        raise RuntimeError(error_message)

    
dataframe = connect_to_tripdata_database()
loading_data = write_metrics_to_sqlite(dataframe)
