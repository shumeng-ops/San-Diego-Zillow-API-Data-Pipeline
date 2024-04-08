from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import json
from datetime import datetime, timedelta
import pandas as pd
import boto3
import sys
sys.path.append("utils")
from extract_zillow_data import extract_zillow_sale_data,extract_zillow_sold_data,extract_zillow_rent_data
from data_preparation import combine_data, remove_duplicate,upload_to_s3,zillow_cleanup
os.environ['NO_PROXY'] = '*'

s3_client = boto3.client('s3')
target_raw_bucket_name='zillow-raw-data-bucket'
target_transform_bucket_name='zillow-transform-data-bucket'
## Load JSON config file
with open('/Users/shumengshi/Career/2024-data-engineering-zoomcamp/Zillow_Project/airflow/zillow_api.json','r') as config_file:
        api_host_key=json.load(config_file)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': 'shumeng.shi9@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=15),
    'start_date': datetime(2024, 1, 1)
}



with DAG('zillow_API_transform_S3',
          default_args=default_args,
          #schedule_interval='@daily',
          catchup=False
          ) as dag:

        zillow_sale_data = PythonOperator(
                task_id='zillow_sale_data',
                python_callable=extract_zillow_sale_data,

                op_kwargs={'url':'https://zillow-com4.p.rapidapi.com/properties/search',\
                        'headers':api_host_key,\
                        'querystring':{"location":"San Diego","resultsPerPage":"1000","page":"1","status":"forSale"}}
                        )
        
        zillow_sold_data = PythonOperator(
                task_id='zillow_sold_data',
                python_callable=extract_zillow_sold_data,
                op_kwargs={'url':'https://zillow-com4.p.rapidapi.com/properties/search',\
                        'headers':api_host_key,\
                        'querystring':{"location":"San Diego","page":"1","resultsPerPage": "1000","status": "sold"}}
                        )
        
        zillow_rent_data = PythonOperator(
                task_id='zillow_rent_data',
                python_callable=extract_zillow_rent_data,
                op_kwargs={'url':'https://zillow-com4.p.rapidapi.com/properties/search',\
                        'headers':api_host_key,\
                        'querystring':{"location":"San Diego","page":"1","resultsPerPage": "1000","status": "forRent"}}
                        )
        
        combine_zillow = PythonOperator(
                task_id='combine_zillow',
                python_callable=combine_data,
                op_kwargs={'directory':'/Users/shumengshi/Career/2024-data-engineering-zoomcamp/Zillow_Project/data/',\
                        'combined_file':'/Users/shumengshi/Career/2024-data-engineering-zoomcamp/Zillow_Project/data/combined_zillow.csv'}
                        )

        remove_duplicate = PythonOperator(
                task_id='remove_duplicate',
                python_callable=remove_duplicate,
                op_kwargs={'raw_file':'/Users/shumengshi/Career/2024-data-engineering-zoomcamp/Zillow_Project/data/combined_zillow.csv',
                           'dedupe_file':'/Users/shumengshi/Career/2024-data-engineering-zoomcamp/Zillow_Project/data/deduped_zillow.csv'}
                        )
                        
        upload_raw_to_s3 = PythonOperator(
                task_id='upload_to_s3',
                python_callable=upload_to_s3,
                op_kwargs={'s3_client':s3_client,
                           'file_directory':'/Users/shumengshi/Career/2024-data-engineering-zoomcamp/Zillow_Project/data/deduped_zillow.csv',
                           'bucket_name':target_raw_bucket_name,
                           'key':'zillow_deduped_data.csv'}
                        )
        
        data_transform = PythonOperator(
                task_id='data_transform',
                python_callable=zillow_cleanup,
                op_kwargs={'file_path':'/Users/shumengshi/Career/2024-data-engineering-zoomcamp/Zillow_Project/data/deduped_zillow.csv',
                           'output_path':'/Users/shumengshi/Career/2024-data-engineering-zoomcamp/Zillow_Project/data/transform_data.csv'}
                        )
        

        upload_transform_to_s3 = PythonOperator(
                task_id='upload_transform_to_s3',
                python_callable=upload_to_s3,
                op_kwargs={'s3_client':s3_client,
                           'file_directory':'/Users/shumengshi/Career/2024-data-engineering-zoomcamp/Zillow_Project/data/transform_data.csv',
                           'bucket_name':target_transform_bucket_name,
                           'key':'zillow_transform_data.csv'}
                        )

        zillow_sale_data >> zillow_sold_data >> zillow_rent_data >> combine_zillow >> remove_duplicate >> upload_raw_to_s3 >> data_transform >> upload_transform_to_s3



