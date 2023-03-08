from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import json

# Define the DAG parameters
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1
}


# Define the functions to:
# Extract: read the JSON file from S3, 
# Transform: convert it to CSV,
# Load: write it back to S3

def extract_data(s3_bucket_name, s3_key_name, aws_conn_id):
    # Instantiate the S3Hook with AWS credentials
    print("Connecting to S3")
    aws_s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    print("Airflow S3Hook connected to S3")

    # Read and return the JSON file from S3 bucket
    input_json = aws_s3_hook.read_key(s3_key_name, s3_bucket_name)
    return input_json

def transform_data(input_json):
    # Convert the JSON data to CSV format
    data = json.loads(input_json)
    csv_string = 'user_id, x_coordinate, y_coordinate, date\n'
    for row in data:
        csv_string += f'{row["user_id"]}, {row["x_coordinate"]}, {row["y_coordinate"]}, {row["date"]}\n'
    
    # Return data in CSV format
    return csv_string

def load_data(aws_conn_id, s3_bucket_name, csv_string):
    aws_s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    print("Writing CSV to S3")
    
    # Write the CSV data back to S3
    aws_s3_hook.load_string(
        csv_string,
        key= "victor_medrano.csv",
        bucket_name=s3_bucket_name,
        replace=True
    )
    print("CSV was written to S3")

# Define the DAG
with DAG('aws_dag', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    # Define the task that calls the json_to_csv function
    extract_data_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        op_kwargs={
            's3_bucket_name': 's3-enroute-public-bucket',
            's3_key_name': 'input.json',
            'aws_conn_id': 'aws_conn'
        }
    )

    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        op_kwargs={
            'input_json': "{{ task_instance.xcom_pull(task_ids='extract_data', key='return_value') }}"
        }
    )

    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        op_kwargs={
            'aws_conn_id': 'aws_conn', 
            's3_bucket_name': 's3-enroute-public-bucket',
            'csv_string': "{{ task_instance.xcom_pull(task_ids='transform_data', key='return_value') }}"
        }
    )

# Set the task dependencies
extract_data_task >> transform_data_task >> load_data_task