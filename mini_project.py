import boto3
from io import StringIO, BytesIO
import pandas as pd
from prefect import task, flow
import datetime
import psycopg2
import os
from sqlalchemy import create_engine

# # # fill your aws account name here
account_name = 'tammawat34' 


# read info from environment variables


# # before running this script, dont forget to assign environment variables using the `export` command in terminal
postgres_info = {
    'username': os.environ['postgres_username'],
    'password': os.environ['postgres_password'],
    'host': os.environ['postgres_host'],
    'port': os.environ['postgres_port'],
}

# Extract data from source
s3_client = boto3.client('s3')
bucket_name = 'fullstackdata2023'
source_path = 'common/data/partitioned/2023/11/30/transaction.csv'

@task(retries=3)
def extract(bucket_name, source_path):
    '''
    Extract data from S3 bucket
    '''
    # Get the object from S3
    response = s3_client.get_object(Bucket=bucket_name, Key=source_path)
    # Read the CSV file content
    csv_string = StringIO(response['Body'].read().decode('utf-8'))
    # Use Pandas to read the CSV file
    df = pd.read_csv(csv_string)
    print(f"*** Extract df with {df.shape[0]} rows")
    return df

@task
def extract_customer_postgres(table_name):
    '''
    Extract data from Postgres database
    '''
    database = postgres_info['username']  # each user has their own database in their username
    database_url = f"postgresql+psycopg2://{postgres_info['username']}:{postgres_info['password']}@{postgres_info['host']}:{postgres_info['port']}/{database}"
    engine = create_engine(database_url)
    query = f"SELECT * FROM {table_name}"
    customer_old = pd.read_sql(query, engine)
    print(f"Read table: {table_name} from Postgres yielding {customer_old.shape[0]} records")
    return customer_old


@task
def transform(df):
    '''
    Transform data
    '''
    # Transform transaction data into customer data
    customer_cols = ['customer_id', 'customer_name', 'customer_province','date']
    customer = df[customer_cols].drop_duplicates()
    customer['recency']=customer['date']
    print(f"*** Transform df to customer with {customer.shape[0]} rows")
    return customer

@task
def load_parquet(customer):
    '''
    Load transformed result as parquet file to S3 bucket
    '''
    # Load data into S3 bucket
    target_path = f'{account_name}/customer/customer.parquet'
    parquet_buffer = BytesIO()
    customer.to_parquet(parquet_buffer, index=False)
    print(f"Uploading to bucket {bucket_name}, at path {target_path}")
    s3_client.put_object(Bucket=bucket_name, Key=target_path, Body=parquet_buffer.getvalue())
    print("Upload successfully!")

@task 
def load_csv(customer):
    '''
    Load transformed result as csv file to S3 bucket
    '''
    # Load data into S3 bucket
    target_path = f'{account_name}/customer/customer.csv'
    csv_buffer = StringIO()
    customer.to_csv(csv_buffer, index=False)
    print(f"Uploading to bucket {bucket_name}, at path {target_path}")
    s3_client.put_object(Bucket=bucket_name, Key=target_path, Body=csv_buffer.getvalue())
    print("Upload successfully!")

@task 
def load_postgres(customer):
    '''
    Load transformed result to Postgres
    '''
    database = postgres_info['username'] # each user has their own database in their username
    table_name = 'customer'
    database_url = f"postgresql+psycopg2://{postgres_info['username']}:{postgres_info['password']}@{postgres_info['host']}:{postgres_info['port']}/{database}"
    engine = create_engine(database_url)
    print(f"Writing to database {postgres_info['username']}.{table_name} with {customer.shape[0]} records")
    customer.to_sql(table_name, engine, if_exists='replace', index=False)
    print("Write successfully!")
    
@flow(log_prints=True)
def pipeline():
    '''
    Flow for ETL pipeline
    '''
    df = extract(bucket_name, source_path)
    customer_old = extract_customer_postgres(table_name='customer')
    customer = transform(df)
    load_postgres(customer)
    
if __name__ == '__main__':
    # pipeline()
    pipeline.serve(name="my_pipeline")
