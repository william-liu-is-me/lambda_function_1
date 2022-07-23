import boto3
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
import os
import datetime
import requests

def mysql_connect(host, user, password, database, port,schema):
    engine = sqlalchemy.create_engine(f'mysql+mysqlconnector://{user}:{password}@{database}.{host}:{port}/{schema}')
    return engine


def lambda_handler(event, context):

    load_dotenv()

    host = os.getenv('host')
    user = os.getenv('user')
    password = os.getenv('password')
    database = os.getenv('database')
    schema = os.getenv('schema')
    port = os.getenv('port')
    url= os.getenv('url')
    
    s3=boto3.resource('s3')
    
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_name = event['Records'][0]['s3']['object']['key']

    s3.Bucket(bucket_name).download_file(object_name, '/tmp/test.json')
   
    #s3.Bucket('lambda-landing-william').put_object(Key =f'{ct}.json',Body= open('/tmp/test.json', 'rb'))

    df=pd.read_json('/tmp/test.json')
    #df.to_csv('/tmp/test.csv', index=False)
    customer_ids= tuple(df['customerID'].to_list())
    #s3.Bucket('lambda-landing-william').put_object(Key =f'{customer_ids[1]}.csv',Body= open('/tmp/test.csv', 'rb'))

    engine=mysql_connect(host, user, password, database, port, schema)
   
    sql_2=f"""
    select CustomerID, CustomerName from customers
        where CustomerID in {customer_ids} 
    """
    
    df_2 = pd.read_sql(sql_2, con = engine)
    
    df_2['Date'] = pd.to_datetime('today').date()

    r = requests.post(url, json=df_2.to_json(orient='records'))
    #print(r.status_code,r.json())
    df_2.to_csv('/tmp/sql2.csv', index=False)
    timestamp = pd.to_datetime('today').strftime("%Y-%m-%d-%H-%M-%S")
    s3.Bucket('lambda-landing-william').put_object(Key =f'sql/{timestamp}.csv',Body= open('/tmp/sql2.csv', 'rb'))

    print(r.status_code)
    return r.status_code
    
