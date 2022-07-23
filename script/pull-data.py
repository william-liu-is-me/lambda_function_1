import pandas as pd
from dotenv import load_dotenv
import os
import sqlalchemy as db
import boto3
import requests
import os
import datetime

load_dotenv()

def mysql_connect(host, user, password, database, port,schema):
    engine = db.create_engine(f'mysql+mysqlconnector://{user}:{password}@{database}.{host}:{port}/{schema}')
    return engine


host = os.getenv('host')
user = os.getenv('user')
password = os.getenv('password')
port = os.getenv('port')
database = os.getenv('database')
schema = os.getenv('schema')
bucket = os.getenv('bucket')
url = os.getenv('url')

engine = mysql_connect(host, user, password, database, port, schema)


sql="""
    select customerID, sum(sales) sum_sales
    from orders
    group by customerID
    order by sum_sales desc
    limit 10;
    """

df = pd.read_sql(sql, con = engine)


df.to_json('test.json', orient='records')

s3 = boto3.resource('s3')
data=open('test.json', 'rb')
ct = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
s3.Bucket(bucket).put_object(Key =f'input/{ct}.json',Body= data)

# no need to store a copy locally
os.remove('test.json')
#below is lambda function on aws

# customer_ids= tuple(df['customerID'].to_list())

# sql_2=f"""
#     select CustomerID, CustomerName from customers
#         where CustomerID in {customer_ids}
# """
# df = pd.read_sql(sql_2, con = engine)

