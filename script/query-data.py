import sqlalchemy
import json
import pandas as pd
from dotenv import load_dotenv
import os
import requests

load_dotenv()
host = os.getenv('host')
user = os.getenv('user')
password = os.getenv('password')
database = os.getenv('database')
schema = os.getenv('schema')
port = os.getenv('port')

data = pd.read_json('top_10_customers.json', orient='records')

customer_ids= tuple(data['customerID'].to_list())


sql=f"""
    select CustomerID, CustomerName from customers
        where CustomerID in {customer_ids} 
"""

def mysql_connect(host, user, password, database, port,schema):
    engine = sqlalchemy.create_engine(f'mysql+mysqlconnector://{user}:{password}@{database}.{host}:{port}/{schema}')
    return engine

engine = mysql_connect(host, user, password, database, port,schema)

