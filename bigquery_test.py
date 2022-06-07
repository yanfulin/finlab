import pandas as pd
from finlab import data
#from finlab.backtest import sim
import finlab
import talib
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import *
import sqlite3
import pymysql
from google.cloud import bigquery as bq
import os
finlab.login('75b0ztI1TfihNQAeO0UyBJVYckRmF9Rr10E3LhE4JlIDY1uh8NLgUJBCXafSgsJf#free')

#fin_data = finlab.data.get('monthly_revenue:當月營收')

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:\\Users\\qw124\\Downloads\\stock-315808-077a218dc2f9.json"
client = bq.Client()
print(client)

# dataset_id = f"{client.project}.NEW_DATA_SET"
# table_id = f"{dataset_id}.TEST_TABLE"
# tschema = [
# bq.SchemaField("full_name", "STRING", mode="NULLABLE"),
# bq.SchemaField("age", "INTEGER", mode="NULLABLE"),
# ]
# dataset = bq.Dataset(dataset_id)
# dataset.location = "asia-east1"
# dataset = client.create_dataset(dataset)
# table = bq.Table(table_id, schema=tschema)
# table = client.create_table(table)  # API request
# print(f"Created dataset {client.project}.{dataset.dataset_id}")
# print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
# 感謝網友陳伯翰修正地區會跳回us的bug

#
# dataset_id = "NEW_DATA_SET"
# dataset_ref = client.dataset(dataset_id)
# table_ref = dataset_ref.table("TEST_TABLE")
# df = pd.DataFrame({u'full_name':['A','B','C','D'],
# u'age':[200, 100, 30, 4]})
# job = client.load_table_from_dataframe(df, table_ref, location="asia-east1")
# job.result()  # Waits for table load to complete.
# assert job.state == "DONE"

finlab_data = {'monthly_revenue:當月營收': 'monthly_revenue:當月營收', 'fundamental_features:營業利益': 'fundamental_features:營業利益'}



for key in finlab_data:
    fin_data = finlab.data.get(key)
    fin_data.columns=fin_data.columns.map(lambda x:"_"+x)
    fin_data=fin_data.reset_index()
    print(fin_data)
    fin_data.to_gbq('datasets.fundamental_features', if_exists='replace')
    #fin_data.to_sql(fin_data.value(), engine, if_exists='replace', index=False)
    print("Write to {} successfully!".format(key))