from datetime import datetime
from airflow import DAG
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import sqlalchemy
from sqlalchemy import create_engine, types
from datetime import timedelta


# 1: global variables:
##################################################
SRC_CONN = 'postgres_default'  # Airflow connection name to Xavier PostgreSQL
##################################################

# 2: json-file:
##################################################
JSON='/app/airflow/files/table2.json'
##################################################

# 3: sql query:
##################################################
TABLE_IN_QUERY="select * from table1"

##################################################


def tuto():
 table2 = pd.read_json(JSON)
 postgres_hook = PostgresHook(postgres_conn_id=SRC_CONN)
 engine = postgres_hook.get_sqlalchemy_engine()
 table1 = pd.read_sql(
             TABLE_IN_QUERY,
             engine)
 table1.columns = [ "FLOAT_VALUE", "ID", "INT_VALUE","STRING_VALUE",  "TABLE2_ID"]
 print(table1.head().to_string())
 
 table2.columns = ['TABLE2_ID', 'ATTRIBUTE1']   

 print(table2.to_string()) 
 
 df=pd.merge(table1, table2,  how='inner', on=['TABLE2_ID'])
 df2 = df[["TABLE2_ID", "FLOAT_VALUE", "INT_VALUE","STRING_VALUE","ATTRIBUTE1"]]
 dataTypeSeries = df2.dtypes
 df2.to_sql('table3', con=engine, if_exists='replace', chunksize=500, index=False)


 return  'OK'

dag = DAG('POSTGRES_TUTORIAL', description='connect_to_postgres', schedule_interval=timedelta(days=1), start_date=datetime(2017, 3, 20), catchup=False)


hello_operator = PythonOperator(task_id='POSTGRES_TUTORIAL', python_callable=tuto, dag=dag)





