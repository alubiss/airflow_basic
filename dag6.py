#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#!/usr/bin/env python
# coding: utf-8

from airflow.models import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd

default_args = {
  'owner': 'alubis',
  'start_date': datetime(2021,10,3),
  'schedule_interval' : '@daily',
  'retries': 0,
  'catchup': True
}
 
table = DAG('final_table', default_args=default_args)

date = "{{ ds }}"

def create_new_tab(date):
    d=[date]
    dane = pd.read_csv("/Users/alubis/Desktop/out.csv")
    new_row = {'date':date, 'name':'Ola'}
    dane = dane.append(new_row, ignore_index=True)
    dane.to_csv('/Users/alubis/Desktop/out.csv', index=False)

new_line_in_tab= PythonOperator(
    task_id='new_line_in_tab',
    python_callable=create_new_tab,
    op_args=[date],
    dag=table
)

check_if_exist = BashOperator(
    task_id='check_if_exist',
    bash_command= "bash /Users/alubis/Desktop/script.sh " ,
    dag=table )

# ON CLOUD
# BigQueryTableExistenceSensor (
# dag = table,
# task_id= 'check',
# project_id= "...",
# dataset_id= "...",
# table_id =  f"...",
# timeout = 60 )
# bigquery.Client() to log new values and PythonOperator

check_if_exist >> new_line_in_tab

