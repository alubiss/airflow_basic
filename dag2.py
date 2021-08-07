from airflow.models import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import pandas as pd

default_args = {
  'owner': 'alubis',
  'start_date': datetime(2021,8,3),
  'schedule_interval' : '@daily',
  'retries': 0,
  'catchup': True
}
 
table = DAG('table', default_args=default_args)

date = "{{ ds }}"

def create_new_tab(date):
    d=[date]
    #df=pd.DataFrame({'date':d, 'name':'Ola'})
    #df.to_csv('/Users/alubis/Desktop/out.csv', index=False)
    dane = pd.read_csv("/Users/alubis/Desktop/out.csv")
    new_row = {'date':date, 'name':'Ola'}
    dane = dane.append(new_row, ignore_index=True)
    dane.to_csv('/Users/alubis/Desktop/out.csv', index=False)

new_tab= PythonOperator(
    task_id='new_tab',
    python_callable=create_new_tab,
    op_args=[date],
    dag=table
)

new_tab