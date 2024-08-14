from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from app.Extract_data import extract_data
from app.Transform_data import transform_data
from app.Load_data import load_data
import os

default_args = {
    'owner': 'ajied',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'catchup': False,
    'email': ['ajiedsageda48@gmail.com'],
    'email_on_failure': True,
}

dag = DAG(
    'Google_Analytics_Data_ETL',
    default_args=default_args,
    description='Google Analytics Data ETL',
    schedule_interval='@daily',
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

def extract():
    extract_data()

def transform():
    transform_data()

def load():
    load_data()

with dag:
    with TaskGroup(group_id='extract') as extract_group:
        extract_task = PythonOperator(
            task_id='get-link-data',
            python_callable=extract,
            dag=dag,
        )

    with TaskGroup(group_id='transform') as transform_group:
        transform_task = PythonOperator(
            task_id='transform-processing',
            python_callable=transform,
            dag=dag,
        )

    with TaskGroup(group_id='load') as load_group:
        load_table_app_infos = PythonOperator(
            task_id='table_app_infos',
            python_callable=load,
            dag=dag,
        )

        load_table_devices = PythonOperator(
            task_id='table_devices',
            python_callable=load,
            dag=dag,
        )

        load_table_events = PythonOperator(
            task_id='table_events',
            python_callable=load,
            dag=dag,
        )

        load_table_geos = PythonOperator(
            task_id='table_geos',
            python_callable=load,
            dag=dag,
        )

        load_table_traffic_sources = PythonOperator(
            task_id='table_traffic_sources',
            python_callable=load,
            dag=dag,
        )

    start >> extract_group >> transform_group >> load_group >> end