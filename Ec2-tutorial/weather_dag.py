from airflow import DAG
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.utils.dates import day_ago
from openweather_api import fetch_and_transform_weather_data

default_args ={
    "owner": "Adedoyin Samuel",
    "start_date": day_ago(0),
    "email":["adedoyinsamuel25@gmail.com"],
    "email_on_failure":True,
    "eamil_on_retry":True,
    "retires":3,
    "retry_delay":timedelta(minutes=5)
}

dag = DAG(
    dag_id = "ETL_weather_data",
    schedule_interval="5 * * * *",
    default_args=default_args,
    description="Running Apache Airflow in AWS EC2 intance"

)

load_lga =  PythonOperator(
    task_id = "fetch_and_transform_weather_data",
    python_callable = fetch_and_transform_weather_data,
    dag = dag
)

