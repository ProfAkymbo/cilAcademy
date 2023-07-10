from airflow import DAG
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.utils.dates import day_ago
from openweather_local import read_data
from openweather_local import openweather_api
from openweather_local import transform_data

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
    task_id = "load_lga_data",
    python_callable = read_data,
    dag = dag
)

extract_weather_data =  PythonOperator(
    task_id = "extract_weather_data",
    python_callable = openweather_api,
    dag = dag
)

transform =  PythonOperator(
    task_id = "extract_weather_data",
    python_callable = transform_data,
    dag = dag
)


load = 

load_lga > extract_weather_data > transform