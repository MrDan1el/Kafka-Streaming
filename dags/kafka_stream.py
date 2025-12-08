from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from kafka import KafkaProducer
import time

from datetime import datetime, timedelta
import json
import requests
import logging


def get_data():
    url = 'https://randomuser.me/api/'
    response = requests.get(url)
    data = response.json()
    data = data['results'][0]
    return data


def format_data(data):
    formated_data = {
        'first_name': data['name']['first'],
        'last_name': data['name']['last'],
        'gender': data['gender'],
        'birthday': data['dob']['date'][:10],
        'phone_number': data['phone'],
        'street': data['location']['street']['name'] + ' ' + str(data['location']['street']['number']),
        'city': data['location']['city'],
        'state': data['location']['state'],
        'country': data['location']['country'],
        'postcode': data['location']['postcode'],
        'email': data['email'],
        'username': data['login']['username'],
        'password': data['login']['password'],
        'registration_date': data['registered']['date'][:10]
    }
    return formated_data


def stream_from_api_to_kafka():
    data = get_data()
    formated_data = format_data(data)
    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        max_block_ms=5000
    )
    producer.send('users_created', json.dumps(formated_data).encode('utf-8'))


default_args = {
    'owner': 'username',
    'start_date': days_ago(1),
    'retries': 5,
    'retry_delay': timedelta(minutes=5) 
}

with DAG(
    dag_id='user_automation',
    description='Stream data from API to Kafka',
    tags=['raw', 'kafka', 'api'],
    default_args=default_args,
    schedule_interval='0 9 * * *',
    catchup=False
) as dag:
    
    start = EmptyOperator(
        task_id='start'
    )

    stream_from_api_to_kafka = PythonOperator(
        task_id='stream_from_api_to_kafka',
        python_callable=stream_from_api_to_kafka
    )

    end = EmptyOperator(
        task_id='end'
    )