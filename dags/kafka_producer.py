from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests
from kafka import KafkaProducer
import time
import logging
import uuid

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1,10,00)
}

def get_data():
    res = requests.get("https://randomuser.me/api/")
    res = res.json()['results'][0]

    return res

def format_data(res):
    data = {}
    data['id'] = str(uuid.uuid4())
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data


def stream_data():
    
    

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)

    current_time = time.time()
    
    while True:
        if time.time() - current_time > 60:  # Retry for 60 seconds
            break
        try:
            res = get_data()
            data = format_data(res)

            producer.send('users_created', json.dumps(data).encode('utf-8'))
        except Exception as e:
            logging.error(f"Failed to send data: {e}")
            continue

with DAG(dag_id='user_automation',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data)
    
    streaming_task

