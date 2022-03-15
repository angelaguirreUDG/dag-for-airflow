from datetime import timedelta
import datetime
from urllib import response
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import logging
import requests

default_args = {

    'owner' : 'Angel Aguirre',
    'depends_on_past' : False,
    'email' : ['angelaguirre@example.com'],
    'email_on_failure' : False,
    'email_on_rety': False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5)
}


def consumeAPI():

    url = 'https://jsonplaceholder.typicode.com/users'
    
    response = requests.get(url)
   
    if(response.status_code == 200):
        logging('I have the data!')


def processInformation():
    logging.info('Processing information...')

def saveFile():
    logging.info('Saving a file...')

with DAG(
    'udg',
    default_args=default_args,
    description= 'An example for the class',
    schedule_interval=timedelta(days=1),
    start_date=datetime.datetime.now(),
    tags=['example']
) as dag:
    
    consumeAPI_task = PythonOperator(task_id = "consumeAPI", python_callable=consumeAPI)
    processInformation_task = PythonOperator(task_id = "processInformation", python_callable=processInformation)
    saveFile_task = PythonOperator(task_id = "saveFile", python_callable=saveFile)

    consumeAPI_task >> processInformation_task >> saveFile_task