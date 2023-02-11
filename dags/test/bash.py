from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import requests
import json
import os

args = {
    'owner': 'airflow',
}


def alert():
    current_time = datetime.now()
    hook = os.environ.get("HOOK")

    payload = json.dumps({
    "text": "Hello, World! {}".format(current_time)
    })
    headers = {
    'Content-type': 'application/json'
    }

    response = requests.request("POST", hook, headers=headers, data=payload)

with DAG(
    dag_id='test_bash_operartor',
    default_args=args,
    schedule_interval=timedelta(minutes=1),
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60)
) as dag:
    task1 = BashOperator(task_id='first_task', bash_command=f"echo Hello World", dag=dag)
    task2 = PythonOperator(task_id="second_task", python_callable=alert, dag=dag)

    # Set fist_task to run before second_task
    task1 >> task2