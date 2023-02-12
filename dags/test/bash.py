from datetime import timedelta, datetime
import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
import requests
import json
import pendulum

args = {
    'owner': 'airflow'
}

def alert():
    current_time = datetime.now()
    hook = Variable.get("hook_slack")
    print(hook)

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
    schedule_interval="*/2 * * * *",
    start_date=pendulum.datetime(2023, 2, 11, tz="UTC"),
    dagrun_timeout=timedelta(minutes=10),
    catchup=False # Don't run previous and backfill, run only latests
) as dag:
    task1 = BashOperator(task_id='first_task', bash_command="echo Hello World", dag=dag)
    task2 = PythonOperator(task_id="second_task", python_callable=alert, dag=dag)

    # Set fist_task to run before second_task
    task1 >> task2