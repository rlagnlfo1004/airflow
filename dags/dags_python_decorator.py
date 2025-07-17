from airflow.sdk import DAG
import pendulum
import datetime
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.definitions.decorators import task

with DAG(
    dag_id ="dags_python_decorator",
    schedule = "090 2 * * 1",
    start_date=pendulum.datetime(2025, 7, 1, tz='Asia/Seoul'),
    catchup=False,
) as dag:

    @task(task_id="python_task_1")
    def print_context(some_input):
        print(some_input)

    python_task_1 = print_context('task_decorator 실행')