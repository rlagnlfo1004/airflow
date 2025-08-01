from airflow.sdk import DAG, task
import pendulum


with DAG(
    dag_id = "dags_python_decorator",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2025, 7, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:

    @task(task_id="python_task_1")
    def python_context(some_input):
        print(some_input)

    python_task_1 = python_context("task_decorator 실행")