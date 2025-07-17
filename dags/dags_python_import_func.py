import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from common.common_func import get_sftp

with DAG(
    dag_id = "dags_python_import_func",
    schedule = "30 6 * * *",
    start_date=pendulum.datetime(2025, 7, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    task_get_sftp = PythonOperator(
        task_id="task_get_sftp",
        python_callable=get_sftp
    )