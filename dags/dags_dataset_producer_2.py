import pendulum
from airflow.sdk import DAG, Asset
from airflow.providers.standard.operators.bash import BashOperator

asset_dags_dataset_producer_2 = Asset("dags_dataset_producer_2")


with DAG(
    dag_id='dags_dataset_producer_2',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2025, 8, 1, tz='Asia/Seoul'),
    catchup=False,
    tags=['asset','producer']
) as dag:

    bash_task = BashOperator(
        task_id='bash_task',
        outlets=[asset_dags_dataset_producer_2],
        bash_command='echo "producer_2 수행 완료"'
    )
