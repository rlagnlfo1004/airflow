import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
    dag_id = "dags_python_with_postgres_with_hook_bulk_load",
    start_date = pendulum.datetime(2025, 8, 1, tz="Asia/Seoul"),
    schedule = '0 0 * * *',
    catchup = False
) as dag:

    def insrt_postgres(postgres_conn_id, tlb_nm, file_nm, **kwargs):
        postgres_hook = PostgresHook(postgres_conn_id)
        postgres_hook.bulk_load(tlb_nm, file_nm)

    insrt_postgres_with_hook = PythonOperator(
        task_id='insrt_postgres_with_hook',
        python_callable=insrt_postgres,
        op_kwargs={
            'postgres_conn_id': 'conn-db-postgres-custom',
            'tlb_nm': 'TbCorona19CountStatus_bulk1',
            'file_nm': '/opt/airflow/files/TbCorona19CountStatus/{{ data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/TbCorona19CountStatus.csv'
        }
    )