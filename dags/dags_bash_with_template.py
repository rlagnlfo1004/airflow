from airflow.sdk import DAG
import pendulum
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_template",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2025, 7, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    bash_t1 = BashOperator(
        task_id='bash_t1',
        bash_command='echo "data_interval_end: {{ data_interval_end }} "'
    )

    bash_t2= BashOperator(
        task_id='bash_t2',
        env={
            'SART_DATE': '{{ data_interval_end | ds }}',
            'END_DATE' : '{{ data_interval_end | ds }}',
        },
        bash_command='echo $SART_DATE && echo $END_DATE'
    )

    bash_t1 >> bash_t2
