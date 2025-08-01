import pendulum
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.sdk import DAG, task

with DAG(
    dag_id='dags_python_email_xcom',
    schedule='10 0 * * *',
    start_date=pendulum.datetime(2025, 7, 1, tz='Asia/Seoul'),
    catchup=False
)as dag:

    @task(task_id='something_task')
    def some_logic(**kwargs):
        from random import choice
        return choice(['Success', 'Fail'])

    send_email = EmailOperator(
        task_id='send_email',
        to='hramst0618@gamil.com',
        subject='{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} some_logic 처리 결과',
        html_content='{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} 처리 결과는 <br> \
            {{ ti.xcom_pull(task_ids="something_task") }} 했습니다. <br>'
    )

    some_logic() >> send_email