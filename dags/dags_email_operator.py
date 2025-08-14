import pendulum
from airflow.sdk import DAG
from airflow.providers.smtp.operators.smtp import EmailOperator

with DAG(
    dag_id='dags_email_operator',
    schedule='0 8 * * *',
    start_date=pendulum.datetime(2025, 8, 1, tz='Asia/Seoul'),
    catchup=False,
) as dag:

    send_email_task = EmailOperator(
        task_id='send_email_task',
        conn_id='conn_smtp_gmail',
        to=['hrkim2001@ajou.ac.kr', 'mkms0222@ajou.ac.kr'],
        subject='(Airflow 성공 메일) ❤️❤️❤️️뚱뚱한 사람에게 보내지는 메일입니다.',
        html_content='Airflow DAG가 성공적으로 실행되었습니다.',
    )
