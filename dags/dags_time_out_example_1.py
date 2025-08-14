import pendulum
from datetime import timedelta
from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.bash import BashOperator

email_str = Variable.get("email_target")
email_list = [email.strip() for email in email_str.split(',')]

with DAG(
    dag_id='dags_time_out_example_1',
    schedule=None,
    start_date=pendulum.datetime(2025, 8, 1, tz='Asia/Seoul'),
    catchup=False,
    dagrun_timeout=timedelta(minutes=1),
    default_args={
        'email_on_failure': True,
        'email': email_list
    }
):

    bash_sleep_30= BashOperator(
        task_id='bash_sleep_30',
        bash_command='sleep 30',
    )

    bash_sleep_10= BashOperator(
        task_id='bash_sleep_10',
        bash_command='sleep 10',
    )

    bash_sleep_30 >> bash_sleep_10