from datetime import timedelta

import pendulum
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Variable, DAG, task

email_str = Variable.get("email_target")
email_list = [email.strip() for email in email_str.split(',')]


with DAG(
    dag_id='dags_email_on_failure',
    schedule='0 8 * * *',
    start_date=pendulum.datetime(2025, 8, 1, tz='Asia/Seoul'),
    catchup=False,
    dagrun_timeout=timedelta(minutes=2),
    default_args={
        'email_on_failure': True,
        'email': email_list
    }
):

    @task(task_id='python_fail')
    def python_task_func():
        raise AirflowException('에러 발생')
    python_task_func()

    bash_fail = BashOperator(
        task_id='bash_fail',
        bash_command='exit 1',
    )

    bash_success = BashOperator(
        task_id='bash_success',
        bash_command='exit 0',
    )

