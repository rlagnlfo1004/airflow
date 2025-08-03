from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task
import pendulum

with DAG(
    dag_id='dags_python_with_trigger_rule_eg2',
    start_date=pendulum.datetime(2025, 7, 1, tz='Asia/Seoul'),
    schedule='0 4 * * *',
    catchup=False
) as dag:

    @task(task_id='branching')
    def random_branch():
        from random import choice
        item_lst = ['A', 'B', 'C']
        selected_item = choice(item_lst)

        if selected_item == 'A':
            return 'task_a'
        elif selected_item in ['B', 'C']:
            return ['task_b', 'task_c']


    task_a = BashOperator(
        task_id='task_a',
        bash_command='echo upstream1'
    )

    @task(task_id='task_b')
    def task_b():
        print('정상 처리')

    @task(task_id='task_c')
    def task_c():
        print('정상 처리')

    @task(task_id='task_d', trigger_rule='none_skipped')
    def task_d():
        print('정상 처리')


    random_branch() >> [task_a, task_b(), task_c()] >> task_d()