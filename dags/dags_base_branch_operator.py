import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.branch import BaseBranchOperator
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id='dags_base_branch_operator',
    start_date=pendulum.datetime(2025, 7, 1, tz='Asia/Seoul'),
    schedule='0 1 * * *',
    catchup=False,
) as dag:


    class CustomBaseBranchOperator(BaseBranchOperator):
        def choose_branch(self, context):
            from random import choice
            item_lst = ['A', 'B', 'C']
            selected_item = choice(item_lst)

            if selected_item == 'A':
                return 'task_a'
            elif selected_item in ['B', 'C']:
                return ['task_b', 'task_c']

    custom_base_branch_operator = CustomBaseBranchOperator(task_id='python_branch_task')

    def common_func(**kwargs):
        print(kwargs['selected'])

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected': 'A'}
    )

    task_b = PythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected': 'B'}
    )

    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected': 'C'}
    )

    custom_base_branch_operator >> [task_a, task_b, task_c]



