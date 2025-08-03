from airflow.sdk import DAG, task, task_group, TaskGroup
import pendulum
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id='dags_python_with_task_group',
    start_date=pendulum.datetime(2025, 7, 1, tz='Asia/Seoul'),
    schedule='0 1 * * *',
    catchup=False,
)as dag:

    def inner_func(**kwargs):
        msg=kwargs.get('msg') or ''
        print(msg)


    @task_group(group_id='first_group')
    def group_1():
        ''' task_group 데커레이터를 이용한 첫 번째 그룹입니다. '''

        @task(task_id='inner_function_1')
        def inner_func1(**kwargs):
            print('첫 번째 TaskGroup 내 첫 번째 task 입니다.')

        inner_function_2=PythonOperator(
            task_id='inner_function_2',
            python_callable=inner_func,
            op_kwargs={'msg': '첫 번째 TaskGroup 내 두 번째 task 입니다.'}
        )

        inner_func1() >> inner_function_2


    with TaskGroup(group_id='second_group', tooltip='두 번째 그룹입니다.') as group_2:
        ''' 여기에 적은 docstirng은 표시되지 않습니다.'''
        @task(task_id='inner_function_1')
        def inner_func1(**kwargs):
            print('두 번째 TaskGroup 내 첫 번째 task 입니다.')

        inner_function_2=PythonOperator(
            task_id='inner_function_2',
            python_callable=inner_func,
            op_kwargs={'msg': '두 번째 TaskGroup 내 두 번째 task 입니다.'}
        )

        inner_func1() >> inner_function_2

    group_1() >> group_2