from airflow.sdk import DAG
import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.decorators import task

with DAG(
    dag_id ='dags_python_template',
    schedule = '30 9 * * *',
    start_date = pendulum.datetime(2025, 7, 24, tz='Asia/Seoul'),
    catchup = False
)as dag:

    def python_function_1(start_date, end_date, **kwargs):
        print(start_date)
        print(end_date)

    python_t1 = PythonOperator(
        task_id = 'python_t1',
        python_callable = python_function_1,
        op_kwargs = {'start_date': '{{ data_interval_start }}', 'end_date': '{{ data_interval_end | ds }}'}
    )

    @task(task_id = 'python_t2')
    def python_function_2(**kwargs):
        print(kwargs)
        print('ds:' + kwargs['ds'])
        print('ts:' + kwargs['ts'])
        print('data_interval_start:' + str(kwargs['data_interval_start']))
        print('data_interval_end:' + str(kwargs['data_interval_end']))
        print('task_instance:' + str(kwargs['ti']))

    python_t1 >> python_function_2()

