import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Variable

with DAG(
    dag_id='dags_bash_with_variable',
    schedule='10 0 * * *',
    start_date=pendulum.datetime(2025, 7, 1, tz='Asia/Seoul'),
    catchup=False
) as dag:

    var_value=Variable.get("sample_key")

    bash_var_1 = BashOperator(
        task_id='bash_var_1',
        bash_command=f'echo variable:{var_value}'
    )

    bash_var_2 = BashOperator(
        task_id='bash_var_2',
        bash_command="echo variable: {{ var.value.sample_key }}"
    )

    # bash_var_1 >> bash_var_2