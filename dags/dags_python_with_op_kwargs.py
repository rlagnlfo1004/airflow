from common.common_func import regist2
import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id = "dags_python_with_op_kwargs",
    schedule = " 30 6 * * *",
    start_date=pendulum.datetime(2025, 7, 1, tz="Asia/Seoul"),
    catchup = False
) as dag :

    regist2_t1 = PythonOperator(
        task_id = "regist2_t1",
        python_callable =regist2,
        op_args=['hrkim', 'man', 'kr', 'seoul'],
        op_kwargs={"email": "hramst0618@gmail.com", "phone": "010-1234-5678"}
    )

    regist2_t1