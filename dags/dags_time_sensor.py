import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.sensors.date_time import DateTimeSensor

with DAG(
    dag_id="dags_time_sensor",
    start_date=pendulum.datetime(2025, 8, 1, 0, 0, 0),
    end_date=pendulum.datetime(2025, 8, 1, 1, 0, 0),
    schedule="*/10 * * * *",
    catchup=True,
) as dag:
    sync_sensor = DateTimeSensor(
        task_id="sync_sensor",
        target_time="""{{ macros.datetime.utcnow() + macros.timedelta(minutes=3) }}""",
    )