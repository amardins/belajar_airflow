from airflow.utils.dates import days_ago
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import timedelta

args = {
    'owner': 'Airflow',
    'start_date': days_ago(0, hour=1, minute=0),
}

dag = DAG(
    dag_id='example_sensors',
    default_args=args,
    schedule_interval="15 * * * *",
    tags=['example'],
    catchup=False
)

def print_sukses():
    print("================================================")
    print("TASK SUKSES!!!")
    print("================================================")

sensor_example_variables = ExternalTaskSensor(
    task_id="sensor_example_variables",
    external_dag_id="example_variables",
    external_task_id="cetak_variable",
    execution_delta=timedelta(minutes=15),
    dag=dag
)

cetak_sukses = PythonOperator(
    task_id='cetak_sukses',
    python_callable=print_sukses,
    dag=dag,
)

sensor_example_variables >> cetak_sukses
