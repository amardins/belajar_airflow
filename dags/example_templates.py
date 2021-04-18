from airflow.utils.dates import days_ago
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'Airflow',
    'start_date': days_ago(0, hour=1, minute=0),
}

dag = DAG(
    dag_id='example_templates',
    default_args=args,
    schedule_interval="0 * * * *",
    tags=['example'],
    catchup=False
)

def print_execution_date(**kwargs):
    print("================================================")
    print("execution_date adalah: ", kwargs["execution_date"])
    print("================================================")

cetak_execution_date = PythonOperator(
    task_id='cetak_execution_date',
    provide_context=True,
    python_callable=print_execution_date,
    templates_dict={"execution_date": "{{ execution_date }}"},
    dag=dag,
)

cetak_execution_date
