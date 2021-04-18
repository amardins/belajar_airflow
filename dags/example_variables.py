from airflow.utils.dates import days_ago
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='example_variables',
    default_args=args,
    schedule_interval=None,
    tags=['example']
)

def print_variable():
    print("================================================")
    print("Variable Contoh adalah: ", Variable.get("Contoh"))
    print("================================================")

run_this = PythonOperator(
    task_id='print_variable',
    python_callable=print_variable,
    dag=dag,
)

run_this
