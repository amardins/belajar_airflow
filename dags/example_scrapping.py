from airflow.utils.dates import days_ago
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator

import pandas as pd
args = {
    'owner': 'Airflow',
    'start_date': days_ago(0, hour=1, minute=0),
}

dag = DAG(
    dag_id='example_scrapping',
    default_args=args,
    schedule_interval="0 * * * *",
    tags=['example'],
    catchup=False
)

def scrap_website():
    dfs=pd.read_html('https://id.wikipedia.org/wiki/Daftar_orang_terkaya_di_Indonesia')
    dfs[7].to_csv('list_orang_terkaya_di_indonesia.csv')
 

scrapping_data = PythonOperator(
    task_id='scrapping_data',
    python_callable=scrap_website,
    dag=dag,
)

send_email = EmailOperator(
        task_id='send_email',
        to='fia.digitalskola@gmail.com ',
        subject='Andi_DigitalSkola_Airflow',
        html_content=""" Daftar Orang terkaya di Indonesia - Andi Mardinsyah """,
        files=['/usr/local/airflow/list_orang_terkaya_di_indonesia.csv'],
        dag=dag
)

scrapping_data >> send_email
