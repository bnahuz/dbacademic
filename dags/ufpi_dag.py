import sys
sys.path.append('/opt/airflow/')
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from plugins.institutes.UFPI import *
from plugins.utils.mongo import drop_collection, get_mongo_db

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['breno.nahuz@discente.ufma.br'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'schedule_interval' : '@once',
    'start_date': days_ago(0),
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ufpi_etl',
    default_args=default_args,
    description='A simple DAG to extract data from UFPI API',
    schedule_interval=timedelta(days=1)
)

drop_docentes = PythonOperator(
    task_id='drop_docentes',
    python_callable=drop_collection,
    op_kwargs={'institute': 'ufpi', 'collection_name': 'docentes'},
    dag=dag,
)

drop_cursos = PythonOperator(
    task_id='drop_cursos',
    python_callable=drop_collection,
    op_kwargs={'institute': 'ufpi', 'collection_name': 'cursos'},
    dag=dag,
)

drop_discentes = PythonOperator(
    task_id='drop_discentes',
    python_callable=drop_collection,
    op_kwargs={'institute': 'ufpi', 'collection_name': 'discentes'},
    dag=dag,
)

run_intake_docentes = PythonOperator(
    task_id='run_intake_docentes',
    python_callable=etl_docentes,
    dag=dag,
)

run_intake_discentes = PythonOperator(
    task_id='run_intake_discentes',
    python_callable=etl_discentes,
    dag=dag,
)

run_intake_courses = PythonOperator(
    task_id='run_intake_courses',
    python_callable=etl_courses,
    dag=dag,
)


[drop_docentes,drop_discentes,drop_cursos] >> \
run_intake_docentes >> run_intake_discentes >> run_intake_courses