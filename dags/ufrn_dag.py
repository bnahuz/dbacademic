import sys
sys.path.append('/opt/airflow/')
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from plugins.institutes.ufrn import ufrn
from plugins.utils.dag_utils import dynamic_drop

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['breno.nahuz@discente.ufma.br'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'schedule_interval' : None,
    'start_date': days_ago(0),
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'ufrn_etl',
    default_args=default_args,
    description='A simple DAG to extract data from UFRN API',
    schedule_interval=timedelta(days=1)
)

collections = ['docentes', 'discentes', 'cursos']
drop_collections = []

for collection in collections:
    task = dynamic_drop(f'drop_{collection}', 'ufpi', collection, dag)
    drop_collections.append(task)

run_intake_docentes = PythonOperator(
    task_id='run_intake_docentes',
    python_callable=ufrn.etl_docentes,
    dag=dag,
)

run_intake_discentes = PythonOperator(
    task_id='run_intake_discentes',
    python_callable=ufrn.etl_discentes,
    dag=dag,
)

run_intake_courses = PythonOperator(
    task_id='run_intake_courses',
    python_callable=ufrn.etl_courses,
    dag=dag,
)


drop_collections >> \
run_intake_docentes >> run_intake_discentes >> run_intake_courses