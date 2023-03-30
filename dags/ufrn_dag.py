import sys
sys.path.append('/opt/airflow/')
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from etl.extraction.UFRN import *

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['breno.nahuz@discente.ufma.br'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': days_ago(2),
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ufrn_etl',
    default_args=default_args,
    description='A simple DAG to extract data from UFRN API',
    schedule_interval=timedelta(days=1)
)

drop_docentes = PythonOperator(
    task_id='drop_docentes',
    python_callable=drop_from_ufrn_db_docentes,
    dag=dag,
)

drop_discentes = PythonOperator(
    task_id='drop_discentes',
    python_callable=drop_from_ufrn_db_discentes,
    dag=dag,
)

drop_tccs = PythonOperator(
    task_id='drop_tccs',
    python_callable=drop_from_ufrn_db_tccs,
    dag=dag,
)

drop_cursos = PythonOperator(
    task_id='drop_cursos',
    python_callable=drop_from_ufrn_db_cursos,
    dag=dag,
)

drop_grupos_pesquisa = PythonOperator(
    task_id='drop_grupos_pesquisa',
    python_callable=drop_from_ufrn_db_grupos_pesquisa,
    dag=dag,
)

drop_unidades = PythonOperator(
    task_id='drop_unidades',
    python_callable=drop_from_ufrn_db_unidades,
    dag=dag,
)

run_intake_docentes = PythonOperator(
    task_id='run_intake_docentes',
    python_callable=get_docentes,
    dag=dag,
)

run_intake_discentes = PythonOperator(
    task_id='run_intake_discentes',
    python_callable=get_students,
    dag=dag,
)

run_intake_tccs = PythonOperator(
    task_id='run_intake_tccs',
    python_callable=get_tccs,
    dag=dag,
)

run_intake_courses = PythonOperator(
    task_id='run_intake_courses',
    python_callable=get_courses,
    dag=dag,
)

run_intake_units = PythonOperator(
    task_id='run_intake_units',
    python_callable=get_units,
    dag=dag,
)

run_intake_research_groups = PythonOperator(
    task_id='run_intake_research_groups',
    python_callable=get_research_groups,
    dag=dag,
)


[drop_docentes,drop_discentes,drop_tccs,drop_grupos_pesquisa,drop_unidades,drop_cursos] >> \
run_intake_docentes >> run_intake_discentes >> run_intake_tccs  >> run_intake_research_groups >> \
run_intake_units >> run_intake_courses