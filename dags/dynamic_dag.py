import sys
sys.path.append('/opt/airflow/')
from datetime import timedelta
from airflow.utils.dates import days_ago
from plugins.utils.dag_utils import *

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

config_dags = {
    ufrn(): ['docentes', 'discentes', 'courses'],
    ufpi(): ['docentes', 'discentes', 'courses'],
}

for institute, collections in config_dags.items():
    dag = dynamic_create_dag(
        dag_id = f'{institute.name}_test', 
        institute = institute, 
        collections = collections, 
        schedule_interval = '@once', 
        start_date = days_ago(0), 
        default_args = default_args)
    globals()[dag.dag_id] = dag