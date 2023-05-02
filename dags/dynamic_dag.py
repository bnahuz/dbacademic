import sys
sys.path.append('/opt/airflow/')
from datetime import timedelta
from airflow.utils.dates import days_ago
from utils.dag_utils import *


import json

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


with open('/opt/airflow/dags/settings.json', 'r') as arquivo:
    config_dags = json.load(arquivo)

for institute, values in config_dags["instituicoes"].items():

    dag = dynamic_create_dag(
        dag_id = f'{institute}', 
        institute = institute,  
        conf = values , 
        generic_mapper = config_dags["mapeamento"],
        schedule_interval = '@once', 
        start_date = days_ago(0), 
        default_args = default_args)
    globals()[dag.dag_id] = dag

dag_ttl = create_dag_ttl ("transform_save_ttl", config_dags, '@once',  days_ago(0), default_args)
globals()[dag_ttl.dag_id] = dag_ttl