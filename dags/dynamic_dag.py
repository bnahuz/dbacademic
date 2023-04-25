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

config_dags = {
    ufrn(): ['docentes', 'discentes', 'courses'],
    ufpi(): ['docentes', 'discentes', 'courses'],
    ifms(): ['discentes', 'courses'],
    ufca(): ['docentes',  'courses'],
}


# vai apra um arquivo
config_dags = {

    "mapeamento" : {

        "docentes" : {
            "nome": ["nome","servidor"],
            "id": ["siape","matricula"],
            "matricula": ["siape","matricula"],
            "sexo": "sexo",
            "formacao": ["formacao"]
        }

    },


    "instituicoes" : {

        "ufrn": {
            "docentes": {
                    "consumer": "ckan",
                    "consumer_params"  : {"main_url": "https://dados.ufrn.br", "resource_id": "6a8e5461-e748-45c6-aac6-432188d88dde"}
                }
        },

        "ifms" : {
            "docentes": {
                    "consumer": "ckan",
                    "consumer_params"  : {"main_url": "http://dados.ifms.edu.br", "resource_id": "4ccd20e6-703d-4682-a300-26a0e3788a4f"}
                }
        }
    }

}

for institute, collections in config_dags["instituicoes"].items():
    dag = dynamic_create_dag(
        dag_id = f'{institute}', 
        institute = institute, 
        collections = collections, 
        generic_mapper = config_dags["mapeamento"],
        schedule_interval = '@once', 
        start_date = days_ago(0), 
        default_args = default_args)
    globals()[dag.dag_id] = dag