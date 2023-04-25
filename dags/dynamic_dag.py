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
                    "consumer": "CkanConsumer",
                    "params"  : {"main_url": "https://dados.ufrn.br", "resource_id": "6a8e5461-e748-45c6-aac6-432188d88dde"}
                }
        },

        "ufca": {
            "docentes": {
                    "consumer": "CkanConsumer",
                    "params"  : {"main_url": "https://dados.ufca.edu.br", "resource_id": "6b2dbca5-58f8-472e-bc6a-eb827e631873"}
                }
        },

        "ufpi": {
            "docentes": {
                    "consumer": "CkanConsumer",
                    "params"  : {"main_url": "dados.ufpi.br", "resource_id": "a34d7d7e-30af-41f0-81cf-cd10b6f078bd"}
                }
        },

        "ifms" : {
            "docentes": {
                    "consumer": "CkanConsumer",
                    "params"  : {"main_url": "http://dados.ifms.edu.br", "resource_id": "4ccd20e6-703d-4682-a300-26a0e3788a4f"}
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