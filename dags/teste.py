import sys
from datetime import timedelta

from utils.dag_utils import extract


import json




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
                    "params"  : {"main_url": "https://dados.ufpi.br", "resource_id": "a34d7d7e-30af-41f0-81cf-cd10b6f078bd"}
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
    for collection, params in collections.items():
        print (institute,collection,params)
        data = extract(institute,collection,params)
        print (data[1:5])