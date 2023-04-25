import sys
from datetime import timedelta

from utils.dag_utils import extract, transform


import json




# vai apra um arquivo
config_dags = {

    "mapeamento" : {

        "docentes" : {
            "nome": ["nome","servidor","SERVIDOR","Nome do Servidor","Nome","NOME_FUNCIONARIO", "nome_servidor","NomeServidor"],
            "id": ["siape","matricula","Matrícula","Matricula","_id","vinculo_servidor","CodigoServidor"],
            "matricula": ["siape","matricula","vinculo_servidor","CodigoServidor"],
            "sexo": ["sexo","Sexo"],
            "formacao": ["formacao","escolaridade","TitulacaoServidor","Escolaridade","TITULAÇÃO"],
            "lotacao" :["Órgão de Lotação (SIAPE)", "setor lotacao","UNIDADE EM EXERCÍCIO"]
        }

    },


    "instituicoes" : {
        '''

        "ufrn": {
            "docentes": {
                    "consumer": "CkanConsumer",
                    "main_url": "https://dados.ufrn.br",
                    "params": {"resource_id": "6a8e5461-e748-45c6-aac6-432188d88dde"}
                }
        },

        "ufca": {
            "docentes": {
                    "consumer": "CkanConsumer",
                    "main_url": "https://dados.ufca.edu.br",
                    "params": {"resource_id": "6b2dbca5-58f8-472e-bc6a-eb827e631873"}
                }
        },

        "ufpi": {
            "docentes": {
                    "consumer": "CkanConsumer",
                    "main_url": "https://dados.ufpi.br",
                    "params": {"resource_id": "a34d7d7e-30af-41f0-81cf-cd10b6f078bd"}
                }
        },

        "ifms" : {
            "docentes": {
                    "consumer": "CkanConsumer",
                    "main_url": "http://dados.ifms.edu.br",
                    "params": {"resource_id": "4ccd20e6-703d-4682-a300-26a0e3788a4f"}
                }
        },

      


     

      "ufcspa" : {
            "docentes": {
                    "consumer": "CkanConsumer",
                    "main_url": "https://dados.ufcspa.edu.br",
                    "params": {"resource_id": "4286a4d5-9de7-4f88-bb37-f0f064415118"}
                }
        },


  

      "unifespa" : {
            "docentes": {
                    "consumer": "CkanConsumer",
                    "main_url": "http://ckan.unifesspa.edu.br",
                    "params": {"resource_id": "eff99b8c-09d3-453b-b7dd-1de846ab18a7"}
                }
        },

         

        "ufv" : {
            "docentes": {
                    "consumer": "CkanConsumer",
                    "main_url": "https://dados.ufv.br",
                    "params": {"resource_id": "a949a903-9536-4d20-87e5-cca5c217771a"}
                }
        },

      

        "ufsj" : {
            "docentes": {
                    "consumer": "CkanConsumer",
                    "main_url": "http://dados.ufsj.edu.br",
                    "params": {"resource_id": "8e2e35ed-e255-4894-b070-ad8857366faf"}
                }
        },

  

        "ufms" : {
            "docentes": {
                    "consumer": "CkanConsumer",
                    "main_url": "https://dadosabertos.ufms.br/",
                    "params": {"resource_id": "a8ca7f30-0824-489b-8c70-faddcbd74f53", "q": "Professor do Magisterio Superior"}
                }
        },


        

        "ufop" : {
            "docentes": {
                    "consumer": "CkanConsumer",
                    "main_url": "http://dados.ufop.br",
                    "params": {"resource_id": "04e65338-1b7f-45b7-893b-05470d17dcad"}
                }
        },


        "ifgoiano" : {
            "docentes": {
                    "consumer": "CkanConsumer",
                    "main_url": "https://dados.ifgoiano.edu.br",
                    "params": {"resource_id": "ecd0ad77-2125-42c4-a8d0-c3fe012731dd"}
                }
        },
        '''
        "ifsuldeminas" : {
            "docentes": {
                    "consumer": "CkanConsumer",
                    "main_url": "https://dados.ifsuldeminas.edu.br",
                    "params": {"resource_id": "7db2014f-577f-4ec3-a6ab-2c7da2015f8b","q":"PROFESSOR"}
                }
        },



    }

}

for institute, collections in config_dags["instituicoes"].items():
    for collection, params in collections.items():
        print (institute,collection,params)
        data = extract(institute,collection,params)[1:5]
        print (data)
        data = transform (data, config_dags["mapeamento"][collection])
        print (data)