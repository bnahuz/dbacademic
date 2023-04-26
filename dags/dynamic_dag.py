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

    "mapeamento" : {

        "docentes" : {
            "nome": ["nome","servidor","SERVIDOR","Nome do Servidor","Nome","NOME_FUNCIONARIO", "nome_servidor","NomeServidor"],
            "id": ["siape","matricula","Matrícula","Matricula","_id","vinculo_servidor","CodigoServidor"],
            "matricula": ["siape","matricula","vinculo_servidor","CodigoServidor"],
            "sexo": ["sexo","Sexo"],
            "formacao": ["formacao","escolaridade","TitulacaoServidor","Escolaridade","TITULAÇÃO"],
            "nome_lotacao" :["Órgão de Lotação (SIAPE)", "setor lotacao"],
            "codigo_lotacao" :["id_unidade_lotacao"],
        }
        ,

        "discentes" : {
            "nome": ["nome","nome_discente"], 
            "id" : ["ra", "matricula"],
            "matricula" : ["ra", "matricula"],
            "sexo" : ["sexo"],
            "data_ingresso" : ["data_inicio"],
            "codigo_curso" : ["id_curso"],
            "nome_curso": ["curso"]
        },

        "cursos" : {
            "nome": ["nome"], 
            "id" : ["id_curso"],
            "codigo" : ["id_curso"],
            "codigo_unidade" : ["id_unidade_responsavel"],
        },

        "unidades" : {
            "nome": ["nome_unidade"], 
            "id" : ["id_unidade"],
            "codigo" : ["id_unidade"],
        },


    },


    "instituicoes" : {

        "ufrn": {
            "dbpedia_pt":"http://pt.dbpedia.org/resource/Instituto_Federal_do_Rio_Grande_do_Norte",
            "colecoes" : {
            "docentes": {
                    "consumer": "CkanConsumer",
                    "main_url": "https://dados.ufrn.br",
                    "params": {"resource_id": "6a8e5461-e748-45c6-aac6-432188d88dde"}
                },

            "discentes": {
                    "consumer": "CkanConsumer",
                    "main_url": "https://dados.ufrn.br",
                    "params": {"resource_id": "a55aef81-e094-4267-8643-f283524e3dd7"}
                },

            "cursos": {
                    "consumer": "CkanConsumer",
                    "main_url": "https://dados.ufrn.br",
                    "params": {"resource_id": "a10bc434-9a2d-491a-ae8c-41cf643c35bc"}
                },
 
           "unidades": {
                    "consumer": "CkanConsumer",
                    "main_url": "https://dados.ufrn.br",
                    "params": {"resource_id": "3f2e4e32-ef1a-4396-8037-cbc22a89d97f"}
                },

        }},

        "ufca": {
            "dbpedia_pt":"http://pt.dbpedia.org/resource/Universidade_Federal_do_Cariri",
            "colecoes" : {
            "docentes": {
                    "consumer": "CkanConsumer",
                    "main_url": "https://dados.ufca.edu.br",
                    "params": {"resource_id": "6b2dbca5-58f8-472e-bc6a-eb827e631873"}
                }
        }},

        "ufpi": {
            "dbpedia_pt":"http://pt.dbpedia.org/resource/Universidade_Federal_do_Piauí",
            "colecoes" : {
            "docentes": {
                    "consumer": "CkanConsumer",
                    "main_url": "https://dados.ufpi.br",
                    "params": {"resource_id": "a34d7d7e-30af-41f0-81cf-cd10b6f078bd"}
                }
        }},




      "ufcspa" : {
            "dbpedia_pt":"http://pt.dbpedia.org/resource/Universidade_Federal_de_Ciências_da_Saúde_de_Porto_Alegre",
            "colecoes" : {
            "docentes": {
                    "consumer": "CkanConsumer",
                    "main_url": "https://dados.ufcspa.edu.br",
                    "params": {"resource_id": "4286a4d5-9de7-4f88-bb37-f0f064415118"}
                }
        }},


  

      "unifespa" : {
            "dbpedia_pt":"http://pt.dbpedia.org/resource/Universidade_Federal_do_Sul_e_Sudeste_do_Pará",
            "colecoes" : {
            "docentes": {
                    "consumer": "CkanConsumer",
                    "main_url": "http://ckan.unifesspa.edu.br",
                    "params": {"resource_id": "eff99b8c-09d3-453b-b7dd-1de846ab18a7"}
                }
        }},

         

        "ufv" : {
            "dbpedia_pt":"http://pt.dbpedia.org/resource/Universidade_Federal_de_Viçosa",
            "colecoes" : {
            "docentes": {
                    "consumer": "CkanConsumer",
                    "main_url": "https://dados.ufv.br",
                    "params": {"resource_id": "a949a903-9536-4d20-87e5-cca5c217771a"}
                }
        }},

      

        "ufsj" : {
            "dbpedia_pt":"http://pt.dbpedia.org/resource/Universidade_Federal_de_S%C3%A3o_Jo%C3%A3o_del-Rei",
            "colecoes" : {
            "docentes": {
                    "consumer": "CkanConsumer",
                    "main_url": "http://dados.ufsj.edu.br",
                    "params": {"resource_id": "8e2e35ed-e255-4894-b070-ad8857366faf"}
                }
        }},

  

        "ufms" : {
            "dbpedia_pt":"http://pt.dbpedia.org/resource/Universidade_Federal_do_Mato_Grosso_do_Sul",
            "colecoes" : {
            "docentes": {
                    "consumer": "CkanConsumer",
                    "main_url": "https://dadosabertos.ufms.br/",
                    "params": {"resource_id": "a8ca7f30-0824-489b-8c70-faddcbd74f53", "q": "Professor do Magisterio Superior"}
                }
        }},


        

        "ufop" : {
            
          
            "dbpedia_pt": "http://pt.dbpedia.org/resource/Universidade_Federal_de_Ouro_Preto",
            "colecoes" : {
            "docentes": {
                    "consumer": "CkanConsumer",
                    "main_url": "http://dados.ufop.br",
                    "params": {"resource_id": "04e65338-1b7f-45b7-893b-05470d17dcad"}
                }
        }},


        "ifgoiano" : {
          "dbpedia_pt": "http://pt.dbpedia.org/resource/Instituto_Federal_Goiano",
          "colecoes" : { 
                "docentes": 
                {
                        "consumer": "CkanConsumer",
                        "main_url": "https://dados.ifgoiano.edu.br",
                        "params": {"resource_id": "ecd0ad77-2125-42c4-a8d0-c3fe012731dd"}
                    }
             }
        },


        "ifms" : {
            "dbpedia_pt":"http://pt.dbpedia.org/resource/Instituto_Federal_de_Mato_Grosso_do_Sul",
            "colecoes" : {
            "docentes": {
                    "consumer": "CkanConsumer",
                    "main_url": "http://dados.ifms.edu.br",
                    "params": {"resource_id": "4ccd20e6-703d-4682-a300-26a0e3788a4f"}
                },

            "discentes" : {
                 "consumer": "CkanConsumer",
                 "main_url": "http://dados.ifms.edu.br",
                 "params": {"resource_id": "b8b4dfdf-98ef-4d57-baff-75c163be6e9a"}

            }
        }},
    }

}


for institute, values in config_dags["instituicoes"].items():
    collections = values["colecoes"]
    dag = dynamic_create_dag(
        dag_id = f'{institute}', 
        institute_data = {"id": institute, "dbpedia": values["dbpedia_pt"]},  
        collections = collections, 
        generic_mapper = config_dags["mapeamento"],
        schedule_interval = '@once', 
        start_date = days_ago(0), 
        default_args = default_args)
    globals()[dag.dag_id] = dag