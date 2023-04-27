import sys
from datetime import timedelta

from utils.dag_utils import  extract, transform,dynamic_ttl

from simpot.serialize import mapper_all,  serialize_to_rdf

import json

import utils

import utils.consumers as consumers

# vai apra um arquivo
# o documento podia ficar mais limpo, colocando a url base apenas uma vez

config_dags = {
    "mapeamento": {
        "docentes": {
            "nome": [
                "NOME",
                "nome",
                "servidor",
                "NOME SERVIDOR",
                "SERVIDOR",
                "Nome do Servidor",
                "Nome",
                "NOME_FUNCIONARIO",
                "nome_servidor",
                "NomeServidor",
                "nome_oficial"
            ],
            "id": ["SIAPE",
                "Siape",
                "siape",
                "matricula",
                "Matrícula",
                "Matricula",
                "_id",
                "vinculo_servidor",
                "CodigoServidor",
            ],
            "matricula": ["SIAPE","Siape","MATRICULA","siape", "matricula", "vinculo_servidor", "CodigoServidor", "_id"],
            "sexo": ["sexo", "Sexo"],
            "formacao": [
                "formacao",
                "escolaridade",
                "TitulacaoServidor",
                "Escolaridade",
                "TITULAÇÃO",
                "NIVEL ESCOLARIDADE",
                "Titulacao",
                "TITULAÇÃO"
            ],
            "nome_lotacao": ["Órgão de Lotação (SIAPE)", "setor lotacao"],
            "codigo_lotacao": ["id_unidade_lotacao", "CÓDIGO DA UNIDADE ORGANIZACIONAL","setor_siape"],
            "email" : ["email"]
        },
        "discentes": {
            "nome": ["nome", "nome_discente"],
            "id": ["ra", "matricula"],
            "matricula": ["ra", "matricula"],
            "sexo": ["sexo"],
            "data_ingresso": ["data_inicio"],
            "codigo_curso": ["id_curso"],
            "nome_curso": ["curso"],
        },
        "cursos": {
            "nome": ["nome"],
            "id": ["id_curso"],
            "codigo": ["id_curso"],
            "codigo_unidade": ["id_unidade_responsavel"],
        },
        "unidades": {
            "nome": ["nome_unidade"],
            "id": ["id_unidade"],
            "codigo": ["id_unidade"],
        },
    },
    "instituicoes": {
        "ufrn": {
            "dbpedia_pt": "http://pt.dbpedia.org/resource/Instituto_Federal_do_Rio_Grande_do_Norte",
            "consumer": "CkanConsumer",
            "main_url": "https://dados.ufrn.br",
            "colecoes": {
                "docentes": {"resource_id": "6a8e5461-e748-45c6-aac6-432188d88dde"},
                "discentes": {"resource_id": "a55aef81-e094-4267-8643-f283524e3dd7"},
                "cursos": {"resource_id": "a10bc434-9a2d-491a-ae8c-41cf643c35bc"},
                "unidades": {"resource_id": "3f2e4e32-ef1a-4396-8037-cbc22a89d97f"},
            },
        },
        "ufca": {
            "dbpedia_pt": "http://pt.dbpedia.org/resource/Universidade_Federal_do_Cariri",
            "consumer": "CkanConsumer",
            "main_url": "https://dados.ufca.edu.br",
            "colecoes": {
                "docentes": {"resource_id": "6b2dbca5-58f8-472e-bc6a-eb827e631873"}
            },
        },
        "ufpi": {
            "dbpedia_pt": "http://pt.dbpedia.org/resource/Universidade_Federal_do_Piauí",
            "consumer": "CkanConsumer",
            "main_url": "https://dados.ufpi.br",
            "colecoes": {
                "docentes": {"resource_id": "a34d7d7e-30af-41f0-81cf-cd10b6f078bd"}
            },
        },
        "ufcspa": {
            "dbpedia_pt": "http://pt.dbpedia.org/resource/Universidade_Federal_de_Ciências_da_Saúde_de_Porto_Alegre",
            "consumer": "CkanConsumer",
            "main_url": "https://dados.ufcspa.edu.br",
            "colecoes": {
                "docentes": {"resource_id": "4286a4d5-9de7-4f88-bb37-f0f064415118", "q": "Professor"}
            },
        },
        "unifespa": {
            "dbpedia_pt": "http://pt.dbpedia.org/resource/Universidade_Federal_do_Sul_e_Sudeste_do_Pará",
            "consumer": "CkanConsumer",
            "main_url": "http://ckan.unifesspa.edu.br",
            "colecoes": {
                "docentes": {"resource_id": "eff99b8c-09d3-453b-b7dd-1de846ab18a7"}
            },
        },
        "ufv": {
            "dbpedia_pt": "http://pt.dbpedia.org/resource/Universidade_Federal_de_Viçosa",
            "consumer": "CkanConsumer",
            "main_url": "https://dados.ufv.br",
            "colecoes": {
                "docentes": {"resource_id": "a949a903-9536-4d20-87e5-cca5c217771a"}
            },
        },
        "ufsj": {
            "dbpedia_pt": "http://pt.dbpedia.org/resource/Universidade_Federal_de_S%C3%A3o_Jo%C3%A3o_del-Rei",
            "consumer": "CkanConsumer",
            "main_url": "http://dados.ufsj.edu.br",
            "colecoes": {
                "docentes": {"resource_id": "8e2e35ed-e255-4894-b070-ad8857366faf"}
            },
        },
        "ufms": {
            "dbpedia_pt": "http://pt.dbpedia.org/resource/Universidade_Federal_do_Mato_Grosso_do_Sul",
            "consumer": "CkanConsumer",
            "main_url": "https://dadosabertos.ufms.br/",
            "colecoes": {
                "docentes": {"resource_id": "a8ca7f30-0824-489b-8c70-faddcbd74f53","q": "Professor do Magisterio Superior"}
            },
        },
        "ufop": {
            "dbpedia_pt": "http://pt.dbpedia.org/resource/Universidade_Federal_de_Ouro_Preto",
            "consumer": "CkanConsumer",
            "main_url": "http://dados.ufop.br",
            "colecoes": {
                "docentes": {"resource_id": "04e65338-1b7f-45b7-893b-05470d17dcad"}
            },
        },
        "ifgoiano": {
            "dbpedia_pt": "http://pt.dbpedia.org/resource/Instituto_Federal_Goiano",
            "consumer": "CkanConsumer",
            "main_url": "https://dados.ifgoiano.edu.br",
            "colecoes": {
                "docentes": {"resource_id": "ecd0ad77-2125-42c4-a8d0-c3fe012731dd"}
            },
        },
        "ifms": {
            "consumer": "CkanConsumer",
            "main_url": "http://dados.ifms.edu.br",
            "dbpedia_pt": "http://pt.dbpedia.org/resource/Instituto_Federal_de_Mato_Grosso_do_Sul",
            "colecoes": {
                "docentes": {"resource_id": "4ccd20e6-703d-4682-a300-26a0e3788a4f"},
                "discentes": {"resource_id": "b8b4dfdf-98ef-4d57-baff-75c163be6e9a"},
            },
        },


        "ufma": {
            "consumer": "CkanConsumer",
            "main_url": "https://dadosabertos.ufma.br/",
            "dbpedia_pt": "http://pt.dbpedia.org/resource/Universidade_Federal_do_Maranhão",
            "colecoes": {
                "docentes": {"resource_id": "55a2d103-d73b-449e-85bc-655df7dfc45a"},
            },
        },

        "ifap": {
            "consumer": "CkanConsumer",
            "main_url": "http://dados.ifap.edu.br/",
            "dbpedia_pt": "http://pt.dbpedia.org/resource/Instituto_Federal_do_Amapá",
            "colecoes": {
                "docentes": {"resource_id": "005896e4-2a0a-4ddb-8420-62258d231871"},
            },
        },

        "univasf": { 
            "consumer": "CkanConsumer",
            "main_url": "http://dados.univasf.edu.br",
            "dbpedia_pt": "http://pt.dbpedia.org/resource/Universidade_Federal_do_Vale_do_São_Francisco",
            "colecoes": {
                "docentes": {"resource_id": "de111b8a-9b29-460e-acd3-7fda0ac62e41"},
            },
        },

        "ufvjm": { 
            "consumer": "CkanConsumer",
            "main_url": "https://dados.ufvjm.edu.br",
            "dbpedia_pt": "http://pt.dbpedia.org/resource/Universidade_Federal_dos_Vales_do_Jequitinhonha_e_Mucuri",
            "colecoes": {
                "docentes": {"resource_id": "0b9aa08a-e251-43d0-959a-e08de3d71e5a"},
            },
        },


        "ufgd": { 
            "consumer": "CkanConsumer",
            "main_url": "http://dadosabertos.ufgd.edu.br",
            "dbpedia_pt": "http://pt.dbpedia.org/resource/Universidade_Federal_da_Grande_Dourados",
            "colecoes": {
                "docentes": {"resource_id": "2249c447-7ae3-440a-afca-aa8ac8bb0596"},
            },
        },


        "uffs": { 
            "consumer": "CkanConsumer",
            "main_url": "https://dados.uffs.edu.br",
            "dbpedia_pt": "http://pt.dbpedia.org/resource/Universidade_Federal_da_Fronteira_Sul",
            "colecoes": {
                "docentes": {"resource_id": "1e801321-6e0b-4716-ba1d-ce79919e87da","q":"Professor"},
            },
        },

        "unifei": { 
            "consumer": "CkanConsumer",
            "main_url": "https://dados.unifei.edu.br",
            "dbpedia_pt": "http://pt.dbpedia.org/resource/Universidade_Federal_de_Itajubá",
            "colecoes": {
                "docentes": {"resource_id": "50024421-d377-4184-ac23-e7f0ee3ad2c1", "q": "Professor"},
            },
        },

        "ufpel": { 
            "consumer": "CkanConsumer",
            "main_url": "http://dados.ufpel.edu.br",
            "dbpedia_pt": "http://pt.dbpedia.org/resource/Universidade_Federal_de_Pelotas",
            "colecoes": {
                "docentes": {"resource_id": "b63c24da-d96d-4ee2-bdaf-f7a8c37f0007", "q" : "Professor"},
            },
        },

        "ifsuldeminas": { 
            "consumer": "CkanConsumer",
            "main_url": "https://dados.ifsuldeminas.edu.br",
            "dbpedia_pt": "http://pt.dbpedia.org/resource/Instituto_Federal_do_Sul_de_Minas",
            "colecoes": {
                "docentes": {"resource_id": "7db2014f-577f-4ec3-a6ab-2c7da2015f8b"},
            },
        },

        "ifpb": { 
            "consumer": "JSONConsumer",
            "main_url": "https://dados.ifpb.edu.br",
            "dbpedia_pt": "http://pt.dbpedia.org/resource/Instituto_Federal_da_Paraíba",
            "colecoes": {
                "docentes": {
                    "resource": "dataset/26d67876-0cb2-41a4-83ed-7bde06eb736c/resource/0d03ee6a-2af1-4dde-9b3d-90419c48fabe/download/servidores.json",
                    "key" : "cargo_emprego", "value" : "PROFESSOR"
                    },
            },
        },


        "ifrn": { 
            "consumer": "JSONConsumer",
            "main_url": "https://dados.ifrn.edu.br",
            "dbpedia_pt": "http://pt.dbpedia.org/resource/Instituto_Federal_do_Rio_Grande_do_Norte",
            "colecoes": {
                "docentes": {
                    "resource": "dataset/0c5c1c1a-7af8-4f24-ba37-a9eda0baddbb/resource/c3f64d5b-f2df-4ef2-8e27-fb4f10a7c3ea/download/dados_extraidos_recursos_servidores.json",
                    "key" : "cargo", "value" : "PROFESSOR"
                    },
            },
        },
    },
}

instituicoes = config_dags["instituicoes"].items()
print ("qt instituticoes ", len(instituicoes))

instituicoes = {k: v for k, v in instituicoes  if k == "ifpb"}
import requests



for institute, values in instituicoes.items():
    collections = values["colecoes"]
    main_url = values["main_url"]
    dbpedia_url = values["dbpedia_pt"]

    consumer = getattr(consumers, values['consumer']) (main_url)

    for collection, params in collections.items():
        print (institute,collection,params)
        
        data = extract(consumer, params)
        print (data)
        generic_mapper = config_dags["mapeamento"][collection]
        
        data = transform (data, generic_mapper, dbpedia_url)
        print (data)
        class_ = getattr(utils.models, collection.capitalize()) 
        ttl = serialize_to_rdf(data, class_)
        print (ttl)