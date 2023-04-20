import sys
import pandas as pd
sys.path.append('/opt/airflow/')
from plugins.consumers.CkanConsumer import CkanConsumer
from plugins.utils.mongo import get_mongo_db, insert_many, drop_collection

def drop_from_ufrn_db_discentes() -> dict:
    return drop_collection(get_mongo_db('ufrn'),'discentes')

def drop_from_ufrn_db_docentes() -> dict:
    return drop_collection(get_mongo_db('ufrn'),'docentes')

def drop_from_ufrn_db_tccs() -> dict:
    return drop_collection(get_mongo_db('ufrn'),'tccs')

def drop_from_ufrn_db_cursos() -> dict:
    return drop_collection(get_mongo_db('ufrn'),'cursos')

def drop_from_ufrn_db_grupos_pesquisa() -> dict:
    return drop_collection(get_mongo_db('ufrn'),'grupos_pesquisa')

def drop_from_ufrn_db_unidades() -> dict:
    return drop_collection(get_mongo_db('ufrn'),'unidades')

ufrn_consumer = CkanConsumer('http://dados.ufrn.br')

#Docente
def etl_docentes() -> pd.DataFrame:
    docentes_ufrn = ufrn_consumer.request('6a8e5461-e748-45c6-aac6-432188d88dde')
    insert_many(get_mongo_db('ufrn'),'docentes',docentes_ufrn.to_dict('records'))
    return 'Inserted docentes'

#Curso
def etl_courses() -> pd.DataFrame:
    cursos_ufrn = ufrn_consumer.request('a10bc434-9a2d-491a-ae8c-41cf643c35bc')
    insert_many(get_mongo_db('ufrn'),'cursos',cursos_ufrn.to_dict('records'))
    return "Inserted courses"

#Discente
def etl_discentes() -> pd.DataFrame:
    discentes_ufrn = ufrn_consumer.request('14afbb6c-395e-411c-b24d-0e494cb95866')
    insert_many(get_mongo_db('ufrn'),'discentes',discentes_ufrn.to_dict('records'))
    return 'Inserted students'

""" #Centro
def get_units() -> pd.DataFrame:
    units = "dataset/da6451a5-1a59-4630-bdc2-97f6be4a59c2/resource/3f2e4e32-ef1a-4396-8037-cbc22a89d97f/download/unidades.csv"
    dataframe = UFRN_Consumer(units).request(data_type='csv')
    insert_many(get_mongo_db('ufrn'),'unidades',dataframe.to_dict('records'))
    return 'Inserted units' """

#Grupos de Pesquisa
""" def get_research_groups() -> pd.DataFrame:
    papers = "dataset/e8835880-9cc8-4d36-b578-af09d16fa5e4/resource/09951a7c-46c4-4d1b-a537-2e50caa070c4/download/grupos-de-pesquisa.csv"
    dataframe = UFRN_Consumer(papers).request(data_type='csv')
    insert_many(get_mongo_db('ufrn'),'grupos_pesquisa',dataframe.to_dict('records'))
    return 'Inserted research groups' """

""" #Monografias
def get_tccs() -> pd.DataFrame:
    tccs = "dataset/69a3c9af-86d6-41c5-9cee-846974770b68/resource/7c01071b-81a4-4793-9a63-acfcd8a1aa83/download/tccs.csv"
    dataframe = UFRN_Consumer(tccs).request(data_type='csv')
    insert_many(get_mongo_db('ufrn'),'tccs',dataframe.to_dict('records'))
    return 'Inserted tccs' """