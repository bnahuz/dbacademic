import sys
import pandas as pd
sys.path.append('/opt/airflow/')
from etl.extraction.consumers.UFRNConsumer import UFRN_Consumer
from etl.utils.mongo import get_mongo_db, insert_many, drop_collection
from datetime import datetime,timedelta

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

#Docente
def get_docentes() -> pd.DataFrame:
    docentes = "dataset/8bf1a468-48ff-4f4d-95ee-b17b7a3a5592/resource/6a8e5461-e748-45c6-aac6-432188d88dde/download/docentes.csv"
    dataframe = UFRN_Consumer(docentes).request(data_type='csv')
    insert_many(get_mongo_db('ufrn'),'docentes',dataframe.to_dict('records'))
    return 'Inserted docentes'

#Curso
def get_courses() -> pd.DataFrame:
    courses = "dataset/08b0dc59-faa9-4281-bd1e-2a39f532489e/resource/949be3d1-e85b-4d0f-9f60-1d9a7484bb06/download/cursos-ufrn.csv"
    dataframe = UFRN_Consumer(courses).request(data_type='csv')
    insert_many(get_mongo_db('ufrn'),'cursos',dataframe.to_dict('records'))
    return "Inserted courses"

#Departamento

#Centro
def get_units() -> pd.DataFrame:
    units = "dataset/da6451a5-1a59-4630-bdc2-97f6be4a59c2/resource/3f2e4e32-ef1a-4396-8037-cbc22a89d97f/download/unidades.csv"
    dataframe = UFRN_Consumer(units).request(data_type='csv')
    insert_many(get_mongo_db('ufrn'),'unidades',dataframe.to_dict('records'))
    return 'Inserted units'

#Grupos de Pesquisa
def get_research_groups() -> pd.DataFrame:
    papers = "dataset/e8835880-9cc8-4d36-b578-af09d16fa5e4/resource/09951a7c-46c4-4d1b-a537-2e50caa070c4/download/grupos-de-pesquisa.csv"
    dataframe = UFRN_Consumer(papers).request(data_type='csv')
    insert_many(get_mongo_db('ufrn'),'grupos_pesquisa',dataframe.to_dict('records'))
    return 'Inserted research groups'

#Monografias
def get_tccs() -> pd.DataFrame:
    tccs = "dataset/69a3c9af-86d6-41c5-9cee-846974770b68/resource/7c01071b-81a4-4793-9a63-acfcd8a1aa83/download/tccs.csv"
    dataframe = UFRN_Consumer(tccs).request(data_type='csv')
    insert_many(get_mongo_db('ufrn'),'tccs',dataframe.to_dict('records'))
    return 'Inserted tccs'

#Discente
def get_students() -> pd.DataFrame:
    year = 2023
    students = f"/dataset/554c2d41-cfce-4278-93c6-eb9aa49c5d16/resource/14afbb6c-395e-411c-b24d-0e494cb95866/download/discentes-{year}.csv"
    dataframe = UFRN_Consumer(students).request(data_type='csv')
    insert_many(get_mongo_db('ufrn'),'discentes',dataframe.to_dict('records'))
    return 'Inserted students'

def save_dataframe(dataframe : pd.DataFrame, file_name : str, format : str) -> None:
    if format == 'csv':
        dataframe.to_csv(f'{file_name}.csv', index=False)
    elif format == 'json':
        dataframe.to_json(f'{file_name}.json', orient='records')

def get_pids(period_date : str = None, historical : bool = True) -> str:
    endpoint = "dataset/ea03b616-16e0-4ddd-afde-4f21d7343540/resource/2e65d53c-b494-4d73-b18b-9989947e7e08/download/pids-{year}.json" 
    present_year = datetime.today().strftime("%Y")
    if historical:
        for year in range(2009,int(present_year) + 1):
            data1 = UFRN_Consumer(endpoint.format(f"{year}1")).request(data_type="json_api")
            data2 = UFRN_Consumer(endpoint.format(f"{year}2")).request(data_type="json_api")
            data_result = data1 + data2
    else:
        present_month = datetime.today().strftime("%m")
        if int(present_month) <= 6:
            period = f"{present_year}1"
        else:
            period = f"{present_year}2"
