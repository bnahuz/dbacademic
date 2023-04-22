import sys
import pandas as pd
sys.path.append('/opt/airflow/')
from plugins.consumers.CkanConsumer import CkanConsumer
from plugins.utils.mongo import get_mongo_db, insert_many, drop_collection

#Dados desatualizados desde 2018
ufpi_consumer = CkanConsumer('dados.ufpi.br', secure=True) 

#https://dados.ufpi.br/dataset/efe01f87-fd4c-4cc1-a081-5c8d29cf7be0/resource/00658685-cd1b-47f4-9f42-107c45a1e4e1/download/discentesgraduacaoativos2bimestre.csv


#Curso
def etl_courses():
    cursos_ufpi = ufpi_consumer.request('afb0de00-8f39-4f9c-9197-4364f7312a98')
    insert_many(get_mongo_db('ufpi'),'cursos',cursos_ufpi.to_dict('records'))
    return "Inserted courses"

#Discente
def etl_docentes():
    docentes_ufpi = ufpi_consumer.request('a34d7d7e-30af-41f0-81cf-cd10b6f078bd')
    insert_many(get_mongo_db('ufpi'),'docentes',docentes_ufpi.to_dict('records'))
    return 'Inserted docentes'

#Discente
def etl_discentes():
    discentes_ufpi = ufpi_consumer.request('00658685-cd1b-47f4-9f42-107c45a1e4e1')
    insert_many(get_mongo_db('ufpi'),'discentes',discentes_ufpi.to_dict('records'))
    return 'Inserted d'