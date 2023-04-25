import sys
sys.path.append('/opt/airflow/')
from consumers.CkanConsumer import CkanConsumer
from utils.mongo import get_mongo_db, insert_many, drop_collection
from utils.mappers import *


class ufpi:
    def __init__(self):
        #Dados desatualizados desde 2018
        self.ufpi_consumer = CkanConsumer('dados.ufpi.br', secure=True)
        self.name = 'UFPI'
        
    #Curso
    def etl_courses(self):
        cursos_ufpi = self.ufpi_consumer.request('afb0de00-8f39-4f9c-9197-4364f7312a98')
        cursos_ufpi = mapper_cursos(dataframe=cursos_ufpi, instituto='UFPI', resource_id='afb0de00-8f39-4f9c-9197-4364f7312a98', nome_curso='nome', id_curso='id_curso')
        insert_many(get_mongo_db('ufpi'),'cursos',cursos_ufpi.to_dict('records'))
        return "Inserted courses"

    #Discente
    def etl_docentes(self):
        docentes_ufpi = self.ufpi_consumer.request('a34d7d7e-30af-41f0-81cf-cd10b6f078bd')
        docentes_ufpi = trunc_date(df=docentes_ufpi, coluna_datetime='admissao', type='ano_periodo')
        docentes_ufpi = mapper_docentes(dataframe=docentes_ufpi, instituto='UFPI', resource_id='a34d7d7e-30af-41f0-81cf-cd10b6f078bd', nome_docente='nome', siape='siape', sexo='sexo', ano_ingresso='ano_ingresso', periodo_ingresso='periodo_ingresso', lotacao='lotacao')
        insert_many(get_mongo_db('ufpi'),'docentes',docentes_ufpi.to_dict('records'))
        return 'Inserted docentes'

    #Discente
    def etl_discentes(self):
        discentes_ufpi = self.ufpi_consumer.request('00658685-cd1b-47f4-9f42-107c45a1e4e1')
        discentes_ufpi = mapper_discentes(dataframe=discentes_ufpi, instituto='UFPI', resource_id='00658685-cd1b-47f4-9f42-107c45a1e4e1', nome_discente='nome', matricula='matricula', sexo='sexo', ano_ingresso='ano_ingresso', periodo_ingresso='periodo_ingresso', nome_curso='curso')
        insert_many(get_mongo_db('ufpi'),'discentes',discentes_ufpi.to_dict('records'))
        return 'Inserted discentes'