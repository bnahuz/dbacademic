import sys
import pandas as pd
sys.path.append('/opt/airflow/')
from utils.mappers import *
from consumers.CkanConsumer import CkanConsumer
from utils.mongo import get_mongo_db, insert_many, drop_collection

class ifms:
    def __init__(self):
        self.name = 'IFMS'
        self.ifsm_consumer = CkanConsumer('dados.ifms.edu.br', secure=False)

    #Curso
    def etl_courses(self):
        cursos_ifsm = self.ifsm_consumer.request('b1913941-fcd6-4216-882f-fc2a81121bcc')
        cursos_ifsm = mapper_cursos(cursos_ifsm, 'IFMS', 'b1913941-fcd6-4216-882f-fc2a81121bcc', 'curso', '_id')
        insert_many(get_mongo_db('ifsm'),'cursos',cursos_ifsm.to_dict('records'))
        return "Inserted courses"

    #Discente
    def etl_discentes(self):
        discentes_ifsm = self.ifsm_consumer.request('b8b4dfdf-98ef-4d57-baff-75c163be6e9a')
        discentes_ifsm = trunc_date(discentes_ifsm, 'data_inicio', 'ano_periodo')
        discentes_ifsm = mapper_discentes(discentes_ifsm, 'IFMS', 'b8b4dfdf-98ef-4d57-baff-75c163be6e9a', 'nome', 'ra', 'sexo', 'ano_ingresso', 'periodo_ingresso', 'curso')
        insert_many(get_mongo_db('ifsm'),'discentes',discentes_ifsm.to_dict('records'))
        return 'Inserted students'