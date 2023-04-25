import sys
import pandas as pd
sys.path.append('/opt/airflow/')
from plugins.consumers.CkanConsumer import CkanConsumer
from plugins.utils.mongo import get_mongo_db, insert_many, drop_collection


class ifms:
    def __init__(self):
        self.name = 'IFMS'
        self.ifsm_consumer = CkanConsumer('dados.ifms.edu.br', secure=False)

    #Curso
    def etl_courses(self):
        cursos_ifsm = self.ifsm_consumer.request('b1913941-fcd6-4216-882f-fc2a81121bcc')
        insert_many(get_mongo_db('ifsm'),'cursos',cursos_ifsm.to_dict('records'))
        return "Inserted courses"

    #Discente
    def etl_discentes(self):
        discentes_ifsm = self.ifsm_consumer.request('b8b4dfdf-98ef-4d57-baff-75c163be6e9a')
        insert_many(get_mongo_db('ifsm'),'discentes',discentes_ifsm.to_dict('records'))
        return 'Inserted students'