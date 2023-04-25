import sys
import pandas as pd
sys.path.append('/opt/airflow/')
from plugins.consumers.CkanConsumer import CkanConsumer
from plugins.utils.mongo import get_mongo_db, insert_many

class ufca:
    def __init__(self):
        self.ufca_consumer = CkanConsumer('dados.ufca.edu.br', secure=True)
        self.name = 'UFCA'

    #Curso
    def etl_courses(self):
        cursos_ufca = self.ufca_consumer.request('5f31e620-a366-42c9-a54c-96da666c93b7')
        insert_many(get_mongo_db('ufca'),'cursos',cursos_ufca.to_dict('records'))
        return "Inserted courses"

    #Discente
    def etl_docentes(self):
        docentes_ufca = self.ufca_consumer.request('6b2dbca5-58f8-472e-bc6a-eb827e631873')
        insert_many(get_mongo_db('ufca'),'docentes',docentes_ufca.to_dict('records'))
        return 'Inserted docentes'