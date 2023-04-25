import sys
sys.path.append('/opt/airflow/')
from plugins.consumers.CkanConsumer import CkanConsumer
from plugins.utils.mongo import get_mongo_db, insert_many
from plugins.utils.mappers import *

class ufrn:
    def __init__(self):
        self.ufrn_consumer = CkanConsumer('dados.ufrn.br')
        self.name = 'UFRN'

    #Docente
    def etl_docentes(self):
        docentes_ufrn = self.ufrn_consumer.request('6a8e5461-e748-45c6-aac6-432188d88dde')
        docentes_ufrn = trunc_date(docentes_ufrn,'admissao','ano_periodo')
        docentes_ufrn = mapper_docentes(docentes_ufrn,'UFRN','6a8e5461-e748-45c6-aac6-432188d88dde','nome','siape','sexo','ano_ingresso','periodo_ingresso','lotacao')
        insert_many(get_mongo_db('ufrn'),'docentes',docentes_ufrn.to_dict('records'))
        return 'Inserted docentes'

    #Curso
    def etl_courses(self):
        cursos_ufrn = self.ufrn_consumer.request('a10bc434-9a2d-491a-ae8c-41cf643c35bc')
        cursos_ufrn = mapper_cursos(cursos_ufrn,'UFRN','a10bc434-9a2d-491a-ae8c-41cf643c35bc','nome','id_curso')
        insert_many(get_mongo_db('ufrn'),'cursos',cursos_ufrn.to_dict('records'))
        return "Inserted courses"

    #Discente
    def etl_discentes(self):
        discentes_ufrn = self.ufrn_consumer.request('14afbb6c-395e-411c-b24d-0e494cb95866')
        discentes_ufrn = mapper_discentes(discentes_ufrn,'UFRN','14afbb6c-395e-411c-b24d-0e494cb95866','nome_discente','matricula','sexo','ano_ingresso','periodo_ingresso','nome_curso')
        insert_many(get_mongo_db('ufrn'),'discentes',discentes_ufrn.to_dict('records'))
        return 'Inserted students'