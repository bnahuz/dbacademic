from airflow import DAG
from airflow.operators.python import PythonOperator
#from institutes.ufpi import ufpi
#from institutes.ufrn import ufrn
#from institutes.ifms import ifms
#from institutes.ufca import ufca
from utils.mongo import drop_collection
from airflow.models.baseoperator import chain

import consumers

import rdf.models

#from consumers.CkanConsumer import CkanConsumer


from simpot.serialize import mapper_all,  serialize_to_rdf, serialize_to_rdf_file

from utils.mongo import get_mongo_db, insert_many, drop_collection


# a partir de um dado, e de um mapper generico, retorna um mapper específico
def mapper_generate (obj, mapeamento):
    new_map = {}
    for collumn, list_collumn in mapeamento.items():
        for current in obj.keys():
            if current in list_collumn:
                new_map[collumn] = current
                break
    return new_map


def dynamic_drop(task_id:str, insitute:str, collection:str, dag:DAG):
    return PythonOperator(
        task_id=task_id,
        python_callable=drop_collection,
        op_kwargs={'institute': insitute, 'collection_name': collection},
        dag=dag,
    )

def extract (instituicao, colecao, conf):
    params = conf['params']
    print (conf['consumer'])
    consumer = getattr(consumers, conf['consumer']) (**params)
    return consumer.request().to_dict('records')

def transform (data, gen_mapper):
    #print (gen_mapper)
    mapper = mapper_generate (data[0], gen_mapper)   
    #print (mapper)   
    return mapper_all(mapper, data)


def dynamic_elt(instituicao, colecao, conf, mapeamento):

    def f():
           data = extract (instituicao, colecao, conf)
           data = transform(data, mapeamento[colecao])
           insert_many(get_mongo_db(instituicao),colecao,data)
           return f"Inserted {colecao} in {instituicao} {data[0:100]}"
        
    return f


def dynamic_ttl (instituicao, colecao, class_):

    def f():
        db = get_mongo_db(instituicao)
        mongo_collection = db[colecao]
        documents = list(mongo_collection.find())
        # depois ira salvar no google drive
        content = serialize_to_rdf(documents, class_)
        #filename = f"/opt/airflow/download/{instituicao}_{colecao}.ttl"
        #save_content_to_file(filename, content)
        return {"ok": content[0:200] }


    return f

def dynamic_create_dag(dag_id:str, institute, collections, generic_mapper, schedule_interval, start_date, default_args):
    dag = DAG(
        f'{dag_id}_etl',
        default_args=default_args,
        description=f'A simple DAG to extract data from {institute} API',
        schedule_interval=schedule_interval,
        start_date=start_date
    )


    drop_task = []
    for collection, paramns in collections.items():
        task = dynamic_drop(f'drop_{collection}', str(institute), collection, dag)
        drop_task.append(task)
    
    
    elt_task = []
    for collection, params in collections.items():
        task = PythonOperator(
            task_id=f'run_intake_{collection}',
            python_callable= dynamic_elt (institute,collection, params, generic_mapper),
            dag=dag,
        )
        elt_task.append(task)


    ttl_task = []
   
    for collection, params in collections.items():
        class_ = getattr(rdf.models, collection.capitalize()) 
        oper = PythonOperator(
            task_id=f'transform_{collection}',
            # com lambda parece que ele mantem a referencia aos dados
            python_callable= dynamic_ttl (institute,collection, class_),
            dag=dag,
        )
        ttl_task.append(oper)

    chain(drop_task, elt_task,ttl_task)
    
    
    return dag