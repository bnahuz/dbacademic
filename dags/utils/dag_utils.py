from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from simpot.serialize import mapper_all,  serialize_to_rdf, serialize_to_rdf_file

from utils.mongo import get_mongo_db, insert_many, drop_collection
import utils.consumers as consumers
import utils.models

import os

from airflow.models import Variable


# a partir de um dado, e de um mapper generico, retorna um mapper específico
def mapper_generate (obj, mapeamento):
    new_map = {}
    for collumn, list_collumn in mapeamento.items():
        for current in list_collumn:
            if current in obj.keys():
                new_map[collumn] = current
                break
    return new_map

def append_key_value (data, key, value):
    return list(map(lambda x: {**x, key: value}, data))

def save_content_to_file(file, content):
    print(f'[INFO] - Saving file {file}')
    with open(file, 'w') as f:
        f.write(content)


def extract (consumer, params):
    data = consumer.request(**params).to_dict('records')
    return data

def transform (data, gen_mapper, dbpedia_url):
    mapper = mapper_generate (data[0], gen_mapper)   
    data = mapper_all(mapper, data)
    return append_key_value (data, "instituicao", dbpedia_url)

def dynamic_elt(institute, collection, consumer, params, generic_mapper, dbpedia_url):
    data = extract (consumer, params)
    data = transform(data, generic_mapper[collection], dbpedia_url)
    insert_many(get_mongo_db(institute),collection,data)
    return f"Inserted {collection} in {institute} {data[0:100]}"
 

def dynamic_ttl (institute, collection, model_class):
    db = get_mongo_db(institute)
    mongo_collection = db[collection]
    documents = list(mongo_collection.find())
    content = serialize_to_rdf(documents, model_class)
    local_save = int(Variable.get("local_save", default_var=0))
    if local_save:
        filename = f"/opt/airflow/download/{institute}_{collection}.ttl"
        save_content_to_file(filename, content)
    return {"ok": content[0:200] }


#####################################################
###
####################################################

def dynamic_create_dag(dag_id:str, institute, conf, generic_mapper, schedule_interval, start_date, default_args):
    collections = conf["colecoes"]
    main_url = conf["main_url"]
    dbpedia_url = conf["dbpedia_pt"]
    extract_total = int (Variable.get("extract_total", default_var=10))
    consumer = getattr(consumers, conf['consumer']) (main_url, extract_total)
    

    dag = DAG(
        f'{dag_id}_etl',
        default_args=default_args,
        description=f'A simple DAG to extract data from {institute} API',
        schedule_interval=schedule_interval,
        start_date=start_date
    )


    drop_task = []
    for collection, paramns in collections.items():
        task = PythonOperator(
            task_id=f'drop_{collection}',
            python_callable=drop_collection,
            op_kwargs={'institute': institute, 'collection_name': collection},
            dag=dag,
        )
        drop_task.append(task)
    
    
    elt_task = []
    for collection, params in collections.items():
        task = PythonOperator(
            task_id=f'run_intake_{collection}',
            python_callable= dynamic_elt,
            op_kwargs={'institute':institute,'collection':collection, 'consumer': consumer, 'params':params, "generic_mapper":generic_mapper, "dbpedia_url":dbpedia_url},
            dag=dag,
        )
        elt_task.append(task)


    ttl_task = []

   
    for collection, params in collections.items():
        class_ = getattr(utils.models, collection.capitalize()) 
        oper = PythonOperator(
            task_id=f'transform_{collection}',
            python_callable= dynamic_ttl,
            op_kwargs={"institute":institute, "collection":collection, "model_class":class_},
            dag=dag,
        )
        ttl_task.append(oper)

    chain(drop_task, elt_task,ttl_task)
    
    
    return dag