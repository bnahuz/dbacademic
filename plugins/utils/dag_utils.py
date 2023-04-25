from airflow import DAG
from airflow.operators.python import PythonOperator
from plugins.institutes.ufpi import ufpi
from plugins.institutes.ufrn import ufrn
from plugins.institutes.ifms import ifms
from plugins.institutes.ufca import ufca
from plugins.utils.mongo import drop_collection
from airflow.models.baseoperator import chain

from plugins.consumers.CkanConsumer import CkanConsumer


from plugins.utils.mongo import get_mongo_db, insert_many, drop_collection

def dynamic_drop(task_id:str, insitute:str, collection:str, dag:DAG):
    return PythonOperator(
        task_id=task_id,
        python_callable=drop_collection,
        op_kwargs={'institute': insitute, 'collection_name': collection},
        dag=dag,
    )


def save_collection_closure(instituicao,collection, params):
    def cl ():
        return save_collection(instituicao,collection, consummer, mapper)
    return cl

def save_collection(instituicao, colecao, params):
    dados = consumer.request()
    #dados = mapper_all(mapper, data)
    if (params["consumer"] == "ckan"):
            ckan_params = params["consumer_params"]
            consumer = CkanConsumer(ckan_params["main_url"])
            dados = consumer.request(ckan_params["resource_id"]) # nao sei se precisaria do pandas
            dados = dados.to_dict('records')
            dados = dados[1:10]
            print (dados)           
            #insert_many(get_mongo_db(instituicao),colecao,dados)
            return f"Inserted {colecao} in {instituicao} {dados}"
    else:
            return "fail to inserted"



def dynamic_create_dag(dag_id:str, institute, collections, schedule_interval, start_date, default_args):
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
            python_callable=save_collection_closure (institute,collection, params),
            dag=dag,
        )
        elt_task.append(task)

    chain(drop_task, elt_task)
    
    return dag