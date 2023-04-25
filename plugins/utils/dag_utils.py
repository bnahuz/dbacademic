from airflow import DAG
from airflow.operators.python import PythonOperator
from plugins.institutes.ufpi import ufpi
from plugins.institutes.ufrn import ufrn
from plugins.institutes.ifms import ifms
from plugins.institutes.ufca import ufca
from plugins.utils.mongo import drop_collection
from airflow.models.baseoperator import chain

def dynamic_drop(task_id:str, insitute:str, collection:str, dag:DAG):
    return PythonOperator(
        task_id=task_id,
        python_callable=drop_collection,
        op_kwargs={'institute': insitute, 'collection_name': collection},
        dag=dag,
    )


def dynamic_create_dag(dag_id:str, institute, collections:list, schedule_interval, start_date, default_args):
    dag = DAG(
        f'{dag_id}_etl',
        default_args=default_args,
        description=f'A simple DAG to extract data from {institute.name} API',
        schedule_interval=schedule_interval,
        start_date=start_date
    )

    drop_task = []
    for collection in collections:
        task = dynamic_drop(f'drop_{collection}', str(institute.name), collection, dag)
        drop_task.append(task)

    elt_task = []
    for collection in collections:
        task = PythonOperator(
            task_id=f'run_intake_{collection}',
            python_callable=getattr(institute, f'etl_{collection}'),
            dag=dag,
        )
        elt_task.append(task)

    chain(drop_task, elt_task)
    
    return dag