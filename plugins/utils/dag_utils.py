from airflow import DAG
from airflow.operators.python import PythonOperator
from plugins.institutes.ufpi import ufpi
from plugins.institutes.ufrn import ufrn
from plugins.utils.mongo import drop_collection, get_mongo_db

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
        description=f'A simple DAG to extract data from {institute.__name__.upper} API',
        schedule_interval=schedule_interval,
        start_date=start_date
    )

    drop_task = []
    for collection in collections:
        task = dynamic_drop(f'drop_{collection}', str(institute.__name__), collection, dag)
        drop_task.append(task)

    # Run each intake function and save data in MongoDB
    elt_task = []
    for collection in collections:
        task = PythonOperator(
            task_id=f'run_intake_{collection}',
            python_callable=getattr(institute, f'etl_{collection}'),
            dag=dag,
        )
        elt_task.append(task)
    
    drop_task >> elt_task[0] >> elt_task[1] >> elt_task[2] #Necessário melhorar essa parte
    
    return dag