from airflow import DAG
from airflow.operators.python import PythonOperator
from plugins.utils.mongo import drop_collection, get_mongo_db

def dynamic_drop(task_id, insitute, collection, dag):
    return PythonOperator(
        task_id=task_id,
        python_callable=drop_collection,
        op_kwargs={'institute': insitute, 'collection_name': collection},
        dag=dag,
    )