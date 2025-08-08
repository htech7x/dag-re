from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def print_hello():
    print(" Hello Alex! ")


with DAG(
    dag_id="simple_hello_dag",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["print hello"],
) as dag:

    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=print_hello,
    )
