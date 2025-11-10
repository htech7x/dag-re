from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="example_kpo_basic",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    run_in_pod = KubernetesPodOperator(
        task_id="say_hello",
        name="hello-pod",
        namespace="default",  # change if needed
        image="python:3.9",
        cmds=["python", "-c"],
        arguments=["print('Hello from Kubernetes Pod!')"],
    )
