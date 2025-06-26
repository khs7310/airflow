from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime

with DAG(
    dag_id='minimal_k8s_nodeSelect_pod',
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    run_pod = KubernetesPodOperator(
        task_id='run_pod_task',
        namespace='default',
        name='hello-python',
        image='python:3.11-slim',
        cmds=['python', '-c'],  # python -c "print('Hello KubernetesPodOperator!')"
        arguments=['print("Hello KubernetesPodOperator!")'],
        image_pull_policy='IfNotPresent',
        in_cluster=True,
        is_delete_operator_pod=True,
        get_logs=True,
        node_selector={
            'eks.amazonaws.com/nodegroup': 'ops-system-2023-2025062606325324670000000a',
        },
    )