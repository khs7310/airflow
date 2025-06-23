from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime, timedelta
import random

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Python 함수 예시
def print_hello():
    print("Hello from Python function!")
    return "Hello World!"

def process_data():
    data = [1, 2, 3, 4, 5]
    result = sum(data)
    print(f"Processed data: {data}, Sum: {result}")
    return result

def random_task():
    number = random.randint(1, 100)
    print(f"Random number generated: {number}")
    if number > 50:
        raise Exception("Random number is too high!")
    return number

# DAG 정의
with DAG(
    'simple_example_dag',
    default_args=default_args,
    description='A simple example DAG with various operators',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['example', 'tutorial'],
) as dag:

    # 시작 태스크
    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    # Bash 명령어 실행
    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "Hello from Bash!" && date',
        dag=dag,
    )

    # Python 함수 실행
    python_task = PythonOperator(
        task_id='python_task',
        python_callable=print_hello,
        dag=dag,
    )

    # 데이터 처리 태스크
    process_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        dag=dag,
    )

    # 랜덤 태스크 (가끔 실패)
    random_task_op = PythonOperator(
        task_id='random_task',
        python_callable=random_task,
        dag=dag,
    )

    # Kubernetes Pod 실행 (airflow namespace 사용)
    k8s_task = KubernetesPodOperator(
        task_id='k8s_python_task',
        namespace='airflow',
        name='python-hello',
        image='python:3.11-slim',
        cmds=['python', '-c'],
        arguments=['print("Hello from Kubernetes Pod!"); import sys; sys.exit(0)'],
        image_pull_policy='IfNotPresent',
        in_cluster=True,
        is_delete_operator_pod=True,
        get_logs=True,
        dag=dag,
    )

    # 종료 태스크
    end = DummyOperator(
        task_id='end',
        dag=dag,
    )

    # 태스크 의존성 설정
    start >> bash_task >> python_task >> process_task >> random_task_op >> k8s_task >> end 