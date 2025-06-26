from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

# Python 함수 예시
def print_hello():
    print("Hello, Airflow!")
    return "Hello, Airflow!"

def print_goodbye():
    print("Goodbye, Airflow!")
    return "Goodbye, Airflow!"

def send_notification():
    print("Notification: Airflow 작업이 완료되었습니다.")
    return "Notification sent"

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'email_on_failure': False,  # 이메일 알림 비활성화
    'email_on_retry': False,
}

with DAG(
    dag_id='basic_operators_example',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['example', 'basic'],
    description='Bash, Python, Dummy Operator 예제',
) as dag:
    # 1. 시작 DummyOperator
    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    # 2. BashOperator: Bash 명령 실행
    bash_task = BashOperator(
        task_id='run_bash',
        bash_command='echo "Hello from BashOperator!" && date',
        dag=dag,
    )

    # 3. PythonOperator: Python 함수 실행
    python_task = PythonOperator(
        task_id='run_python',
        python_callable=print_hello,
        dag=dag,
    )

    # 4. Notification PythonOperator: 알림 대체
    notification_task = PythonOperator(
        task_id='send_notification',
        python_callable=send_notification,
        dag=dag,
    )

    # 5. 종료 DummyOperator
    end = DummyOperator(
        task_id='end',
        dag=dag,
    )

    # 태스크 의존성 설정
    start >> bash_task >> python_task >> notification_task >> end 