"""
무조건 에러가 나는 Kubernetes Operator DAG
이 DAG는 테스트 목적으로 다양한 종류의 에러를 의도적으로 발생시킵니다.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# DAG 기본 설정
default_args = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # 재시도 없음으로 설정
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=5),  # 5분 실행 타임아웃 추가
}

# DAG 정의
dag = DAG(
    'must_error_dag',
    default_args=default_args,
    description='무조건 에러가 나는 Kubernetes Operator DAG',
    schedule=None,  # 수동 실행만
    catchup=False,
    tags=['test', 'error', 'kubernetes'],
)

def python_error_task():
    """Python 에러를 발생시키는 함수"""
    raise Exception("의도적으로 발생시킨 Python 에러입니다!")

# Task 1: 존재하지 않는 이미지로 KubernetesPodOperator 실행
k8s_error_task_1 = KubernetesPodOperator(
    task_id='k8s_nonexistent_image_error',
    name='error-pod-1',
    namespace='default',
    image='nonexistent/invalid-image:latest',  # 존재하지 않는 이미지
    cmds=['echo'],
    arguments=['This will fail because image does not exist'],
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# Task 2: 잘못된 명령어로 KubernetesPodOperator 실행
k8s_error_task_2 = KubernetesPodOperator(
    task_id='k8s_invalid_command_error',
    name='error-pod-2',
    namespace='default',
    image='alpine:latest',
    cmds=['invalid_command_that_does_not_exist'],  # 존재하지 않는 명령어
    arguments=['--fail'],
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# Task 3: 잘못된 네임스페이스로 KubernetesPodOperator 실행
k8s_error_task_3 = KubernetesPodOperator(
    task_id='k8s_invalid_namespace_error',
    name='error-pod-3',
    namespace='nonexistent-namespace',  # 존재하지 않는 네임스페이스
    image='alpine:latest',
    cmds=['echo'],
    arguments=['This will fail because namespace does not exist'],
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# Task 4: 메모리 부족을 일으키는 KubernetesPodOperator
k8s_error_task_4 = KubernetesPodOperator(
    task_id='k8s_memory_error',
    name='error-pod-4',
    namespace='default',
    image='alpine:latest',
    cmds=['sh'],
    arguments=['-c', 'yes | tr \\n x | head -c 1000000000 | grep n'],  # 메모리 과다 사용
    container_resources={'memory': '100Mi', 'cpu': '100m'},
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# Task 5: 종료 코드 1로 실패하는 KubernetesPodOperator
k8s_error_task_5 = KubernetesPodOperator(
    task_id='k8s_exit_code_error',
    name='error-pod-5',
    namespace='default',
    image='alpine:latest',
    cmds=['sh'],
    arguments=['-c', 'echo "Starting task..."; sleep 2; echo "Failing now!"; exit 1'],  # 의도적 실패
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# Task 6: Python 에러 태스크
python_error_task_op = PythonOperator(
    task_id='python_error_task',
    python_callable=python_error_task,
    dag=dag,
)

# Task 7: Bash 명령어 에러
bash_error_task = BashOperator(
    task_id='bash_error_task',
    bash_command='exit 1',  # 의도적으로 에러 코드 반환
    dag=dag,
)

# Task 8: 존재하지 않는 Bash 명령어
bash_invalid_command = BashOperator(
    task_id='bash_invalid_command',
    bash_command='nonexistent_command_that_will_fail',
    dag=dag,
)

# Task 간 의존성 설정 (순차 실행으로 모든 에러 확인)
k8s_error_task_1 >> k8s_error_task_2 >> k8s_error_task_3 >> k8s_error_task_4 >> k8s_error_task_5 >> python_error_task_op >> bash_error_task >> bash_invalid_command
