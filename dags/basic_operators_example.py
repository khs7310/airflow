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

def cpu_intensive_task():
    """CPU 집약적인 작업을 수행하는 함수"""
    print("CPU 집약적인 작업을 시작합니다...")
    
    # 소수 계산 (CPU 집약적)
    def is_prime(n):
        if n < 2:
            return False
        for i in range(2, int(n**0.5) + 1):
            if n % i == 0:
                return False
        return True
    
    # 10000까지의 소수 찾기
    primes = []
    for i in range(2, 10000):
        if is_prime(i):
            primes.append(i)
    
    print(f"찾은 소수 개수: {len(primes)}")
    
    # 매트릭스 연산 (CPU 집약적)
    import random
    size = 500
    matrix_a = [[random.random() for _ in range(size)] for _ in range(size)]
    matrix_b = [[random.random() for _ in range(size)] for _ in range(size)]
    
    # 매트릭스 곱셈
    result = [[0 for _ in range(size)] for _ in range(size)]
    for i in range(size):
        for j in range(size):
            for k in range(size):
                result[i][j] += matrix_a[i][k] * matrix_b[k][j]
    
    print(f"매트릭스 연산 완료: {size}x{size}")
    return f"CPU 집약적인 작업 완료 - 소수 {len(primes)}개, 매트릭스 {size}x{size} 연산"

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

    # 4. CPU 집약적인 Bash 작업
    cpu_bash_task = BashOperator(
        task_id='cpu_intensive_bash',
        bash_command='''
        echo "CPU 집약적인 Bash 작업 시작..."
        
        # CPU를 많이 사용하는 find 명령어 (파일 시스템 전체 검색)
        find /usr -name "*.so" -type f 2>/dev/null | wc -l
        
        # CPU 집약적인 계산 작업
        echo "큰 수의 소인수분해 계산 중..."
        factor 982451653 991133941 997123123 998765432
        
        # 압축 해제 작업 (CPU 집약적)
        echo "압축 작업으로 CPU 사용량 증가..."
        echo "테스트 데이터" | gzip | gunzip
        
        # yes 명령어로 CPU 사용 (10초간)
        timeout 10 yes > /dev/null
        
        echo "CPU 집약적인 Bash 작업 완료"
        ''',
        dag=dag,
    )

    # 5. CPU 집약적인 Python 작업
    cpu_python_task = PythonOperator(
        task_id='cpu_intensive_python',
        python_callable=cpu_intensive_task,
        dag=dag,
    )

    # 6. Notification PythonOperator: 알림 대체
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
    start >> bash_task >> python_task >> cpu_bash_task >> cpu_python_task >> notification_task >> end 