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

def extreme_cpu_memory_task():
    """3분간 CPU와 1GB 메모리를 사용하는 극집약적인 작업"""
    import time
    import random
    import math
    
    print("극집약적인 CPU/메모리 작업을 시작합니다... (3분간)")
    start_time = time.time()
    duration = 180  # 3분 = 180초
    
    # 1GB 메모리 할당 (약 125,000,000개의 double)
    print("1GB 메모리 할당 중...")
    large_list = []
    for i in range(125_000_000):  # 약 1GB 메모리 사용
        large_list.append(random.random())
    print(f"메모리 할당 완료: {len(large_list):,}개 요소")
    
    # CPU 집약적인 작업을 3분간 수행
    iteration = 0
    while time.time() - start_time < duration:
        iteration += 1
        
        # 복잡한 수학 계산
        for i in range(10000):
            math.sin(i) * math.cos(i) * math.tan(i % 89 + 1)
            math.sqrt(abs(math.log(i + 1)))
        
        # 리스트 조작으로 메모리 사용량 유지
        if iteration % 10 == 0:
            # 리스트의 일부를 무작위로 변경
            for _ in range(1000):
                idx = random.randint(0, len(large_list) - 1)
                large_list[idx] = random.random()
        
        # 진행 상황 출력
        elapsed = time.time() - start_time
        if iteration % 100 == 0:
            print(f"진행 중... {elapsed:.1f}초 경과 (반복: {iteration})")
    
    elapsed_total = time.time() - start_time
    memory_mb = len(large_list) * 8 / (1024 * 1024)  # MB 단위
    
    print(f"극집약적인 작업 완료!")
    print(f"실행 시간: {elapsed_total:.1f}초")
    print(f"메모리 사용량: {memory_mb:.1f}MB")
    print(f"총 반복 횟수: {iteration:,}")
    
    # 메모리 해제
    del large_list
    
    return f"극집약적인 작업 완료 - {elapsed_total:.1f}초, {memory_mb:.1f}MB 사용, {iteration:,} 반복"

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

    # 6. 극집약적인 Bash 작업 (3분간 CPU/메모리 사용)
    extreme_bash_task = BashOperator(
        task_id='extreme_cpu_memory_bash',
        bash_command='''
        echo "극집약적인 Bash 작업 시작... (3분간)"
        start_time=$(date +%s)
        duration=180  # 3분
        
        # 대용량 임시 파일 생성 (약 1GB)
        echo "1GB 임시 파일 생성 중..."
        dd if=/dev/zero of=/tmp/large_file_$$ bs=1M count=1024 2>/dev/null
        echo "파일 생성 완료"
        
        # CPU 집약적인 작업을 3분간 반복
        iteration=0
        while [ $(($(date +%s) - start_time)) -lt $duration ]; do
            iteration=$((iteration + 1))
            
            # CPU 집약적인 계산
            seq 1 50000 | while read i; do
                echo "scale=10; sqrt($i) + l($i)" | bc -l >/dev/null 2>&1
            done
            
            # 파일 압축/해제로 CPU 사용
            if [ $((iteration % 5)) -eq 0 ]; then
                gzip -c /tmp/large_file_$$ > /tmp/compressed_$$.gz 2>/dev/null
                gunzip -c /tmp/compressed_$$.gz > /tmp/decompressed_$$ 2>/dev/null
                rm -f /tmp/compressed_$$.gz /tmp/decompressed_$$
            fi
            
            # 진행 상황 출력
            if [ $((iteration % 10)) -eq 0 ]; then
                elapsed=$(($(date +%s) - start_time))
                echo "진행 중... ${elapsed}초 경과 (반복: $iteration)"
            fi
        done
        
        # 정리
        rm -f /tmp/large_file_$$
        elapsed=$(($(date +%s) - start_time))
        echo "극집약적인 Bash 작업 완료!"
        echo "실행 시간: ${elapsed}초, 총 반복: $iteration"
        ''',
        dag=dag,
    )

    # 7. 극집약적인 Python 작업 (3분간 CPU + 1GB 메모리)
    extreme_python_task = PythonOperator(
        task_id='extreme_cpu_memory_python',
        python_callable=extreme_cpu_memory_task,
        dag=dag,
    )

    # 8. Notification PythonOperator: 알림 대체
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
    start >> bash_task >> python_task >> cpu_bash_task >> cpu_python_task >> extreme_bash_task >> extreme_python_task >> notification_task >> end 