# Docker 환경 구축 트러블슈팅 기록

> 이커머스 로그 파이프라인 프로젝트 — Apache Airflow + Spark + PostgreSQL Docker 환경 구축 과정에서 발생한 문제와 해결 과정을 기록한 문서입니다.

---

## 초기 설계 구성

처음 설계한 구성. 트러블슈팅 과정에서 일부 이미지와 설정이 변경됨

| 서비스 | 초기 이미지 | 최종 이미지 | 포트 | 역할 |
|--------|------------|------------|------|------|
| PostgreSQL | `postgres:15` | `postgres:15` | 5432 | Airflow 메타데이터 DB + 파이프라인 DB |
| Airflow Webserver | `apache/airflow:2.8.1` | `apache/airflow:2.9.2` | 8080 | DAG 모니터링 UI |
| Airflow Scheduler | `apache/airflow:2.8.1` | `apache/airflow:2.9.2` | - | DAG 스케줄링 |
| Spark Master | `bitnami/spark:3.5` | `apache/spark:3.5.0` | 7077, 8081 | Spark 클러스터 마스터 |
| Spark Worker | `bitnami/spark:3.5` | `apache/spark:3.5.0` | - | Spark 작업 실행 |

---

## 문제 1 — `bitnami/spark:3.5` 이미지 없음

### 증상

```
manifest for bitnami/spark:3.5 not found: manifest unknown
```

`docker compose up` 실행 즉시 이미지 pull 단계에서 실패하며 전체 스택이 올라오지 않음

### 원인

`bitnami/spark` 이미지가 Docker Hub 공식 지원 종료(deprecated)로 해당 태그가 존재하지 않음

### 해결

`bitnami/spark:3.5` → `apache/spark:3.5.0` 으로 교체.  
단, 두 이미지는 실행 인터페이스가 달라 `command`와 환경변수를 함께 수정해야 했음

```yaml
# 변경 전 (bitnami 방식 — 환경변수로 모드 제어)
spark-master:
  image: bitnami/spark:3.5
  environment:
    SPARK_MODE: master

spark-worker:
  image: bitnami/spark:3.5
  environment:
    SPARK_MODE: worker
    SPARK_MASTER_URL: spark://spark-master:7077

# 변경 후 (apache 공식 이미지 — 직접 클래스 실행)
spark-master:
  image: apache/spark:3.5.0
  command: /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
  environment:
    SPARK_MASTER_HOST: spark-master

spark-worker:
  image: apache/spark:3.5.0
  command: /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
  environment:
    SPARK_WORKER_MEMORY: 2G
    SPARK_WORKER_CORES: 2
```

---

## 문제 2 — Airflow 공식 템플릿 파일로 덮어씌워짐

### 증상

`docker compose up` 실행 시 설계한 적 없는 컨테이너들이 올라옴

```
docker-airflow-worker-1      Up   # Celery Worker
docker-airflow-triggerer-1   Up   # Triggerer
docker-redis-1               Up   # Redis Broker
```

### 원인

`docker/docker-compose.yml`이 Apache 공식 CeleryExecutor 템플릿으로 교체되어 있었음.  
원래 프로젝트 설계는 단순한 LocalExecutor 기반 구성이었으나, 외부 템플릿이 파일을 덮어씀

### 해결

프로젝트 요구사항에 맞는 LocalExecutor 기반 `docker-compose.yml`을 처음부터 재작성

```yaml
# 핵심 차이: CeleryExecutor(Redis 필요) → LocalExecutor(단일 프로세스)
AIRFLOW__CORE__EXECUTOR: LocalExecutor
```

---

## 문제 3 — airflow-webserver / airflow-scheduler 무한 재시작 (핵심 문제)

### 증상

```
docker-airflow-webserver-1   Restarting (0) 2 seconds ago
docker-airflow-scheduler-1   Restarting (0) 2 seconds ago
```

ExitCode **0**(정상 종료 코드)으로 즉시 종료 후 `restart: always` 정책에 의해 무한 반복됨.  
Docker Desktop Logs 탭 완전히 비어있음 — stdout/stderr 출력 자체가 없음

---

### 원인 파악 과정

#### 1단계 — 일반적인 원인 제거

| 확인 항목 | 결과 | 의미 |
|-----------|------|------|
| `OOMKilled` | `false` | 메모리 부족 아님 |
| ExitCode | `0` | 에러 크래시 아님, 정상 종료 |
| 로그 출력 | 완전히 없음 | 프로세스 시작 전 종료 |

#### 2단계 — entrypoint 직접 실행으로 에러 확인

```bash
docker compose run --entrypoint bash airflow-webserver -c "airflow version"
# 결과: ModuleNotFoundError: No module named 'airflow'
```

`PYTHONPATH: /opt/airflow` 설정이 Python 기본 패키지 경로보다 앞에 삽입되어 airflow 모듈 자체를 찾지 못하는 것으로 초기 판단

---

### 시도한 해결책들 (실패 기록)

실제 문제 해결에는 이르지 못했지만, 원인을 좁혀나가는 데 기여한 시도들

| 시도 | 내용 | 결과 |
|------|------|------|
| 1 | `PYTHONPATH: /opt/airflow` 환경변수 제거 | 실패 |
| 2 | `airflow-init`의 `restart: on-failure` 제거 | 실패 |
| 3 | `depends_on: airflow-init: condition: service_completed_successfully` 추가 | 실패 |
| 4 | `AIRFLOW__CORE__FERNET_KEY` 유효한 값 생성 및 추가 | 실패 |
| 5 | `AIRFLOW__WEBSERVER__SECRET_KEY` 추가 | 실패 |
| 6 | named volume → 로컬 바인드 마운트 + `chmod 777` | 실패 |
| 7 | `command: ["bash", "-c", "airflow webserver"]`로 변경 | 실패 |

---

### 실제 원인

`apache/airflow:2.8.1` 이미지의 entrypoint 구조가 문제였음

```
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
CMD []
```

`/entrypoint` 스크립트는 실행 전 환경 검증(권한, 볼륨, 환경변수 등)을 수행하는데,  
**Windows + Docker Desktop 환경의 볼륨 마운트 조건**에서 검증 실패 시 아무 로그도 남기지 않고 ExitCode 0으로 조용히 종료됨

> Windows 환경에서는 `docker logs`, `docker compose logs` 명령의 stdout이 PowerShell/CMD로 전달되지 않는 현상도 있어 에러 메시지 파악이 매우 어려웠음

---

### 최종 해결

`/entrypoint` 스크립트를 완전히 우회하고 airflow CLI를 직접 실행

```yaml
# 변경 전
# dumb-init → /entrypoint → webserver 인자 전달 (entrypoint 스크립트 의존)
airflow-webserver:
  command: webserver

airflow-scheduler:
  command: scheduler

# 변경 후
# airflow CLI를 직접 실행 (entrypoint 스크립트 우회)
airflow-webserver:
  entrypoint: ["airflow"]
  command: ["webserver"]

airflow-scheduler:
  entrypoint: ["airflow"]
  command: ["scheduler"]
```

---

## 핵심 교훈

### 1. `command: webserver` vs `entrypoint + command` 분리의 차이

- **`command: webserver`**  
  이미지 기본 entrypoint(`dumb-init -- /entrypoint`)가 살아있는 상태에서 `/entrypoint webserver` 형태로 실행됨.  
  entrypoint 스크립트가 환경 검증 도중 실패하면 **아무 로그 없이 ExitCode 0으로 종료**될 수 있음

- **`entrypoint: ["airflow"] + command: ["webserver"]`**  
  entrypoint를 완전히 교체하여 `airflow webserver`를 직접 실행.  
  entrypoint 스크립트의 영향을 받지 않으므로 환경 문제와 무관하게 Airflow CLI가 직접 구동됨

### 2. 체계적인 트러블슈팅 접근법

```
에러 메시지 확인
    ↓
OOM / ExitCode 확인 → 크래시인지 정상종료인지 구분
    ↓
로그 확인 → 아무것도 없으면 프로세스 시작 전 문제
    ↓
entrypoint 우회 후 직접 실행 → 실제 에러 확인
    ↓
이미지 구조(Dockerfile, entrypoint 스크립트) 분석
    ↓
근본 원인 해결
```

### 3. Windows 환경에서의 Docker 디버깅

- PowerShell/CMD에서 `docker logs` 출력이 캡처되지 않는 경우가 있음
- **Docker Desktop GUI의 Logs 탭**이 더 신뢰도 높은 로그 확인 수단
- Linux 환경과 달리 볼륨 마운트 권한 처리 방식이 달라 예상치 못한 동작이 발생할 수 있음

---

## 문제 4 — `ModuleNotFoundError: No module named 'airflow'` (최종 원인)

### 증상

Docker Desktop Logs 탭에서 드디어 실제 에러 확인

```
ModuleNotFoundError: No module named 'airflow'
/usr/local/bin/python: No module named airflow
```

entrypoint를 `["airflow"]`, `["python", "-m", "airflow"]` 등으로 바꿔도 동일하게 반복

### 원인

`apache/airflow:2.8.1` 이미지 자체의 버그.  
airflow 패키지가 `/home/airflow/.local/lib/python3.12/site-packages`에 설치되어 있는데,  
이미지의 기본 Python(`/usr/local/bin/python`)이 해당 경로를 인식하지 못하는 상태였음

다양한 entrypoint 경로를 시도했으나 모두 실패:

| 시도 | 결과 |
|------|------|
| `entrypoint: ["airflow"]` | `No module named 'airflow'` |
| `entrypoint: ["python", "-m", "airflow"]` | `/usr/local/bin/python: No module named airflow` |
| `entrypoint: ["/home/airflow/.local/bin/airflow"]` | init 컨테이너 exit 1 |

### 최종 해결

**`apache/airflow:2.8.1` → `apache/airflow:2.9.2`** 로 이미지 버전 교체

`2.9.2`로 변경하자 entrypoint 수정 없이 기본 `command: ["webserver"]` 설정만으로 정상 기동됨

```yaml
# 변경 전 — 이미지 자체 버그로 동작 불가
image: apache/airflow:2.8.1

# 변경 후 — 정상 동작
image: apache/airflow:2.9.2
```

```
# 2.9.2 기동 후 정상 로그
[INFO] Starting gunicorn 22.0.0
{override.py} INFO - Inserted Role: Admin
{override.py} INFO - Added user admin
```

### Admin 계정 생성

`airflow-init`에서 `users create`가 빠진 경우 아래 명령으로 별도 생성

```bash
docker compose exec airflow-webserver airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.com
```

---

## 최종 구성 (수정 후)

| 서비스 | 이미지 | 포트 | 역할 |
|--------|--------|------|------|
| PostgreSQL | `postgres:15` | 5432 | Airflow 메타데이터 DB + 파이프라인 DB |
| Airflow Webserver | `apache/airflow:2.9.2` | 8080 | DAG 모니터링 UI |
| Airflow Scheduler | `apache/airflow:2.9.2` | - | DAG 스케줄링 |
| Spark Master | `apache/spark:3.5.0` | 7077, 8081 | Spark 클러스터 마스터 |
| Spark Worker | `apache/spark:3.5.0` | - | Spark 작업 실행 |

---

---

## 문제 8 — DAG 실행 중 발생한 연쇄 오류들

### 8-1. `spark-submit: command not found` (exit code 127)

`BashOperator`는 Airflow 컨테이너 내부에서 실행되므로, Airflow 이미지에 `spark-submit`이 없으면 실패.  
→ `Dockerfile.airflow`에 `openjdk-17` + `pyspark==3.5.3` 설치로 해결

### 8-2. PySpark 버전 불일치 (`InvalidClassException: serialVersionUID mismatch`)

Driver(Airflow)의 PySpark 버전과 Executor(Spark worker) 버전이 달라 직렬화 충돌.

```
Driver: pyspark==3.5.0  ←→  Worker: apache/spark:3.5.3  → 충돌
```

→ `Dockerfile.airflow`에서 `pyspark==3.5.3`으로 맞추고 재빌드

### 8-3. Spark worker가 Airflow 컨테이너 파일에 접근 불가 (`SparkFileNotFoundException`)

`--master spark://spark-master:7077`으로 제출 시 executor가 worker 컨테이너에서 실행되는데,  
파일은 Airflow 컨테이너 볼륨에만 있어 worker가 접근 불가.

```
File file:/opt/airflow/data/raw/events_2020-10-02.csv does not exist
```

→ `SparkSession`을 `local[*]` 모드로 변경하여 Airflow 컨테이너 안에서 직접 실행

```python
SparkSession.builder.master("local[*]").appName("...").getOrCreate()
```

### 8-4. `event_time` 파싱 실패로 전체 행 필터링 (0행 적재)

Kaggle 데이터의 `event_time` 형식: `2020-10-03 00:00:16+00:00`  
`transform.py`의 파싱 포맷: `yyyy-MM-dd HH:mm:ss z` → `+00:00` 형식 인식 불가 → 전체 NULL → 필터링

```python
# 변경 전
F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss z")

# 변경 후
F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ssxxx")  # xxx = +00:00 형식
```

### 최종 결과

```
generate_logs    ✅ success  (Parquet → CSV 슬라이싱)
spark_transform  ✅ success  (local[*] PySpark 집계)
load_to_postgres ✅ success  (2,529개 세션 적재)
validate_load    ✅ success  (정합성 검증 통과)
```

```sql
-- pipeline_db 적재 결과
etl_date    | sessions | purchases | revenue
2020-10-04  |   2,529  |    189    |  16,811
```

---

## 최종 docker-compose.yml 핵심 구조

```yaml
x-airflow-common: &airflow-common
  image: apache/airflow:2.9.2
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__FERNET_KEY: '<생성된 키>'
    AIRFLOW__WEBSERVER__SECRET_KEY: '<생성된 키>'
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'

services:
  airflow-init:
    <<: *airflow-common
    command: >
      bash -c "airflow db migrate &&
               (airflow users create --username admin ... || true)"
    depends_on:
      postgres:
        condition: service_healthy

  airflow-webserver:
    <<: *airflow-common
    entrypoint: ["airflow"]       # ← 핵심 수정
    command: ["webserver"]
    depends_on:
      airflow-init:
        condition: service_completed_successfully  # ← init 완료 후 시작

  airflow-scheduler:
    <<: *airflow-common
    entrypoint: ["airflow"]       # ← 핵심 수정
    command: ["scheduler"]
    depends_on:
      airflow-init:
        condition: service_completed_successfully
```
