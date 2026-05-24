# Ecommerce Log Pipeline

이커머스 서비스의 사용자 행동 로그를 수집·정제·적재하는 배치 데이터 파이프라인.

Kaggle [REES46 ecommerce dataset](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)을 사용해 실제 운영 환경과 유사한 파이프라인을 구성했다.

## 아키텍처

```
[Raw CSV]
    │
    ▼
log_generator.py          # 날짜 파티션 Parquet → CSV 슬라이싱
    │
    ▼ (Airflow DAG: 매일 02:00)
generate_logs ──► spark_transform ──► load_to_postgres ──► validate_load
                       │                      │
                  PySpark 정제·집계        PostgreSQL 적재
                  (Parquet 저장)          (session_stats)
```

**사용 기술**

| 역할 | 기술 |
|------|------|
| 오케스트레이션 | Apache Airflow 2.x (LocalExecutor) |
| 데이터 처리 | PySpark 3.5 (local mode) |
| 저장소 | PostgreSQL 15 |
| 컨테이너 | Docker Compose |

## 파이프라인 흐름

1. **generate_logs** — 날짜별 Parquet 파티션에서 해당 날짜 CSV를 생성
2. **spark_transform** — PySpark로 타입 변환·null 필터링·이상값 제거 후 세션 단위 집계
3. **load_to_postgres** — 집계 결과(Parquet)를 `session_stats` 테이블에 적재
4. **validate_load** — 적재 요청 건수 vs DB 실제 건수 정합성 검증

## 디렉터리 구조

```
ecommerce-log-pipeline/
├── dags/
│   └── ecommerce_pipeline_dag.py   # Airflow DAG 정의
├── spark_jobs/
│   └── transform.py                # PySpark 정제·집계 잡
├── scripts/
│   ├── log_generator.py            # 날짜별 CSV 생성
│   └── prepare_parquet.py          # Kaggle CSV → Parquet 1회 변환
├── sql/
│   └── init_schema.sql             # session_stats 테이블 + daily_summary 뷰
├── docker/
│   ├── docker-compose.yml
│   ├── Dockerfile.airflow
│   └── Dockerfile.spark
├── tests/
│   └── test_log_generator.py
├── .env.example
└── requirements.txt
```

## 실행 방법

### 1. 사전 준비

```bash
# 저장소 클론
git clone https://github.com/your-username/ecommerce-log-pipeline.git
cd ecommerce-log-pipeline

# 환경변수 설정
cp .env.example .env
# .env 파일에서 패스워드 및 키 값 수정
```

Fernet 키와 Secret 키는 아래 명령으로 생성:

```bash
# Fernet Key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Secret Key
python -c "import secrets; print(secrets.token_hex(32))"
```

### 2. 데이터 준비

[Kaggle REES46 데이터셋](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)에서 `2019-Oct.csv` (또는 `2020-Apr.csv`)를 다운로드한 뒤 Parquet으로 변환:

```bash
# CSV를 data/raw/events_2020-09.csv 로 위치시킨 후
pip install -r requirements.txt
python scripts/prepare_parquet.py
```

### 3. Docker 실행

```bash
cd docker
docker compose up -d

# 초기화 완료 대기 (약 30초)
docker compose ps
```

Airflow UI: [http://localhost:8080](http://localhost:8080)

### 4. DAG 실행

Airflow UI에서 `ecommerce_log_pipeline` DAG를 활성화하거나 CLI로 수동 실행:

```bash
docker exec -it docker-airflow-scheduler-1 \
  airflow dags trigger ecommerce_log_pipeline --conf '{"ds": "2020-09-24"}'
```

### 5. 결과 확인

```sql
-- 일별 집계 확인
SELECT * FROM public.daily_summary LIMIT 10;

-- 세션 상세 조회
SELECT * FROM public.session_stats WHERE etl_date = '2020-09-24' LIMIT 5;
```

## 테스트

```bash
pip install -r requirements.txt
pytest tests/
```

## 주요 설계 결정

- **LocalExecutor + local[*] Spark**: 단일 머신에서 운영 파이프라인 구조를 재현하되 인프라 복잡도 최소화
- **적재 후 검증 태스크 분리**: `validate_load`를 별도 태스크로 분리해 실패 시 알림·재시도 가능
- **탈락률 5% 경고**: Spark 정제 단계에서 데이터 품질 지표를 로그로 출력
- **XCom으로 카운트 전달**: 적재 건수를 XCom으로 전달해 다음 태스크에서 정합성 검증
