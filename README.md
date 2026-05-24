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
| 시각화 | Grafana 10 |
| 컨테이너 | Docker Compose |
| CI | GitHub Actions |

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
│   ├── Dockerfile.spark
│   └── grafana/
│       └── provisioning/       # Grafana 데이터소스·대시보드 자동 설정
├── tests/
│   ├── test_log_generator.py
│   ├── test_transform.py       # PySpark clean/aggregate 단위 테스트
│   └── test_dag.py             # DAG 구조·의존성 검증
├── .github/
│   └── workflows/ci.yml        # GitHub Actions (pytest 자동 실행)
├── config.py                   # 파이프라인 파라미터 설정
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

| 서비스 | URL |
|--------|-----|
| Airflow UI | http://localhost:8080 |
| Grafana | http://localhost:3000 (admin / `.env`의 `GRAFANA_ADMIN_PASSWORD`) |

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

## Grafana 대시보드

DAG 실행 후 [http://localhost:3000](http://localhost:3000)에서 자동 프로비저닝된 **Ecommerce Pipeline** 대시보드를 확인할 수 있다.

- 일별 총 세션 수 / 고유 사용자 수 추이
- 일별 전환율 (구매 완료 세션 비율)
- 일별 매출액 합계
- 최근 7일 요약 테이블

## 설정 파라미터 (`config.py`)

환경변수로 동작을 조정할 수 있다. 기본값은 `config.py`에 명시되어 있다.

| 환경변수 | 기본값 | 설명 |
|----------|--------|------|
| `SPARK_SHUFFLE_PARTITIONS` | `4` | Spark shuffle 파티션 수 |
| `DROP_RATE_WARN_THRESHOLD` | `0.05` | 데이터 탈락률 경고 임계값 |
| `PG_CHUNKSIZE` | `5000` | PostgreSQL 적재 청크 크기 |
| `DAG_RETRIES` | `2` | 태스크 실패 시 재시도 횟수 |
| `DAG_RETRY_DELAY_MIN` | `5` | 재시도 대기 시간(분) |

## 테스트 및 CI

```bash
pip install -r requirements.txt
pytest tests/
```

push 또는 PR 시 GitHub Actions가 자동으로 `test_log_generator`, `test_transform`을 실행한다.

## 주요 설계 결정

- **LocalExecutor + local[*] Spark**: 단일 머신에서 운영 파이프라인 구조를 재현하되 인프라 복잡도 최소화
- **적재 후 검증 태스크 분리**: `validate_load`를 별도 태스크로 분리해 실패 시 알림·재시도 가능
- **탈락률 경고 임계값 외부화**: `DROP_RATE_WARN_THRESHOLD` 환경변수로 재배포 없이 조정 가능
- **XCom으로 카운트 전달**: 적재 건수를 XCom으로 전달해 다음 태스크에서 정합성 검증
- **Grafana 자동 프로비저닝**: 데이터소스·대시보드를 코드로 관리해 `docker compose up` 한 번으로 시각화 환경 구성
