import os

# Spark
SPARK_SHUFFLE_PARTITIONS: int = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))

# 데이터 품질
DROP_RATE_WARN_THRESHOLD: float = float(os.getenv("DROP_RATE_WARN_THRESHOLD", "0.05"))

# PostgreSQL 적재
PG_CHUNKSIZE: int = int(os.getenv("PG_CHUNKSIZE", "5000"))
PG_CONN_ID: str = os.getenv("PG_CONN_ID", "pipeline_postgres")

# 재시도
DAG_RETRIES: int = int(os.getenv("DAG_RETRIES", "2"))
DAG_RETRY_DELAY_MIN: int = int(os.getenv("DAG_RETRY_DELAY_MIN", "5"))
