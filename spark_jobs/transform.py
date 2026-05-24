"""
PySpark 정제·집계 잡.

입력 : data/raw/events_{date}.csv   (Kaggle REES46 ecommerce events)
컬럼 : event_time, event_type, product_id, category_id, category_code,
        brand, price, user_id, user_session

출력 :
  - data/parquet/events_{date}/          (정제된 원본, Parquet)
  - data/processed/session_stats_{date}/ (세션 단위 집계, Parquet)
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

RAW_BASE = "/opt/airflow/data/raw"
PARQUET_BASE = "/opt/airflow/data/parquet"
PROCESSED_BASE = "/opt/airflow/data/processed"


def build_spark():
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("ecommerce-log-transform")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_raw(spark: SparkSession, path: str):
    return spark.read.option("header", True).csv(path)


def clean(df):
    """타입 변환, null 필터링, 이상값 제거."""
    df = (
        df
        .withColumnRenamed("user_session", "session_id")
        .withColumn("price", F.col("price").cast(DoubleType()))
        .withColumn("event_time", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ssxxx"))
        .filter(F.col("session_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("event_time").isNotNull())
        .filter(~((F.col("event_type") == "purchase") & (F.col("price") <= 0)))
        .filter(F.col("event_time") <= F.current_timestamp())
    )
    return df


def aggregate_sessions(df, etl_date: str):
    """세션 단위 집계: 이벤트 수, 구매 여부, 구매 금액."""
    return (
        df.groupBy("session_id", "user_id")
        .agg(
            F.count("*").alias("event_count"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
            F.sum(F.when(F.col("event_type") == "purchase", F.coalesce(F.col("price"), F.lit(0))).otherwise(0)).alias("total_revenue"),
            F.min("event_time").alias("session_start"),
            F.max("event_time").alias("session_end"),
        )
        .withColumn(
            "session_duration_min",
            F.round((F.unix_timestamp("session_end") - F.unix_timestamp("session_start")) / 60, 2),
        )
        .withColumn("converted", F.col("purchase_count") > 0)
        .withColumn("device", F.lit(None).cast("string"))
        .withColumn("etl_date", F.lit(etl_date))
        .withColumn("loaded_at", F.current_timestamp())
    )


def validate(raw_count: int, clean_count: int) -> None:
    drop_rate = (raw_count - clean_count) / raw_count if raw_count else 0
    print(f"[validate] 원본={raw_count:,}  정제후={clean_count:,}  탈락률={drop_rate:.2%}")
    if drop_rate > 0.05:
        print(f"[validate] WARNING: 탈락률 {drop_rate:.2%} > 5% — 로그 품질 확인 필요")


def run(date: str):
    spark = build_spark()

    path = f"{RAW_BASE}/events_{date}.csv"
    raw_df = read_raw(spark, path)
    raw_count = raw_df.count()

    clean_df = clean(raw_df)
    clean_count = clean_df.count()

    validate(raw_count, clean_count)

    clean_df.write.mode("overwrite").partitionBy("event_type").parquet(f"{PARQUET_BASE}/events_{date}")

    session_df = aggregate_sessions(clean_df, date)
    session_df.write.mode("overwrite").parquet(f"{PROCESSED_BASE}/session_stats_{date}")

    print(f"[transform] 완료 — date={date}, sessions={session_df.count():,}")
    spark.stop()


if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2020-09"
    run(target_date)
