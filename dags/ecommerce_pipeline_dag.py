"""
이커머스 로그 파이프라인 DAG.

스케줄: 매일 새벽 2시 (전날 로그 처리)
흐름:
  generate_logs → spark_transform → load_to_postgres → validate_load
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner": "data-engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

TARGET_DATE = "{{ ds }}"  # Airflow execution date (YYYY-MM-DD)


def load_parquet_to_postgres(ds: str, **_):
    """세션 집계 Parquet을 PostgreSQL에 적재."""
    import pandas as pd

    parquet_path = f"/opt/airflow/data/processed/session_stats_{ds}"
    hook = PostgresHook(postgres_conn_id="pipeline_postgres")
    engine = hook.get_sqlalchemy_engine()

    df = pd.read_parquet(parquet_path)
    df["etl_date"] = ds

    df.to_sql(
        "session_stats",
        engine,
        schema="public",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000,
    )
    print(f"[load] {len(df):,}행 적재 완료 → session_stats (date={ds})")
    return len(df)


def validate_load(ds: str, **context):
    """적재 전·후 레코드 수 정합성 검증."""
    hook = PostgresHook(postgres_conn_id="pipeline_postgres")

    row = hook.get_first(
        "SELECT COUNT(*) FROM public.session_stats WHERE etl_date = %s", parameters=(ds,)
    )
    db_count = row[0] if row else 0

    loaded_count = context["task_instance"].xcom_pull(task_ids="load_to_postgres")

    print(f"[validate_load] 적재 요청={loaded_count:,}  DB 확인={db_count:,}")
    if db_count != loaded_count:
        raise ValueError(f"정합성 오류: 적재 요청 {loaded_count} ≠ DB 확인 {db_count}")
    print("[validate_load] OK")


with DAG(
    dag_id="ecommerce_log_pipeline",
    default_args=default_args,
    description="이커머스 서비스 로그 배치 파이프라인",
    schedule="0 2 * * *",
    start_date=datetime(2020, 9, 24),
    catchup=False,
    tags=["ecommerce", "log", "batch"],
) as dag:

    generate_logs = BashOperator(
        task_id="generate_logs",
        bash_command=(
            "python /opt/airflow/scripts/log_generator.py "
            "--date {{ ds }} "
            "--output-dir /opt/airflow/data/raw"
        ),
    )

    spark_transform = BashOperator(
        task_id="spark_transform",
        bash_command=(
            "/home/airflow/.local/bin/spark-submit "
            "--conf spark.driver.extraJavaOptions=-Duser.timezone=UTC "
            "/opt/airflow/spark_jobs/transform.py {{ ds }}"
        ),
        env={
            "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64",
            "PATH": "/usr/lib/jvm/java-17-openjdk-amd64/bin:/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
            "PYSPARK_PYTHON": "/usr/local/bin/python",
            "PYSPARK_DRIVER_PYTHON": "/usr/local/bin/python",
        },
    )

    load_to_postgres = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_parquet_to_postgres,
        op_kwargs={"ds": TARGET_DATE},
    )

    validate_load_task = PythonOperator(
        task_id="validate_load",
        python_callable=validate_load,
        op_kwargs={"ds": TARGET_DATE},
    )

    generate_logs >> spark_transform >> load_to_postgres >> validate_load_task
