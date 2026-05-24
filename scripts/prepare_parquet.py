"""
Kaggle REES46 CSV를 날짜 파티션 Parquet으로 1회 변환.

실행: python scripts/prepare_parquet.py

입력: data/raw/events_2020-09.csv
출력: data/lake/events/date=YYYY-MM-DD/part-*.parquet
"""

import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

SOURCE = "data/raw/events_2020-09.csv"
OUTPUT_DIR = "data/lake/events"


def main():
    print(f"[prepare] CSV 로딩: {SOURCE}")
    # PyArrow로 CSV 읽기 (pandas보다 빠름)
    table = pa.csv.read_csv(SOURCE)
    df = table.to_pandas()
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
    df["date"] = df["event_time"].dt.strftime("%Y-%m-%d")

    dates = sorted(df["date"].unique())
    print(f"[prepare] 총 {len(df):,}행 / {len(dates)}일치 데이터")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for date in dates:
        day_df = df[df["date"] == date].drop(columns=["date"])
        out_path = os.path.join(OUTPUT_DIR, f"date={date}")
        os.makedirs(out_path, exist_ok=True)
        # PyArrow로 Parquet 저장 (snappy 압축)
        pq.write_table(
            pa.Table.from_pandas(day_df, preserve_index=False),
            os.path.join(out_path, "part-0.parquet"),
            compression="snappy"
        )
        print(f"[prepare] {date} → {len(day_df):,}행 저장")

    print(f"[prepare] 완료 → {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
