"""
data/lake/events/date={date}/ 파티션에서 해당 날짜 데이터를
data/raw/events_{date}.csv 로 복사한다.

날짜 파티션이 없으면 가장 가까운 날짜 파티션을 사용한다.
"""

import argparse
import os
import sys

import pandas as pd

LAKE_DIR = "/opt/airflow/data/lake/events"
FALLBACK_LAKE_DIR = "data/lake/events"
OUTPUT_BASE = "/opt/airflow/data/raw"
FALLBACK_OUTPUT_BASE = "data/raw"


def get_lake_dir():
    return LAKE_DIR if os.path.exists(LAKE_DIR) else FALLBACK_LAKE_DIR


def get_output_base():
    return OUTPUT_BASE if os.path.exists(OUTPUT_BASE) else FALLBACK_OUTPUT_BASE


def slice_by_date(target_date: str, output_dir: str) -> str:
    lake = get_lake_dir()
    partition = os.path.join(lake, f"date={target_date}")

    if os.path.exists(partition):
        df = pd.read_parquet(partition)
        print(f"[log_generator] {target_date} 파티션 로딩: {len(df):,}행")
    else:
        # 가장 가까운 날짜 파티션 사용
        available = sorted([
            d.replace("date=", "")
            for d in os.listdir(lake)
            if d.startswith("date=")
        ])
        if not available:
            print(f"[log_generator] ERROR: lake 디렉터리 비어있음 → {lake}", file=sys.stderr)
            sys.exit(1)

        closest = min(available, key=lambda d: abs(
            pd.Timestamp(d) - pd.Timestamp(target_date)
        ))
        print(f"[log_generator] {target_date} 파티션 없음 — {closest} 사용")
        df = pd.read_parquet(os.path.join(lake, f"date={closest}"))
        df["event_time"] = df["event_time"].apply(
            lambda t: t.replace(year=int(target_date[:4]),
                                month=int(target_date[5:7]),
                                day=int(target_date[8:10]))
        )

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"events_{target_date}.csv")
    df.to_csv(output_path, index=False)
    print(f"[log_generator] {len(df):,}건 저장 → {output_path}")
    return output_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--output-dir", default=get_output_base())
    args = parser.parse_args()

    slice_by_date(args.date, args.output_dir)
