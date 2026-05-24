import os
import sys
import tempfile

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scripts.log_generator import slice_by_date


def _make_lake(tmp_dir: str, date: str, n: int = 100) -> str:
    """테스트용 Parquet 파티션 생성."""
    partition = os.path.join(tmp_dir, f"date={date}")
    os.makedirs(partition, exist_ok=True)

    df = pd.DataFrame({
        "event_time": pd.date_range(f"{date} 00:00:00", periods=n, freq="1min", tz="UTC"),
        "event_type": (["view", "purchase"] * (n // 2 + 1))[:n],
        "product_id": [f"p{i}" for i in range(n)],
        "category_id": [f"c{i % 10}" for i in range(n)],
        "category_code": ["electronics.phone"] * n,
        "brand": ["samsung"] * n,
        "price": [float(i * 10 + 9.99) for i in range(n)],
        "user_id": [f"u{i % 20}" for i in range(n)],
        "user_session": [f"sess{i % 30}" for i in range(n)],
    })
    df.to_parquet(os.path.join(partition, "part-0.parquet"), index=False)
    return tmp_dir


def test_slice_exact_date():
    """해당 날짜 파티션이 있으면 그 데이터를 CSV로 저장한다."""
    with tempfile.TemporaryDirectory() as lake, tempfile.TemporaryDirectory() as out:
        _make_lake(lake, "2020-09-24", n=200)

        # LAKE_DIR 우회: slice_by_date 내부 get_lake_dir()가 lake를 반환하도록 monkeypatch 대신 직접 경로 주입
        import scripts.log_generator as lg
        original = lg.get_lake_dir
        lg.get_lake_dir = lambda: lake
        try:
            path = slice_by_date("2020-09-24", out)
        finally:
            lg.get_lake_dir = original

        assert os.path.exists(path)
        df = pd.read_csv(path)
        assert len(df) == 200


def test_slice_fallback_to_closest_date():
    """요청 날짜 파티션이 없으면 가장 가까운 날짜 파티션을 사용하고 event_time 날짜를 교체한다."""
    with tempfile.TemporaryDirectory() as lake, tempfile.TemporaryDirectory() as out:
        _make_lake(lake, "2020-09-01", n=50)

        import scripts.log_generator as lg
        original = lg.get_lake_dir
        lg.get_lake_dir = lambda: lake
        try:
            path = slice_by_date("2020-09-24", out)
        finally:
            lg.get_lake_dir = original

        df = pd.read_csv(path, parse_dates=["event_time"])
        assert len(df) == 50
        dates = pd.to_datetime(df["event_time"]).dt.date.astype(str).unique()
        assert all(d == "2020-09-24" for d in dates)


def test_output_filename_matches_date():
    """출력 CSV 파일명에 날짜가 포함된다."""
    with tempfile.TemporaryDirectory() as lake, tempfile.TemporaryDirectory() as out:
        _make_lake(lake, "2020-09-10")

        import scripts.log_generator as lg
        original = lg.get_lake_dir
        lg.get_lake_dir = lambda: lake
        try:
            path = slice_by_date("2020-09-10", out)
        finally:
            lg.get_lake_dir = original

        assert path.endswith("events_2020-09-10.csv")


def test_empty_lake_raises():
    """lake 디렉터리가 비어있으면 SystemExit이 발생한다."""
    with tempfile.TemporaryDirectory() as lake, tempfile.TemporaryDirectory() as out:
        import scripts.log_generator as lg
        original = lg.get_lake_dir
        lg.get_lake_dir = lambda: lake
        try:
            with pytest.raises(SystemExit):
                slice_by_date("2020-09-24", out)
        finally:
            lg.get_lake_dir = original
