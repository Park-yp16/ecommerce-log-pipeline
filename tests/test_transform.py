import os
import sys
from datetime import datetime, timezone

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

pytest.importorskip("pyspark", reason="pyspark 미설치 시 스킵")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_jobs.transform import clean, aggregate_sessions, validate


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-transform")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _make_df(spark, rows: list[dict]):
    return spark.createDataFrame(rows)


# ── clean() ─────────────────────────────────────────────────────────────────

class TestClean:
    def test_renames_user_session_to_session_id(self, spark):
        df = _make_df(spark, [
            {"event_time": "2020-09-24 10:00:00+00:00", "event_type": "view",
             "product_id": "p1", "category_id": "c1", "category_code": "cat",
             "brand": "brand", "price": "9.99", "user_id": "u1", "user_session": "sess1"},
        ])
        result = clean(df)
        assert "session_id" in result.columns
        assert "user_session" not in result.columns

    def test_filters_null_session_id(self, spark):
        df = _make_df(spark, [
            {"event_time": "2020-09-24 10:00:00+00:00", "event_type": "view",
             "product_id": "p1", "category_id": "c1", "category_code": "cat",
             "brand": "brand", "price": "9.99", "user_id": "u1", "user_session": None},
            {"event_time": "2020-09-24 10:01:00+00:00", "event_type": "view",
             "product_id": "p2", "category_id": "c1", "category_code": "cat",
             "brand": "brand", "price": "9.99", "user_id": "u1", "user_session": "sess1"},
        ])
        result = clean(df)
        assert result.count() == 1

    def test_filters_null_user_id(self, spark):
        df = _make_df(spark, [
            {"event_time": "2020-09-24 10:00:00+00:00", "event_type": "view",
             "product_id": "p1", "category_id": "c1", "category_code": "cat",
             "brand": "brand", "price": "9.99", "user_id": None, "user_session": "sess1"},
            {"event_time": "2020-09-24 10:01:00+00:00", "event_type": "view",
             "product_id": "p2", "category_id": "c1", "category_code": "cat",
             "brand": "brand", "price": "9.99", "user_id": "u1", "user_session": "sess2"},
        ])
        result = clean(df)
        assert result.count() == 1

    def test_filters_purchase_with_zero_or_negative_price(self, spark):
        df = _make_df(spark, [
            {"event_time": "2020-09-24 10:00:00+00:00", "event_type": "purchase",
             "product_id": "p1", "category_id": "c1", "category_code": "cat",
             "brand": "brand", "price": "0.0", "user_id": "u1", "user_session": "sess1"},
            {"event_time": "2020-09-24 10:01:00+00:00", "event_type": "purchase",
             "product_id": "p2", "category_id": "c1", "category_code": "cat",
             "brand": "brand", "price": "-5.0", "user_id": "u1", "user_session": "sess2"},
            {"event_time": "2020-09-24 10:02:00+00:00", "event_type": "purchase",
             "product_id": "p3", "category_id": "c1", "category_code": "cat",
             "brand": "brand", "price": "19.99", "user_id": "u1", "user_session": "sess3"},
        ])
        result = clean(df)
        assert result.count() == 1

    def test_non_purchase_zero_price_kept(self, spark):
        """purchase가 아닌 이벤트는 price=0이어도 유지된다."""
        df = _make_df(spark, [
            {"event_time": "2020-09-24 10:00:00+00:00", "event_type": "view",
             "product_id": "p1", "category_id": "c1", "category_code": "cat",
             "brand": "brand", "price": "0.0", "user_id": "u1", "user_session": "sess1"},
        ])
        result = clean(df)
        assert result.count() == 1

    def test_price_cast_to_double(self, spark):
        df = _make_df(spark, [
            {"event_time": "2020-09-24 10:00:00+00:00", "event_type": "view",
             "product_id": "p1", "category_id": "c1", "category_code": "cat",
             "brand": "brand", "price": "29.99", "user_id": "u1", "user_session": "sess1"},
        ])
        result = clean(df)
        assert dict(result.dtypes)["price"] == "double"


# ── aggregate_sessions() ─────────────────────────────────────────────────────

class TestAggregateSessions:
    def _base_rows(self, spark):
        return clean(_make_df(spark, [
            {"event_time": "2020-09-24 10:00:00+00:00", "event_type": "view",
             "product_id": "p1", "category_id": "c1", "category_code": "cat",
             "brand": "b", "price": "9.99", "user_id": "u1", "user_session": "s1"},
            {"event_time": "2020-09-24 10:05:00+00:00", "event_type": "purchase",
             "product_id": "p2", "category_id": "c1", "category_code": "cat",
             "brand": "b", "price": "19.99", "user_id": "u1", "user_session": "s1"},
            {"event_time": "2020-09-24 11:00:00+00:00", "event_type": "view",
             "product_id": "p3", "category_id": "c2", "category_code": "cat",
             "brand": "b", "price": "5.00", "user_id": "u2", "user_session": "s2"},
        ]))

    def test_session_count(self, spark):
        result = aggregate_sessions(self._base_rows(spark), "2020-09-24")
        assert result.count() == 2

    def test_converted_flag(self, spark):
        result = aggregate_sessions(self._base_rows(spark), "2020-09-24")
        s1 = result.filter(F.col("session_id") == "s1").first()
        s2 = result.filter(F.col("session_id") == "s2").first()
        assert s1["converted"] is True
        assert s2["converted"] is False

    def test_total_revenue(self, spark):
        result = aggregate_sessions(self._base_rows(spark), "2020-09-24")
        s1 = result.filter(F.col("session_id") == "s1").first()
        assert abs(s1["total_revenue"] - 19.99) < 0.01

    def test_event_count(self, spark):
        result = aggregate_sessions(self._base_rows(spark), "2020-09-24")
        s1 = result.filter(F.col("session_id") == "s1").first()
        assert s1["event_count"] == 2

    def test_session_duration_non_negative(self, spark):
        result = aggregate_sessions(self._base_rows(spark), "2020-09-24")
        durations = [r["session_duration_min"] for r in result.collect()]
        assert all(d >= 0 for d in durations)

    def test_etl_date_set(self, spark):
        result = aggregate_sessions(self._base_rows(spark), "2020-09-24")
        dates = {r["etl_date"] for r in result.collect()}
        assert dates == {"2020-09-24"}


# ── validate() ───────────────────────────────────────────────────────────────

class TestValidate:
    def test_no_warning_within_threshold(self, capsys):
        validate(raw_count=1000, clean_count=960)
        captured = capsys.readouterr()
        assert "WARNING" not in captured.out

    def test_warning_above_threshold(self, capsys):
        validate(raw_count=1000, clean_count=900)
        captured = capsys.readouterr()
        assert "WARNING" in captured.out

    def test_zero_raw_count_no_crash(self):
        validate(raw_count=0, clean_count=0)
