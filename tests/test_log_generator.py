import csv
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scripts.log_generator import generate_logs

EVENT_TYPES = {"page_view", "product_click", "add_to_cart", "purchase", "search", "wishlist"}


def test_generates_correct_count():
    with tempfile.TemporaryDirectory() as tmp:
        generate_logs("2026-05-01", 1000, tmp)
        path = os.path.join(tmp, "events_2026-05-01.csv")
        with open(path, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 1000


def test_event_types_valid():
    with tempfile.TemporaryDirectory() as tmp:
        generate_logs("2026-05-01", 500, tmp)
        path = os.path.join(tmp, "events_2026-05-01.csv")
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                assert row["event_type"] in EVENT_TYPES


def test_purchase_has_price():
    with tempfile.TemporaryDirectory() as tmp:
        generate_logs("2026-05-01", 2000, tmp)
        path = os.path.join(tmp, "events_2026-05-01.csv")
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row["event_type"] == "purchase":
                    assert row["price"] not in (None, "", "None"), \
                        f"purchase 이벤트에 price 없음: {row}"


def test_no_duplicate_event_ids():
    with tempfile.TemporaryDirectory() as tmp:
        generate_logs("2026-05-01", 1000, tmp)
        path = os.path.join(tmp, "events_2026-05-01.csv")
        with open(path, newline="", encoding="utf-8") as f:
            ids = [row["event_id"] for row in csv.DictReader(f)]
        assert len(ids) == len(set(ids)), "event_id 중복 발생"
