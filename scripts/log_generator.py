"""
이커머스 유저 행동 로그 시뮬레이터.
실행 시 data/raw/ 디렉터리에 CSV 파일을 생성한다.
"""

import argparse
import csv
import os
import random
import uuid
from datetime import datetime, timedelta

EVENT_TYPES = ["page_view", "product_click", "add_to_cart", "purchase", "search", "wishlist"]
CATEGORIES = ["electronics", "fashion", "beauty", "food", "sports", "books"]
DEVICES = ["mobile", "desktop", "tablet"]

PRODUCT_POOL = [f"PROD-{str(i).zfill(5)}" for i in range(1, 501)]
USER_POOL = [f"USER-{str(i).zfill(6)}" for i in range(1, 10001)]


def generate_event(base_time: datetime) -> dict:
    event_type = random.choices(
        EVENT_TYPES,
        weights=[40, 25, 15, 5, 10, 5],
    )[0]

    offset_seconds = random.randint(0, 3599)
    ts = base_time + timedelta(seconds=offset_seconds)

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(USER_POOL),
        "session_id": str(uuid.uuid4()),
        "event_type": event_type,
        "product_id": random.choice(PRODUCT_POOL) if event_type != "search" else None,
        "category": random.choice(CATEGORIES),
        "device": random.choice(DEVICES),
        "price": round(random.uniform(1000, 500000), 0) if event_type == "purchase" else None,
        "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
    }


def generate_logs(target_date: str, num_events: int, output_dir: str) -> str:
    base_time = datetime.strptime(target_date, "%Y-%m-%d")
    os.makedirs(output_dir, exist_ok=True)

    filename = os.path.join(output_dir, f"events_{target_date}.csv")
    fieldnames = ["event_id", "user_id", "session_id", "event_type",
                  "product_id", "category", "device", "price", "timestamp"]

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for _ in range(num_events):
            writer.writerow(generate_event(base_time))

    print(f"[log_generator] {num_events:,}건 생성 → {filename}")
    return filename


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="이커머스 로그 생성기")
    parser.add_argument("--date", default=datetime.today().strftime("%Y-%m-%d"), help="YYYY-MM-DD")
    parser.add_argument("--events", type=int, default=100_000, help="생성할 이벤트 수")
    parser.add_argument("--output-dir", default="data/raw", help="출력 디렉터리")
    args = parser.parse_args()

    generate_logs(args.date, args.events, args.output_dir)
