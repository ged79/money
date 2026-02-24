"""⑤ Crypto Fear & Greed Index 수집기"""
import requests
from db import get_connection


FEAR_GREED_URL = "https://api.alternative.me/fng/"


def collect_fear_greed():
    """공포/탐욕 지수 수집"""
    try:
        resp = requests.get(FEAR_GREED_URL, params={"limit": 1, "format": "json"}, timeout=10)
        resp.raise_for_status()
        data = resp.json()["data"][0]

        value = int(data["value"])
        classification = data["value_classification"]
        timestamp = int(data["timestamp"])

        conn = get_connection()
        conn.execute(
            "INSERT INTO fear_greed (value, classification, fg_timestamp) VALUES (?, ?, ?)",
            (value, classification, timestamp),
        )
        conn.commit()
        conn.close()

        print(f"[F&G] {value} — {classification}")
    except Exception as e:
        print(f"[F&G] 수집 실패: {e}")


if __name__ == "__main__":
    from db import init_db
    init_db()
    collect_fear_greed()
