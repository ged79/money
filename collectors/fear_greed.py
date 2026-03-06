"""⑤ Crypto Fear & Greed Index 수집기"""
import time
import requests
from db import get_connection


FEAR_GREED_URL = "https://api.alternative.me/fng/"
_MAX_RETRIES = 3


def collect_fear_greed():
    """공포/탐욕 지수 수집 (최대 3회 재시도)"""
    for attempt in range(1, _MAX_RETRIES + 1):
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
            return
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            if status == 429 and attempt < _MAX_RETRIES:
                print(f"[F&G] 레이트 리밋 — 60초 대기 (시도 {attempt}/{_MAX_RETRIES})")
                time.sleep(60)
            elif attempt < _MAX_RETRIES:
                delay = 2 ** attempt
                print(f"[F&G] HTTP {status} — {delay}초 후 재시도 (시도 {attempt}/{_MAX_RETRIES})")
                time.sleep(delay)
            else:
                print(f"[F&G] 수집 실패 — 최대 재시도 초과: {e}")
        except Exception as e:
            if attempt < _MAX_RETRIES:
                delay = 2 ** attempt
                print(f"[F&G] 요청 실패 — {delay}초 후 재시도 (시도 {attempt}/{_MAX_RETRIES})")
                time.sleep(delay)
            else:
                print(f"[F&G] 수집 실패 — 최대 재시도 초과: {e}")


if __name__ == "__main__":
    from db import init_db
    init_db()
    collect_fear_greed()
