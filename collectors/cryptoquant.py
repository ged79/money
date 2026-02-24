"""④ 온체인 메트릭 수집기 — Santiment API + Binance Taker Ratio
(CryptoQuant/BGeometrics 대체 → 완전 무료)

Santiment: MVRV, 넷플로우, SOPR (GraphQL, 1000 req/월, 키 불필요, 30일 딜레이)
Binance: Taker Buy/Sell Ratio (REST, 무제한, 실시간)
"""
import time
import requests
from datetime import date, timedelta
from db import get_connection
from config import BINANCE_FUTURES_BASE, SYMBOLS

# Santiment GraphQL
SANTIMENT_URL = "https://api.santiment.net/graphql"

# Santiment 쿼리 날짜 범위 (무료: 30일 전까지만)
SANTIMENT_DELAY_DAYS = 35  # 여유 있게 35일 전부터
SANTIMENT_RANGE_DAYS = 7   # 7일치 수집

# 심볼 매핑
SYMBOL_TO_ASSET = {
    "BTCUSDT": "btc",
    "ETHUSDT": "eth",
    "SOLUSDT": "sol",
}

# Santiment slug 매핑
ASSET_TO_SLUG = {
    "btc": "bitcoin",
    "eth": "ethereum",
    "sol": "solana",
}


def _santiment_query(metric: str, slug: str = "bitcoin",
                     from_date: str = None, to_date: str = None) -> list | None:
    """Santiment GraphQL API 호출"""
    if not from_date:
        end = date.today() - timedelta(days=SANTIMENT_DELAY_DAYS - SANTIMENT_RANGE_DAYS)
        start = end - timedelta(days=SANTIMENT_RANGE_DAYS)
        from_date = start.isoformat() + "T00:00:00Z"
        to_date = end.isoformat() + "T00:00:00Z"

    query = """
    {
      getMetric(metric: "%s") {
        timeseriesData(
          slug: "%s"
          from: "%s"
          to: "%s"
          interval: "1d"
        ) {
          datetime
          value
        }
      }
    }
    """ % (metric, slug, from_date, to_date)

    try:
        resp = requests.post(SANTIMENT_URL,
                             json={"query": query},
                             headers={"Content-Type": "application/json"},
                             timeout=15)
        resp.raise_for_status()
        data = resp.json()

        if "errors" in data:
            print(f"[Santiment] GraphQL 에러: {data['errors'][0].get('message', '')[:100]}")
            return None

        series = data.get("data", {}).get("getMetric", {}).get("timeseriesData", [])
        return series
    except Exception as e:
        print(f"[Santiment] 요청 실패: {e}")
        return None


# ============================
# Santiment 수집 함수들
# ============================

def collect_exchange_netflow():
    """BTC 거래소 넷플로우 수집 (Santiment exchange_balance)"""
    conn = get_connection()

    data = _santiment_query("exchange_balance", "bitcoin")
    if not data:
        print("[Santiment] 넷플로우 데이터 없음")
        conn.close()
        return

    inserted = 0
    for entry in data:
        dt = entry.get("datetime", "")
        value = float(entry.get("value", 0))
        # datetime → unix timestamp
        try:
            from datetime import datetime
            ts = int(datetime.fromisoformat(dt.replace("Z", "+00:00")).timestamp())
        except Exception:
            continue

        exists = conn.execute(
            "SELECT 1 FROM exchange_netflow WHERE asset = 'btc' AND timestamp = ?",
            (ts,),
        ).fetchone()
        if exists:
            continue

        conn.execute(
            "INSERT INTO exchange_netflow (asset, netflow, timestamp) VALUES (?, ?, ?)",
            ("btc", value, ts),
        )
        inserted += 1

    conn.commit()

    if data:
        latest_val = float(data[-1].get("value", 0))
        direction = "유입(매도압)" if latest_val > 0 else "유출(축적)" if latest_val < 0 else "중립"
        print(f"[Santiment] BTC 넷플로우: {latest_val:+,.2f} BTC ({direction}) | "
              f"{inserted}건 신규 (30일 딜레이)")

    conn.close()


def collect_mvrv():
    """MVRV 비율 수집"""
    conn = get_connection()

    data = _santiment_query("mvrv_usd", "bitcoin")
    if not data:
        print("[Santiment] MVRV 데이터 없음")
        conn.close()
        return

    latest = data[-1] if data else {}
    mvrv = float(latest.get("value", 0))
    dt = latest.get("datetime", "")

    try:
        from datetime import datetime
        ts = int(datetime.fromisoformat(dt.replace("Z", "+00:00")).timestamp())
    except Exception:
        ts = 0

    conn.execute(
        "INSERT OR REPLACE INTO onchain_metrics (metric, value, timestamp) VALUES (?, ?, ?)",
        ("mvrv", mvrv, ts),
    )
    conn.commit()

    if mvrv > 3.5:
        signal = "과열(매도 신호)"
    elif mvrv > 2.5:
        signal = "고평가"
    elif mvrv < 1.0:
        signal = "저평가(매수 신호)"
    elif mvrv < 1.5:
        signal = "약간 저평가"
    else:
        signal = "중립"

    print(f"[Santiment] MVRV: {mvrv:.4f} ({signal}) [30일 딜레이]")
    conn.close()


def collect_sopr():
    """SOPR → network_profit_loss로 대체"""
    conn = get_connection()

    data = _santiment_query("network_profit_loss", "bitcoin")
    if not data:
        print("[Santiment] SOPR(NPL) 데이터 없음")
        conn.close()
        return

    latest = data[-1] if data else {}
    value = float(latest.get("value", 0))
    dt = latest.get("datetime", "")

    try:
        from datetime import datetime
        ts = int(datetime.fromisoformat(dt.replace("Z", "+00:00")).timestamp())
    except Exception:
        ts = 0

    conn.execute(
        "INSERT OR REPLACE INTO onchain_metrics (metric, value, timestamp) VALUES (?, ?, ?)",
        ("network_profit_loss", value, ts),
    )
    conn.commit()

    signal = "수익 실현 중" if value > 0 else "손실 매도 중" if value < 0 else "중립"
    print(f"[Santiment] NPL: {value:,.0f} USD ({signal}) [30일 딜레이]")
    conn.close()


# ============================
# Binance Taker Ratio (실시간)
# ============================

def collect_taker_ratio():
    """Binance Taker Buy/Sell Ratio 수집 (실시간 매수/매도 압력)"""
    conn = get_connection()

    for symbol in SYMBOLS:
        try:
            resp = requests.get(
                f"{BINANCE_FUTURES_BASE}/futures/data/takerlongshortRatio",
                params={"symbol": symbol, "period": "1h", "limit": 12},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()

            for entry in data:
                ts = int(entry["timestamp"])
                ratio = float(entry["buySellRatio"])
                buy_vol = float(entry["buyVol"])
                sell_vol = float(entry["sellVol"])

                exists = conn.execute(
                    "SELECT 1 FROM taker_ratio WHERE symbol = ? AND timestamp = ?",
                    (symbol, ts),
                ).fetchone()
                if exists:
                    continue

                conn.execute(
                    "INSERT INTO taker_ratio (symbol, buy_sell_ratio, buy_vol, sell_vol, timestamp) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (symbol, ratio, buy_vol, sell_vol, ts),
                )

            latest = data[-1] if data else {}
            r = float(latest.get("buySellRatio", 0))
            pressure = "매수 우세" if r > 1.0 else "매도 우세" if r < 1.0 else "균형"
            print(f"[Taker] {symbol}: ratio={r:.4f} ({pressure})")

        except Exception as e:
            print(f"[Taker] {symbol} 수집 실패: {e}")

    conn.commit()
    conn.close()


def collect_all_onchain():
    """모든 온체인 메트릭 일괄 수집"""
    # Santiment (30일 딜레이, 월 1000 req 제한 → 최소한으로)
    collect_exchange_netflow()
    collect_mvrv()
    collect_sopr()
    # Binance taker (실시간, 무제한)
    collect_taker_ratio()


# ============================
# 스코어링 엔진용 분석 함수
# ============================

def get_netflow_signal(asset: str = "btc") -> dict:
    """넷플로우 방향 분석 (M.netflow 스코어링용)"""
    conn = get_connection()
    rows = conn.execute(
        "SELECT netflow, timestamp FROM exchange_netflow "
        "WHERE asset = ? ORDER BY timestamp DESC LIMIT 7",
        (asset if asset in ("btc",) else "btc",),
    ).fetchall()
    conn.close()

    if not rows:
        return {
            "direction": "neutral", "latest_netflow": 0,
            "avg_7d": 0, "trend": "flat", "score": 0.0,
        }

    latest = rows[0][0]
    avg_7d = sum(r[0] for r in rows) / len(rows)

    if latest > 0:
        direction = "inflow"
    elif latest < 0:
        direction = "outflow"
    else:
        direction = "neutral"

    if len(rows) >= 4:
        recent = sum(r[0] for r in rows[:3]) / 3
        older = sum(r[0] for r in rows[3:]) / len(rows[3:])
        if recent > older:
            trend = "increasing_inflow"
        elif recent < older:
            trend = "increasing_outflow"
        else:
            trend = "flat"
    else:
        trend = "flat"

    if direction == "neutral":
        score = 0.0
    elif (direction == "outflow" and trend == "increasing_outflow") or \
         (direction == "inflow" and trend == "increasing_inflow"):
        score = 1.0
    else:
        score = 0.5

    return {
        "direction": direction,
        "latest_netflow": round(latest, 4),
        "avg_7d": round(avg_7d, 4),
        "trend": trend,
        "score": round(score, 2),
    }


def get_mvrv_signal() -> dict:
    """MVRV 분석 (V.mvrv 스코어링용)"""
    conn = get_connection()
    row = conn.execute(
        "SELECT value FROM onchain_metrics WHERE metric = 'mvrv' "
        "ORDER BY timestamp DESC LIMIT 1",
    ).fetchone()
    conn.close()

    if not row:
        return {"mvrv": 0, "signal": "no_data", "score": 0.0}

    mvrv = row[0]

    if mvrv > 3.5:
        return {"mvrv": mvrv, "signal": "overheated_bearish", "score": 0.5}
    elif mvrv < 1.0:
        return {"mvrv": mvrv, "signal": "undervalued_bullish", "score": 0.5}
    elif mvrv > 2.5:
        return {"mvrv": mvrv, "signal": "elevated", "score": 0.25}
    elif mvrv < 1.5:
        return {"mvrv": mvrv, "signal": "low", "score": 0.25}
    else:
        return {"mvrv": mvrv, "signal": "neutral", "score": 0.0}


def get_taker_signal(symbol: str = "BTCUSDT") -> dict:
    """Taker ratio 분석 (실시간 매수/매도 압력)"""
    conn = get_connection()
    rows = conn.execute(
        "SELECT buy_sell_ratio FROM taker_ratio "
        "WHERE symbol = ? ORDER BY timestamp DESC LIMIT 12",
        (symbol,),
    ).fetchall()
    conn.close()

    if not rows:
        return {"ratio": 0, "direction": "neutral", "score": 0.0}

    latest = rows[0][0]
    avg = sum(r[0] for r in rows) / len(rows)

    if latest > 1.05 and avg > 1.0:
        return {"ratio": latest, "avg": avg, "direction": "buy_dominant", "score": 0.5}
    elif latest < 0.95 and avg < 1.0:
        return {"ratio": latest, "avg": avg, "direction": "sell_dominant", "score": 0.5}
    else:
        return {"ratio": latest, "avg": avg, "direction": "neutral", "score": 0.0}


if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8")
    from db import init_db
    init_db()
    collect_all_onchain()
    print(f"\nNetflow: {get_netflow_signal()}")
    print(f"MVRV: {get_mvrv_signal()}")
    print(f"Taker BTC: {get_taker_signal('BTCUSDT')}")
