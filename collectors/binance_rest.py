"""② 바이낸스 REST API 수집기 — OI, 펀딩비, 롱숏비율, 오더북, klines"""
import time
import hashlib
import hmac
import requests
import numpy as np
from db import get_connection
from config import (
    BINANCE_API_KEY, BINANCE_SECRET_KEY, BINANCE_FUTURES_BASE,
    SYMBOLS, ORDERBOOK_DEPTH_LIMIT, ORDERBOOK_WALL_PERCENTILE,
)


def _signed_params(params: dict) -> dict:
    """바이낸스 서명 파라미터 생성"""
    params["timestamp"] = int(time.time() * 1000)
    query = "&".join(f"{k}={v}" for k, v in params.items())
    signature = hmac.new(BINANCE_SECRET_KEY.encode(), query.encode(), hashlib.sha256).hexdigest()
    params["signature"] = signature
    return params


def _headers() -> dict:
    return {"X-MBX-APIKEY": BINANCE_API_KEY}


def _get(endpoint: str, params: dict = None, signed: bool = False) -> dict | list:
    """바이낸스 REST API 호출"""
    url = f"{BINANCE_FUTURES_BASE}{endpoint}"
    params = params or {}
    if signed:
        params = _signed_params(params)
    resp = requests.get(url, params=params, headers=_headers(), timeout=10)
    resp.raise_for_status()
    return resp.json()


# === OI 수집 ===
def collect_open_interest():
    """모든 심볼의 Open Interest 수집"""
    conn = get_connection()
    for symbol in SYMBOLS:
        try:
            data = _get("/fapi/v1/openInterest", {"symbol": symbol})
            oi = float(data["openInterest"])
            conn.execute(
                "INSERT INTO oi_snapshots (symbol, open_interest) VALUES (?, ?)",
                (symbol, oi),
            )
            print(f"[OI] {symbol}: {oi:,.2f}")
        except Exception as e:
            print(f"[OI] {symbol} 수집 실패: {e}")
    conn.commit()
    conn.close()


# === 펀딩비 수집 ===
def collect_funding_rate():
    """최신 펀딩비 수집"""
    conn = get_connection()
    for symbol in SYMBOLS:
        try:
            data = _get("/fapi/v1/fundingRate", {"symbol": symbol, "limit": 1})
            if data:
                rate = float(data[0]["fundingRate"])
                ftime = int(data[0]["fundingTime"])
                conn.execute(
                    "INSERT INTO funding_rates (symbol, funding_rate, funding_time) VALUES (?, ?, ?)",
                    (symbol, rate, ftime),
                )
                print(f"[펀딩비] {symbol}: {rate:.6f} ({rate*100:.4f}%)")
        except Exception as e:
            print(f"[펀딩비] {symbol} 수집 실패: {e}")
    conn.commit()
    conn.close()


# === 롱/숏 비율 수집 ===
def collect_long_short_ratio():
    """글로벌 롱/숏 비율 수집"""
    conn = get_connection()
    for symbol in SYMBOLS:
        try:
            data = _get("/futures/data/globalLongShortAccountRatio", {
                "symbol": symbol, "period": "1h", "limit": 1,
            })
            if data:
                d = data[0]
                conn.execute(
                    "INSERT INTO long_short_ratios (symbol, long_short_ratio, long_account, short_account, timestamp) VALUES (?, ?, ?, ?, ?)",
                    (symbol, float(d["longShortRatio"]), float(d["longAccount"]), float(d["shortAccount"]), int(d["timestamp"])),
                )
                long_pct = float(d["longAccount"]) * 100
                print(f"[롱숏] {symbol}: 롱 {long_pct:.1f}% / 숏 {100-long_pct:.1f}%")
        except Exception as e:
            print(f"[롱숏] {symbol} 수집 실패: {e}")
    conn.commit()
    conn.close()


# === 오더북 벽 수집 ===
def collect_orderbook_walls():
    """오더북 1000단계에서 상위 10% 벽 추출"""
    conn = get_connection()
    scan_id = int(time.time())

    for symbol in SYMBOLS:
        try:
            data = _get("/fapi/v1/depth", {"symbol": symbol, "limit": ORDERBOOK_DEPTH_LIMIT})

            # 매수벽 (bids): 큰 주문량 상위 10%
            bids = [(float(p), float(q)) for p, q in data["bids"]]
            bid_quantities = [q for _, q in bids]
            bid_threshold = np.percentile(bid_quantities, ORDERBOOK_WALL_PERCENTILE)
            bid_walls = [(p, q) for p, q in bids if q >= bid_threshold]

            # 매도벽 (asks): 큰 주문량 상위 10%
            asks = [(float(p), float(q)) for p, q in data["asks"]]
            ask_quantities = [q for _, q in asks]
            ask_threshold = np.percentile(ask_quantities, ORDERBOOK_WALL_PERCENTILE)
            ask_walls = [(p, q) for p, q in asks if q >= ask_threshold]

            for price, qty in bid_walls:
                conn.execute(
                    "INSERT INTO orderbook_walls (symbol, side, price, quantity, scan_id) VALUES (?, ?, ?, ?, ?)",
                    (symbol, "BID", price, qty, scan_id),
                )
            for price, qty in ask_walls:
                conn.execute(
                    "INSERT INTO orderbook_walls (symbol, side, price, quantity, scan_id) VALUES (?, ?, ?, ?, ?)",
                    (symbol, "ASK", price, qty, scan_id),
                )

            print(f"[오더북] {symbol}: 매수벽 {len(bid_walls)}개 / 매도벽 {len(ask_walls)}개")
        except Exception as e:
            print(f"[오더북] {symbol} 수집 실패: {e}")

    conn.commit()
    conn.close()


# === Klines 수집 (ATR 계산용 - 일봉) ===
def collect_klines():
    """일봉 14일치 수집 (ATR 계산용)"""
    conn = get_connection()
    for symbol in SYMBOLS:
        try:
            data = _get("/fapi/v1/klines", {
                "symbol": symbol, "interval": "1d", "limit": 15,
            })
            for k in data:
                conn.execute(
                    """INSERT OR REPLACE INTO klines
                    (symbol, interval, open_time, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (symbol, "1d", int(k[0]), float(k[1]), float(k[2]),
                     float(k[3]), float(k[4]), float(k[5])),
                )

            # ATR 계산 (참고 출력)
            highs = [float(k[2]) for k in data]
            lows = [float(k[3]) for k in data]
            closes = [float(k[4]) for k in data]
            tr_list = []
            for i in range(1, len(data)):
                tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
                tr_list.append(tr)
            atr = np.mean(tr_list) if tr_list else 0
            atr_pct = (atr / closes[-1]) * 100 if closes else 0
            print(f"[ATR] {symbol}: ATR(14d) = ${atr:,.2f} ({atr_pct:.2f}%) → 스톱로스 {atr_pct*1.5:.2f}%")

        except Exception as e:
            print(f"[Klines] {symbol} 일봉 수집 실패: {e}")
    conn.commit()
    conn.close()


# === 5분봉 수집 (실시간 가격 + 전략 판단용) ===
def collect_klines_5m():
    """5분봉 최근 100개 수집 (약 8시간치, 전략 엔진 실시간 판단용)"""
    conn = get_connection()
    for symbol in SYMBOLS:
        try:
            data = _get("/fapi/v1/klines", {
                "symbol": symbol, "interval": "5m", "limit": 100,
            })
            for k in data:
                conn.execute(
                    """INSERT OR REPLACE INTO klines
                    (symbol, interval, open_time, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (symbol, "5m", int(k[0]), float(k[1]), float(k[2]),
                     float(k[3]), float(k[4]), float(k[5])),
                )

            latest_close = float(data[-1][4]) if data else 0
            print(f"[5m] {symbol}: {len(data)}개 수집 | 최신가 ${latest_close:,.2f}")

        except Exception as e:
            print(f"[Klines] {symbol} 5분봉 수집 실패: {e}")
    conn.commit()
    conn.close()


if __name__ == "__main__":
    from db import init_db
    init_db()
    print("=== OI ===")
    collect_open_interest()
    print("=== 펀딩비 ===")
    collect_funding_rate()
    print("=== 롱숏비율 ===")
    collect_long_short_ratio()
    print("=== 오더북 ===")
    collect_orderbook_walls()
    print("=== Klines 일봉 ===")
    collect_klines()
    print("=== Klines 5분봉 ===")
    collect_klines_5m()
