"""백테스트용 히스토리 데이터 다운로더 — Binance REST + alternative.me"""
import time
import sqlite3
import requests
from datetime import datetime, timedelta

from backtest.config_bt import BT_DB_PATH, BT_DAYS, BT_SYMBOLS

BINANCE_FUTURES_BASE = "https://fapi.binance.com"
BINANCE_DATA_BASE = "https://fapi.binance.com/futures/data"

# 요청 간 대기 (rate limit 방지)
REQUEST_DELAY = 0.3


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(BT_DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def download_all(days: int = None):
    """모든 히스토리 데이터 다운로드"""
    days = days or BT_DAYS
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - (days * 86400 * 1000)

    # Binance /futures/data/* 엔드포인트는 최대 30일만 지원
    # 30일 제한 적용
    max_data_days = 29
    data_start_ms = max(start_ms, end_ms - (max_data_days * 86400 * 1000))

    for symbol in BT_SYMBOLS:
        print(f"\n{'='*50}")
        print(f"  다운로드 시작: {symbol} ({days}일)")
        print(f"{'='*50}")

        download_klines_5m(symbol, start_ms, end_ms)
        download_klines_1d(symbol, start_ms, end_ms)
        # OI/LS/Taker: Binance API는 최근 30일만 지원
        download_oi_history(symbol, data_start_ms, end_ms)
        download_funding_rates(symbol, start_ms, end_ms)  # funding은 1000개까지 OK
        download_long_short_ratio(symbol, data_start_ms, end_ms)
        download_taker_ratio(symbol, data_start_ms, end_ms)

    download_fear_greed(days)
    print(f"\n[Download] 전체 다운로드 완료!")


# ============================
# 5분봉 (5m klines)
# ============================

def download_klines_5m(symbol: str, start_ms: int, end_ms: int):
    """5분봉 다운로드 (limit=1500 per request)"""
    conn = _get_conn()
    total = 0
    current_start = start_ms
    chunk_size = 1500  # max per request
    interval_ms = 300_000  # 5분 = 300,000ms

    while current_start < end_ms:
        try:
            resp = requests.get(
                f"{BINANCE_FUTURES_BASE}/fapi/v1/klines",
                params={
                    "symbol": symbol,
                    "interval": "5m",
                    "startTime": current_start,
                    "limit": chunk_size,
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"[Download] 5m klines 실패: {e}")
            break

        if not data:
            break

        for k in data:
            open_time = int(k[0])
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO klines "
                    "(symbol, interval, open_time, open, high, low, close, volume) "
                    "VALUES (?, '5m', ?, ?, ?, ?, ?, ?)",
                    (symbol, open_time,
                     float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])),
                )
            except sqlite3.IntegrityError:
                pass

        total += len(data)
        current_start = int(data[-1][0]) + interval_ms

        if len(data) < chunk_size:
            break

        time.sleep(REQUEST_DELAY)

    conn.commit()
    conn.close()
    print(f"[Download] {symbol} 5m klines: {total}건")


# ============================
# 일봉 (1d klines)
# ============================

def download_klines_1d(symbol: str, start_ms: int, end_ms: int):
    """일봉 다운로드 — ATR(14) 계산을 위해 최소 100일치 확보"""
    conn = _get_conn()

    # ATR(14)는 최소 15일 필요. 넉넉히 100일 전부터 다운로드
    min_start = end_ms - (100 * 86400 * 1000)
    actual_start = min(start_ms, min_start)

    try:
        resp = requests.get(
            f"{BINANCE_FUTURES_BASE}/fapi/v1/klines",
            params={
                "symbol": symbol,
                "interval": "1d",
                "startTime": actual_start,
                "limit": 100,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"[Download] 1d klines 실패: {e}")
        return

    for k in data:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO klines "
                "(symbol, interval, open_time, open, high, low, close, volume) "
                "VALUES (?, '1d', ?, ?, ?, ?, ?, ?)",
                (symbol, int(k[0]),
                 float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])),
            )
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    conn.close()
    print(f"[Download] {symbol} 1d klines: {len(data)}건")
    time.sleep(REQUEST_DELAY)


# ============================
# OI 히스토리
# ============================

def download_oi_history(symbol: str, start_ms: int, end_ms: int):
    """Open Interest 히스토리 다운로드 (1h 간격)"""
    conn = _get_conn()
    total = 0
    current_start = start_ms

    while current_start < end_ms:
        try:
            resp = requests.get(
                f"{BINANCE_DATA_BASE}/openInterestHist",
                params={
                    "symbol": symbol,
                    "period": "1h",
                    "startTime": current_start,
                    "limit": 500,
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"[Download] OI history 실패: {e}")
            break

        if not data:
            break

        for entry in data:
            ts = int(entry["timestamp"])
            oi = float(entry["sumOpenInterest"])
            collected_at = datetime.fromtimestamp(ts / 1000).isoformat()
            conn.execute(
                "INSERT INTO oi_snapshots (symbol, open_interest, collected_at) "
                "VALUES (?, ?, ?)",
                (symbol, oi, collected_at),
            )

        total += len(data)
        current_start = int(data[-1]["timestamp"]) + 3600_000  # +1h

        if len(data) < 500:
            break

        time.sleep(REQUEST_DELAY)

    conn.commit()
    conn.close()
    print(f"[Download] {symbol} OI history: {total}건")


# ============================
# 펀딩비
# ============================

def download_funding_rates(symbol: str, start_ms: int, end_ms: int):
    """Funding rate 히스토리 다운로드"""
    conn = _get_conn()
    total = 0
    current_start = start_ms

    while current_start < end_ms:
        try:
            resp = requests.get(
                f"{BINANCE_FUTURES_BASE}/fapi/v1/fundingRate",
                params={
                    "symbol": symbol,
                    "startTime": current_start,
                    "limit": 1000,
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"[Download] funding rates 실패: {e}")
            break

        if not data:
            break

        for entry in data:
            funding_time = int(entry["fundingTime"])
            funding_rate = float(entry["fundingRate"])
            collected_at = datetime.fromtimestamp(funding_time / 1000).isoformat()
            conn.execute(
                "INSERT INTO funding_rates (symbol, funding_rate, funding_time, collected_at) "
                "VALUES (?, ?, ?, ?)",
                (symbol, funding_rate, funding_time, collected_at),
            )

        total += len(data)
        current_start = int(data[-1]["fundingTime"]) + 1

        if len(data) < 1000:
            break

        time.sleep(REQUEST_DELAY)

    conn.commit()
    conn.close()
    print(f"[Download] {symbol} funding rates: {total}건")


# ============================
# 롱숏비율
# ============================

def download_long_short_ratio(symbol: str, start_ms: int, end_ms: int):
    """Global Long/Short Account Ratio 히스토리"""
    conn = _get_conn()
    total = 0
    current_start = start_ms

    while current_start < end_ms:
        try:
            resp = requests.get(
                f"{BINANCE_DATA_BASE}/globalLongShortAccountRatio",
                params={
                    "symbol": symbol,
                    "period": "1h",
                    "startTime": current_start,
                    "limit": 500,
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"[Download] L/S ratio 실패: {e}")
            break

        if not data:
            break

        for entry in data:
            ts = int(entry["timestamp"])
            ratio = float(entry["longShortRatio"])
            long_acc = float(entry["longAccount"])
            short_acc = float(entry["shortAccount"])
            collected_at = datetime.fromtimestamp(ts / 1000).isoformat()
            conn.execute(
                "INSERT INTO long_short_ratios "
                "(symbol, long_short_ratio, long_account, short_account, timestamp, collected_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (symbol, ratio, long_acc, short_acc, ts, collected_at),
            )

        total += len(data)
        current_start = int(data[-1]["timestamp"]) + 3600_000

        if len(data) < 500:
            break

        time.sleep(REQUEST_DELAY)

    conn.commit()
    conn.close()
    print(f"[Download] {symbol} L/S ratio: {total}건")


# ============================
# 테이커 비율
# ============================

def download_taker_ratio(symbol: str, start_ms: int, end_ms: int):
    """Taker Buy/Sell Ratio 히스토리"""
    conn = _get_conn()
    total = 0
    current_start = start_ms

    while current_start < end_ms:
        try:
            resp = requests.get(
                f"{BINANCE_DATA_BASE}/takerlongshortRatio",
                params={
                    "symbol": symbol,
                    "period": "1h",
                    "startTime": current_start,
                    "limit": 500,
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"[Download] taker ratio 실패: {e}")
            break

        if not data:
            break

        for entry in data:
            ts = int(entry["timestamp"])
            ratio = float(entry["buySellRatio"])
            buy_vol = float(entry["buyVol"])
            sell_vol = float(entry["sellVol"])
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO taker_ratio "
                    "(symbol, buy_sell_ratio, buy_vol, sell_vol, timestamp) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (symbol, ratio, buy_vol, sell_vol, ts),
                )
            except sqlite3.IntegrityError:
                pass

        total += len(data)
        current_start = int(data[-1]["timestamp"]) + 3600_000

        if len(data) < 500:
            break

        time.sleep(REQUEST_DELAY)

    conn.commit()
    conn.close()
    print(f"[Download] {symbol} taker ratio: {total}건")


# ============================
# Fear & Greed Index
# ============================

def download_fear_greed(days: int):
    """Crypto Fear & Greed Index 다운로드"""
    conn = _get_conn()

    try:
        resp = requests.get(
            f"https://api.alternative.me/fng/",
            params={"limit": days, "format": "json"},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json().get("data", [])
    except Exception as e:
        print(f"[Download] F&G 실패: {e}")
        return

    for entry in data:
        value = int(entry["value"])
        classification = entry["value_classification"]
        fg_timestamp = int(entry["timestamp"])
        collected_at = datetime.fromtimestamp(fg_timestamp).isoformat()

        conn.execute(
            "INSERT INTO fear_greed (value, classification, fg_timestamp, collected_at) "
            "VALUES (?, ?, ?, ?)",
            (value, classification, fg_timestamp, collected_at),
        )

    conn.commit()
    conn.close()
    print(f"[Download] F&G index: {len(data)}건")


# ============================
# 합성 청산 데이터 생성
# ============================

def generate_synthetic_liquidations(symbols: list = None):
    """OI 변동 기반 합성 청산 데이터 생성

    논리: 1시간 OI 변동률 > 2% → 대형 청산 이벤트 가정
    - OI 감소 + 가격 하락 → SELL (롱 청산)
    - OI 감소 + 가격 상승 → BUY (숏 청산)
    """
    symbols = symbols or BT_SYMBOLS
    conn = _get_conn()

    for symbol in symbols:
        # OI 스냅샷 (시간순)
        oi_rows = conn.execute(
            "SELECT open_interest, collected_at FROM oi_snapshots "
            "WHERE symbol = ? ORDER BY collected_at ASC",
            (symbol,),
        ).fetchall()

        if len(oi_rows) < 2:
            print(f"[Synthetic] {symbol}: OI 데이터 부족")
            continue

        # 5m kline으로 가격 조회용 캐시 구축
        kline_rows = conn.execute(
            "SELECT open_time, close FROM klines "
            "WHERE symbol = ? AND interval = '5m' ORDER BY open_time ASC",
            (symbol,),
        ).fetchall()

        if not kline_rows:
            print(f"[Synthetic] {symbol}: kline 데이터 부족")
            continue

        # open_time(ms) → close 가격 딕셔너리
        price_map = {int(r[0]): r[1] for r in kline_rows}
        price_times = sorted(price_map.keys())

        total_events = 0

        for i in range(1, len(oi_rows)):
            oi_now = oi_rows[i][0]
            oi_prev = oi_rows[i - 1][0]
            collected_at = oi_rows[i][1]

            if oi_prev == 0:
                continue

            oi_delta_pct = (oi_now - oi_prev) / oi_prev

            # 1% OI 변동 이상 시 합성 청산 이벤트 생성
            if abs(oi_delta_pct) <= 0.01:
                continue

            # collected_at → timestamp (ms)
            try:
                ts = int(datetime.fromisoformat(collected_at.replace("Z", "+00:00")).timestamp() * 1000)
            except Exception:
                continue

            # 가장 가까운 5m kline에서 가격 가져오기
            price = _find_nearest_price(price_times, price_map, ts)
            if not price:
                continue

            # 이전 시점 가격
            prev_collected_at = oi_rows[i - 1][1]
            try:
                prev_ts = int(datetime.fromisoformat(
                    prev_collected_at.replace("Z", "+00:00")).timestamp() * 1000)
            except Exception:
                continue

            prev_price = _find_nearest_price(price_times, price_map, prev_ts)
            if not prev_price:
                continue

            price_delta = price - prev_price

            # 방향 결정
            if oi_delta_pct < 0 and price_delta < 0:
                side = "SELL"  # 롱 청산
            elif oi_delta_pct < 0 and price_delta >= 0:
                side = "BUY"   # 숏 청산
            elif oi_delta_pct > 0 and price_delta < 0:
                side = "BUY"   # 숏 스퀴즈
            else:
                side = "SELL"  # 롱 스퀴즈

            # 청산 규모 추정 (OI 변동 = 청산된 BTC 수량)
            liq_qty_btc = abs(oi_now - oi_prev)  # BTC 단위

            # 여러 건으로 분산 (현실감)
            event_count = max(1, int(abs(oi_delta_pct) * 100))
            qty_per_event = liq_qty_btc / event_count

            for j in range(event_count):
                trade_time = ts + (j * 60_000)  # 1분 간격으로 분산
                conn.execute(
                    "INSERT INTO liquidations (symbol, side, price, qty, trade_time) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (symbol, side, price, round(qty_per_event, 6), trade_time),
                )
                total_events += 1

        conn.commit()
        print(f"[Synthetic] {symbol}: {total_events}건 합성 청산 생성")

    conn.close()


def _find_nearest_price(sorted_times: list, price_map: dict, target_ms: int) -> float | None:
    """이진 탐색으로 가장 가까운 5m kline 가격 찾기"""
    if not sorted_times:
        return None

    import bisect
    idx = bisect.bisect_left(sorted_times, target_ms)

    if idx == 0:
        return price_map[sorted_times[0]]
    if idx >= len(sorted_times):
        return price_map[sorted_times[-1]]

    before = sorted_times[idx - 1]
    after = sorted_times[idx]

    if (target_ms - before) <= (after - target_ms):
        return price_map[before]
    else:
        return price_map[after]


if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8")
    from backtest.db_bt import init_backtest_db
    init_backtest_db()
    download_all()
    generate_synthetic_liquidations()
