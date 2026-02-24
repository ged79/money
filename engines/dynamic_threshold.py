"""Engine 2: 동적 임계점 - 청산 캐스케이드 감지 + 트리거 판정"""
import time
from db import get_connection
from config import SYMBOLS, L2_TRIGGER_THRESHOLD_PCT


def calculate_threshold(symbol: str = None) -> dict | None:
    """동적 임계점 계산 후 threshold_signals 테이블 저장"""
    symbols = [symbol] if symbol else SYMBOLS
    result = None
    for sym in symbols:
        r = _calc_single(sym)
        if r:
            result = r
    return result


def _calc_single(symbol: str) -> dict | None:
    conn = get_connection()

    # 1. 최근 1시간 청산 금액 (side별)
    now_ms = int(time.time() * 1000)
    one_hour_ago_ms = now_ms - 3600_000

    liq_rows = conn.execute(
        "SELECT side, SUM(price * qty) as amount FROM liquidations "
        "WHERE symbol = ? AND trade_time > ? GROUP BY side",
        (symbol, one_hour_ago_ms),
    ).fetchall()

    buy_liq = 0.0   # BUY side = 숏 청산
    sell_liq = 0.0   # SELL side = 롱 청산
    for row in liq_rows:
        if row[0] == "BUY":
            buy_liq = row[1] or 0.0
        elif row[0] == "SELL":
            sell_liq = row[1] or 0.0

    liq_amount_1h = buy_liq + sell_liq

    # 2. 현재 OI
    oi_row = conn.execute(
        "SELECT open_interest FROM oi_snapshots "
        "WHERE symbol = ? ORDER BY collected_at DESC LIMIT 1",
        (symbol,),
    ).fetchone()

    if not oi_row:
        print(f"[Threshold] {symbol}: OI 데이터 없음 - 스킵")
        conn.close()
        return None

    current_oi = oi_row[0]

    # 3. 현재가 (최신 kline 종가)
    price_row = conn.execute(
        "SELECT close FROM klines WHERE symbol = ? AND interval = '1d' "
        "ORDER BY open_time DESC LIMIT 1",
        (symbol,),
    ).fetchone()

    current_price = price_row[0] if price_row else 0

    # 4. 유동성 계수 = 당일 거래량 / 30일 평균 거래량
    vol_rows = conn.execute(
        "SELECT volume FROM klines WHERE symbol = ? AND interval = '1d' "
        "ORDER BY open_time DESC LIMIT 30",
        (symbol,),
    ).fetchall()

    if vol_rows:
        current_volume = vol_rows[0][0]
        avg_volume = sum(r[0] for r in vol_rows) / len(vol_rows)
        liquidity_coeff = current_volume / avg_volume if avg_volume > 0 else 1.0
        # 극단값 방지
        liquidity_coeff = max(0.1, min(10.0, liquidity_coeff))
    else:
        liquidity_coeff = 1.0

    # 5. 임계점 계산
    # OI는 BTC 수량이므로 USD 환산: oi_usd = current_oi * current_price
    oi_usd = current_oi * current_price if current_price > 0 else current_oi

    if oi_usd > 0:
        threshold_value = (liq_amount_1h / oi_usd) * liquidity_coeff
    else:
        threshold_value = 0.0

    # 6. 트리거 판정: 청산금액 > OI_USD × 1%
    trigger_active = liq_amount_1h > (oi_usd * L2_TRIGGER_THRESHOLD_PCT)

    # 7. 방향 판정
    direction = None
    if trigger_active:
        if buy_liq > sell_liq:
            direction = "SHORT_CASCADE"  # 숏 청산 우세 = 가격 상승 방향
        else:
            direction = "LONG_CASCADE"   # 롱 청산 우세 = 가격 하락 방향

    # DB 저장
    conn.execute(
        "INSERT INTO threshold_signals "
        "(symbol, threshold_value, liq_amount_1h, current_oi, liquidity_coeff, trigger_active, direction) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (symbol, round(threshold_value, 8), round(liq_amount_1h, 2),
         current_oi, round(liquidity_coeff, 4),
         1 if trigger_active else 0, direction),
    )
    conn.commit()
    conn.close()

    result = {
        "symbol": symbol,
        "threshold_value": round(threshold_value, 8),
        "liq_amount_1h": round(liq_amount_1h, 2),
        "current_oi": current_oi,
        "oi_usd": round(oi_usd, 2),
        "liquidity_coeff": round(liquidity_coeff, 4),
        "trigger_active": trigger_active,
        "direction": direction,
    }

    trigger_str = "ON" if trigger_active else "OFF"
    dir_str = f" [{direction}]" if direction else ""
    print(f"[Threshold] {symbol}: threshold={threshold_value:.6f} | "
          f"1h_liq=${liq_amount_1h:,.0f} | OI={current_oi:,.0f} | "
          f"coeff={liquidity_coeff:.2f} | trigger={trigger_str}{dir_str}")

    return result


def get_latest_threshold(symbol: str = "BTCUSDT") -> dict | None:
    """threshold_signals에서 최신 결과 조회"""
    conn = get_connection()
    row = conn.execute(
        "SELECT threshold_value, liq_amount_1h, current_oi, liquidity_coeff, "
        "trigger_active, direction, calculated_at "
        "FROM threshold_signals WHERE symbol = ? ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    conn.close()

    if not row:
        return None

    return {
        "symbol": symbol,
        "threshold_value": row[0],
        "liq_amount_1h": row[1],
        "current_oi": row[2],
        "liquidity_coeff": row[3],
        "trigger_active": bool(row[4]),
        "direction": row[5],
        "calculated_at": row[6],
    }


if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8")
    from db import init_db
    init_db()
    calculate_threshold()
