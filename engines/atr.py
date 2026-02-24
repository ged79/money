"""Engine 1: ATR 계산기 - klines 기반 ATR + 스톱로스 산출"""
from db import get_connection
from config import SYMBOLS, ATR_STOP_LOSS_MULTIPLIER


def calculate_atr(symbol: str = None, period: int = 14) -> dict | None:
    """klines 테이블에서 ATR 계산 후 atr_values에 저장"""
    symbols = [symbol] if symbol else SYMBOLS
    result = None

    for sym in symbols:
        r = _calc_single(sym, period)
        if r:
            result = r

    return result


def _calc_single(symbol: str, period: int) -> dict | None:
    conn = get_connection()

    # 최근 period+1일 캔들 조회 (TR 계산에 전일 종가 필요)
    rows = conn.execute(
        "SELECT open, high, low, close, volume FROM klines "
        "WHERE symbol = ? AND interval = '1d' "
        "ORDER BY open_time DESC LIMIT ?",
        (symbol, period + 1),
    ).fetchall()

    if len(rows) < period + 1:
        print(f"[ATR Engine] {symbol}: 데이터 부족 ({len(rows)}/{period+1}일) - 스킵")
        conn.close()
        return None

    # 시간 순서로 뒤집기 (oldest first)
    rows = list(reversed(rows))

    # TR 계산
    tr_values = []
    for i in range(1, len(rows)):
        h = rows[i][1]
        l = rows[i][2]
        c_prev = rows[i - 1][3]
        tr = max(h - l, abs(h - c_prev), abs(l - c_prev))
        tr_values.append(tr)

    atr = sum(tr_values) / len(tr_values)
    current_price = rows[-1][3]  # 최신 종가
    atr_pct = (atr / current_price) * 100
    stop_loss_pct = atr_pct * ATR_STOP_LOSS_MULTIPLIER

    # DB 저장
    conn.execute(
        "INSERT INTO atr_values (symbol, atr, atr_pct, stop_loss_pct, current_price) "
        "VALUES (?, ?, ?, ?, ?)",
        (symbol, round(atr, 2), round(atr_pct, 4), round(stop_loss_pct, 4), current_price),
    )
    conn.commit()
    conn.close()

    result = {
        "symbol": symbol,
        "atr": round(atr, 2),
        "atr_pct": round(atr_pct, 4),
        "stop_loss_pct": round(stop_loss_pct, 4),
        "current_price": current_price,
    }

    print(f"[ATR Engine] {symbol}: ATR(14d) = ${atr:,.2f} ({atr_pct:.2f}%) "
          f"-> 스톱로스 {stop_loss_pct:.2f}%")

    return result


def get_latest_atr(symbol: str = "BTCUSDT") -> dict | None:
    """atr_values 테이블에서 최신 ATR 조회 (재계산 없이)"""
    conn = get_connection()
    row = conn.execute(
        "SELECT atr, atr_pct, stop_loss_pct, current_price, calculated_at "
        "FROM atr_values WHERE symbol = ? ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    conn.close()

    if not row:
        return None

    return {
        "symbol": symbol,
        "atr": row[0],
        "atr_pct": row[1],
        "stop_loss_pct": row[2],
        "current_price": row[3],
        "calculated_at": row[4],
    }


if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8")
    from db import init_db
    init_db()
    calculate_atr()
