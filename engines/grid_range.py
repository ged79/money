"""Engine 3: 그리드 범위 계산기 - 오더북 벽 기반 + 스푸핑 방어"""
from db import get_connection
from config import SYMBOLS, GRID_COUNT_MIN, GRID_COUNT_MAX
from engines.atr import get_latest_atr

# 스푸핑 방어: 가격 허용 오차 (±0.1%)
SPOOFING_PRICE_TOLERANCE = 0.001


def calculate_grid_range(symbol: str = None) -> dict | None:
    """오더북 벽 기반 그리드 범위 계산 후 grid_configs 테이블 저장"""
    symbols = [symbol] if symbol else SYMBOLS
    result = None
    for sym in symbols:
        r = _calc_single(sym)
        if r:
            result = r
    return result


def _calc_single(symbol: str) -> dict | None:
    conn = get_connection()

    # 1. 최근 2회 스캔 ID 가져오기
    scan_ids = conn.execute(
        "SELECT DISTINCT scan_id FROM orderbook_walls "
        "WHERE symbol = ? ORDER BY scan_id DESC LIMIT 2",
        (symbol,),
    ).fetchall()

    if not scan_ids:
        print(f"[Grid] {symbol}: 오더북 데이터 없음 - 폴백 사용")
        result = _fallback_grid(symbol, conn)
        conn.close()
        return result

    latest_scan = scan_ids[0][0]
    has_two_scans = len(scan_ids) >= 2
    prev_scan = scan_ids[1][0] if has_two_scans else None

    # 2. 최신 스캔의 벽 로드
    latest_walls = conn.execute(
        "SELECT side, price, quantity FROM orderbook_walls "
        "WHERE symbol = ? AND scan_id = ?",
        (symbol, latest_scan),
    ).fetchall()

    # 3. 스푸핑 방어: 2회 연속 출현 벽만 채택
    spoofing_filtered = 0
    if has_two_scans:
        prev_walls = conn.execute(
            "SELECT side, price, quantity FROM orderbook_walls "
            "WHERE symbol = ? AND scan_id = ?",
            (symbol, prev_scan),
        ).fetchall()

        confirmed_walls = []
        for wall in latest_walls:
            side, price, qty = wall
            # 이전 스캔에서 동일 가격대(±0.1%) 벽 존재 여부
            matched = any(
                w[0] == side and abs(w[1] - price) / price < SPOOFING_PRICE_TOLERANCE
                for w in prev_walls
            )
            if matched:
                confirmed_walls.append(wall)
            else:
                spoofing_filtered += 1

        walls = confirmed_walls
    else:
        walls = latest_walls
        spoofing_filtered = -1  # 스푸핑 필터 미적용 표시

    # 4. BID/ASK 벽 분리
    bid_walls = [(price, qty) for side, price, qty in walls if side == "BID"]
    ask_walls = [(price, qty) for side, price, qty in walls if side == "ASK"]

    if not bid_walls or not ask_walls:
        print(f"[Grid] {symbol}: 확인된 벽 부족 (bid={len(bid_walls)}, ask={len(ask_walls)}) - 폴백 사용")
        result = _fallback_grid(symbol, conn)
        conn.close()
        return result

    # 5. 수량 가중 대표 가격 계산
    # BID: 상위 벽들의 가중 평균 → 하한 (지지선)
    bid_walls.sort(key=lambda x: x[1], reverse=True)  # 수량 큰 순
    top_bids = bid_walls[:10]  # 상위 10개 벽
    bid_total_qty = sum(q for _, q in top_bids)
    lower_bound = sum(p * q for p, q in top_bids) / bid_total_qty if bid_total_qty > 0 else top_bids[0][0]

    # ASK: 상위 벽들의 가중 평균 → 상한 (저항선)
    ask_walls.sort(key=lambda x: x[1], reverse=True)
    top_asks = ask_walls[:10]
    ask_total_qty = sum(q for _, q in top_asks)
    upper_bound = sum(p * q for p, q in top_asks) / ask_total_qty if ask_total_qty > 0 else top_asks[0][0]

    # 범위 유효성 검증
    if lower_bound >= upper_bound:
        print(f"[Grid] {symbol}: 범위 역전 (lower={lower_bound:.0f} >= upper={upper_bound:.0f}) - 폴백 사용")
        result = _fallback_grid(symbol, conn)
        conn.close()
        return result

    # 6. 그리드 수 결정 (ATR 기반)
    atr_data = get_latest_atr(symbol)
    grid_range = upper_bound - lower_bound

    if atr_data and atr_data["atr"] > 0:
        raw_count = round(grid_range / atr_data["atr"])
        grid_count = max(GRID_COUNT_MIN, min(GRID_COUNT_MAX, raw_count))
    else:
        grid_count = 12  # 기본값

    grid_spacing = grid_range / grid_count
    mid_price = (lower_bound + upper_bound) / 2
    grid_spacing_pct = (grid_spacing / mid_price) * 100

    # DB 저장
    conn.execute(
        "INSERT INTO grid_configs "
        "(symbol, lower_bound, upper_bound, grid_count, grid_spacing, grid_spacing_pct, spoofing_filtered) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (symbol, round(lower_bound, 2), round(upper_bound, 2),
         grid_count, round(grid_spacing, 2), round(grid_spacing_pct, 4),
         spoofing_filtered),
    )
    conn.commit()
    conn.close()

    result = {
        "symbol": symbol,
        "lower_bound": round(lower_bound, 2),
        "upper_bound": round(upper_bound, 2),
        "grid_count": grid_count,
        "grid_spacing": round(grid_spacing, 2),
        "grid_spacing_pct": round(grid_spacing_pct, 4),
        "spoofing_filtered": spoofing_filtered,
    }

    spoof_str = f"spoofing={spoofing_filtered}" if spoofing_filtered >= 0 else "spoofing=N/A(1scan)"
    print(f"[Grid] {symbol}: ${lower_bound:,.0f} - ${upper_bound:,.0f} | "
          f"{grid_count} grids @ ${grid_spacing:,.0f} ({grid_spacing_pct:.2f}%) | {spoof_str}")

    return result


def _fallback_grid(symbol: str, conn) -> dict | None:
    """벽 데이터 없을 때 ATR 기반 폴백"""
    atr_data = get_latest_atr(symbol)
    if not atr_data:
        print(f"[Grid] {symbol}: ATR 데이터도 없음 - 그리드 생성 불가")
        return None

    price = atr_data["current_price"]
    atr = atr_data["atr"]
    lower_bound = price - 2 * atr
    upper_bound = price + 2 * atr
    grid_count = 12
    grid_spacing = (upper_bound - lower_bound) / grid_count
    grid_spacing_pct = (grid_spacing / price) * 100

    conn.execute(
        "INSERT INTO grid_configs "
        "(symbol, lower_bound, upper_bound, grid_count, grid_spacing, grid_spacing_pct, spoofing_filtered) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (symbol, round(lower_bound, 2), round(upper_bound, 2),
         grid_count, round(grid_spacing, 2), round(grid_spacing_pct, 4), -1),
    )
    conn.commit()

    result = {
        "symbol": symbol,
        "lower_bound": round(lower_bound, 2),
        "upper_bound": round(upper_bound, 2),
        "grid_count": grid_count,
        "grid_spacing": round(grid_spacing, 2),
        "grid_spacing_pct": round(grid_spacing_pct, 4),
        "spoofing_filtered": -1,
    }

    print(f"[Grid] {symbol}: ${lower_bound:,.0f} - ${upper_bound:,.0f} (ATR 폴백) | "
          f"{grid_count} grids @ ${grid_spacing:,.0f} ({grid_spacing_pct:.2f}%)")

    return result


def get_latest_grid(symbol: str = "BTCUSDT") -> dict | None:
    """grid_configs에서 최신 그리드 설정 조회"""
    conn = get_connection()
    row = conn.execute(
        "SELECT id, lower_bound, upper_bound, grid_count, grid_spacing, "
        "grid_spacing_pct, spoofing_filtered, calculated_at "
        "FROM grid_configs WHERE symbol = ? ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    conn.close()

    if not row:
        return None

    return {
        "id": row[0],
        "symbol": symbol,
        "lower_bound": row[1],
        "upper_bound": row[2],
        "grid_count": row[3],
        "grid_spacing": row[4],
        "grid_spacing_pct": row[5],
        "spoofing_filtered": row[6],
        "calculated_at": row[7],
    }


if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8")
    from db import init_db
    init_db()
    calculate_grid_range()
