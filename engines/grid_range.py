"""Engine 3: 그리드 범위 계산기 - 오더북 벽 + 거래량 프로파일 + 스푸핑 방어"""
from db import get_connection
from config import SYMBOLS, GRID_COUNT_MIN, GRID_COUNT_MAX, MIN_GRID_SPACING_PCT
from engines.atr import get_latest_atr

# 스푸핑 방어: 가격 허용 오차 (±0.1%)
SPOOFING_PRICE_TOLERANCE = 0.001

# 볼륨 프로파일: 벽 가중치 조정용
VOLUME_BOOST_TOLERANCE = 0.005   # 벽 가격 ±0.5% 범위에서 거래량 탐색
VOLUME_BOOST_MAX = 2.0           # 최대 부스트 배율
VOLUME_DISCOUNT = 0.7            # 거래량 미동반 벽 감소 배율


def _get_volume_profile(conn, symbol: str) -> dict[float, float]:
    """5분봉에서 가격별 거래량 프로파일 생성 (최근 288봉 = 24시간)

    각 캔들의 대표가격 (high+low+close)/3 에 거래량을 배분,
    가격의 0.1% 단위로 비닝하여 가격대별 총 거래량 반환.
    """
    rows = conn.execute(
        "SELECT high, low, close, volume FROM klines "
        "WHERE symbol = ? AND interval = '5m' "
        "ORDER BY open_time DESC LIMIT 288",
        (symbol,),
    ).fetchall()

    if not rows:
        return {}

    volume_at_price: dict[float, float] = {}
    for high, low, close, volume in rows:
        typical = (high + low + close) / 3
        # 가격의 0.1% 단위로 비닝
        bin_size = typical * 0.001
        if bin_size <= 0:
            continue
        price_bin = round(round(typical / bin_size) * bin_size, 2)
        volume_at_price[price_bin] = volume_at_price.get(price_bin, 0) + volume

    return volume_at_price


def _apply_volume_boost(walls: list[tuple], volume_profile: dict) -> tuple[list[tuple], int, int]:
    """오더북 벽에 거래량 부스트/감소 적용

    - 해당 가격대에 거래량 밀집 → 가중치 부스트 (진짜 매물대)
    - 거래량 미미 → 가중치 감소 (스푸핑 가능)

    Returns: (boosted_walls, boosted_count, discounted_count)
    """
    if not volume_profile:
        return walls, 0, 0

    avg_vol = sum(volume_profile.values()) / len(volume_profile)
    if avg_vol <= 0:
        return walls, 0, 0

    boosted = []
    boosted_count = 0
    discounted_count = 0

    for price, qty in walls:
        # 해당 가격 ±0.5% 범위 거래량 합산
        nearby_vol = sum(
            vol for p, vol in volume_profile.items()
            if abs(p - price) / price < VOLUME_BOOST_TOLERANCE
        )

        if nearby_vol >= avg_vol * 1.5:
            # 거래량 1.5배 이상 → 강한 매물대 → 부스트
            factor = min(VOLUME_BOOST_MAX, 1.0 + (nearby_vol / avg_vol - 1) * 0.5)
            boosted_count += 1
        elif nearby_vol >= avg_vol * 0.5:
            # 보통 거래량 → 변경 없음
            factor = 1.0
        else:
            # 거래량 거의 없음 → 약한 매물대 → 감소
            factor = VOLUME_DISCOUNT
            discounted_count += 1

        boosted.append((price, qty * factor))

    return boosted, boosted_count, discounted_count


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

    # 4-1. 볼륨 프로파일 기반 벽 가중치 조정
    volume_profile = _get_volume_profile(conn, symbol)
    bid_walls, bid_boosted, bid_discounted = _apply_volume_boost(bid_walls, volume_profile)
    ask_walls, ask_boosted, ask_discounted = _apply_volume_boost(ask_walls, volume_profile)
    vol_boosted = bid_boosted + ask_boosted
    vol_discounted = bid_discounted + ask_discounted

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

    # 최소 간격 체크: 수수료보다 좁으면 그리드 수 축소 또는 스킵
    if grid_spacing_pct < MIN_GRID_SPACING_PCT:
        # 최소 간격 확보 가능한 그리드 수 계산
        min_spacing_abs = mid_price * MIN_GRID_SPACING_PCT / 100
        new_count = int(grid_range / min_spacing_abs)
        if new_count >= 2:
            grid_count = new_count
            grid_spacing = grid_range / grid_count
            grid_spacing_pct = (grid_spacing / mid_price) * 100
            print(f"[Grid] {symbol}: 간격 확보 위해 그리드 수 축소 → {grid_count}개 ({grid_spacing_pct:.4f}%)")
        else:
            # 범위가 너무 좁아 2칸도 불가 → 그리드 스킵
            print(f"[Grid] {symbol}: 범위 너무 좁음 (총 {grid_range/mid_price*100:.4f}% < 최소 {MIN_GRID_SPACING_PCT*2:.4f}%) - 그리드 비활성화")
            conn.close()
            return None

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
    vol_str = f"vol_boost={vol_boosted}/discount={vol_discounted}" if volume_profile else "vol=N/A"
    print(f"[Grid] {symbol}: ${lower_bound:,.0f} - ${upper_bound:,.0f} | "
          f"{grid_count} grids @ ${grid_spacing:,.0f} ({grid_spacing_pct:.2f}%) | {spoof_str} | {vol_str}")

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

    # 최소 간격 검증: 수수료 이하면 그리드 수 축소
    while grid_spacing_pct < MIN_GRID_SPACING_PCT and grid_count > GRID_COUNT_MIN:
        grid_count -= 1
        grid_spacing = (upper_bound - lower_bound) / grid_count
        grid_spacing_pct = (grid_spacing / price) * 100
    if grid_spacing_pct < MIN_GRID_SPACING_PCT:
        print(f"[Grid] {symbol}: ATR 폴백 간격 {grid_spacing_pct:.4f}% < 최소 {MIN_GRID_SPACING_PCT}% — 생성 불가")
        return None

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
