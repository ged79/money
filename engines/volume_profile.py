"""Engine: 볼륨 프로파일 — POC, HVN/LVN, 오더북 결합 S/R"""
from db import get_connection
from config import SYMBOLS


def build_volume_profile(symbol: str, interval: str = "4h", lookback: int = 180, n_buckets: int = 50) -> dict:
    """가격대별 거래량 분포(볼륨 프로파일) 생성

    Returns:
        buckets: [{"price_low": f, "price_high": f, "volume": f}, ...]
        poc: float (Point of Control — 최대 거래량 가격대)
        value_area_high: float (거래량 70% 상위)
        value_area_low: float (거래량 70% 하위)
    """
    conn = get_connection()
    rows = conn.execute(
        "SELECT high, low, close, volume FROM klines "
        "WHERE symbol = ? AND interval = ? ORDER BY open_time DESC LIMIT ?",
        (symbol, interval, lookback),
    ).fetchall()
    conn.close()

    if len(rows) < 10:
        return {"buckets": [], "poc": 0, "value_area_high": 0, "value_area_low": 0}

    # 전체 가격 범위
    all_highs = [r[0] for r in rows]
    all_lows = [r[1] for r in rows]
    price_min = min(all_lows)
    price_max = max(all_highs)

    if price_max <= price_min:
        return {"buckets": [], "poc": 0, "value_area_high": 0, "value_area_low": 0}

    bucket_size = (price_max - price_min) / n_buckets

    # 버킷 초기화
    buckets = []
    for i in range(n_buckets):
        low = price_min + i * bucket_size
        high = price_min + (i + 1) * bucket_size
        buckets.append({"price_low": round(low, 4), "price_high": round(high, 4), "volume": 0.0})

    # 각 캔들의 거래량을 해당 가격 범위 버킷에 분배
    for h, l, c, vol in rows:
        candle_range = h - l
        if candle_range <= 0:
            # 동일 가격인 경우 close 위치 버킷에 할당
            idx = min(int((c - price_min) / bucket_size), n_buckets - 1)
            idx = max(0, idx)
            buckets[idx]["volume"] += vol
            continue

        # 캔들이 걸치는 버킷들에 비례 분배
        start_idx = max(0, int((l - price_min) / bucket_size))
        end_idx = min(n_buckets - 1, int((h - price_min) / bucket_size))

        for bi in range(start_idx, end_idx + 1):
            # 버킷과 캔들의 겹침 비율
            overlap_low = max(buckets[bi]["price_low"], l)
            overlap_high = min(buckets[bi]["price_high"], h)
            if overlap_high > overlap_low:
                overlap_ratio = (overlap_high - overlap_low) / candle_range
                buckets[bi]["volume"] += vol * overlap_ratio

    # POC: 최대 거래량 버킷의 중간값
    max_vol_idx = max(range(n_buckets), key=lambda i: buckets[i]["volume"])
    poc = (buckets[max_vol_idx]["price_low"] + buckets[max_vol_idx]["price_high"]) / 2

    # Value Area: 총 거래량의 70% 포함 범위
    total_vol = sum(b["volume"] for b in buckets)
    target_vol = total_vol * 0.70

    # POC에서 양방향으로 확장
    va_indices = {max_vol_idx}
    accumulated = buckets[max_vol_idx]["volume"]
    left = max_vol_idx - 1
    right = max_vol_idx + 1

    while accumulated < target_vol and (left >= 0 or right < n_buckets):
        left_vol = buckets[left]["volume"] if left >= 0 else 0
        right_vol = buckets[right]["volume"] if right < n_buckets else 0

        if left_vol >= right_vol and left >= 0:
            va_indices.add(left)
            accumulated += left_vol
            left -= 1
        elif right < n_buckets:
            va_indices.add(right)
            accumulated += right_vol
            right += 1
        else:
            break

    va_low = buckets[min(va_indices)]["price_low"]
    va_high = buckets[max(va_indices)]["price_high"]

    return {
        "buckets": buckets,
        "poc": round(poc, 2),
        "value_area_high": round(va_high, 2),
        "value_area_low": round(va_low, 2),
    }


def find_hvn_lvn(profile: dict) -> dict:
    """High/Low Volume Node 감지

    HVN: 상위 20% 거래량 버킷 → 실질 지지/저항
    LVN: 하위 20% 거래량 버킷 → 빠른 이동 구간
    """
    buckets = profile.get("buckets", [])
    if not buckets:
        return {"hvn": [], "lvn": []}

    volumes = sorted([b["volume"] for b in buckets])
    hvn_threshold = volumes[int(len(volumes) * 0.8)]
    lvn_threshold = volumes[int(len(volumes) * 0.2)]

    hvn = []
    lvn = []
    for b in buckets:
        mid = (b["price_low"] + b["price_high"]) / 2
        if b["volume"] >= hvn_threshold and hvn_threshold > 0:
            hvn.append(round(mid, 2))
        elif b["volume"] <= lvn_threshold:
            lvn.append(round(mid, 2))

    return {"hvn": hvn, "lvn": lvn}


def combine_with_orderbook(symbol: str, profile: dict) -> dict:
    """볼륨 프로파일 HVN + 오더북 벽 교차 → 강한 S/R 확인

    ±1% 이내 겹치면 "confirmed" 마킹
    """
    hvn_lvn = find_hvn_lvn(profile)
    hvn_prices = hvn_lvn["hvn"]

    if not hvn_prices:
        return {"confirmed_supports": [], "confirmed_resistances": []}

    # 현재가
    conn = get_connection()
    price_row = conn.execute(
        "SELECT close FROM klines WHERE symbol = ? AND interval = '5m' "
        "ORDER BY open_time DESC LIMIT 1", (symbol,),
    ).fetchone()

    # 최신 오더북 스캔
    scan_row = conn.execute(
        "SELECT scan_id FROM orderbook_walls WHERE symbol = ? ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()

    if not scan_row or not price_row:
        conn.close()
        return {"confirmed_supports": [], "confirmed_resistances": []}

    current_price = price_row[0]
    scan_id = scan_row[0]

    # 오더북 벽 가격
    bid_walls = conn.execute(
        "SELECT price FROM orderbook_walls WHERE symbol = ? AND scan_id = ? AND side = 'BID'",
        (symbol, scan_id),
    ).fetchall()
    ask_walls = conn.execute(
        "SELECT price FROM orderbook_walls WHERE symbol = ? AND scan_id = ? AND side = 'ASK'",
        (symbol, scan_id),
    ).fetchall()
    conn.close()

    bid_prices = [r[0] for r in bid_walls]
    ask_prices = [r[0] for r in ask_walls]

    confirmed_supports = []
    confirmed_resistances = []

    for hvn_p in hvn_prices:
        # HVN이 오더북 bid wall과 ±1% 이내에 겹침 → confirmed support
        for bp in bid_prices:
            if abs(hvn_p - bp) / bp < 0.01:
                confirmed_supports.append(round(hvn_p, 2))
                break

        # HVN이 오더북 ask wall과 ±1% 이내에 겹침 → confirmed resistance
        for ap in ask_prices:
            if abs(hvn_p - ap) / ap < 0.01:
                confirmed_resistances.append(round(hvn_p, 2))
                break

    # 현재가 기준 분류
    confirmed_supports = sorted([p for p in confirmed_supports if p < current_price], reverse=True)
    confirmed_resistances = sorted([p for p in confirmed_resistances if p > current_price])

    return {
        "confirmed_supports": confirmed_supports[:5],
        "confirmed_resistances": confirmed_resistances[:5],
    }


if __name__ == "__main__":
    from db import init_db
    init_db()

    for sym in SYMBOLS:
        print(f"\n{'='*50}")
        print(f"  {sym} Volume Profile")
        print(f"{'='*50}")

        profile = build_volume_profile(sym, "4h", 180)
        if not profile["buckets"]:
            print("  데이터 부족")
            continue

        print(f"  POC: ${profile['poc']:,.2f}")
        print(f"  Value Area: ${profile['value_area_low']:,.2f} ~ ${profile['value_area_high']:,.2f}")

        nodes = find_hvn_lvn(profile)
        print(f"\n  HVN (지지/저항): {', '.join(f'${p:,.2f}' for p in nodes['hvn'][:5])}")
        print(f"  LVN (빈 구간): {', '.join(f'${p:,.2f}' for p in nodes['lvn'][:5])}")

        confirmed = combine_with_orderbook(sym, profile)
        if confirmed["confirmed_supports"] or confirmed["confirmed_resistances"]:
            print(f"\n  [확인된 S/R]")
            if confirmed["confirmed_supports"]:
                print(f"  Confirmed Supports: {', '.join(f'${p:,.2f}' for p in confirmed['confirmed_supports'])}")
            if confirmed["confirmed_resistances"]:
                print(f"  Confirmed Resistances: {', '.join(f'${p:,.2f}' for p in confirmed['confirmed_resistances'])}")
        else:
            print(f"\n  [확인된 S/R] 없음 (HVN-오더북 겹침 없음)")
