"""Engine: 멀티 타임프레임 볼륨 프로파일 — POC, VA, HVN/LVN, 합성 VP, 돌파 강도"""
import json
from datetime import datetime, timezone
from db import get_connection
from config import (
    SYMBOLS, VP_DAILY_LOOKBACK, VP_4H_LOOKBACK, VP_1H_LOOKBACK,
    VP_5M_LOOKBACK, VP_BUCKETS, VP_VALUE_AREA_PCT, VP_COMPOSITE_WEIGHTS,
    VP_VOLUME_BREAK_RATIO,
)

_TF_MAP = {
    "daily": ("1d", VP_DAILY_LOOKBACK),
    "4h":    ("4h", VP_4H_LOOKBACK),
    "1h":    ("1h", VP_1H_LOOKBACK),
    "5m":    ("5m", VP_5M_LOOKBACK),
}
_EMPTY = {"buckets": [], "poc": 0, "value_area_high": 0, "value_area_low": 0}


def build_volume_profile(symbol: str, interval: str = "4h",
                         lookback: int = 180, n_buckets: int = 50) -> dict:
    """가격대별 거래량 분포 생성 → buckets, poc, value_area_high/low"""
    conn = get_connection()
    rows = conn.execute(
        "SELECT high, low, close, volume FROM klines "
        "WHERE symbol = ? AND interval = ? ORDER BY open_time DESC LIMIT ?",
        (symbol, interval, lookback),
    ).fetchall()
    conn.close()
    if len(rows) < 10:
        return dict(_EMPTY)

    price_min = min(r[1] for r in rows)
    price_max = max(r[0] for r in rows)
    if price_max <= price_min:
        return dict(_EMPTY)

    bs = (price_max - price_min) / n_buckets
    buckets = [{"price_low": round(price_min + i * bs, 4),
                "price_high": round(price_min + (i + 1) * bs, 4),
                "volume": 0.0} for i in range(n_buckets)]

    for h, l, c, vol in rows:
        cr = h - l
        if cr <= 0:
            idx = max(0, min(int((c - price_min) / bs), n_buckets - 1))
            buckets[idx]["volume"] += vol
            continue
        si = max(0, int((l - price_min) / bs))
        ei = min(n_buckets - 1, int((h - price_min) / bs))
        for bi in range(si, ei + 1):
            ol = max(buckets[bi]["price_low"], l)
            oh = min(buckets[bi]["price_high"], h)
            if oh > ol:
                buckets[bi]["volume"] += vol * (oh - ol) / cr

    mi = max(range(n_buckets), key=lambda i: buckets[i]["volume"])
    poc = (buckets[mi]["price_low"] + buckets[mi]["price_high"]) / 2

    total_vol = sum(b["volume"] for b in buckets)
    target_vol = total_vol * VP_VALUE_AREA_PCT
    va_idx = {mi}
    acc = buckets[mi]["volume"]
    left, right = mi - 1, mi + 1
    while acc < target_vol and (left >= 0 or right < n_buckets):
        lv = buckets[left]["volume"] if left >= 0 else 0
        rv = buckets[right]["volume"] if right < n_buckets else 0
        if lv >= rv and left >= 0:
            va_idx.add(left); acc += lv; left -= 1
        elif right < n_buckets:
            va_idx.add(right); acc += rv; right += 1
        else:
            break

    return {
        "buckets": buckets,
        "poc": round(poc, 2),
        "value_area_high": round(buckets[max(va_idx)]["price_high"], 2),
        "value_area_low": round(buckets[min(va_idx)]["price_low"], 2),
    }


def build_composite_vp(symbol: str) -> dict:
    """Multi-TF VP 합성 — daily/4h/1h/5m 가중 합산

    각 TF의 거래량 분포를 정규화(총합=1.0) 후 가중 합산.
    여러 TF에서 동시에 거래량이 높은 구간 = 진짜 강한 지지/저항.
    """
    profiles = {}
    for tf_key, (interval, lookback) in _TF_MAP.items():
        p = build_volume_profile(symbol, interval, lookback, VP_BUCKETS)
        if p.get("buckets"):
            profiles[tf_key] = p

    daily = profiles.get("daily")
    if not daily or not daily.get("buckets"):
        return dict(_EMPTY)

    # 공통 가격 범위 = 전체 TF의 min/max
    all_lows = [b["price_low"] for p in profiles.values() for b in p["buckets"]]
    all_highs = [b["price_high"] for p in profiles.values() for b in p["buckets"]]
    price_min = min(all_lows)
    price_max = max(all_highs)
    if price_max <= price_min:
        return dict(_EMPTY)

    bs = (price_max - price_min) / VP_BUCKETS
    composite = [{"price_low": round(price_min + i * bs, 4),
                  "price_high": round(price_min + (i + 1) * bs, 4),
                  "volume": 0.0} for i in range(VP_BUCKETS)]

    # 각 TF를 정규화(합=1) 후 가중치 적용하여 공통 버킷에 누적
    for tf_key, profile in profiles.items():
        weight = VP_COMPOSITE_WEIGHTS.get(tf_key, 0)
        if weight <= 0:
            continue
        tf_total = sum(b["volume"] for b in profile["buckets"])
        if tf_total <= 0:
            continue
        for b in profile["buckets"]:
            b_mid = (b["price_low"] + b["price_high"]) / 2
            idx = int((b_mid - price_min) / bs)
            idx = max(0, min(VP_BUCKETS - 1, idx))
            composite[idx]["volume"] += (b["volume"] / tf_total) * weight

    # POC
    mi = max(range(VP_BUCKETS), key=lambda i: composite[i]["volume"])
    poc = (composite[mi]["price_low"] + composite[mi]["price_high"]) / 2

    # Value Area
    total_vol = sum(b["volume"] for b in composite)
    if total_vol <= 0:
        return dict(_EMPTY)
    target_vol = total_vol * VP_VALUE_AREA_PCT
    va_idx = {mi}
    acc = composite[mi]["volume"]
    left, right = mi - 1, mi + 1
    while acc < target_vol and (left >= 0 or right < VP_BUCKETS):
        lv = composite[left]["volume"] if left >= 0 else 0
        rv = composite[right]["volume"] if right < VP_BUCKETS else 0
        if lv >= rv and left >= 0:
            va_idx.add(left); acc += lv; left -= 1
        elif right < VP_BUCKETS:
            va_idx.add(right); acc += rv; right += 1
        else:
            break

    return {
        "buckets": composite,
        "poc": round(poc, 2),
        "value_area_high": round(composite[max(va_idx)]["price_high"], 2),
        "value_area_low": round(composite[min(va_idx)]["price_low"], 2),
    }


def check_level_holdable(symbol: str, price: float) -> dict:
    """특정 가격대의 지지/저항 강도 판단

    원리: VP 거래량 > 현재 거래량 → 돌파 어려움 (그리드 유지)
          VP 거래량 < 현재 거래량 → 돌파 가능 (주문 회피)

    Returns: {holdable: bool, ratio: float, vp_strength: str}
      - holdable: True면 해당 레벨 주문 유지, False면 돌파 위험
      - ratio: 현재거래량 / VP거래량 (1 미만이면 VP가 더 강함)
      - vp_strength: "strong" / "moderate" / "weak"
    """
    conn = get_connection()

    # 1) 합성 VP에서 해당 가격 버킷의 거래량 비중 조회
    cache_row = conn.execute(
        "SELECT data_json FROM vp_cache WHERE symbol=? AND timeframe='composite'",
        (symbol,)).fetchone()

    vp_fraction = 0.0
    if cache_row:
        try:
            data = json.loads(cache_row[0])
            buckets = data.get("buckets", [])
            for b in buckets:
                if b["price_low"] <= price <= b["price_high"]:
                    vp_fraction = b["volume"]
                    break
        except (json.JSONDecodeError, TypeError):
            pass

    # 2) 최근 5m 캔들 6개(30분)에서 해당 가격대를 통과한 거래량
    recent = conn.execute(
        "SELECT volume, high, low FROM klines WHERE symbol=? AND interval='5m' "
        "ORDER BY open_time DESC LIMIT 6", (symbol,)).fetchall()

    # 3) 과거 5m 캔들 36개(3시간)의 해당 가격대 평균 거래량
    history = conn.execute(
        "SELECT volume, high, low FROM klines WHERE symbol=? AND interval='5m' "
        "ORDER BY open_time DESC LIMIT ? OFFSET 6",
        (symbol, VP_5M_LOOKBACK)).fetchall()
    conn.close()

    # 해당 가격대를 지나간 캔들의 거래량만 합산
    recent_vol = sum(vol for vol, h, l in recent if l <= price <= h) if recent else 0
    hist_vols = [vol for vol, h, l in history if l <= price <= h]
    avg_hist_vol = sum(hist_vols) / max(len(hist_vols), 1) if hist_vols else 0

    # VP 강도 판정
    avg_fraction = 1.0 / VP_BUCKETS  # 균등 분포시 기대 비중
    if vp_fraction >= avg_fraction * 2:
        vp_strength = "strong"
    elif vp_fraction >= avg_fraction:
        vp_strength = "moderate"
    else:
        vp_strength = "weak"

    # 돌파 비율: 최근 거래량 / 과거 평균
    if avg_hist_vol > 0:
        ratio = recent_vol / (avg_hist_vol * len(recent)) if recent else 0
    else:
        ratio = 0

    # holdable 판단: VP 강한데 거래량 낮으면 유지, VP 약한데 거래량 높으면 위험
    if vp_strength == "strong":
        holdable = ratio < VP_VOLUME_BREAK_RATIO * 1.5  # 강한 VP는 더 높은 거래량 필요
    elif vp_strength == "moderate":
        holdable = ratio < VP_VOLUME_BREAK_RATIO
    else:  # weak
        holdable = ratio < VP_VOLUME_BREAK_RATIO * 0.7  # 약한 VP는 적은 거래량으로도 돌파

    return {
        "holdable": holdable,
        "ratio": round(ratio, 3),
        "vp_strength": vp_strength,
        "vp_fraction": round(vp_fraction, 4),
        "recent_vol": round(recent_vol, 1),
        "avg_hist_vol": round(avg_hist_vol, 1),
    }


def find_hvn_lvn(profile: dict) -> dict:
    """HVN(상위 20%) / LVN(하위 20%) 감지"""
    buckets = profile.get("buckets", [])
    if not buckets:
        return {"hvn": [], "lvn": []}
    volumes = sorted(b["volume"] for b in buckets)
    hvn_th = volumes[int(len(volumes) * 0.8)]
    lvn_th = volumes[int(len(volumes) * 0.2)]
    hvn, lvn = [], []
    for b in buckets:
        mid = round((b["price_low"] + b["price_high"]) / 2, 2)
        if b["volume"] >= hvn_th and hvn_th > 0:
            hvn.append(mid)
        elif b["volume"] <= lvn_th:
            lvn.append(mid)
    return {"hvn": hvn, "lvn": lvn}


def _cache_vp(conn, symbol: str, tf: str, poc: float,
              va_h: float, va_l: float, data: dict):
    """VP 결과 → vp_cache INSERT OR REPLACE"""
    conn.execute(
        "INSERT OR REPLACE INTO vp_cache "
        "(symbol, timeframe, poc, va_high, va_low, data_json, calculated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (symbol, tf, poc, va_h, va_l,
         json.dumps(data), datetime.now(timezone.utc).isoformat()),
    )


def get_multi_tf_vp(symbol: str) -> dict:
    """멀티 타임프레임 VP — daily/4h/1h/5m + 합성 VP 빌드 + 캐시

    Returns: daily/4h/1h/5m 각 {poc, va_high, va_low, hvn, lvn}
             composite {poc, va_high, va_low, hvn, lvn}
             grid_range {upper, lower}, edge_warning bool
    """
    result = {}
    conn = get_connection()
    for tf_key, (interval, lookback) in _TF_MAP.items():
        p = build_volume_profile(symbol, interval, lookback, VP_BUCKETS)
        n = find_hvn_lvn(p)
        e = {"poc": p["poc"], "va_high": p["value_area_high"],
             "va_low": p["value_area_low"], "hvn": n["hvn"], "lvn": n["lvn"]}
        result[tf_key] = e
        _cache_vp(conn, symbol, tf_key, e["poc"], e["va_high"], e["va_low"], e)

    # 합성 VP
    comp = build_composite_vp(symbol)
    comp_hvn = find_hvn_lvn(comp)
    comp_e = {"poc": comp["poc"], "va_high": comp["value_area_high"],
              "va_low": comp["value_area_low"],
              "hvn": comp_hvn["hvn"], "lvn": comp_hvn["lvn"]}
    result["composite"] = comp_e
    _cache_vp(conn, symbol, "composite", comp_e["poc"],
              comp_e["va_high"], comp_e["va_low"],
              {"buckets": comp["buckets"], **comp_e})

    conn.commit()
    conn.close()

    # Grid 범위 = composite VA (multi-TF 합의)
    c = result["composite"]
    result["grid_range"] = {"upper": c["va_high"], "lower": c["va_low"]}

    # Edge warning: 1h VA가 composite VA 경계 10% 이내 접근
    h1 = result["1h"]
    dr = c["va_high"] - c["va_low"] if c["va_high"] > c["va_low"] else 1
    result["edge_warning"] = (
        (c["va_high"] - h1["va_high"]) / dr < 0.10 or
        (h1["va_low"] - c["va_low"]) / dr < 0.10
    )
    return result


def check_vp_breakout(symbol: str) -> dict:
    """VP 브레이크아웃 감지 — 현재가 vs composite VA + 거래량 강도

    Returns: breaking bool, direction "LONG"|"SHORT"|None, strength float
    """
    empty = {"breaking": False, "direction": None, "strength": 0.0}
    conn = get_connection()

    price_row = conn.execute(
        "SELECT close FROM klines WHERE symbol = ? AND interval = '5m' "
        "ORDER BY open_time DESC LIMIT 1", (symbol,),
    ).fetchone()
    if not price_row:
        conn.close()
        return empty
    price = price_row[0]

    # composite VA 사용 (multi-TF 합의)
    cache_row = conn.execute(
        "SELECT va_high, va_low FROM vp_cache "
        "WHERE symbol = ? AND timeframe = 'composite'", (symbol,),
    ).fetchone()
    if not cache_row:
        # fallback: daily
        cache_row = conn.execute(
            "SELECT va_high, va_low FROM vp_cache "
            "WHERE symbol = ? AND timeframe = 'daily'", (symbol,),
        ).fetchone()
    if not cache_row:
        conn.close()
        return empty
    va_h, va_l = cache_row
    va_range = va_h - va_l
    if va_range <= 0:
        conn.close()
        return empty

    # 거래량 확인: 최근 1h vs 평균
    vol_rows = conn.execute(
        "SELECT volume FROM klines WHERE symbol = ? AND interval = '1h' "
        "ORDER BY open_time DESC LIMIT ?", (symbol, VP_1H_LOOKBACK),
    ).fetchall()
    conn.close()

    vol_ok = False
    if len(vol_rows) >= 2:
        avg_vol = sum(r[0] for r in vol_rows[1:]) / len(vol_rows[1:])
        vol_ok = vol_rows[0][0] > avg_vol * 1.5

    if price > va_h:
        # VA 상단 돌파 강도 체크
        level_check = check_level_holdable(symbol, va_h)
        vol_confirmed = vol_ok and not level_check["holdable"]
        return {"breaking": vol_confirmed, "direction": "LONG",
                "strength": round((price - va_h) / va_range, 4),
                "vol_ratio": level_check["ratio"]}
    elif price < va_l:
        level_check = check_level_holdable(symbol, va_l)
        vol_confirmed = vol_ok and not level_check["holdable"]
        return {"breaking": vol_confirmed, "direction": "SHORT",
                "strength": round((va_l - price) / va_range, 4),
                "vol_ratio": level_check["ratio"]}
    return empty


def get_cached_vp(symbol: str) -> dict | None:
    """vp_cache 테이블에서 빠른 조회 (재계산 없음)"""
    conn = get_connection()
    rows = conn.execute(
        "SELECT timeframe, data_json, calculated_at FROM vp_cache "
        "WHERE symbol = ? ORDER BY timeframe", (symbol,),
    ).fetchall()
    conn.close()
    if not rows:
        return None
    result = {}
    for tf, dj, ca in rows:
        try:
            result[tf] = json.loads(dj)
            result[tf]["calculated_at"] = ca
        except (json.JSONDecodeError, TypeError):
            continue
    return result if result else None


if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if sys.platform == "win32": sys.stdout.reconfigure(encoding="utf-8")
    from db import init_db
    init_db()
    for sym in SYMBOLS:
        print(f"\n{'='*60}\n  {sym} Multi-TF Volume Profile\n{'='*60}")
        mtf = get_multi_tf_vp(sym)
        for tk in ("daily", "4h", "1h", "5m", "composite"):
            v = mtf.get(tk, {})
            if not v.get("poc"): continue
            print(f"\n  [{tk.upper():9s}] POC: ${v['poc']:,.2f}  "
                  f"VA: ${v['va_low']:,.2f} ~ ${v['va_high']:,.2f}")
            if v.get("hvn"):
                print(f"    HVN: {', '.join(f'${p:,.2f}' for p in v['hvn'][:5])}")
            if v.get("lvn"):
                print(f"    LVN: {', '.join(f'${p:,.2f}' for p in v['lvn'][:5])}")
        gr = mtf["grid_range"]
        print(f"\n  [Grid Range] ${gr['lower']:,.2f} ~ ${gr['upper']:,.2f}")
        print(f"  [Edge Warning] {'YES' if mtf['edge_warning'] else 'No'}")
        bo = check_vp_breakout(sym)
        if bo["direction"]:
            st = "CONFIRMED" if bo["breaking"] else "NO VOL CONFIRM"
            print(f"  [Breakout] {bo['direction']} | {bo['strength']:.2%} | {st}")
        else:
            print(f"  [Breakout] 없음 — VA 내부")
        # 레벨 강도 테스트
        mark_row = None
        conn = get_connection()
        mark_row = conn.execute(
            "SELECT close FROM klines WHERE symbol=? AND interval='5m' "
            "ORDER BY open_time DESC LIMIT 1", (sym,)).fetchone()
        conn.close()
        if mark_row:
            ls = check_level_holdable(sym, mark_row[0])
            print(f"\n  [현재가 ${mark_row[0]:,.2f} 강도]")
            print(f"    VP강도: {ls['vp_strength']} | 비율: {ls['ratio']:.2f} | "
                  f"유지: {'O' if ls['holdable'] else 'X'}")
