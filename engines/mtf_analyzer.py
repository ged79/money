"""Engine: 멀티 타임프레임 분석 — 스윙 감지, 패턴 인식, MTF 정렬, 핵심 레벨"""
import json
import math
import time
from db import get_connection
from config import SYMBOLS

# 적응형 스윙 감지 파라미터 (5분봉 전용)
ADAPTIVE_WINDOW = 20    # rolling 평균/σ 계산 윈도우 (20캔들 = 100분)
ADAPTIVE_K = 1.5        # σ 배수 (1.5σ 이상 이탈 시 스윙)
ADAPTIVE_CLUSTER_GAP = 5  # 연속 신호 클러스터링 간격 (5캔들 = 25분)


# === 스윙 포인트 감지 ===
def detect_swing_points(symbol: str, interval: str, lookback: int = None) -> list[dict]:
    """스윙 High/Low 감지

    5분봉: 적응형 (rolling 평균 대비 σ 이탈 + 클러스터링)
    기타 TF: Fractal 기반 (좌우 N캔들 비교)
    """
    if interval == "5m":
        return _detect_swing_adaptive(symbol, lookback or 228)
    return _detect_swing_fractal(symbol, interval, lookback)


def _detect_swing_fractal(symbol: str, interval: str, lookback: int = None) -> list[dict]:
    """Fractal 기반 스윙 감지 (1W/1D/4H/1H용)"""
    n_map = {"1w": 2, "1d": 3, "4h": 5, "1h": 5}
    n = n_map.get(interval, 3)

    limit = lookback or {"1w": 52, "1d": 90, "4h": 180, "1h": 168}.get(interval, 90)

    conn = get_connection()
    rows = conn.execute(
        "SELECT open_time, high, low, close FROM klines "
        "WHERE symbol = ? AND interval = ? ORDER BY open_time DESC LIMIT ?",
        (symbol, interval, limit),
    ).fetchall()
    conn.close()

    if len(rows) < (2 * n + 1):
        return []

    rows = list(reversed(rows))

    swings = []
    for i in range(n, len(rows) - n):
        high_i = rows[i][1]
        low_i = rows[i][2]
        open_time = rows[i][0]

        is_swing_high = all(high_i > rows[i - j][1] for j in range(1, n + 1)) and \
                        all(high_i > rows[i + j][1] for j in range(1, n + 1))

        is_swing_low = all(low_i < rows[i - j][2] for j in range(1, n + 1)) and \
                       all(low_i < rows[i + j][2] for j in range(1, n + 1))

        if is_swing_high:
            swings.append({"type": "high", "price": high_i, "time": open_time})
        if is_swing_low:
            swings.append({"type": "low", "price": low_i, "time": open_time})

    return swings


def _detect_swing_adaptive(symbol: str, lookback: int = 228) -> list[dict]:
    """적응형 스윙 감지 (5분봉 전용)

    rolling 평균 대비 K*σ 이상 이탈하면 스윙 포인트.
    횡보 시 σ 작아져 작은 움직임 감지, 급변 시 σ 커져 노이즈 필터링.
    연속 이탈은 클러스터링하여 극단값만 남김.
    """
    conn = get_connection()
    rows = conn.execute(
        "SELECT open_time, high, low, close FROM klines "
        "WHERE symbol = ? AND interval = '5m' ORDER BY open_time DESC LIMIT ?",
        (symbol, lookback),
    ).fetchall()
    conn.close()

    if len(rows) < ADAPTIVE_WINDOW + 1:
        return []

    rows = list(reversed(rows))

    # 1단계: σ 이탈 감지
    raw_signals = []
    for i in range(ADAPTIVE_WINDOW, len(rows)):
        window = rows[i - ADAPTIVE_WINDOW:i]

        avg_high = sum(r[1] for r in window) / ADAPTIVE_WINDOW
        avg_low = sum(r[2] for r in window) / ADAPTIVE_WINDOW
        std_high = math.sqrt(sum((r[1] - avg_high) ** 2 for r in window) / ADAPTIVE_WINDOW)
        std_low = math.sqrt(sum((r[2] - avg_low) ** 2 for r in window) / ADAPTIVE_WINDOW)

        curr_high = rows[i][1]
        curr_low = rows[i][2]
        ot = rows[i][0]

        if std_high > 0 and curr_high > avg_high + ADAPTIVE_K * std_high:
            dev = (curr_high - avg_high) / std_high
            raw_signals.append({"type": "high", "price": curr_high, "time": ot, "dev": dev, "_idx": i})

        if std_low > 0 and curr_low < avg_low - ADAPTIVE_K * std_low:
            dev = (avg_low - curr_low) / std_low
            raw_signals.append({"type": "low", "price": curr_low, "time": ot, "dev": dev, "_idx": i})

    # 2단계: 클러스터링 — 연속 신호에서 극단값만 남기기
    if not raw_signals:
        return []

    clustered = []
    cluster = [raw_signals[0]]

    for s in raw_signals[1:]:
        if s["type"] == cluster[-1]["type"] and s["_idx"] - cluster[-1]["_idx"] <= ADAPTIVE_CLUSTER_GAP:
            cluster.append(s)
        else:
            if cluster[0]["type"] == "high":
                best = max(cluster, key=lambda x: x["price"])
            else:
                best = min(cluster, key=lambda x: x["price"])
            clustered.append(best)
            cluster = [s]

    # 마지막 클러스터
    if cluster[0]["type"] == "high":
        best = max(cluster, key=lambda x: x["price"])
    else:
        best = min(cluster, key=lambda x: x["price"])
    clustered.append(best)

    # _idx 제거 후 반환
    return [{"type": s["type"], "price": s["price"], "time": s["time"]} for s in clustered]


# === 추세 패턴 감지 ===
def detect_trend_pattern(symbol: str, interval: str) -> dict:
    """스윙 포인트 기반 추세 패턴 감지

    Returns:
        pattern: "ascending" | "descending" | "uptrend" | "downtrend" | "sideways"
        swing_lows: 최근 스윙 로우들
        swing_highs: 최근 스윙 하이들
        confidence: 패턴 확실도 (0~1)
    """
    swings = detect_swing_points(symbol, interval)

    if len(swings) < 3:
        return {"pattern": "insufficient_data", "swing_lows": [], "swing_highs": [], "confidence": 0}

    # 최근 스윙 분리
    swing_highs = [s for s in swings if s["type"] == "high"]
    swing_lows = [s for s in swings if s["type"] == "low"]

    # 최근 5개만 사용
    recent_highs = swing_highs[-5:] if len(swing_highs) >= 2 else swing_highs
    recent_lows = swing_lows[-5:] if len(swing_lows) >= 2 else swing_lows

    # Higher Low / Lower Low 판정
    higher_lows = _check_sequence(recent_lows, ascending=True) if len(recent_lows) >= 2 else False
    lower_lows = _check_sequence(recent_lows, ascending=False) if len(recent_lows) >= 2 else False

    # Higher High / Lower High 판정
    higher_highs = _check_sequence(recent_highs, ascending=True) if len(recent_highs) >= 2 else False
    lower_highs = _check_sequence(recent_highs, ascending=False) if len(recent_highs) >= 2 else False

    # 패턴 결정
    if higher_lows and not higher_highs and len(recent_lows) >= 3:
        # Higher Lows + 전고점 미돌파 → ascending triangle
        pattern = "ascending"
        confidence = min(1.0, len(recent_lows) / 5)
    elif lower_highs and not lower_lows and len(recent_highs) >= 3:
        # Lower Highs + 저점 유지 → descending triangle
        pattern = "descending"
        confidence = min(1.0, len(recent_highs) / 5)
    elif higher_lows and higher_highs:
        pattern = "uptrend"
        confidence = min(1.0, (len(recent_lows) + len(recent_highs)) / 8)
    elif lower_lows and lower_highs:
        pattern = "downtrend"
        confidence = min(1.0, (len(recent_lows) + len(recent_highs)) / 8)
    else:
        pattern = "sideways"
        confidence = 0.3

    return {
        "pattern": pattern,
        "swing_lows": [{"price": s["price"], "time": s["time"]} for s in recent_lows],
        "swing_highs": [{"price": s["price"], "time": s["time"]} for s in recent_highs],
        "confidence": round(confidence, 2),
    }


def _check_sequence(swings: list[dict], ascending: bool) -> bool:
    """스윙 포인트가 연속으로 상승/하락하는지 확인

    조건:
    1) 전체 쌍 중 ceil(2/3) 이상 방향 일치 (2쌍이면 2/2 필요)
    2) 가장 최근 쌍이 반드시 방향 일치 (현재 추세 반영)
    """
    if len(swings) < 2:
        return False

    pairs_ok = 0
    pairs_total = 0

    for i in range(1, len(swings)):
        pairs_total += 1
        if ascending and swings[i]["price"] > swings[i - 1]["price"]:
            pairs_ok += 1
        elif not ascending and swings[i]["price"] < swings[i - 1]["price"]:
            pairs_ok += 1

    # 과반수 이상 일치 (2쌍→2/2, 3쌍→2/3, 4쌍→3/4)
    threshold = math.ceil(pairs_total * 2 / 3)
    if pairs_ok < threshold:
        return False

    # 압도적 다수(75%+)면 일시적 반전 허용 (예: 4쌍 중 3쌍 하락인데 마지막만 반등)
    if pairs_total >= 3 and pairs_ok / pairs_total >= 0.75:
        return True

    # 그 외: 최근 쌍 방향 일치 필수 — 현재 추세가 맞아야 함
    if ascending:
        return swings[-1]["price"] > swings[-2]["price"]
    return swings[-1]["price"] < swings[-2]["price"]


# === MTF 정렬 점수 ===
def calculate_mtf_alignment(symbol: str) -> dict:
    """4개 타임프레임에서 MA(7) vs MA(25) 비교하여 정렬 점수 산출

    Returns:
        alignment: -1.0 ~ +1.0 (4개 TF 합산 / 4)
        per_tf: {"1d": 1, "4h": 1, "1h": -1, "5m": 1}
        bias: "bullish" | "bearish" | "mixed"
    """
    conn = get_connection()
    per_tf = {}

    for tf in ["1d", "4h", "1h", "5m"]:
        ma_count = 25
        rows = conn.execute(
            "SELECT close FROM klines WHERE symbol = ? AND interval = ? "
            "ORDER BY open_time DESC LIMIT ?",
            (symbol, tf, ma_count),
        ).fetchall()

        if len(rows) < ma_count:
            per_tf[tf] = 0
            continue

        closes = [r[0] for r in reversed(rows)]
        ma7 = sum(closes[-7:]) / 7
        ma25 = sum(closes) / ma_count

        per_tf[tf] = 1 if ma7 > ma25 else -1

    conn.close()

    alignment = sum(per_tf.values()) / max(len(per_tf), 1)

    if alignment >= 0.5:
        bias = "bullish"
    elif alignment <= -0.5:
        bias = "bearish"
    else:
        bias = "mixed"

    return {
        "alignment": round(alignment, 2),
        "per_tf": per_tf,
        "bias": bias,
    }


# === 핵심 지지/저항 레벨 ===
def get_key_levels(symbol: str) -> dict:
    """1d + 4h 스윙 포인트에서 주요 지지/저항 추출 (클러스터링)

    Returns:
        supports: [price, ...]
        resistances: [price, ...]
        nearest_support: float
        nearest_resistance: float
    """
    # 현재가 조회
    conn = get_connection()
    price_row = conn.execute(
        "SELECT close FROM klines WHERE symbol = ? AND interval = '5m' "
        "ORDER BY open_time DESC LIMIT 1", (symbol,),
    ).fetchone()
    conn.close()

    current_price = price_row[0] if price_row else 0

    # 1d + 4h 스윙 수집
    swings_1d = detect_swing_points(symbol, "1d")
    swings_4h = detect_swing_points(symbol, "4h")
    all_swings = swings_1d + swings_4h

    if not all_swings or current_price == 0:
        return {"supports": [], "resistances": [], "nearest_support": 0, "nearest_resistance": 0}

    # 클러스터링: ±1% 이내 가격대 합치기
    prices = sorted(set(s["price"] for s in all_swings))
    clusters = []
    used = set()

    for i, p in enumerate(prices):
        if i in used:
            continue
        cluster = [p]
        used.add(i)
        for j in range(i + 1, len(prices)):
            if j in used:
                continue
            if abs(prices[j] - p) / p < 0.01:  # ±1%
                cluster.append(prices[j])
                used.add(j)
        # 클러스터의 평균 가격
        avg_price = sum(cluster) / len(cluster)
        clusters.append({"price": round(avg_price, 2), "count": len(cluster)})

    # 현재가 기준 지지/저항 분리
    supports = sorted(
        [c["price"] for c in clusters if c["price"] < current_price],
        reverse=True,  # 현재가에 가까운 순
    )
    resistances = sorted(
        [c["price"] for c in clusters if c["price"] > current_price],
    )

    return {
        "supports": supports[:5],
        "resistances": resistances[:5],
        "nearest_support": supports[0] if supports else 0,
        "nearest_resistance": resistances[0] if resistances else 0,
    }


# === MTF 종합 분석 + DB 저장 ===
def calculate_mtf(symbol: str = None) -> dict | None:
    """MTF 종합 분석 후 mtf_analysis 테이블 저장"""
    symbols = [symbol] if symbol else SYMBOLS
    result = None

    for sym in symbols:
        r = _calc_mtf_single(sym)
        if r:
            result = r

    return result


def _calc_mtf_single(symbol: str) -> dict:
    # 1. MTF 정렬
    alignment = calculate_mtf_alignment(symbol)

    # 2. 패턴 감지 (1d, 4h)
    pattern_1d = detect_trend_pattern(symbol, "1d")
    pattern_4h = detect_trend_pattern(symbol, "4h")

    # 3. 핵심 레벨
    levels = get_key_levels(symbol)

    # 4. DB 저장
    detail = {
        "alignment": alignment,
        "pattern_1d": pattern_1d,
        "pattern_4h": pattern_4h,
        "levels": levels,
    }

    conn = get_connection()
    conn.execute(
        """INSERT INTO mtf_analysis
        (symbol, alignment_score, bias, pattern_1d, pattern_4h,
         nearest_support, nearest_resistance, detail_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (symbol, alignment["alignment"], alignment["bias"],
         pattern_1d["pattern"], pattern_4h["pattern"],
         levels["nearest_support"], levels["nearest_resistance"],
         json.dumps(detail, default=str)),
    )
    conn.commit()
    conn.close()

    # 로그
    print(f"[MTF] {symbol}: alignment={alignment['alignment']:+.2f} ({alignment['bias']}) | "
          f"1D={pattern_1d['pattern']} | 4H={pattern_4h['pattern']} | "
          f"S=${levels['nearest_support']:,.2f} R=${levels['nearest_resistance']:,.2f}")

    # 패턴 변화 감지 시 텔레그램 알림
    _check_pattern_alert(symbol, pattern_1d, pattern_4h, levels)

    return detail


# === 패턴 변화 텔레그램 알림 ===
_last_patterns = {}  # {symbol: {"1d": str, "4h": str}}


def _check_pattern_alert(symbol: str, pattern_1d: dict, pattern_4h: dict, levels: dict):
    """새 패턴 전환 감지 시 텔레그램 알림"""
    global _last_patterns

    prev = _last_patterns.get(symbol, {})
    alerts = []

    if prev.get("1d") and prev["1d"] != pattern_1d["pattern"] and pattern_1d["pattern"] != "sideways":
        lows_str = " → ".join(f"${s['price']:,.2f}" for s in pattern_1d["swing_lows"][-3:])
        highs_str = " → ".join(f"${s['price']:,.2f}" for s in pattern_1d["swing_highs"][-3:])
        alerts.append(
            f"[MTF] {symbol} 1D: {pattern_1d['pattern']} 감지\n"
            f"  HL: {lows_str}\n  HH: {highs_str}\n"
            f"  S=${levels['nearest_support']:,.2f} R=${levels['nearest_resistance']:,.2f}"
        )

    if prev.get("4h") and prev["4h"] != pattern_4h["pattern"] and pattern_4h["pattern"] != "sideways":
        alerts.append(
            f"[MTF] {symbol} 4H: {pattern_4h['pattern']} 감지\n"
            f"  S=${levels['nearest_support']:,.2f} R=${levels['nearest_resistance']:,.2f}"
        )

    _last_patterns[symbol] = {"1d": pattern_1d["pattern"], "4h": pattern_4h["pattern"]}

    if alerts:
        try:
            from engines.live_trader import _send_telegram
            for alert in alerts:
                _send_telegram(alert)
        except Exception:
            for alert in alerts:
                print(alert)


# === 최신 MTF 결과 조회 (다른 엔진 참조용) ===
def get_latest_mtf(symbol: str) -> dict | None:
    """최신 MTF 분석 결과 반환"""
    conn = get_connection()
    row = conn.execute(
        "SELECT alignment_score, bias, pattern_1d, pattern_4h, "
        "nearest_support, nearest_resistance, detail_json "
        "FROM mtf_analysis WHERE symbol = ? ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    conn.close()

    if not row:
        return None

    return {
        "alignment_score": row[0],
        "bias": row[1],
        "pattern_1d": row[2],
        "pattern_4h": row[3],
        "nearest_support": row[4],
        "nearest_resistance": row[5],
        "detail": json.loads(row[6]) if row[6] else {},
    }


if __name__ == "__main__":
    from db import init_db
    init_db()

    for sym in SYMBOLS:
        print(f"\n{'='*50}")
        print(f"  {sym} MTF Analysis")
        print(f"{'='*50}")

        # 스윙 감지
        for tf in ["1d", "4h"]:
            swings = detect_swing_points(sym, tf)
            highs = [s for s in swings if s["type"] == "high"]
            lows = [s for s in swings if s["type"] == "low"]
            print(f"\n[{tf}] Swing High {len(highs)}개, Swing Low {len(lows)}개")
            for s in lows[-3:]:
                print(f"  Low: ${s['price']:,.2f}")
            for s in highs[-3:]:
                print(f"  High: ${s['price']:,.2f}")

        # 패턴
        for tf in ["1d", "4h"]:
            p = detect_trend_pattern(sym, tf)
            print(f"\n[{tf}] Pattern: {p['pattern']} (confidence={p['confidence']})")

        # MTF 정렬
        a = calculate_mtf_alignment(sym)
        print(f"\n[MTF Alignment] {a['alignment']:+.2f} ({a['bias']})")
        for tf, v in a["per_tf"].items():
            print(f"  {tf}: {'bullish' if v > 0 else 'bearish' if v < 0 else 'neutral'}")

        # 핵심 레벨
        levels = get_key_levels(sym)
        print(f"\n[Key Levels]")
        print(f"  Supports: {', '.join(f'${p:,.2f}' for p in levels['supports'][:3])}")
        print(f"  Resistances: {', '.join(f'${p:,.2f}' for p in levels['resistances'][:3])}")

        # DB 저장
        calculate_mtf(sym)
