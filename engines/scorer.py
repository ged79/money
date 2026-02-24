"""Engine 4: SSM+V+T 스코어링 - 5요소 복합 점수 산출"""
import json
from db import get_connection
from config import SYMBOLS
from engines.dynamic_threshold import get_latest_threshold
from engines.gemini_client import analyze_sentiment_majority
from collectors.arkham import get_whale_direction, SYMBOL_TO_TOKEN
from collectors.cryptoquant import get_netflow_signal, get_mvrv_signal, SYMBOL_TO_ASSET


def calculate_score(symbol: str = None) -> dict | None:
    """SSM+V+T 전체 점수 계산 후 ssm_scores 테이블 저장"""
    symbols = [symbol] if symbol else SYMBOLS
    result = None
    for sym in symbols:
        r = _calc_single(sym)
        if r:
            result = r
    return result


def _calc_single(symbol: str) -> dict:
    detail = {}

    # === T (Trigger) - 게이트 ===
    threshold = get_latest_threshold(symbol)
    trigger_active = threshold["trigger_active"] if threshold else False
    detail["trigger"] = {
        "active": trigger_active,
        "threshold": threshold["threshold_value"] if threshold else 0,
        "liq_1h": threshold["liq_amount_1h"] if threshold else 0,
    }

    # === 방향 추적 ===
    bullish_signals = 0
    bearish_signals = 0

    # === M (Momentum) - max 2.0pt ===
    m_score, m_detail = _score_momentum(symbol)
    detail["momentum"] = m_detail
    if m_detail.get("direction") == "bullish":
        bullish_signals += 1
    elif m_detail.get("direction") == "bearish":
        bearish_signals += 1

    # === S_sentiment - max 1.5pt ===
    s_sent_score, s_sent_detail = _score_sentiment(symbol)
    detail["sentiment"] = s_sent_detail
    if s_sent_detail.get("direction") == "bullish":
        bullish_signals += 1
    elif s_sent_detail.get("direction") == "bearish":
        bearish_signals += 1

    # === S_story (Gemini) - max 1.0pt ===
    # 트리거 활성 시에만 Gemini 호출 (예산 절약)
    s_story_score = 0.0
    gemini_calls = 0
    if trigger_active:
        story_result = analyze_sentiment_majority(symbol, calls=3)
        gemini_calls = story_result.get("calls_used", 0)
        agreement = story_result.get("agreement", 0)
        s_story_score = round(agreement * 1.0, 2)  # 일치도 × 1.0pt
        detail["story"] = {
            "score": s_story_score,
            "sentiment": story_result.get("sentiment"),
            "agreement": agreement,
            "votes": story_result.get("votes"),
        }
        if story_result.get("sentiment") == "bullish":
            bullish_signals += 1
        elif story_result.get("sentiment") == "bearish":
            bearish_signals += 1
    else:
        detail["story"] = {"score": 0, "reason": "trigger_inactive", "gemini_skipped": True}

    # === V (Value) - max 0.5pt ===
    v_score, v_detail = _score_value(symbol)
    detail["value"] = v_detail

    # === 합계 ===
    total_score = round(m_score + s_sent_score + s_story_score + v_score, 2)
    total_score = min(5.0, total_score)  # 상한 캡

    # === 방향 결정 ===
    if bullish_signals > bearish_signals:
        direction = "BULLISH"
    elif bearish_signals > bullish_signals:
        direction = "BEARISH"
    else:
        direction = "NEUTRAL"

    # 트리거의 방향도 반영
    if threshold and threshold.get("direction"):
        if threshold["direction"] == "SHORT_CASCADE":
            # 숏 청산 = 가격 상승 방향
            if direction == "NEUTRAL":
                direction = "BULLISH"
        elif threshold["direction"] == "LONG_CASCADE":
            if direction == "NEUTRAL":
                direction = "BEARISH"

    # DB 저장
    conn = get_connection()
    conn.execute(
        "INSERT INTO ssm_scores "
        "(symbol, trigger_active, momentum_score, sentiment_score, story_score, "
        "value_score, total_score, direction, score_detail, gemini_calls_used) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (symbol, 1 if trigger_active else 0,
         m_score, s_sent_score, s_story_score, v_score,
         total_score, direction, json.dumps(detail, ensure_ascii=False),
         gemini_calls),
    )
    conn.commit()
    conn.close()

    result = {
        "symbol": symbol,
        "trigger_active": trigger_active,
        "momentum_score": m_score,
        "sentiment_score": s_sent_score,
        "story_score": s_story_score,
        "value_score": v_score,
        "total_score": total_score,
        "direction": direction,
        "score_detail": detail,
        "gemini_calls_used": gemini_calls,
    }

    # 콘솔 출력
    t_str = "ON" if trigger_active else "OFF"
    print(f"[SSM] {symbol}: T={t_str} | M={m_score:.1f} | Ss={s_sent_score:.1f} | "
          f"Ss_story={s_story_score:.1f} | V={v_score:.1f} | "
          f"total={total_score:.2f} -> {direction}")

    return result


def _score_momentum(symbol: str) -> tuple[float, dict]:
    """M (Momentum): max 2.0pt - whale + netflow + volume"""
    score = 0.0
    detail = {"max": 2.0}
    direction = "neutral"

    conn = get_connection()

    # M.whale (1.0pt) - Arkham 고래 데이터
    token_id = SYMBOL_TO_TOKEN.get(symbol, "")
    whale = get_whale_direction(token_id, hours=6) if token_id else None

    if whale and whale["tx_count"] > 0:
        whale_score = whale["score"]
        score += whale_score
        if whale["direction"] == "exchange_outflow":
            direction = "bullish"
        elif whale["direction"] == "exchange_inflow":
            direction = "bearish"
        detail["whale"] = {
            "score": whale_score, "direction": whale["direction"],
            "net_flow_usd": whale["net_flow_usd"], "tx_count": whale["tx_count"],
        }
        print(f"[SSM] M.whale: {whale['direction']} (net ${whale['net_flow_usd']:,.0f}) -> {whale_score}pt")
    else:
        detail["whale"] = {"score": 0, "status": "no_data", "msg": "Arkham 데이터 없음"}
        print("[SSM] M.whale: 데이터 없음 -> 0.0pt")

    # M.netflow (1.0pt) - CryptoQuant 넷플로우
    cq_asset = SYMBOL_TO_ASSET.get(symbol, "")
    netflow_sig = get_netflow_signal(cq_asset) if cq_asset else None

    if netflow_sig and netflow_sig["direction"] != "neutral":
        nf_score = netflow_sig["score"]
        score += nf_score
        if netflow_sig["direction"] == "outflow":
            if direction == "neutral":
                direction = "bullish"
        elif netflow_sig["direction"] == "inflow":
            if direction == "neutral":
                direction = "bearish"
        detail["netflow"] = {
            "score": nf_score, "direction": netflow_sig["direction"],
            "latest": netflow_sig["latest_netflow"], "trend": netflow_sig["trend"],
        }
        print(f"[SSM] M.netflow: {netflow_sig['direction']} (trend={netflow_sig['trend']}) -> {nf_score}pt")
    else:
        detail["netflow"] = {"score": 0, "status": "no_data", "msg": "CryptoQuant 데이터 없음"}
        print("[SSM] M.netflow: 데이터 없음 -> 0.0pt")

    # M.volume (0.5pt 보너스) - 거래량 변화
    vol_rows = conn.execute(
        "SELECT volume FROM klines WHERE symbol = ? AND interval = '1d' "
        "ORDER BY open_time DESC LIMIT 30",
        (symbol,),
    ).fetchall()

    if len(vol_rows) >= 2:
        current_vol = vol_rows[0][0]
        avg_vol = sum(r[0] for r in vol_rows) / len(vol_rows)
        vol_ratio = current_vol / avg_vol if avg_vol > 0 else 1.0

        if vol_ratio >= 1.3:  # 30% 이상 증가
            score += 0.5
            detail["volume"] = {"score": 0.5, "ratio": round(vol_ratio, 2), "signal": "high_volume"}
        else:
            detail["volume"] = {"score": 0, "ratio": round(vol_ratio, 2), "signal": "normal"}
    else:
        detail["volume"] = {"score": 0, "status": "insufficient_data"}

    conn.close()

    # 상한 캡
    score = min(2.0, score)
    detail["total"] = score
    detail["direction"] = direction

    return score, detail


def _score_sentiment(symbol: str) -> tuple[float, dict]:
    """S_sentiment: max 1.5pt - F&G + L/S ratio"""
    score = 0.0
    detail = {"max": 1.5}
    direction = "neutral"

    conn = get_connection()

    # Fear & Greed (1.0pt)
    fg = conn.execute(
        "SELECT value, classification FROM fear_greed ORDER BY collected_at DESC LIMIT 1"
    ).fetchone()

    if fg:
        fg_value = fg[0]
        if fg_value <= 25:  # 극공포 -> 상승 신호
            score += 1.0
            direction = "bullish"
            detail["fear_greed"] = {"score": 1.0, "value": fg_value, "class": fg[1], "signal": "extreme_fear_bullish"}
        elif fg_value <= 40:  # 공포 -> 약간 상승
            score += 0.5
            direction = "bullish"
            detail["fear_greed"] = {"score": 0.5, "value": fg_value, "class": fg[1], "signal": "fear_mild_bullish"}
        elif fg_value >= 76:  # 극탐욕 -> 하락 신호
            score += 1.0
            direction = "bearish"
            detail["fear_greed"] = {"score": 1.0, "value": fg_value, "class": fg[1], "signal": "extreme_greed_bearish"}
        elif fg_value >= 61:  # 탐욕 -> 약간 하락
            score += 0.5
            direction = "bearish"
            detail["fear_greed"] = {"score": 0.5, "value": fg_value, "class": fg[1], "signal": "greed_mild_bearish"}
        else:
            detail["fear_greed"] = {"score": 0, "value": fg_value, "class": fg[1], "signal": "neutral"}
    else:
        detail["fear_greed"] = {"score": 0, "status": "no_data"}

    # Long/Short Ratio (0.5pt)
    ls = conn.execute(
        "SELECT long_account FROM long_short_ratios "
        "WHERE symbol = ? ORDER BY collected_at DESC LIMIT 1",
        (symbol,),
    ).fetchone()

    if ls:
        long_pct = ls[0]
        if long_pct >= 0.75:  # 롱 과밀 -> 하락 신호
            score += 0.5
            if direction == "neutral":
                direction = "bearish"
            detail["long_short"] = {"score": 0.5, "long_pct": long_pct, "signal": "excessive_long_bearish"}
        elif long_pct <= 0.25:  # 숏 과밀 -> 상승 신호
            score += 0.5
            if direction == "neutral":
                direction = "bullish"
            detail["long_short"] = {"score": 0.5, "long_pct": long_pct, "signal": "excessive_short_bullish"}
        else:
            detail["long_short"] = {"score": 0, "long_pct": long_pct, "signal": "normal"}
    else:
        detail["long_short"] = {"score": 0, "status": "no_data"}

    conn.close()

    score = min(1.5, score)
    detail["total"] = score
    detail["direction"] = direction

    return score, detail


def _score_value(symbol: str) -> tuple[float, dict]:
    """V (Value): max 0.5pt - MVRV 기반 밸류에이션"""
    detail = {"max": 0.5}
    score = 0.0

    # MVRV (0.5pt) - BGeometrics
    mvrv = get_mvrv_signal()
    mvrv_score = mvrv["score"]
    score += mvrv_score
    detail["mvrv"] = {
        "score": mvrv_score, "value": mvrv["mvrv"], "signal": mvrv["signal"],
    }

    if mvrv["signal"] != "no_data":
        print(f"[SSM] V.mvrv: {mvrv['mvrv']:.4f} ({mvrv['signal']}) -> {mvrv_score}pt")
    else:
        print("[SSM] V.mvrv: 데이터 없음 -> 0.0pt")

    score = min(0.5, score)
    detail["total"] = score
    return score, detail


def get_latest_score(symbol: str = "BTCUSDT") -> dict | None:
    """ssm_scores에서 최신 점수 조회"""
    conn = get_connection()
    row = conn.execute(
        "SELECT trigger_active, momentum_score, sentiment_score, story_score, "
        "value_score, total_score, direction, score_detail, gemini_calls_used, calculated_at "
        "FROM ssm_scores WHERE symbol = ? ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    conn.close()

    if not row:
        return None

    return {
        "symbol": symbol,
        "trigger_active": bool(row[0]),
        "momentum_score": row[1],
        "sentiment_score": row[2],
        "story_score": row[3],
        "value_score": row[4],
        "total_score": row[5],
        "direction": row[6],
        "score_detail": json.loads(row[7]) if row[7] else {},
        "gemini_calls_used": row[8],
        "calculated_at": row[9],
    }


if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8")
    from db import init_db
    init_db()
    calculate_score()
