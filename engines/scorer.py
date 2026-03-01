"""Engine 4: SSM+V+T 스코어링 - 5요소 복합 점수 산출"""
import json
import time
from db import get_connection
from config import SYMBOLS, LIVE_SYMBOLS
from engines.dynamic_threshold import get_latest_threshold
from engines.gemini_client import analyze_sentiment_majority
from collectors.cryptoquant import get_netflow_signal, get_mvrv_signal, get_taker_signal, SYMBOL_TO_ASSET

# Story(Gemini) 캐시: 4시간 주기 호출, 5시간 TTL
_story_cache = {}  # {symbol: {"time": timestamp, "result": {...}}}
_STORY_CACHE_TTL = 5 * 3600  # 5시간
_STORY_CALL_INTERVAL = 4 * 3600  # 4시간


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
    # LIVE_SYMBOLS만 호출 (예산 절약), 4시간 주기 캐시
    s_story_score = 0.0
    gemini_calls = 0
    now = time.time()
    cached = _story_cache.get(symbol)
    cache_valid = cached and (now - cached["time"]) < _STORY_CACHE_TTL

    if symbol in LIVE_SYMBOLS:
        need_call = not cache_valid or (now - (cached["time"] if cached else 0)) >= _STORY_CALL_INTERVAL
        if need_call:
            story_result = analyze_sentiment_majority(symbol, calls=3)
            gemini_calls = story_result.get("calls_used", 0)
            if gemini_calls > 0:
                _story_cache[symbol] = {"time": now, "result": story_result}
                cached = _story_cache[symbol]
                cache_valid = True

    if cache_valid:
        story_result = cached["result"]
        agreement = story_result.get("agreement", 0)
        s_story_score = round(agreement * 1.0, 2)
        detail["story"] = {
            "score": s_story_score,
            "sentiment": story_result.get("sentiment"),
            "agreement": agreement,
            "votes": story_result.get("votes"),
            "cached": True,
        }
        if story_result.get("sentiment") == "bullish":
            bullish_signals += 1
        elif story_result.get("sentiment") == "bearish":
            bearish_signals += 1
    else:
        detail["story"] = {"score": 0, "reason": "not_live_symbol", "gemini_skipped": True}

    # === V (Value) - max 0.5pt ===
    v_score, v_detail = _score_value(symbol)
    detail["value"] = v_detail
    if v_detail.get("direction") == "bullish":
        bullish_signals += 1
    elif v_detail.get("direction") == "bearish":
        bearish_signals += 1

    # === Trigger 보너스 (0.5pt) ===
    trigger_bonus = 0.5 if trigger_active else 0.0
    if trigger_active:
        detail["trigger_bonus"] = 0.5

    # === 합계 ===
    total_score = round(m_score + s_sent_score + s_story_score + v_score + trigger_bonus, 2)
    total_score = min(5.0, total_score)  # 상한 캡

    # === SSM 급락 방지 (Carry Forward) ===
    # 10분 내 50% 이상 하락 시 이전 점수의 90%로 완충
    prev = get_latest_score(symbol)
    if prev and prev["total_score"] > 0:
        drop_ratio = (prev["total_score"] - total_score) / prev["total_score"]
        if drop_ratio >= 0.5:
            carried = round(prev["total_score"] * 0.9, 2)
            if carried > total_score:
                detail["carry_forward"] = {
                    "prev_score": prev["total_score"],
                    "raw_score": total_score,
                    "carried_score": carried,
                }
                print(f"[SSM] {symbol}: 급락 방지 — {prev['total_score']:.2f}→{total_score:.2f} "
                      f"(carry: {carried:.2f})")
                total_score = carried

    # === 방향 결정 (최소 2표 이상 차이 또는 과반 필요) ===
    total_votes = bullish_signals + bearish_signals
    if total_votes >= 2 and bullish_signals > bearish_signals:
        direction = "BULLISH"
    elif total_votes >= 2 and bearish_signals > bullish_signals:
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
    """M (Momentum): max 2.0pt - OI변화 + 테이커 + 오더북 + MTF정렬 + netflow + volume"""
    score = 0.0
    detail = {"max": 2.0}
    direction = "neutral"

    conn = get_connection()

    # M.oi_change (0.3pt) - OI 변화율 (4시간)
    oi_rows = conn.execute(
        "SELECT open_interest FROM oi_snapshots "
        "WHERE symbol = ? ORDER BY id DESC LIMIT 5",
        (symbol,),
    ).fetchall()

    if len(oi_rows) >= 2:
        current_oi = oi_rows[0][0]
        oldest_oi = oi_rows[-1][0]
        oi_change_pct = (current_oi - oldest_oi) / oldest_oi * 100 if oldest_oi > 0 else 0

        if abs(oi_change_pct) >= 3.0:
            score += 0.3
            # OI 증가 + 가격 상승 → bullish, OI 증가 + 가격 하락 → bearish
            kline_now = conn.execute(
                "SELECT close FROM klines WHERE symbol = ? AND interval = '5m' "
                "ORDER BY open_time DESC LIMIT 1", (symbol,),
            ).fetchone()
            kline_old = conn.execute(
                "SELECT close FROM klines WHERE symbol = ? AND interval = '5m' "
                "ORDER BY open_time DESC LIMIT 1 OFFSET 48", (symbol,),
            ).fetchone()
            if kline_now and kline_old and kline_now[0] > kline_old[0]:
                direction = "bullish"
            elif kline_now and kline_old:
                direction = "bearish"
            detail["oi_change"] = {
                "score": 0.3, "change_pct": round(oi_change_pct, 2),
                "current_oi": current_oi, "signal": "oi_surge",
            }
            print(f"[SSM] M.oi: OI {oi_change_pct:+.2f}% -> 0.3pt")
        else:
            detail["oi_change"] = {
                "score": 0, "change_pct": round(oi_change_pct, 2), "signal": "normal",
            }
    else:
        detail["oi_change"] = {"score": 0, "status": "insufficient_data"}
        print("[SSM] M.oi: 데이터 부족 -> 0.0pt")

    # M.taker (0.3pt) - 테이커 매수/매도 비율
    taker = get_taker_signal(symbol)
    taker_score = min(0.3, taker["score"] * 0.6)  # 0.5pt → 0.3pt 스케일
    score += taker_score
    if taker["direction"] == "buy_dominant":
        if direction == "neutral":
            direction = "bullish"
    elif taker["direction"] == "sell_dominant":
        if direction == "neutral":
            direction = "bearish"
    detail["taker"] = {
        "score": round(taker_score, 2), "ratio": taker.get("ratio", 0),
        "direction": taker["direction"],
    }
    if taker_score > 0:
        print(f"[SSM] M.taker: {taker['direction']} (ratio={taker.get('ratio', 0):.4f}) -> {taker_score:.2f}pt")

    # M.orderbook (0.2pt) - 오더북 비대칭
    latest_scan = conn.execute(
        "SELECT scan_id FROM orderbook_walls WHERE symbol = ? ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()

    if latest_scan:
        scan_id = latest_scan[0]
        bid_total = conn.execute(
            "SELECT COALESCE(SUM(quantity), 0) FROM orderbook_walls "
            "WHERE symbol = ? AND scan_id = ? AND side = 'bid'",
            (symbol, scan_id),
        ).fetchone()[0]
        ask_total = conn.execute(
            "SELECT COALESCE(SUM(quantity), 0) FROM orderbook_walls "
            "WHERE symbol = ? AND scan_id = ? AND side = 'ask'",
            (symbol, scan_id),
        ).fetchone()[0]
        bid_ask_ratio = bid_total / ask_total if ask_total > 0 else 1.0

        if bid_ask_ratio >= 1.5:
            score += 0.2
            if direction == "neutral":
                direction = "bullish"
            detail["orderbook"] = {
                "score": 0.2, "bid_ask_ratio": round(bid_ask_ratio, 2),
                "signal": "bid_dominant",
            }
            print(f"[SSM] M.ob: bid/ask={bid_ask_ratio:.2f} (매수벽 우세) -> 0.2pt")
        elif bid_ask_ratio <= 0.67:  # 1/1.5
            score += 0.2
            if direction == "neutral":
                direction = "bearish"
            detail["orderbook"] = {
                "score": 0.2, "bid_ask_ratio": round(bid_ask_ratio, 2),
                "signal": "ask_dominant",
            }
            print(f"[SSM] M.ob: bid/ask={bid_ask_ratio:.2f} (매도벽 우세) -> 0.2pt")
        else:
            detail["orderbook"] = {
                "score": 0, "bid_ask_ratio": round(bid_ask_ratio, 2), "signal": "balanced",
            }
    else:
        detail["orderbook"] = {"score": 0, "status": "no_data"}

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

    # M.volume (0.5pt 보너스) - rolling 24h 거래량 vs 일봉 평균
    vol_5m = conn.execute(
        "SELECT volume FROM klines WHERE symbol = ? AND interval = '5m' "
        "ORDER BY open_time DESC LIMIT 288",  # 288 × 5분 = 24시간
        (symbol,),
    ).fetchall()
    vol_daily = conn.execute(
        "SELECT volume FROM klines WHERE symbol = ? AND interval = '1d' "
        "ORDER BY open_time DESC LIMIT 30 OFFSET 1",  # 미완성 당일봉 제외
        (symbol,),
    ).fetchall()

    if len(vol_5m) >= 12 and len(vol_daily) >= 1:  # 최소 1시간 5분봉 + 1일봉
        current_vol = sum(r[0] for r in vol_5m)
        avg_daily_vol = sum(r[0] for r in vol_daily) / len(vol_daily)
        vol_ratio = current_vol / avg_daily_vol if avg_daily_vol > 0 else 1.0

        if vol_ratio >= 1.3:  # 30% 이상 증가
            score += 0.5
            detail["volume"] = {"score": 0.5, "ratio": round(vol_ratio, 2), "signal": "high_volume"}
        else:
            detail["volume"] = {"score": 0, "ratio": round(vol_ratio, 2), "signal": "normal"}
    else:
        detail["volume"] = {"score": 0, "status": "insufficient_data"}

    conn.close()

    # M.trend (0.2pt) - MTF 정렬 점수
    try:
        from engines.mtf_analyzer import get_latest_mtf
        mtf = get_latest_mtf(symbol)
        if mtf and abs(mtf["alignment_score"]) >= 0.75:
            score += 0.2
            # MTF 방향이 전체 방향과 일치하는지 확인
            mtf_dir = "bullish" if mtf["alignment_score"] > 0 else "bearish"
            if direction == "neutral":
                direction = mtf_dir
            detail["trend"] = {
                "score": 0.2, "alignment": mtf["alignment_score"],
                "bias": mtf["bias"], "pattern_1d": mtf.get("pattern_1d", ""),
            }
            print(f"[SSM] M.trend: alignment={mtf['alignment_score']:+.2f} ({mtf['bias']}) -> 0.2pt")
        else:
            detail["trend"] = {
                "score": 0,
                "alignment": mtf["alignment_score"] if mtf else 0,
                "status": "weak_alignment" if mtf else "no_data",
            }
    except Exception:
        detail["trend"] = {"score": 0, "status": "mtf_not_ready"}

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
    if not mvrv:
        mvrv = {"mvrv": 0, "signal": "no_data", "score": 0.0}
    mvrv_score = mvrv["score"]
    score += mvrv_score
    detail["mvrv"] = {
        "score": mvrv_score, "value": mvrv["mvrv"], "signal": mvrv["signal"],
    }

    # MVRV 방향: 저평가=bullish, 과열=bearish
    if mvrv["signal"] in ("undervalued_bullish", "low"):
        detail["direction"] = "bullish"
    elif mvrv["signal"] in ("overheated_bearish", "elevated"):
        detail["direction"] = "bearish"

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
