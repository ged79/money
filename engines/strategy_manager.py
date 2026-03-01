"""Engine 5: 전략 매니저 - L1/L2/L4 상태머신 + 시그널 생성"""
import json
import time
from datetime import date, datetime

from db import get_connection, check_data_freshness
from config import (
    SYMBOLS,
    L1_FUNDING_THRESHOLD, L1_LS_RATIO_THRESHOLD, L1_FUNDING_EXIT,
    L2_MAX_DIRECTION_CHANGES, L2_STEP1_PCT, L2_STEP2_PCT, L2_STEP3_PCT,
    L2_STEP2_DELAY, L2_STEP3_DELAY,
    L2_MIN_SSM_SCORE, L2_BREAKOUT_CONFIRM_CANDLES,
    L2_TRAILING_STOP_ACTIVATE, L2_TRAILING_STOP_DISTANCE,
    BOX_PRICE_TOLERANCE, BOX_DURATION_MIN, OI_RECOVERY_THRESHOLD,
)
from engines.atr import get_latest_atr
from engines.dynamic_threshold import get_latest_threshold
from engines.grid_range import get_latest_grid
from engines.scorer import get_latest_score
from engines.macro_guard import check_macro_block
from engines.mtf_analyzer import get_latest_mtf


def run_strategy(symbol: str = None) -> dict | None:
    """전략 매니저 메인 루프 - 상태 머신 실행"""
    symbols = [symbol] if symbol else SYMBOLS
    result = None
    for sym in symbols:
        r = _run_single(sym)
        if r:
            result = r
    return result


def _run_single(symbol: str) -> dict:
    # 현재 상태 로드 (없으면 초기화)
    state = _get_current_state(symbol)

    # 일일 방향 전환 카운터 리셋
    today = date.today().isoformat()
    if state["l2_last_reset_date"] != today:
        state["l2_direction_changes_today"] = 0
        state["l2_last_reset_date"] = today

    # 데이터 신선도 체크 (5분봉 10분 이상 미갱신 시 경고)
    freshness = check_data_freshness(symbol, max_age_seconds=600)
    stale_keys = [k for k, v in freshness.items() if v["stale"]]
    if stale_keys:
        print(f"[Strategy] {symbol}: 데이터 지연 경고 - {', '.join(stale_keys)}")

    # 엔진 출력 로드
    atr = get_latest_atr(symbol)
    threshold = get_latest_threshold(symbol)
    grid = get_latest_grid(symbol)
    score = get_latest_score(symbol)
    macro = check_macro_block(symbol)
    mtf = get_latest_mtf(symbol)

    # 매크로 가드 업데이트
    state["macro_blocked"] = macro["blocked"]
    state["macro_block_reason"] = macro["reason"] if macro["blocked"] else None

    signals = []

    # === L1 평가 (항상 실행) ===
    l1_result = _check_l1(symbol, state)
    if l1_result["action"] == "ENTER" and not state["l1_active"]:
        state["l1_active"] = True
        state["l1_entry_reason"] = l1_result["reason"]
        signals.append(_emit_signal(symbol, "L1_ENTRY", "NEUTRAL", l1_result))
    elif l1_result["action"] == "EXIT" and state["l1_active"]:
        state["l1_active"] = False
        state["l1_entry_reason"] = None
        signals.append(_emit_signal(symbol, "L1_EXIT", "NEUTRAL", l1_result))

    # === 상태 머신 ===
    if state["state"] == "A":
        # State A: L4 그리드 가동, L2 대기
        if not state["l4_active"] and grid:
            state["l4_active"] = True
            state["l4_grid_config_id"] = grid["id"]
            signals.append(_emit_signal(symbol, "L4_GRID_SET", "NEUTRAL", grid))

        # 그리드 이탈 감지 → breakout 확인 → State B 전환
        active_grid = grid
        if state["l4_active"] and state["l4_grid_config_id"]:
            active_grid = _get_grid_by_id(state["l4_grid_config_id"]) or grid
        if active_grid and not macro["blocked"]:
            breakout = _detect_breakout(symbol, active_grid)
            if breakout["detected"]:
                if state["l2_direction_changes_today"] >= L2_MAX_DIRECTION_CHANGES:
                    print(f"[Strategy] {symbol}: 방향 전환 한도 도달 ({L2_MAX_DIRECTION_CHANGES}회/일)")
                else:
                    # SSM 점수 게이트: 최소 점수 미달시 Price Action 경로 시도
                    ssm_total = score["total_score"] if score else 0
                    # MTF 패턴 보너스: ascending triangle + 강한 alignment → +0.3
                    mtf_bonus = 0
                    if mtf:
                        if mtf.get("pattern_1d") == "ascending" and mtf.get("alignment_score", 0) >= 0.5:
                            mtf_bonus = 0.3
                            print(f"[Strategy] {symbol}: MTF ascending triangle + alignment 보너스 +0.3")
                        elif mtf.get("pattern_4h") == "ascending" and mtf.get("alignment_score", 0) >= 0.5:
                            mtf_bonus = 0.2
                            print(f"[Strategy] {symbol}: MTF 4H ascending + alignment 보너스 +0.2")
                    ssm_total += mtf_bonus
                    # 거래량 급증 체크: 2배+ 시 1캔들 확인, 아니면 3캔들
                    vol_surge = _check_volume_surge(symbol, threshold=2.0)
                    confirm_candles = 1 if vol_surge else None  # None = 기본값(3)
                    if vol_surge:
                        print(f"[Strategy] {symbol}: 거래량 급증 감지 → 1캔들 빠른 확인")

                    if ssm_total < L2_MIN_SSM_SCORE:
                        # Price Action 경로: SSM 미달이지만 강한 돌파 시 절반 포지션
                        pa_confirmed = _confirm_breakout(symbol, active_grid, breakout["direction"],
                                                         candles=confirm_candles)
                        if vol_surge and pa_confirmed:
                            pa_entry_pct = L2_STEP1_PCT * 0.5  # 절반 포지션 (7.5%)
                            state["state"] = "B"
                            state["l2_active"] = True
                            state["l2_step"] = 1
                            state["l2_entry_pct"] = pa_entry_pct
                            state["l2_direction"] = breakout["direction"]
                            state["l2_step1_time"] = datetime.now().isoformat()
                            state["l2_avg_entry_price"] = breakout["price"]
                            state["l2_trailing_stop_price"] = None
                            state["l4_active"] = False
                            signals.append(_emit_signal(
                                symbol, "L2_STEP1_PA", breakout["direction"],
                                {"entry_pct": pa_entry_pct, "price": breakout["price"],
                                 "ssm_score": ssm_total, "reason": "price_action_only",
                                 "stop_loss": _calc_stop_loss(breakout["price"], atr, breakout["direction"])},
                                score=ssm_total,
                            ))
                            signals.append(_emit_signal(symbol, "L4_PAUSE", "NEUTRAL", {}))
                            print(f"[Strategy] {symbol}: L2 PA진입 ({breakout['direction']}, {pa_entry_pct*100:.1f}%)")
                        else:
                            print(f"[Strategy] {symbol}: SSM 점수 부족 ({ssm_total:.2f} < {L2_MIN_SSM_SCORE}) & PA 미충족 - L2 진입 보류")
                    else:
                        # SSM + MTF 방향 일치 확인
                        ssm_direction = score["direction"] if score else "NEUTRAL"
                        breakout_dir = breakout["direction"]
                        # MTF alignment이 강하면 방향 불일치 무시
                        mtf_override = False
                        if mtf and abs(mtf.get("alignment_score", 0)) >= 0.75:
                            mtf_dir = "LONG" if mtf["alignment_score"] > 0 else "SHORT"
                            if mtf_dir == breakout_dir:
                                mtf_override = True
                                print(f"[Strategy] {symbol}: MTF 강한 정렬 ({mtf['bias']}) - SSM 방향 충돌 무시")
                        direction_conflict = (
                            (ssm_direction == "BEARISH" and breakout_dir == "LONG") or
                            (ssm_direction == "BULLISH" and breakout_dir == "SHORT")
                        ) and not mtf_override
                        if direction_conflict:
                            print(f"[Strategy] {symbol}: SSM 방향 불일치 ({ssm_direction} vs {breakout_dir}) - L2 진입 보류")
                        else:
                            # Breakout 확인: 거래량 급증 시 1캔들, 아니면 3캔들
                            confirmed = _confirm_breakout(symbol, active_grid, breakout["direction"],
                                                          candles=confirm_candles)
                            n_candles = confirm_candles or L2_BREAKOUT_CONFIRM_CANDLES
                            if not confirmed:
                                print(f"[Strategy] {symbol}: breakout 미확인 (캔들 {n_candles}개 미충족)")
                            else:
                                # 모든 조건 충족 → L2 진입
                                state["state"] = "B"
                                state["l2_active"] = True
                                state["l2_step"] = 1
                                state["l2_entry_pct"] = L2_STEP1_PCT
                                state["l2_direction"] = breakout["direction"]
                                state["l2_step1_time"] = datetime.now().isoformat()
                                state["l2_avg_entry_price"] = breakout["price"]
                                state["l2_trailing_stop_price"] = None
                                state["l4_active"] = False
                                signals.append(_emit_signal(
                                    symbol, "L2_STEP1", breakout["direction"],
                                    {"entry_pct": L2_STEP1_PCT, "price": breakout["price"],
                                     "ssm_score": ssm_total,
                                     "stop_loss": _calc_stop_loss(breakout["price"], atr, breakout["direction"])},
                                    score=ssm_total,
                                ))
                                signals.append(_emit_signal(symbol, "L4_PAUSE", "NEUTRAL", {}))

    elif state["state"] == "B":
        # State B: L2 단계별 진행
        _progress_l2(symbol, state, atr, score, grid, signals)

    # 상태 저장
    state["pending_signal"] = json.dumps([s["signal_type"] for s in signals]) if signals else None
    _save_state(symbol, state)

    # 콘솔 출력
    l1_str = "ON" if state["l1_active"] else "OFF"
    l2_str = f"ON(step {state['l2_step']}, {state['l2_direction']})" if state["l2_active"] else "OFF"
    l4_str = "ON" if state["l4_active"] else "OFF"
    macro_str = "BLOCKED" if state["macro_blocked"] else "OK"
    print(f"[Strategy] {symbol}: State={state['state']} | L1={l1_str} | L2={l2_str} | "
          f"L4={l4_str} | macro={macro_str}")

    if signals:
        for s in signals:
            print(f"[Signal] {s['signal_type']} | {s.get('direction', '-')} | "
                  f"{json.dumps(s.get('details', {}), ensure_ascii=False)[:100]}")
    else:
        print("[Signal] 신호 없음 - 대기 중")

    return state


def _check_l1(symbol: str, state: dict) -> dict:
    """L1 델타 뉴트럴 진입/청산 조건 확인"""
    conn = get_connection()

    # 최신 펀딩비
    fr_row = conn.execute(
        "SELECT funding_rate FROM funding_rates "
        "WHERE symbol = ? ORDER BY collected_at DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    funding = fr_row[0] if fr_row else 0

    # 최신 롱/숏 비율
    ls_row = conn.execute(
        "SELECT long_account FROM long_short_ratios "
        "WHERE symbol = ? ORDER BY collected_at DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    long_pct = ls_row[0] if ls_row else 0.5

    conn.close()

    # 진입 조건: 펀딩비 ≥ 0.05% AND 롱비율 ≥ 65%
    if not state["l1_active"]:
        if funding >= L1_FUNDING_THRESHOLD and long_pct >= L1_LS_RATIO_THRESHOLD:
            return {
                "action": "ENTER",
                "reason": f"funding={funding*100:.4f}%(>={L1_FUNDING_THRESHOLD*100:.2f}%) "
                          f"long={long_pct*100:.1f}%(>={L1_LS_RATIO_THRESHOLD*100:.0f}%)",
                "funding": funding,
                "long_pct": long_pct,
            }
        return {"action": "HOLD", "funding": funding, "long_pct": long_pct}

    # 청산 조건: 펀딩비 마이너스 OR ≤ 0.01% OR L/S → 50:50
    if funding < 0:
        return {"action": "EXIT", "reason": f"funding 마이너스 전환 ({funding*100:.4f}%)",
                "funding": funding, "long_pct": long_pct}
    if funding <= L1_FUNDING_EXIT:
        return {"action": "EXIT", "reason": f"funding 정상화 ({funding*100:.4f}% <= {L1_FUNDING_EXIT*100:.2f}%)",
                "funding": funding, "long_pct": long_pct}
    if abs(long_pct - 0.5) < 0.05:
        return {"action": "EXIT", "reason": f"L/S 비율 정상화 ({long_pct*100:.1f}% ~ 50:50)",
                "funding": funding, "long_pct": long_pct}

    return {"action": "HOLD", "funding": funding, "long_pct": long_pct}


def _detect_breakout(symbol: str, grid: dict) -> dict:
    """그리드 범위 이탈 감지 (5분봉 기반)"""
    conn = get_connection()
    # 5분봉 우선, 없으면 일봉 폴백
    row = conn.execute(
        "SELECT close FROM klines WHERE symbol = ? AND interval = '5m' "
        "ORDER BY open_time DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    if not row:
        row = conn.execute(
            "SELECT close FROM klines WHERE symbol = ? AND interval = '1d' "
            "ORDER BY open_time DESC LIMIT 1",
            (symbol,),
        ).fetchone()
    conn.close()

    if not row:
        return {"detected": False}

    price = row[0]
    lower = grid["lower_bound"]
    upper = grid["upper_bound"]

    if price > upper:
        return {"detected": True, "direction": "LONG", "price": price,
                "reason": f"price ${price:,.0f} > upper ${upper:,.0f}"}
    elif price < lower:
        return {"detected": True, "direction": "SHORT", "price": price,
                "reason": f"price ${price:,.0f} < lower ${lower:,.0f}"}

    return {"detected": False, "price": price}


def _confirm_breakout(symbol: str, grid: dict, direction: str,
                      candles: int = None) -> bool:
    """Breakout 확인: 최근 N개 5분봉이 연속으로 그리드 이탈 유지하는지 확인

    candles: 확인 캔들 수 (None이면 L2_BREAKOUT_CONFIRM_CANDLES 사용)
    """
    n = candles or L2_BREAKOUT_CONFIRM_CANDLES
    conn = get_connection()
    rows = conn.execute(
        "SELECT close FROM klines WHERE symbol = ? AND interval = '5m' "
        "ORDER BY open_time DESC LIMIT ?",
        (symbol, n),
    ).fetchall()
    conn.close()

    if len(rows) < n:
        return False  # 데이터 부족 → 미확인

    upper = grid["upper_bound"]
    lower = grid["lower_bound"]

    if direction == "LONG":
        return all(r[0] > upper for r in rows)
    else:
        return all(r[0] < lower for r in rows)


def _progress_l2(symbol: str, state: dict, atr: dict, score: dict, grid: dict, signals: list):
    """L2 단계 진행 관리"""
    if not state["l2_step1_time"]:
        return

    step1_time = datetime.fromisoformat(state["l2_step1_time"])
    elapsed = (datetime.now() - step1_time).total_seconds()

    if state["l2_step"] == 1:
        # 30분 경과 확인 (증가: 15→30분)
        if elapsed >= L2_STEP2_DELAY:
            # 방향 유지 + SSM 점수 재확인
            price_maintained = _check_price_direction(symbol, state["l2_direction"])
            ssm_total = score["total_score"] if score else 0

            if not price_maintained:
                # 방향 되돌림 → probe 손절, State A 복귀
                _exit_l2(state, symbol, "price_reversal_step1", signals)
            elif ssm_total < L2_MIN_SSM_SCORE:
                # SSM 점수 하락 → probe만 유지, 증액 거부
                print(f"[Strategy] {symbol}: Step2 SSM 부족 ({ssm_total:.2f} < {L2_MIN_SSM_SCORE}) - 15% 유지, 대기")
            else:
                state["l2_step"] = 2
                state["l2_entry_pct"] = L2_STEP1_PCT + L2_STEP2_PCT  # 40%

                # 평균 진입가 재계산
                current_price = _get_current_price(symbol)
                if current_price and state["l2_avg_entry_price"]:
                    avg = (state["l2_avg_entry_price"] * L2_STEP1_PCT +
                           current_price * L2_STEP2_PCT) / (L2_STEP1_PCT + L2_STEP2_PCT)
                    state["l2_avg_entry_price"] = round(avg, 2)

                stop = _calc_stop_loss(state["l2_avg_entry_price"], atr, state["l2_direction"])
                signals.append(_emit_signal(
                    symbol, "L2_STEP2", state["l2_direction"],
                    {"entry_pct": state["l2_entry_pct"], "avg_price": state["l2_avg_entry_price"],
                     "stop_loss": stop, "ssm_score": ssm_total},
                    score=ssm_total,
                ))

    elif state["l2_step"] == 2:
        # 30분(step1 기준) 경과 + SSM 점수 확인
        if elapsed >= L2_STEP3_DELAY:
            total = score["total_score"] if score else 0

            if total >= L2_MIN_SSM_SCORE:
                ratio = _score_to_ratio(total)
                remaining = L2_STEP3_PCT * ratio
                state["l2_step"] = 3
                state["l2_entry_pct"] = L2_STEP1_PCT + L2_STEP2_PCT + remaining
                state["l2_score_at_entry"] = total

                current_price = _get_current_price(symbol)
                if current_price and state["l2_avg_entry_price"]:
                    prev_pct = L2_STEP1_PCT + L2_STEP2_PCT
                    avg = (state["l2_avg_entry_price"] * prev_pct +
                           current_price * remaining) / (prev_pct + remaining)
                    state["l2_avg_entry_price"] = round(avg, 2)

                stop = _calc_stop_loss(state["l2_avg_entry_price"], atr, state["l2_direction"])
                signals.append(_emit_signal(
                    symbol, "L2_STEP3", state["l2_direction"],
                    {"entry_pct": state["l2_entry_pct"], "ratio": ratio,
                     "score": total, "avg_price": state["l2_avg_entry_price"],
                     "stop_loss": stop},
                    score=total,
                ))
            else:
                # 점수 부족 → 60%에서 유지, step 3으로 마크 (추가 진입 없음)
                state["l2_step"] = 3
                state["l2_score_at_entry"] = total
                print(f"[Strategy] {symbol}: L2 Step3 점수 부족 ({total:.2f} < {L2_MIN_SSM_SCORE}) - 60% 유지")

    # Trailing stop 업데이트 (모든 step에서 작동)
    if state["l2_active"]:
        _update_trailing_stop(symbol, state)

    if state["l2_step"] == 3:
        # 청산 조건 모니터링 (우선순위: trailing stop > stop loss > box formation)
        if _check_trailing_stop_hit(symbol, state):
            _exit_l2(state, symbol, "trailing_stop", signals)
        elif _check_stop_loss_hit(symbol, state, atr):
            _exit_l2(state, symbol, "stop_loss", signals)
        elif _check_box_formation(symbol):
            _exit_l2(state, symbol, "new_box_formation", signals)


def _exit_l2(state: dict, symbol: str, reason: str, signals: list):
    """L2 청산 → State A 복귀"""
    signals.append(_emit_signal(symbol, "L2_EXIT", state["l2_direction"],
                                {"reason": reason, "entry_pct": state["l2_entry_pct"],
                                 "trailing_stop": state.get("l2_trailing_stop_price")}))
    state["state"] = "A"
    state["l2_active"] = False
    state["l2_step"] = 0
    state["l2_entry_pct"] = 0
    state["l2_direction"] = None
    state["l2_avg_entry_price"] = None
    state["l2_step1_time"] = None
    state["l2_score_at_entry"] = None
    state["l2_trailing_stop_price"] = None
    if reason != "price_reversal_step1":
        state["l2_direction_changes_today"] += 1
    state["l4_active"] = True
    # L4 재활성화 시 최신 그리드로 갱신 (breakout은 새 그리드 기준으로 판정)
    from engines.grid_range import get_latest_grid
    latest_grid = get_latest_grid(symbol)
    if latest_grid:
        state["l4_grid_config_id"] = latest_grid["id"]
    signals.append(_emit_signal(symbol, "L4_RESUME", "NEUTRAL", {}))


def _check_price_direction(symbol: str, direction: str) -> bool:
    """가격이 L2 방향을 유지하는지 확인 (5분봉 3개 = 15분 추세)"""
    conn = get_connection()
    # 5분봉 최근 3개로 단기 추세 확인
    rows = conn.execute(
        "SELECT close FROM klines WHERE symbol = ? AND interval = '5m' "
        "ORDER BY open_time DESC LIMIT 3",
        (symbol,),
    ).fetchall()
    if len(rows) < 2:
        # 5분봉 부족 시 일봉 폴백
        rows = conn.execute(
            "SELECT close FROM klines WHERE symbol = ? AND interval = '1d' "
            "ORDER BY open_time DESC LIMIT 2",
            (symbol,),
        ).fetchall()
    conn.close()

    if len(rows) < 2:
        return True  # 데이터 부족시 유지로 간주

    current = rows[0][0]
    oldest = rows[-1][0]

    if direction == "LONG":
        return current >= oldest
    else:
        return current <= oldest


def _update_trailing_stop(symbol: str, state: dict):
    """Trailing stop 가격 업데이트 — 수익이 활성화 수준 이상일 때 작동"""
    if not state["l2_avg_entry_price"]:
        return

    current_price = _get_current_price(symbol)
    if not current_price:
        return

    entry = state["l2_avg_entry_price"]
    direction = state["l2_direction"]

    # 현재 수익률 계산
    if direction == "LONG":
        pnl_pct = (current_price - entry) / entry
    else:
        pnl_pct = (entry - current_price) / entry

    # 활성화 수준 도달 확인
    if pnl_pct < L2_TRAILING_STOP_ACTIVATE:
        return

    # trailing stop 가격 계산
    if direction == "LONG":
        new_trail = current_price * (1 - L2_TRAILING_STOP_DISTANCE)
        old_trail = state.get("l2_trailing_stop_price")
        if old_trail is None or new_trail > old_trail:
            state["l2_trailing_stop_price"] = round(new_trail, 2)
            print(f"[Strategy] {symbol}: trailing stop 갱신 ${state['l2_trailing_stop_price']:,.2f} "
                  f"(수익 {pnl_pct*100:+.1f}%)")
    else:  # SHORT
        new_trail = current_price * (1 + L2_TRAILING_STOP_DISTANCE)
        old_trail = state.get("l2_trailing_stop_price")
        if old_trail is None or new_trail < old_trail:
            state["l2_trailing_stop_price"] = round(new_trail, 2)
            print(f"[Strategy] {symbol}: trailing stop 갱신 ${state['l2_trailing_stop_price']:,.2f} "
                  f"(수익 {pnl_pct*100:+.1f}%)")


def _check_trailing_stop_hit(symbol: str, state: dict) -> bool:
    """Trailing stop 발동 확인"""
    trail_price = state.get("l2_trailing_stop_price")
    if not trail_price:
        return False

    current_price = _get_current_price(symbol)
    if not current_price:
        return False

    if state["l2_direction"] == "LONG":
        return current_price <= trail_price
    else:  # SHORT
        return current_price >= trail_price


def _check_stop_loss_hit(symbol: str, state: dict, atr: dict) -> bool:
    """스톱로스 발동 확인"""
    if not atr or not state["l2_avg_entry_price"]:
        return False

    current_price = _get_current_price(symbol)
    if not current_price:
        return False

    stop = _calc_stop_loss(state["l2_avg_entry_price"], atr, state["l2_direction"])

    if state["l2_direction"] == "LONG":
        return current_price <= stop
    else:  # SHORT
        return current_price >= stop


def _check_box_formation(symbol: str) -> bool:
    """박스권 형성 감지 (3개 중 2개 충족)"""
    conn = get_connection()
    conditions_met = 0

    # 조건 1: 4시간+ ±2% 횡보 (5분봉 48개 = 4시간)
    recent = conn.execute(
        "SELECT close FROM klines WHERE symbol = ? AND interval = '5m' "
        "ORDER BY open_time DESC LIMIT 48",
        (symbol,),
    ).fetchall()
    if len(recent) < 6:
        # 5분봉 부족 시 일봉 폴백
        recent = conn.execute(
            "SELECT close FROM klines WHERE symbol = ? AND interval = '1d' "
            "ORDER BY open_time DESC LIMIT 3",
            (symbol,),
        ).fetchall()
    if len(recent) >= 2:
        prices = [r[0] for r in recent]
        min_p, max_p = min(prices), max(prices)
        if min_p > 0:
            range_pct = (max_p - min_p) / min_p
            if range_pct <= BOX_PRICE_TOLERANCE:
                conditions_met += 1

    # 조건 2: 새 청산 밀집 구간 (최근 1시간 청산 건수)
    now_ms = int(time.time() * 1000)
    liq_count = conn.execute(
        "SELECT COUNT(*) FROM liquidations WHERE symbol = ? AND trade_time > ?",
        (symbol, now_ms - 3600_000),
    ).fetchone()[0]
    if liq_count >= 10:  # 1시간 10건 이상 = 청산 밀집
        conditions_met += 1

    # 조건 3: OI 80% 이상 회복
    oi_rows = conn.execute(
        "SELECT open_interest FROM oi_snapshots "
        "WHERE symbol = ? ORDER BY collected_at DESC LIMIT 5",
        (symbol,),
    ).fetchall()
    if len(oi_rows) >= 3:
        current_oi = oi_rows[0][0]
        peak_oi = max(r[0] for r in oi_rows)
        if peak_oi > 0 and current_oi >= peak_oi * OI_RECOVERY_THRESHOLD:
            conditions_met += 1

    conn.close()
    return conditions_met >= 2


def _check_volume_surge(symbol: str, threshold: float = 2.0) -> bool:
    """거래량 급증 확인: 최근 5분봉 거래량이 평균의 N배 이상"""
    conn = get_connection()
    vol_rows = conn.execute(
        "SELECT volume FROM klines WHERE symbol = ? AND interval = '5m' "
        "ORDER BY open_time DESC LIMIT 288",  # 24시간
        (symbol,),
    ).fetchall()
    conn.close()

    if len(vol_rows) < 12:  # 최소 1시간
        return False

    recent_vol = sum(r[0] for r in vol_rows[:3])  # 최근 15분
    avg_vol = sum(r[0] for r in vol_rows) / len(vol_rows) * 3  # 15분 단위 평균
    if avg_vol <= 0:
        return False

    return recent_vol >= avg_vol * threshold


def _calc_stop_loss(entry_price: float, atr: dict, direction: str) -> float:
    """ATR 기반 스톱로스 계산"""
    if not atr:
        # ATR 없으면 5% 고정
        return entry_price * (0.95 if direction == "LONG" else 1.05)

    stop_distance = entry_price * (atr["stop_loss_pct"] / 100)
    if direction == "LONG":
        return round(entry_price - stop_distance, 2)
    else:
        return round(entry_price + stop_distance, 2)


def _score_to_ratio(total_score: float) -> float:
    """SSM 점수 → L2 Step3 진입 비율"""
    if total_score >= 4.0:
        return 1.00
    elif total_score >= 3.0:
        return 0.60
    elif total_score >= 2.0:
        return 0.30
    elif total_score >= 1.5:
        return 0.15
    return 0.0


def _get_grid_by_id(grid_id: int) -> dict | None:
    """특정 ID의 그리드 설정 조회 (활성 그리드 breakout 판정용)"""
    conn = get_connection()
    row = conn.execute(
        "SELECT id, lower_bound, upper_bound, grid_count, grid_spacing, grid_spacing_pct "
        "FROM grid_configs WHERE id = ?",
        (grid_id,),
    ).fetchone()
    conn.close()
    if not row:
        return None
    return {
        "id": row[0],
        "lower_bound": row[1],
        "upper_bound": row[2],
        "grid_count": row[3],
        "grid_spacing": row[4],
        "grid_spacing_pct": row[5],
    }


def _get_current_price(symbol: str) -> float | None:
    """최신 종가 조회 (5분봉 우선, 일봉 폴백)"""
    conn = get_connection()
    row = conn.execute(
        "SELECT close FROM klines WHERE symbol = ? AND interval = '5m' "
        "ORDER BY open_time DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    if not row:
        row = conn.execute(
            "SELECT close FROM klines WHERE symbol = ? AND interval = '1d' "
            "ORDER BY open_time DESC LIMIT 1",
            (symbol,),
        ).fetchone()
    conn.close()
    return row[0] if row else None


def _emit_signal(symbol: str, signal_type: str, direction: str, details: dict,
                 score: float = None) -> dict:
    """시그널 생성 + signal_log 테이블에 기록"""
    conn = get_connection()
    conn.execute(
        "INSERT INTO signal_log (symbol, signal_type, direction, details, ssm_score) "
        "VALUES (?, ?, ?, ?, ?)",
        (symbol, signal_type, direction,
         json.dumps(details, ensure_ascii=False, default=str), score),
    )
    conn.commit()
    conn.close()

    return {
        "symbol": symbol,
        "signal_type": signal_type,
        "direction": direction,
        "details": details,
        "ssm_score": score,
    }


def _get_current_state(symbol: str) -> dict:
    """strategy_state에서 최신 상태 로드 (없으면 초기 상태)"""
    conn = get_connection()
    row = conn.execute(
        "SELECT state, l1_active, l1_entry_reason, "
        "l2_active, l2_direction, l2_step, l2_entry_pct, "
        "l2_avg_entry_price, l2_step1_time, l2_score_at_entry, "
        "l2_direction_changes_today, l2_last_reset_date, "
        "l4_active, l4_grid_config_id, macro_blocked, macro_block_reason, "
        "l2_trailing_stop_price "
        "FROM strategy_state WHERE symbol = ? ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    conn.close()

    if row:
        return {
            "state": row[0],
            "l1_active": bool(row[1]),
            "l1_entry_reason": row[2],
            "l2_active": bool(row[3]),
            "l2_direction": row[4],
            "l2_step": row[5],
            "l2_entry_pct": row[6],
            "l2_avg_entry_price": row[7],
            "l2_step1_time": row[8],
            "l2_score_at_entry": row[9],
            "l2_direction_changes_today": row[10],
            "l2_last_reset_date": row[11],
            "l4_active": bool(row[12]),
            "l4_grid_config_id": row[13],
            "macro_blocked": bool(row[14]),
            "macro_block_reason": row[15],
            "l2_trailing_stop_price": row[16],
            "pending_signal": None,
        }

    # 초기 상태
    return {
        "state": "A",
        "l1_active": False, "l1_entry_reason": None,
        "l2_active": False, "l2_direction": None, "l2_step": 0,
        "l2_entry_pct": 0, "l2_avg_entry_price": None,
        "l2_step1_time": None, "l2_score_at_entry": None,
        "l2_trailing_stop_price": None,
        "l2_direction_changes_today": 0, "l2_last_reset_date": date.today().isoformat(),
        "l4_active": False, "l4_grid_config_id": None,
        "macro_blocked": False, "macro_block_reason": None,
        "pending_signal": None,
    }


def _save_state(symbol: str, state: dict):
    """strategy_state 테이블에 상태 저장 (SQLite UPSERT: 심볼당 1행 유지)"""
    conn = get_connection()
    vals = (
        symbol,
        state["state"],
        1 if state["l1_active"] else 0, state["l1_entry_reason"],
        1 if state["l2_active"] else 0, state["l2_direction"],
        state["l2_step"], state["l2_entry_pct"],
        state["l2_avg_entry_price"], state["l2_step1_time"],
        state["l2_score_at_entry"],
        state["l2_direction_changes_today"], state["l2_last_reset_date"],
        1 if state["l4_active"] else 0, state["l4_grid_config_id"],
        1 if state["macro_blocked"] else 0, state["macro_block_reason"],
        state["pending_signal"], state.get("l2_trailing_stop_price"),
    )
    conn.execute(
        "INSERT INTO strategy_state "
        "(symbol, state, l1_active, l1_entry_reason, "
        "l2_active, l2_direction, l2_step, l2_entry_pct, "
        "l2_avg_entry_price, l2_step1_time, l2_score_at_entry, "
        "l2_direction_changes_today, l2_last_reset_date, "
        "l4_active, l4_grid_config_id, macro_blocked, macro_block_reason, "
        "pending_signal, l2_trailing_stop_price) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(symbol) DO UPDATE SET "
        "state=excluded.state, l1_active=excluded.l1_active, "
        "l1_entry_reason=excluded.l1_entry_reason, "
        "l2_active=excluded.l2_active, l2_direction=excluded.l2_direction, "
        "l2_step=excluded.l2_step, l2_entry_pct=excluded.l2_entry_pct, "
        "l2_avg_entry_price=excluded.l2_avg_entry_price, "
        "l2_step1_time=excluded.l2_step1_time, "
        "l2_score_at_entry=excluded.l2_score_at_entry, "
        "l2_direction_changes_today=excluded.l2_direction_changes_today, "
        "l2_last_reset_date=excluded.l2_last_reset_date, "
        "l4_active=excluded.l4_active, "
        "l4_grid_config_id=excluded.l4_grid_config_id, "
        "macro_blocked=excluded.macro_blocked, "
        "macro_block_reason=excluded.macro_block_reason, "
        "pending_signal=excluded.pending_signal, "
        "l2_trailing_stop_price=excluded.l2_trailing_stop_price, "
        "updated_at=CURRENT_TIMESTAMP",
        vals,
    )
    conn.commit()
    conn.close()


if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8")
    from db import init_db
    init_db()
    run_strategy()
