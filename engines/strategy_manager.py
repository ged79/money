"""Engine 5: 전략 매니저 - L1/L2/L4 상태머신 + 시그널 생성"""
import json
import time
from datetime import date, datetime

from db import get_connection
from config import (
    SYMBOLS,
    L1_FUNDING_THRESHOLD, L1_LS_RATIO_THRESHOLD, L1_FUNDING_EXIT,
    L2_MAX_DIRECTION_CHANGES, L2_STEP1_PCT, L2_STEP2_PCT, L2_STEP3_PCT,
    L2_STEP2_DELAY, L2_STEP3_DELAY,
    BOX_PRICE_TOLERANCE, BOX_DURATION_MIN, OI_RECOVERY_THRESHOLD,
)
from engines.atr import get_latest_atr
from engines.dynamic_threshold import get_latest_threshold
from engines.grid_range import get_latest_grid
from engines.scorer import get_latest_score
from engines.macro_guard import check_macro_block


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

    # 엔진 출력 로드
    atr = get_latest_atr(symbol)
    threshold = get_latest_threshold(symbol)
    grid = get_latest_grid(symbol)
    score = get_latest_score(symbol)
    macro = check_macro_block(symbol)

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

        # 그리드 이탈 감지 → State B 전환
        # 활성 그리드 기준으로 breakout 판정 (최신 그리드가 아닌 L4 설정 시점의 그리드)
        active_grid = grid
        if state["l4_active"] and state["l4_grid_config_id"]:
            active_grid = _get_grid_by_id(state["l4_grid_config_id"]) or grid
        if active_grid and not macro["blocked"]:
            breakout = _detect_breakout(symbol, active_grid)
            if breakout["detected"]:
                if state["l2_direction_changes_today"] < L2_MAX_DIRECTION_CHANGES:
                    state["state"] = "B"
                    state["l2_active"] = True
                    state["l2_step"] = 1
                    state["l2_entry_pct"] = L2_STEP1_PCT
                    state["l2_direction"] = breakout["direction"]
                    state["l2_step1_time"] = datetime.now().isoformat()
                    state["l2_avg_entry_price"] = breakout["price"]
                    state["l4_active"] = False
                    signals.append(_emit_signal(
                        symbol, "L2_STEP1", breakout["direction"],
                        {"entry_pct": L2_STEP1_PCT, "price": breakout["price"],
                         "stop_loss": _calc_stop_loss(breakout["price"], atr, breakout["direction"])},
                        score=score["total_score"] if score else None,
                    ))
                    signals.append(_emit_signal(symbol, "L4_PAUSE", "NEUTRAL", {}))
                else:
                    print(f"[Strategy] {symbol}: 방향 전환 한도 도달 ({L2_MAX_DIRECTION_CHANGES}회/일)")

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


def _progress_l2(symbol: str, state: dict, atr: dict, score: dict, grid: dict, signals: list):
    """L2 단계 진행 관리"""
    if not state["l2_step1_time"]:
        return

    step1_time = datetime.fromisoformat(state["l2_step1_time"])
    elapsed = (datetime.now() - step1_time).total_seconds()

    if state["l2_step"] == 1:
        # 15분 경과 확인
        if elapsed >= L2_STEP2_DELAY:
            # 방향 유지 확인
            breakout = _detect_breakout(symbol, grid) if grid else {"detected": False}
            price_maintained = _check_price_direction(symbol, state["l2_direction"])

            if price_maintained:
                state["l2_step"] = 2
                state["l2_entry_pct"] = L2_STEP1_PCT + L2_STEP2_PCT  # 60%

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
                     "stop_loss": stop},
                    score=score["total_score"] if score else None,
                ))
            else:
                # 방향 되돌림 → 30% 손절, State A 복귀
                _exit_l2(state, symbol, "price_reversal_step1", signals)

    elif state["l2_step"] == 2:
        # 30분(step1 기준) 경과 + SSM 점수 확인
        if elapsed >= L2_STEP3_DELAY:
            total = score["total_score"] if score else 0

            if total >= 2.0:
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
                print(f"[Strategy] {symbol}: L2 Step3 점수 부족 ({total:.2f} < 2.0) - 60% 유지")

    elif state["l2_step"] == 3:
        # 청산 조건 모니터링
        if _check_stop_loss_hit(symbol, state, atr):
            _exit_l2(state, symbol, "stop_loss", signals)
        elif _check_box_formation(symbol):
            _exit_l2(state, symbol, "new_box_formation", signals)


def _exit_l2(state: dict, symbol: str, reason: str, signals: list):
    """L2 청산 → State A 복귀"""
    signals.append(_emit_signal(symbol, "L2_EXIT", state["l2_direction"],
                                {"reason": reason, "entry_pct": state["l2_entry_pct"]}))
    state["state"] = "A"
    state["l2_active"] = False
    state["l2_step"] = 0
    state["l2_entry_pct"] = 0
    state["l2_direction"] = None
    state["l2_avg_entry_price"] = None
    state["l2_step1_time"] = None
    state["l2_score_at_entry"] = None
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
        "l4_active, l4_grid_config_id, macro_blocked, macro_block_reason "
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
            "pending_signal": None,
        }

    # 초기 상태
    return {
        "state": "A",
        "l1_active": False, "l1_entry_reason": None,
        "l2_active": False, "l2_direction": None, "l2_step": 0,
        "l2_entry_pct": 0, "l2_avg_entry_price": None,
        "l2_step1_time": None, "l2_score_at_entry": None,
        "l2_direction_changes_today": 0, "l2_last_reset_date": date.today().isoformat(),
        "l4_active": False, "l4_grid_config_id": None,
        "macro_blocked": False, "macro_block_reason": None,
        "pending_signal": None,
    }


def _save_state(symbol: str, state: dict):
    """strategy_state 테이블에 상태 저장 (INSERT)"""
    conn = get_connection()
    conn.execute(
        "INSERT INTO strategy_state "
        "(symbol, state, l1_active, l1_entry_reason, "
        "l2_active, l2_direction, l2_step, l2_entry_pct, "
        "l2_avg_entry_price, l2_step1_time, l2_score_at_entry, "
        "l2_direction_changes_today, l2_last_reset_date, "
        "l4_active, l4_grid_config_id, macro_blocked, macro_block_reason, "
        "pending_signal) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (symbol, state["state"],
         1 if state["l1_active"] else 0, state["l1_entry_reason"],
         1 if state["l2_active"] else 0, state["l2_direction"],
         state["l2_step"], state["l2_entry_pct"],
         state["l2_avg_entry_price"], state["l2_step1_time"],
         state["l2_score_at_entry"],
         state["l2_direction_changes_today"], state["l2_last_reset_date"],
         1 if state["l4_active"] else 0, state["l4_grid_config_id"],
         1 if state["macro_blocked"] else 0, state["macro_block_reason"],
         state["pending_signal"]),
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
