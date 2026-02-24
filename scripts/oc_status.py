"""OpenClaw용 전체 상태 조회 - JSON 출력 (멀티 심볼)"""
import sys, os, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from db import get_connection
from config import SYMBOLS


def get_symbol_status(conn, symbol):
    result = {"symbol": symbol}

    # 전략 상태
    state = conn.execute(
        "SELECT symbol, state, l1_active, l2_active, l2_direction, l2_step, "
        "l2_entry_pct, l2_avg_entry_price, l4_active, macro_blocked, "
        "macro_block_reason, updated_at "
        "FROM strategy_state WHERE symbol=? ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    if state:
        result["strategy"] = {
            "state": state[1],
            "l1_active": bool(state[2]),
            "l2_active": bool(state[3]), "l2_direction": state[4],
            "l2_step": state[5], "l2_entry_pct": state[6],
            "l2_avg_entry_price": state[7],
            "l4_active": bool(state[8]),
            "macro_blocked": bool(state[9]), "macro_reason": state[10],
            "updated_at": state[11],
        }

    # SSM+V+T 점수
    score = conn.execute(
        "SELECT trigger_active, momentum_score, sentiment_score, story_score, "
        "value_score, total_score, direction, calculated_at "
        "FROM ssm_scores WHERE symbol=? ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    if score:
        result["score"] = {
            "trigger": "ON" if score[0] else "OFF",
            "momentum": score[1], "sentiment": score[2],
            "story": score[3], "value": score[4],
            "total": score[5], "direction": score[6],
            "calculated_at": score[7],
        }

    # 그리드
    grid = conn.execute(
        "SELECT lower_bound, upper_bound, grid_count, grid_spacing, "
        "grid_spacing_pct, calculated_at "
        "FROM grid_configs WHERE symbol=? ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    if grid:
        result["grid"] = {
            "lower": grid[0], "upper": grid[1],
            "count": grid[2], "spacing_usd": grid[3],
            "spacing_pct": grid[4], "calculated_at": grid[5],
        }

    # ATR
    atr = conn.execute(
        "SELECT atr, atr_pct, stop_loss_pct, current_price, calculated_at "
        "FROM atr_values WHERE symbol=? ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    if atr:
        result["atr"] = {
            "atr_usd": atr[0], "atr_pct": atr[1],
            "stop_loss_pct": atr[2], "price": atr[3],
            "calculated_at": atr[4],
        }

    # 임계점
    thr = conn.execute(
        "SELECT trigger_active, liq_amount_1h, current_oi, threshold_value, "
        "direction, calculated_at "
        "FROM threshold_signals WHERE symbol=? ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    if thr:
        result["threshold"] = {
            "trigger": "ON" if thr[0] else "OFF",
            "liq_1h_usd": thr[1], "oi": thr[2],
            "value": thr[3], "direction": thr[4],
            "calculated_at": thr[5],
        }

    # 시장 데이터
    fr = conn.execute(
        "SELECT funding_rate FROM funding_rates WHERE symbol=? ORDER BY collected_at DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    ls = conn.execute(
        "SELECT long_account, short_account FROM long_short_ratios WHERE symbol=? ORDER BY collected_at DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    oi = conn.execute(
        "SELECT open_interest FROM oi_snapshots WHERE symbol=? ORDER BY collected_at DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    result["market"] = {
        "funding_rate": fr[0] if fr else None,
        "long_short": {"long": ls[0], "short": ls[1]} if ls else None,
        "open_interest": oi[0] if oi else None,
    }

    # 최근 시그널 3개
    signals = conn.execute(
        "SELECT signal_type, direction, ssm_score, created_at "
        "FROM signal_log WHERE symbol=? ORDER BY id DESC LIMIT 3",
        (symbol,),
    ).fetchall()
    result["recent_signals"] = [
        {"type": s[0], "direction": s[1], "score": s[2], "time": s[3]}
        for s in signals
    ]

    return result


def get_status():
    conn = get_connection()
    result = {"symbols": {}}

    # F&G (글로벌)
    fg = conn.execute("SELECT value, classification FROM fear_greed ORDER BY collected_at DESC LIMIT 1").fetchone()
    result["fear_greed"] = {"value": fg[0], "class": fg[1]} if fg else None

    # 각 심볼 상태
    for symbol in SYMBOLS:
        result["symbols"][symbol] = get_symbol_status(conn, symbol)

    conn.close()
    return result


if __name__ == "__main__":
    data = get_status()
    print(json.dumps(data, ensure_ascii=False, indent=2, default=str))
