"""현재 시스템 상태 조회 - python status.py"""
import sys
import os
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from db import get_connection


def show_status():
    conn = get_connection()

    print("=" * 60)
    print("  Auto Trading System - Status")
    print("=" * 60)

    # 전략 상태
    state = conn.execute(
        "SELECT symbol, state, l1_active, l2_active, l2_direction, l2_step, "
        "l2_entry_pct, l2_avg_entry_price, l4_active, macro_blocked, updated_at "
        "FROM strategy_state ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if state:
        print(f"\n[전략 상태] {state[0]}")
        print(f"  State: {state[1]} | L1={'ON' if state[2] else 'OFF'} | "
              f"L2={'ON' if state[3] else 'OFF'} (step {state[5]}, {state[4] or '-'}) | "
              f"L4={'ON' if state[8] else 'OFF'}")
        if state[3]:  # L2 active
            print(f"  L2 진입: {state[6]*100:.0f}% @ ${state[7]:,.0f}" if state[7] else "")
        print(f"  매크로: {'BLOCKED' if state[9] else 'OK'}")
        print(f"  갱신: {state[10]}")

    # 최신 점수
    score = conn.execute(
        "SELECT trigger_active, momentum_score, sentiment_score, story_score, "
        "value_score, total_score, direction, calculated_at "
        "FROM ssm_scores ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if score:
        print(f"\n[SSM+V+T 점수]")
        print(f"  T={'ON' if score[0] else 'OFF'} | M={score[1]:.1f} | "
              f"Ss={score[2]:.1f} | Story={score[3]:.1f} | V={score[4]:.1f}")
        print(f"  합계: {score[5]:.2f}/5.0 -> {score[6]}")

    # 최신 그리드
    grid = conn.execute(
        "SELECT lower_bound, upper_bound, grid_count, grid_spacing_pct, calculated_at "
        "FROM grid_configs ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if grid:
        print(f"\n[그리드 범위]")
        print(f"  ${grid[0]:,.0f} - ${grid[1]:,.0f} | {grid[2]} grids ({grid[3]:.2f}%)")

    # ATR
    atr = conn.execute(
        "SELECT atr, atr_pct, stop_loss_pct, current_price "
        "FROM atr_values ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if atr:
        print(f"\n[ATR] ${atr[0]:,.0f} ({atr[1]:.2f}%) -> 스톱로스 {atr[2]:.2f}% | 현재가 ${atr[3]:,.0f}")

    # 임계점
    thr = conn.execute(
        "SELECT trigger_active, liq_amount_1h, threshold_value, direction "
        "FROM threshold_signals ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if thr:
        print(f"\n[Threshold] trigger={'ON' if thr[0] else 'OFF'} | "
              f"1h 청산 ${thr[1]:,.0f} | 임계점 {thr[2]:.6f}")

    # 최근 시그널
    signals = conn.execute(
        "SELECT signal_type, direction, ssm_score, created_at "
        "FROM signal_log ORDER BY id DESC LIMIT 10"
    ).fetchall()
    if signals:
        print(f"\n[최근 시그널] ({len(signals)}건)")
        for s in signals:
            score_str = f" score={s[2]:.2f}" if s[2] is not None else ""
            print(f"  {s[3]} | {s[0]} | {s[1]}{score_str}")

    # DB 통계
    print(f"\n[DB 통계]")
    for table in ['liquidations', 'oi_snapshots', 'funding_rates', 'long_short_ratios',
                   'orderbook_walls', 'klines', 'fear_greed',
                   'atr_values', 'threshold_signals', 'grid_configs', 'ssm_scores',
                   'strategy_state', 'signal_log']:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        if count > 0:
            print(f"  {table}: {count}건")

    conn.close()
    print()


if __name__ == "__main__":
    show_status()
