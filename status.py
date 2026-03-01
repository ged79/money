"""현재 시스템 상태 조회 - python status.py"""
import sys
import os

# 현재 스크립트의 상위 디렉토리(money)를 sys.path에 추가하여 모듈 임포트 가능하게 함
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from db import get_connection
import config # config 모듈 전체를 임포트
from engines.binance_executor import BinanceExecutor # BinanceExecutor 임포트

def show_status():
    conn = get_connection()

    print("=" * 60)
    print("  Auto Trading System - Status")
    print("=" * 60)

    # 라이브 트레이딩 활성화 여부 출력
    print(f"[설정] 라이브 트레이딩 활성화: {config.LIVE_TRADING_ENABLED} (테스트넷: {config.LIVE_USE_TESTNET})")

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

    # 현재가 (from live_trader)
    def _get_current_price(conn, symbol: str) -> float | None:
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
        return row[0] if row else None

    # 라이브 트레이딩 활성 여부 (config에서)
    from config import LIVE_TRADING_ENABLED, LIVE_USE_TESTNET, LIVE_SYMBOLS

    # 라이브 포지션
    if LIVE_TRADING_ENABLED:
        from engines.binance_executor import BinanceExecutor
        try:
            ex = BinanceExecutor(use_testnet=LIVE_USE_TESTNET)
            open_positions = ex.get_positions()
            print(f"\n[현재 오픈 포지션] ({len(open_positions)}건)")
            if open_positions:
                for p in open_positions:
                    # entryPrice는 문자열일 수 있으니 float 변환
                    entry_price = float(p.get('entryPrice', 0))
                    position_amt = float(p.get('positionAmt', 0))
                    unrealized_pnl = float(p.get('unRealizedProfit', 0))
                    print(f"  종목: {p['symbol']}, 수량: {position_amt:.4f}, "
                          f"평균 진입가: ${entry_price:,.2f}, "
                          f"미실현 PnL: ${unrealized_pnl:,.2f}")
            else:
                print("  오픈 포지션이 없습니다.")
        except Exception as e:
            print(f"[오류] 라이브 포지션 조회 실패: {e}")

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
