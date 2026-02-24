"""Engine 7: 페이퍼 트레이딩 - L1/L2/L4 통합 가상 포지션 관리

Phase 3 핵심: L1/L2 조율 로직
- L2 SHORT 진입 시 → L1 롱 스팟 축소/청산 (상쇄 방지)
- L2 LONG 진입 시 → L1 롱 스팟 유지 (같은 방향)
- L1 펀딩비 수익 별도 추적
- L4 그리드 매매 추적
"""
import json
from datetime import date, datetime

from db import get_connection
from config import SYMBOLS, L1_FUNDING_THRESHOLD


def run_paper_trader(symbol: str = None):
    """페이퍼 트레이더 메인 루프"""
    symbols = [symbol] if symbol else SYMBOLS
    for sym in symbols:
        _process_l1_funding(sym)
        _process_l2_signals(sym)
        _process_l4_grid(sym)


# ============================
# L1 펀딩비 수익 추적
# ============================

def _process_l1_funding(symbol: str):
    """L1 활성 중이면 펀딩비 수익 누적"""
    conn = get_connection()

    # L1 활성 여부 확인
    state = conn.execute(
        "SELECT l1_active, l2_active, l2_direction FROM strategy_state "
        "WHERE symbol = ? ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()

    if not state or not state[0]:  # l1_active = False
        conn.close()
        return

    l2_active = bool(state[1])
    l2_direction = state[2]

    # L1/L2 조율: L2 SHORT 활성 시 L1 롱 스팟 효과 감소
    l1_effective = 1.0
    if l2_active and l2_direction == "SHORT":
        l1_effective = 0.0  # L1 롱 스팟 실질 무효화 (숏이 상쇄)
        # 단, L1 숏 선물 레그의 펀딩비 수익은 계속 발생

    # 최신 펀딩비 조회
    fr_row = conn.execute(
        "SELECT funding_rate, collected_at FROM funding_rates "
        "WHERE symbol = ? ORDER BY collected_at DESC LIMIT 1",
        (symbol,),
    ).fetchone()

    if not fr_row:
        conn.close()
        return

    funding_rate = fr_row[0]
    today = date.today().isoformat()

    # 오늘 이미 기록했는지 확인 (펀딩비는 8시간마다이므로 하루 최대 3회)
    last_record = conn.execute(
        "SELECT id, collected_at FROM paper_l1_funding "
        "WHERE symbol = ? ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()

    # 같은 collected_at이면 스킵
    if last_record and last_record[1] == fr_row[1]:
        conn.close()
        return

    # 펀딩비 수익 계산
    # 숏 선물 포지션이 펀딩비 받는 구조 (양수 펀딩비 = 숏이 받음)
    funding_pnl = funding_rate * 100  # % 단위
    effective_pnl = funding_pnl * l1_effective if l1_effective > 0 else funding_pnl

    conn.execute(
        "INSERT INTO paper_l1_funding "
        "(symbol, funding_rate, funding_pnl_pct, l1_effective, l2_conflict, collected_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (symbol, funding_rate, round(effective_pnl, 6), l1_effective,
         1 if l2_active and l2_direction == "SHORT" else 0,
         fr_row[1]),
    )
    conn.commit()

    conflict_str = " [L2 SHORT 충돌 - 스팟 무효화]" if l1_effective == 0 else ""
    print(f"[Paper L1] {symbol}: 펀딩비 {funding_rate*100:.4f}% → "
          f"수익 {effective_pnl:+.4f}%{conflict_str}")
    conn.close()


# ============================
# L2 방향성 포지션 추적
# ============================

def _process_l2_signals(symbol: str):
    """L2 시그널 기반 가상 포지션 관리 + L1/L2 조율"""
    conn = get_connection()

    # 현재 OPEN 포지션 조회
    open_trade = conn.execute(
        "SELECT id, direction, entry_price, entry_pct, l2_step, stop_loss, last_signal_id "
        "FROM paper_trades WHERE symbol = ? AND status = 'OPEN' "
        "ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()

    # 마지막 처리한 signal_id
    if open_trade:
        last_id = open_trade[6]
    else:
        last_closed = conn.execute(
            "SELECT last_signal_id FROM paper_trades WHERE symbol = ? "
            "ORDER BY id DESC LIMIT 1",
            (symbol,),
        ).fetchone()
        last_id = last_closed[0] if last_closed else 0

    # 새 시그널 조회 (L2 관련만)
    new_signals = conn.execute(
        "SELECT id, signal_type, direction, details, ssm_score, created_at "
        "FROM signal_log WHERE symbol = ? AND id > ? "
        "AND signal_type IN ('L2_STEP1', 'L2_STEP2', 'L2_STEP3', 'L2_EXIT') "
        "ORDER BY id ASC",
        (symbol, last_id),
    ).fetchall()

    for sig in new_signals:
        sig_id, sig_type, direction, details_json, score, created_at = sig
        details = {}
        try:
            details = json.loads(details_json) if details_json else {}
        except Exception:
            pass

        if sig_type == "L2_STEP1" and not open_trade:
            entry_price = details.get("price", 0)
            stop_loss = details.get("stop_loss", 0)
            entry_pct = details.get("entry_pct", 0.30)

            # L1/L2 조율 기록
            l1_state = conn.execute(
                "SELECT l1_active FROM strategy_state "
                "WHERE symbol = ? ORDER BY id DESC LIMIT 1",
                (symbol,),
            ).fetchone()
            l1_was_active = bool(l1_state[0]) if l1_state else False

            conn.execute(
                "INSERT INTO paper_trades "
                "(symbol, direction, status, entry_price, entry_pct, l2_step, "
                "stop_loss, last_signal_id) "
                "VALUES (?, ?, 'OPEN', ?, ?, 1, ?, ?)",
                (symbol, direction, entry_price, entry_pct, stop_loss, sig_id),
            )
            conn.commit()

            l1_note = ""
            if l1_was_active and direction == "SHORT":
                l1_note = " [L1 롱스팟 무효화 → 숏 선물 펀딩비만 유지]"
            elif l1_was_active and direction == "LONG":
                l1_note = " [L1 롱스팟 유지 → 이중 롱 효과]"

            print(f"[Paper L2] {symbol}: OPEN {direction} @ ${entry_price:,.2f} "
                  f"(30%) SL=${stop_loss:,.2f}{l1_note}")

            open_trade = conn.execute(
                "SELECT id, direction, entry_price, entry_pct, l2_step, stop_loss, last_signal_id "
                "FROM paper_trades WHERE symbol = ? AND status = 'OPEN' "
                "ORDER BY id DESC LIMIT 1",
                (symbol,),
            ).fetchone()

        elif sig_type == "L2_STEP2" and open_trade:
            avg_price = details.get("avg_price", open_trade[2])
            entry_pct = details.get("entry_pct", 0.60)
            stop_loss = details.get("stop_loss", open_trade[5])

            conn.execute(
                "UPDATE paper_trades SET entry_price = ?, entry_pct = ?, "
                "l2_step = 2, stop_loss = ?, last_signal_id = ? WHERE id = ?",
                (avg_price, entry_pct, stop_loss, sig_id, open_trade[0]),
            )
            conn.commit()
            print(f"[Paper L2] {symbol}: STEP2 avg=${avg_price:,.2f} ({entry_pct*100:.0f}%)")

            open_trade = conn.execute(
                "SELECT id, direction, entry_price, entry_pct, l2_step, stop_loss, last_signal_id "
                "FROM paper_trades WHERE id = ?",
                (open_trade[0],),
            ).fetchone()

        elif sig_type == "L2_STEP3" and open_trade:
            avg_price = details.get("avg_price", open_trade[2])
            entry_pct = details.get("entry_pct", open_trade[3])
            stop_loss = details.get("stop_loss", open_trade[5])

            conn.execute(
                "UPDATE paper_trades SET entry_price = ?, entry_pct = ?, "
                "l2_step = 3, stop_loss = ?, last_signal_id = ? WHERE id = ?",
                (avg_price, entry_pct, stop_loss, sig_id, open_trade[0]),
            )
            conn.commit()
            print(f"[Paper L2] {symbol}: STEP3 avg=${avg_price:,.2f} ({entry_pct*100:.0f}%)")

            open_trade = conn.execute(
                "SELECT id, direction, entry_price, entry_pct, l2_step, stop_loss, last_signal_id "
                "FROM paper_trades WHERE id = ?",
                (open_trade[0],),
            ).fetchone()

        elif sig_type == "L2_EXIT" and open_trade:
            exit_reason = details.get("reason", "unknown")
            exit_price = _get_current_price(conn, symbol)

            if exit_price and open_trade[2]:
                entry_price = open_trade[2]
                direction = open_trade[1]
                entry_pct = open_trade[3]

                if direction == "LONG":
                    pnl_pct = (exit_price - entry_price) / entry_price * 100
                else:
                    pnl_pct = (entry_price - exit_price) / entry_price * 100

                pnl_weighted = pnl_pct * entry_pct

                conn.execute(
                    "UPDATE paper_trades SET status = 'CLOSED', exit_price = ?, "
                    "pnl_pct = ?, pnl_weighted = ?, exit_reason = ?, "
                    "exit_time = CURRENT_TIMESTAMP, last_signal_id = ? WHERE id = ?",
                    (exit_price, round(pnl_pct, 4), round(pnl_weighted, 4),
                     exit_reason, sig_id, open_trade[0]),
                )
                conn.commit()

                result = "WIN" if pnl_pct > 0 else "LOSS"
                print(f"[Paper L2] {symbol}: CLOSED {direction} @ ${exit_price:,.2f} "
                      f"| {result} {pnl_pct:+.2f}% (가중 {pnl_weighted:+.2f}%) "
                      f"| 사유: {exit_reason}")

                _update_summary(conn, symbol, pnl_pct)
            else:
                conn.execute(
                    "UPDATE paper_trades SET status = 'CLOSED', exit_price = 0, "
                    "pnl_pct = 0, pnl_weighted = 0, exit_reason = ?, "
                    "exit_time = CURRENT_TIMESTAMP, last_signal_id = ? WHERE id = ?",
                    (exit_reason, sig_id, open_trade[0]),
                )
                conn.commit()
                print(f"[Paper L2] {symbol}: CLOSED (가격 없음) | 사유: {exit_reason}")

            open_trade = None

    # Floating PnL 출력 (OPEN 포지션 있을 때)
    if open_trade:
        current = _get_current_price(conn, symbol)
        if current and open_trade[2]:
            entry_price = open_trade[2]
            direction = open_trade[1]
            if direction == "LONG":
                floating = (current - entry_price) / entry_price * 100
            else:
                floating = (entry_price - current) / entry_price * 100
            floating_w = floating * open_trade[3]

            print(f"[Paper L2] {symbol}: {direction} step{open_trade[4]} "
                  f"진입=${entry_price:,.2f} 현재=${current:,.2f} "
                  f"PnL={floating:+.2f}% (가중 {floating_w:+.2f}%)")

    conn.close()


# ============================
# L4 그리드 매매 추적
# ============================

def _process_l4_grid(symbol: str):
    """L4 그리드 가상 매매 — 그리드 레벨별 가상 체결 추적"""
    conn = get_connection()

    # L4 활성 여부
    state = conn.execute(
        "SELECT l4_active, l4_grid_config_id FROM strategy_state "
        "WHERE symbol = ? ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()

    if not state or not state[0]:  # l4_active = False
        conn.close()
        return

    grid_id = state[1]
    if not grid_id:
        conn.close()
        return

    # 그리드 설정 로드
    grid = conn.execute(
        "SELECT lower_bound, upper_bound, grid_count, grid_spacing "
        "FROM grid_configs WHERE id = ?",
        (grid_id,),
    ).fetchone()

    if not grid:
        conn.close()
        return

    lower, upper, count, spacing = grid
    current_price = _get_current_price(conn, symbol)

    if not current_price:
        conn.close()
        return

    # 그리드 레벨 생성
    levels = [round(lower + i * spacing, 2) for i in range(count + 1)]

    # 현재 가격이 어느 그리드 레벨 사이인지
    for i in range(len(levels) - 1):
        grid_low = levels[i]
        grid_high = levels[i + 1]

        if grid_low <= current_price <= grid_high:
            # 현재 가격이 이 구간 안에 있음
            # 이전 체크 시 다른 구간이었다면 매매 발생

            last_grid = conn.execute(
                "SELECT grid_level FROM paper_l4_grid "
                "WHERE symbol = ? ORDER BY id DESC LIMIT 1",
                (symbol,),
            ).fetchone()

            last_level = last_grid[0] if last_grid else -1

            if last_level != i:
                # 그리드 레벨 변경 = 매매 발생
                if last_level >= 0:
                    if i > last_level:
                        # 가격 상승 → 매도 (그리드 이익, 1/grid_count 가중)
                        grid_pnl = (grid_high - grid_low) / grid_low * 100 / count
                        side = "SELL"
                    else:
                        # 가격 하락 → 매수 (나중 매도 대기)
                        grid_pnl = 0  # 매수는 아직 이익 아님
                        side = "BUY"

                    conn.execute(
                        "INSERT INTO paper_l4_grid "
                        "(symbol, grid_level, grid_price, side, pnl_pct, grid_config_id) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (symbol, i, current_price, side, round(grid_pnl, 4), grid_id),
                    )
                    conn.commit()

                    if grid_pnl > 0:
                        print(f"[Paper L4] {symbol}: {side} @ ${current_price:,.2f} "
                              f"| grid#{i} PnL={grid_pnl:+.2f}%")
                else:
                    # 초기 레벨 기록
                    conn.execute(
                        "INSERT INTO paper_l4_grid "
                        "(symbol, grid_level, grid_price, side, pnl_pct, grid_config_id) "
                        "VALUES (?, ?, ?, 'INIT', 0, ?)",
                        (symbol, i, current_price, grid_id),
                    )
                    conn.commit()
            break

    conn.close()


# ============================
# 헬퍼 함수
# ============================

def _get_current_price(conn, symbol: str) -> float | None:
    """최신 종가 조회 (5분봉 우선, 일봉 폴백)"""
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


def _update_summary(conn, symbol: str, pnl_pct: float):
    """일별 요약 갱신"""
    today = date.today().isoformat()

    existing = conn.execute(
        "SELECT id, total_trades, wins, losses, total_pnl_pct, "
        "best_trade_pct, worst_trade_pct "
        "FROM paper_summary WHERE symbol = ? AND summary_date = ?",
        (symbol, today),
    ).fetchone()

    if existing:
        total = existing[1] + 1
        wins = existing[2] + (1 if pnl_pct > 0 else 0)
        losses = existing[3] + (1 if pnl_pct <= 0 else 0)
        total_pnl = existing[4] + pnl_pct
        best = max(existing[5] or pnl_pct, pnl_pct)
        worst = min(existing[6] or pnl_pct, pnl_pct)

        conn.execute(
            "UPDATE paper_summary SET total_trades = ?, wins = ?, losses = ?, "
            "total_pnl_pct = ?, best_trade_pct = ?, worst_trade_pct = ?, "
            "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (total, wins, losses, round(total_pnl, 4), best, worst, existing[0]),
        )
    else:
        conn.execute(
            "INSERT INTO paper_summary "
            "(symbol, summary_date, total_trades, wins, losses, "
            "total_pnl_pct, best_trade_pct, worst_trade_pct) "
            "VALUES (?, ?, 1, ?, ?, ?, ?, ?)",
            (symbol, today,
             1 if pnl_pct > 0 else 0,
             1 if pnl_pct <= 0 else 0,
             round(pnl_pct, 4), pnl_pct, pnl_pct),
        )

    conn.commit()


# ============================
# 성과 조회 (OpenClaw 연동용)
# ============================

def get_performance(symbol: str = None) -> dict:
    """전체 성과 조회 — L1 펀딩 + L2 방향성 + L4 그리드 통합"""
    conn = get_connection()
    result = {}

    symbols = [symbol] if symbol else SYMBOLS

    for sym in symbols:
        # L2 통계
        stats = conn.execute(
            "SELECT COUNT(*), "
            "SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN pnl_pct <= 0 THEN 1 ELSE 0 END), "
            "SUM(pnl_weighted), "
            "MAX(pnl_pct), MIN(pnl_pct), AVG(pnl_pct) "
            "FROM paper_trades WHERE symbol = ? AND status = 'CLOSED'",
            (sym,),
        ).fetchone()

        total = stats[0] or 0
        wins = stats[1] or 0
        losses = stats[2] or 0

        # L2 OPEN 포지션
        open_pos = conn.execute(
            "SELECT direction, entry_price, entry_pct, l2_step, stop_loss, entry_time "
            "FROM paper_trades WHERE symbol = ? AND status = 'OPEN' "
            "ORDER BY id DESC LIMIT 1",
            (sym,),
        ).fetchone()

        floating_pnl = None
        if open_pos:
            current = _get_current_price(conn, sym)
            if current and open_pos[1]:
                if open_pos[0] == "LONG":
                    floating_pnl = (current - open_pos[1]) / open_pos[1] * 100
                else:
                    floating_pnl = (open_pos[1] - current) / open_pos[1] * 100

        # L1 펀딩비 누적 수익
        l1_stats = conn.execute(
            "SELECT COUNT(*), SUM(funding_pnl_pct), "
            "SUM(CASE WHEN l2_conflict = 1 THEN 1 ELSE 0 END) "
            "FROM paper_l1_funding WHERE symbol = ?",
            (sym,),
        ).fetchone()

        l1_count = l1_stats[0] or 0
        l1_total_pnl = l1_stats[1] or 0
        l1_conflicts = l1_stats[2] or 0

        # L4 그리드 수익
        l4_stats = conn.execute(
            "SELECT COUNT(*), SUM(CASE WHEN pnl_pct > 0 THEN pnl_pct ELSE 0 END) "
            "FROM paper_l4_grid WHERE symbol = ? AND side = 'SELL'",
            (sym,),
        ).fetchone()

        l4_trades = l4_stats[0] or 0
        l4_total_pnl = l4_stats[1] or 0

        # 통합 수익
        combined_pnl = (stats[3] or 0) + l1_total_pnl + l4_total_pnl

        result[sym] = {
            "l2": {
                "total_trades": total,
                "wins": wins,
                "losses": losses,
                "win_rate": round(wins / total * 100, 1) if total > 0 else 0,
                "total_pnl_weighted": round(stats[3], 4) if stats[3] else 0,
                "best_trade": round(stats[4], 2) if stats[4] else None,
                "worst_trade": round(stats[5], 2) if stats[5] else None,
                "open_position": {
                    "direction": open_pos[0],
                    "entry_price": open_pos[1],
                    "entry_pct": open_pos[2],
                    "step": open_pos[3],
                    "stop_loss": open_pos[4],
                    "entry_time": open_pos[5],
                    "floating_pnl": round(floating_pnl, 2) if floating_pnl is not None else None,
                } if open_pos else None,
            },
            "l1": {
                "funding_collections": l1_count,
                "total_funding_pnl": round(l1_total_pnl, 4),
                "l2_conflicts": l1_conflicts,
            },
            "l4": {
                "grid_trades": l4_trades,
                "total_grid_pnl": round(l4_total_pnl, 4),
            },
            "combined_pnl": round(combined_pnl, 4),
        }

    conn.close()
    return result


if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8")
    from db import init_db
    init_db()
    run_paper_trader()

    print("\n=== 성과 리포트 ===")
    perf = get_performance()
    for sym, data in perf.items():
        print(f"\n{sym}:")
        print(f"  L1 펀딩: {data['l1']['funding_collections']}회 | "
              f"PnL={data['l1']['total_funding_pnl']:+.4f}% | "
              f"L2충돌={data['l1']['l2_conflicts']}회")
        print(f"  L2 방향: {data['l2']['total_trades']}건 | "
              f"승률={data['l2']['win_rate']}% | "
              f"PnL={data['l2']['total_pnl_weighted']:+.4f}%")
        print(f"  L4 그리드: {data['l4']['grid_trades']}건 | "
              f"PnL={data['l4']['total_grid_pnl']:+.4f}%")
        print(f"  → 통합 PnL: {data['combined_pnl']:+.4f}%")
