"""OpenClaw용 페이퍼 트레이딩 성과 조회 - 텍스트 출력"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from db import get_connection
from config import SYMBOLS


def performance():
    conn = get_connection()
    lines = []

    lines.append("=== 페이퍼 트레이딩 성과 ===")
    lines.append("")

    grand_total = 0
    grand_wins = 0
    grand_pnl = 0

    for symbol in SYMBOLS:
        base = symbol.replace("USDT", "")

        # 전체 통계
        stats = conn.execute(
            "SELECT COUNT(*), "
            "SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN pnl_pct <= 0 THEN 1 ELSE 0 END), "
            "SUM(pnl_weighted), "
            "MAX(pnl_pct), MIN(pnl_pct), AVG(pnl_pct) "
            "FROM paper_trades WHERE symbol = ? AND status = 'CLOSED'",
            (symbol,),
        ).fetchone()

        total = stats[0] or 0
        wins = stats[1] or 0
        losses = stats[2] or 0
        sum_pnl = stats[3] or 0
        grand_total += total
        grand_wins += wins
        grand_pnl += sum_pnl

        win_rate = round(wins / total * 100, 1) if total > 0 else 0

        lines.append(f"[{base}] 거래 {total}건 | 승률 {win_rate}% ({wins}W/{losses}L) | 누적 PnL {sum_pnl:+.2f}%")
        if stats[4] is not None:
            lines.append(f"  최고 {stats[4]:+.2f}% | 최저 {stats[5]:+.2f}% | 평균 {stats[6]:+.2f}%")

        # 현재 OPEN 포지션
        open_pos = conn.execute(
            "SELECT direction, entry_price, entry_pct, l2_step, stop_loss, entry_time "
            "FROM paper_trades WHERE symbol = ? AND status = 'OPEN' "
            "ORDER BY id DESC LIMIT 1",
            (symbol,),
        ).fetchone()

        if open_pos:
            current = conn.execute(
                "SELECT close FROM klines WHERE symbol = ? AND interval = '1d' "
                "ORDER BY open_time DESC LIMIT 1",
                (symbol,),
            ).fetchone()
            current_price = current[0] if current else 0

            if current_price and open_pos[1]:
                if open_pos[0] == "LONG":
                    floating = (current_price - open_pos[1]) / open_pos[1] * 100
                else:
                    floating = (open_pos[1] - current_price) / open_pos[1] * 100
                floating_w = floating * open_pos[2]
                lines.append(f"  -> OPEN {open_pos[0]} step{open_pos[3]} | "
                             f"진입 ${open_pos[1]:,.2f} -> 현재 ${current_price:,.2f} | "
                             f"PnL {floating:+.2f}% (가중 {floating_w:+.2f}%) | "
                             f"SL ${open_pos[4]:,.2f}")
            else:
                lines.append(f"  -> OPEN {open_pos[0]} step{open_pos[3]} | 진입 ${open_pos[1]:,.2f}")
        else:
            lines.append(f"  -> 포지션 없음")

        lines.append("")

    # 전체 요약
    lines.append("--- 전체 ---")
    grand_wr = round(grand_wins / grand_total * 100, 1) if grand_total > 0 else 0
    lines.append(f"총 {grand_total}건 | 승률 {grand_wr}% | 누적 PnL {grand_pnl:+.2f}%")

    # 최근 청산 10건
    recent = conn.execute(
        "SELECT symbol, direction, entry_price, exit_price, pnl_pct, "
        "pnl_weighted, exit_reason, exit_time "
        "FROM paper_trades WHERE status = 'CLOSED' "
        "ORDER BY id DESC LIMIT 10",
    ).fetchall()

    if recent:
        lines.append("")
        lines.append("=== 최근 거래 ===")
        for r in recent:
            base = r[0].replace("USDT", "")
            result = "WIN" if r[4] > 0 else "LOSS"
            lines.append(f"  [{r[7]}] {base} {r[1]} | "
                         f"${r[2]:,.0f}->${r[3]:,.0f} | "
                         f"{result} {r[4]:+.2f}% | {r[6]}")

    conn.close()
    return "\n".join(lines)


if __name__ == "__main__":
    print(performance())
