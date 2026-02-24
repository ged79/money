"""백테스트 리포트 — 성과 지표 계산 + 출력"""
import csv
import math
import sqlite3
from datetime import datetime
from pathlib import Path

from backtest.config_bt import BT_DB_PATH, BT_INITIAL_CAPITAL


def generate_report(symbols: list, start_ts: float, end_ts: float,
                    equity_data: dict = None, export_csv: bool = False) -> dict:
    """백테스트 리포트 생성 및 출력

    Args:
        symbols: 심볼 리스트
        start_ts: 시작 타임스탬프
        end_ts: 종료 타임스탬프
        equity_data: runner에서 수집한 equity 스냅샷 (선택)
        export_csv: CSV 내보내기 여부

    Returns:
        dict: 심볼별 성과 지표
    """
    conn = sqlite3.connect(str(BT_DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")

    start_date = datetime.fromtimestamp(start_ts).strftime("%Y-%m-%d")
    end_date = datetime.fromtimestamp(end_ts).strftime("%Y-%m-%d")
    days = int((end_ts - start_ts) / 86400)

    all_results = {}

    for symbol in symbols:
        result = _calc_symbol_metrics(conn, symbol)
        all_results[symbol] = result

    conn.close()

    # 리포트 출력
    _print_report(all_results, start_date, end_date, days, equity_data)

    # CSV 내보내기
    if export_csv:
        _export_csv(all_results, start_date, end_date)

    return all_results


def _calc_symbol_metrics(conn: sqlite3.Connection, symbol: str) -> dict:
    """심볼별 성과 지표 계산"""

    # ---- L2 Directional ----
    l2_trades = conn.execute(
        "SELECT direction, entry_price, exit_price, entry_pct, pnl_pct, pnl_weighted, "
        "exit_reason, entry_time, exit_time "
        "FROM paper_trades WHERE symbol = ? AND status = 'CLOSED' ORDER BY id ASC",
        (symbol,),
    ).fetchall()

    l2_total = len(l2_trades)
    l2_wins = sum(1 for t in l2_trades if (t[4] or 0) > 0)
    l2_losses = l2_total - l2_wins
    l2_win_rate = (l2_wins / l2_total * 100) if l2_total > 0 else 0

    l2_pnl_list = [t[5] or 0 for t in l2_trades]  # pnl_weighted
    l2_total_pnl = sum(l2_pnl_list)
    l2_best = max((t[4] or 0) for t in l2_trades) if l2_trades else 0
    l2_worst = min((t[4] or 0) for t in l2_trades) if l2_trades else 0

    # 평균 보유 시간
    holding_times = []
    for t in l2_trades:
        if t[7] and t[8]:
            try:
                entry_dt = datetime.fromisoformat(t[7])
                exit_dt = datetime.fromisoformat(t[8])
                holding_times.append((exit_dt - entry_dt).total_seconds() / 3600)
            except Exception:
                pass
    avg_holding_hours = (sum(holding_times) / len(holding_times)) if holding_times else 0

    # ---- L1 Funding ----
    l1_row = conn.execute(
        "SELECT COUNT(*), COALESCE(SUM(funding_pnl_pct), 0), "
        "COALESCE(SUM(CASE WHEN l2_conflict = 1 THEN 1 ELSE 0 END), 0) "
        "FROM paper_l1_funding WHERE symbol = ?",
        (symbol,),
    ).fetchone()

    l1_collections = l1_row[0]
    l1_total_pnl = l1_row[1]
    l1_conflicts = l1_row[2]

    # ---- L4 Grid ----
    l4_row = conn.execute(
        "SELECT COUNT(*), COALESCE(SUM(CASE WHEN pnl_pct > 0 THEN pnl_pct ELSE 0 END), 0) "
        "FROM paper_l4_grid WHERE symbol = ? AND side = 'SELL'",
        (symbol,),
    ).fetchone()

    l4_trades = l4_row[0]
    l4_total_pnl = l4_row[1]

    # ---- Combined ----
    combined_pnl = l2_total_pnl + l1_total_pnl + l4_total_pnl

    # ---- Daily Returns (Sharpe / Max DD 계산용) ----
    daily_returns = _calc_daily_returns(conn, symbol)
    sharpe = _calc_sharpe(daily_returns)
    max_dd = _calc_max_drawdown(daily_returns)

    # ---- Monthly Breakdown ----
    monthly = _calc_monthly_breakdown(conn, symbol)

    # ---- Signal 통계 ----
    signal_count = conn.execute(
        "SELECT COUNT(*) FROM signal_log WHERE symbol = ?",
        (symbol,),
    ).fetchone()[0]

    return {
        "l2": {
            "total_trades": l2_total,
            "wins": l2_wins,
            "losses": l2_losses,
            "win_rate": round(l2_win_rate, 1),
            "total_pnl": round(l2_total_pnl, 4),
            "best_trade": round(l2_best, 2),
            "worst_trade": round(l2_worst, 2),
            "avg_holding_hours": round(avg_holding_hours, 1),
        },
        "l1": {
            "collections": l1_collections,
            "total_pnl": round(l1_total_pnl, 4),
            "conflicts": l1_conflicts,
        },
        "l4": {
            "trades": l4_trades,
            "total_pnl": round(l4_total_pnl, 4),
        },
        "combined": {
            "total_pnl": round(combined_pnl, 4),
            "sharpe": round(sharpe, 2),
            "max_drawdown": round(max_dd, 2),
        },
        "monthly": monthly,
        "signal_count": signal_count,
    }


def _calc_daily_returns(conn: sqlite3.Connection, symbol: str) -> list[float]:
    """일별 수익률 계산 (paper_summary + L1 + L4)"""
    # paper_summary에서 일별 L2 PnL
    summaries = conn.execute(
        "SELECT summary_date, total_pnl_pct FROM paper_summary "
        "WHERE symbol = ? ORDER BY summary_date ASC",
        (symbol,),
    ).fetchall()

    daily_map = {}
    for date_str, pnl in summaries:
        daily_map[date_str] = pnl

    # L1 펀딩비 일별 합산
    l1_daily = conn.execute(
        "SELECT DATE(created_at), SUM(funding_pnl_pct) FROM paper_l1_funding "
        "WHERE symbol = ? GROUP BY DATE(created_at)",
        (symbol,),
    ).fetchall()

    for date_str, pnl in l1_daily:
        if date_str:
            daily_map[date_str] = daily_map.get(date_str, 0) + (pnl or 0)

    # L4 그리드 일별 합산
    l4_daily = conn.execute(
        "SELECT DATE(created_at), SUM(pnl_pct) FROM paper_l4_grid "
        "WHERE symbol = ? AND side = 'SELL' GROUP BY DATE(created_at)",
        (symbol,),
    ).fetchall()

    for date_str, pnl in l4_daily:
        if date_str:
            daily_map[date_str] = daily_map.get(date_str, 0) + (pnl or 0)

    if not daily_map:
        return []

    return [daily_map[d] for d in sorted(daily_map.keys())]


def _calc_sharpe(daily_returns: list[float], risk_free: float = 0.0) -> float:
    """Sharpe Ratio 계산 (연간화)"""
    if len(daily_returns) < 2:
        return 0.0

    avg = sum(daily_returns) / len(daily_returns)
    variance = sum((r - avg) ** 2 for r in daily_returns) / (len(daily_returns) - 1)
    std = math.sqrt(variance) if variance > 0 else 0

    if std == 0:
        return 0.0

    daily_sharpe = (avg - risk_free) / std
    return daily_sharpe * math.sqrt(365)  # 연간화 (crypto = 365일)


def _calc_max_drawdown(daily_returns: list[float]) -> float:
    """최대 낙폭 (%) 계산"""
    if not daily_returns:
        return 0.0

    cumulative = 0.0
    peak = 0.0
    max_dd = 0.0

    for ret in daily_returns:
        cumulative += ret
        if cumulative > peak:
            peak = cumulative
        dd = peak - cumulative
        if dd > max_dd:
            max_dd = dd

    return max_dd


def _calc_monthly_breakdown(conn: sqlite3.Connection, symbol: str) -> list[dict]:
    """월별 수익률 breakdown"""
    # L2 월별
    l2_monthly = conn.execute(
        "SELECT SUBSTR(summary_date, 1, 7) as month, SUM(total_pnl_pct) "
        "FROM paper_summary WHERE symbol = ? GROUP BY month ORDER BY month",
        (symbol,),
    ).fetchall()

    monthly_map = {}
    for month, pnl in l2_monthly:
        if month:
            monthly_map[month] = pnl or 0

    # L1 월별
    l1_monthly = conn.execute(
        "SELECT SUBSTR(DATE(created_at), 1, 7), SUM(funding_pnl_pct) "
        "FROM paper_l1_funding WHERE symbol = ? GROUP BY 1",
        (symbol,),
    ).fetchall()

    for month, pnl in l1_monthly:
        if month:
            monthly_map[month] = monthly_map.get(month, 0) + (pnl or 0)

    # L4 월별
    l4_monthly = conn.execute(
        "SELECT SUBSTR(DATE(created_at), 1, 7), SUM(pnl_pct) "
        "FROM paper_l4_grid WHERE symbol = ? AND side = 'SELL' GROUP BY 1",
        (symbol,),
    ).fetchall()

    for month, pnl in l4_monthly:
        if month:
            monthly_map[month] = monthly_map.get(month, 0) + (pnl or 0)

    return [{"month": m, "pnl": round(monthly_map[m], 4)}
            for m in sorted(monthly_map.keys())]


def _print_report(results: dict, start_date: str, end_date: str,
                  days: int, equity_data: dict = None):
    """콘솔 리포트 출력"""
    width = 52

    print(f"\n{'='*width}")
    print(f"  BACKTEST REPORT: {start_date} ~ {end_date}")
    print(f"{'='*width}")
    print(f"  Period: {days} days | Initial Capital: ${BT_INITIAL_CAPITAL:,}")

    for symbol, r in results.items():
        print(f"\n  Symbol: {symbol}")
        print(f"  {'-'*48}")

        # L2
        l2 = r["l2"]
        print(f"\n  --- L2 Directional ---")
        print(f"    Trades: {l2['total_trades']} | "
              f"Wins: {l2['wins']} | Losses: {l2['losses']}")
        print(f"    Win Rate: {l2['win_rate']}%")
        print(f"    Total PnL: {l2['total_pnl']:+.2f}%")
        if l2['total_trades'] > 0:
            print(f"    Best: {l2['best_trade']:+.2f}% | "
                  f"Worst: {l2['worst_trade']:+.2f}%")
            print(f"    Avg Holding: {l2['avg_holding_hours']:.1f}h")

        # L1
        l1 = r["l1"]
        print(f"\n  --- L1 Funding ---")
        print(f"    Collections: {l1['collections']}")
        print(f"    Total PnL: {l1['total_pnl']:+.4f}%")
        print(f"    L2 Conflicts: {l1['conflicts']}")

        # L4
        l4 = r["l4"]
        print(f"\n  --- L4 Grid ---")
        print(f"    Grid Trades: {l4['trades']}")
        print(f"    Total PnL: {l4['total_pnl']:+.4f}%")

        # Combined
        c = r["combined"]
        print(f"\n  --- Combined ---")
        print(f"    Total PnL: {c['total_pnl']:+.2f}%")
        print(f"    Sharpe: {c['sharpe']}")
        print(f"    Max Drawdown: -{c['max_drawdown']:.2f}%")

        # Monthly
        if r["monthly"]:
            print(f"\n  --- Monthly ---")
            for m in r["monthly"]:
                print(f"    {m['month']}: {m['pnl']:+.2f}%")

        print(f"\n    Signals generated: {r['signal_count']}")

    print(f"\n{'='*width}")


def _export_csv(results: dict, start_date: str, end_date: str):
    """CSV 파일 내보내기"""
    csv_path = BT_DB_PATH.parent / f"backtest_report_{start_date}_{end_date}.csv"

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Symbol", "L2 Trades", "L2 Win Rate", "L2 PnL",
            "L1 Collections", "L1 PnL",
            "L4 Trades", "L4 PnL",
            "Combined PnL", "Sharpe", "Max Drawdown",
        ])

        for symbol, r in results.items():
            writer.writerow([
                symbol,
                r["l2"]["total_trades"],
                f"{r['l2']['win_rate']}%",
                f"{r['l2']['total_pnl']:+.4f}%",
                r["l1"]["collections"],
                f"{r['l1']['total_pnl']:+.4f}%",
                r["l4"]["trades"],
                f"{r['l4']['total_pnl']:+.4f}%",
                f"{r['combined']['total_pnl']:+.4f}%",
                r["combined"]["sharpe"],
                f"-{r['combined']['max_drawdown']:.2f}%",
            ])

    print(f"\n[Report] CSV 내보내기: {csv_path}")
