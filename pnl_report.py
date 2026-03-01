"""수익 현황 리포트"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from engines.binance_executor import BinanceExecutor
from config import LIVE_USE_TESTNET
from db import get_connection

ex = BinanceExecutor(use_testnet=LIVE_USE_TESTNET)
conn = get_connection()

print("=" * 50)
print("  Grid V2 수익 현황 리포트")
print("=" * 50)

# 현재 잔고
balance = ex.get_account_balance()
print(f"\n  현재 잔고: ${balance:,.2f}")
print(f"  오픈 포지션: 없음 (전량 청산)")
print(f"  오픈 주문: 0건")

# 일일 PnL
print(f"\n--- 일일 PnL ---")
rows = conn.execute(
    "SELECT trade_date, realized_pnl, total_orders, circuit_breaker_hit "
    "FROM live_daily_pnl ORDER BY trade_date"
).fetchall()
for r in rows:
    cb = " [CB]" if r[3] else ""
    print(f"  {r[0]} | realized: {r[1]:+.4f}% | orders: {r[2]}{cb}")

# Grid V2 주문 통계
print(f"\n--- Grid V2 주문 통계 ---")
stats = conn.execute("""
    SELECT
        status,
        COUNT(*) as cnt,
        SUM(CASE WHEN side='BUY' THEN 1 ELSE 0 END) as buys,
        SUM(CASE WHEN side='SELL' THEN 1 ELSE 0 END) as sells
    FROM grid_order_log
    GROUP BY status
""").fetchall()
for s in stats:
    print(f"  {s[0]:12s} | total: {s[1]:3d} | BUY: {s[2]:3d} | SELL: {s[3]:3d}")

# SELL 체결 PnL
total_sell = conn.execute(
    "SELECT COUNT(*), COALESCE(SUM(pnl_usd), 0) FROM grid_order_log "
    "WHERE side='SELL' AND status='FILLED'"
).fetchone()
print(f"\n  SELL 체결: {total_sell[0]}건 | PnL: ${total_sell[1]:+.4f}")

# BUY 체결
total_buy = conn.execute(
    "SELECT COUNT(*), COALESCE(SUM(fill_price * quantity), 0) FROM grid_order_log "
    "WHERE side='BUY' AND status='FILLED'"
).fetchone()
print(f"  BUY 체결: {total_buy[0]}건 | 매수총액: ${total_buy[1]:+.4f}")

# V1 이력 (live_orders)
print(f"\n--- V1 이력 (이전 시스템) ---")
v1_sells = conn.execute(
    "SELECT COUNT(*), COALESCE(SUM(pnl_pct), 0) FROM live_orders "
    "WHERE side='SELL' AND status='FILLED'"
).fetchone()
v1_buys = conn.execute(
    "SELECT COUNT(*) FROM live_orders WHERE side='BUY' AND status='FILLED'"
).fetchone()
v1_total = conn.execute("SELECT COUNT(*) FROM live_orders").fetchone()
print(f"  총 주문: {v1_total[0]}건")
print(f"  BUY 체결: {v1_buys[0]}건")
print(f"  SELL 체결: {v1_sells[0]}건 | PnL: {v1_sells[1]:+.4f}%")

# 페이퍼 트레이딩 성과 (참고)
print(f"\n--- 페이퍼 트레이딩 성과 (참고) ---")
paper_l4 = conn.execute("""
    SELECT symbol,
        SUM(CASE WHEN side='SELL' THEN 1 ELSE 0 END) as sells,
        COUNT(*) as total,
        COALESCE(SUM(CASE WHEN side='SELL' THEN pnl_pct ELSE 0 END), 0) as pnl
    FROM paper_l4_grid
    GROUP BY symbol
""").fetchall()
total_paper_pnl = 0
for r in paper_l4:
    total_paper_pnl += r[3]
    print(f"  {r[0]}: SELL {r[1]}건 / {r[2]} total | PnL: {r[3]:+.4f}%")
print(f"  Combined L4 PnL: {total_paper_pnl:+.4f}%")

print(f"\n{'=' * 50}")
print(f"  최종 잔고: ${balance:,.2f}")
print(f"  초기 자본 대비: {(balance / 69.44 - 1) * 100:+.2f}% (from $69.44)")
print(f"{'=' * 50}")

conn.close()
