"""최근 1시간 거래 내역 + 수익 확인"""
import sys, os
from datetime import datetime, timezone, timedelta
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
from db import get_connection

conn = get_connection()

# 1시간 전 시각
now = datetime.now(timezone.utc)
one_hour_ago = (now - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")

print("=" * 65)
print(f"  최근 1시간 거래 내역 ({one_hour_ago} ~)")
print("=" * 65)

# FILLED 건만
filled = conn.execute(
    "SELECT id, symbol, side, direction, grid_price, quantity, limit_price, "
    "fill_price, fee, pnl_usd, created_at "
    "FROM grid_order_log WHERE status='FILLED' AND created_at >= ? "
    "ORDER BY id",
    (one_hour_ago,)
).fetchall()

total_pnl = 0.0
total_fee = 0.0
buy_count = 0
sell_count = 0

if filled:
    for r in filled:
        oid, sym, side, dirn, gp, qty, lp, fp, fee, pnl, created = r
        fee = fee or 0
        pnl = pnl or 0
        total_pnl += pnl
        total_fee += fee
        if side == "BUY":
            buy_count += 1
        else:
            sell_count += 1
        pnl_str = f"PnL=${pnl:+.4f}" if pnl != 0 else ""
        print(f"  #{oid} {created[11:19]} | {side:4s}({dirn:5s}) "
              f"@ ${gp:.2f} | fill=${fp:.2f} qty={qty} | fee=${fee:.4f} {pnl_str}")
    
    print(f"\n{'─'*65}")
    print(f"  체결: {len(filled)}건 (BUY {buy_count} + SELL {sell_count})")
    print(f"  총 PnL: ${total_pnl:+.4f}")
    print(f"  총 수수료: ${total_fee:.4f}")
    print(f"  순수익: ${total_pnl - total_fee:+.4f}")
else:
    print(f"  체결 없음")

# PLACED (미체결 대기)
placed = conn.execute(
    "SELECT COUNT(*) FROM grid_order_log WHERE status='PLACED' AND created_at >= ?",
    (one_hour_ago,)
).fetchone()[0]

# CANCELLED
cancelled = conn.execute(
    "SELECT COUNT(*) FROM grid_order_log WHERE status='CANCELLED' AND created_at >= ?",
    (one_hour_ago,)
).fetchone()[0]

# FAILED
failed = conn.execute(
    "SELECT COUNT(*) FROM grid_order_log WHERE status='FAILED' AND created_at >= ?",
    (one_hour_ago,)
).fetchone()[0]

print(f"\n  기타: PLACED={placed} CANCELLED={cancelled} FAILED={failed}")

# 일일 누적
print(f"\n{'─'*65}")
from datetime import date
today = date.today().isoformat()
daily = conn.execute(
    "SELECT COUNT(*), COALESCE(SUM(pnl_usd),0), COALESCE(SUM(fee),0) "
    "FROM grid_order_log WHERE status='FILLED' AND created_at >= ?",
    (today,)
).fetchone()
if daily:
    print(f"  오늘 누적: {daily[0]}건 체결 | PnL=${daily[1]:+.4f} | fee=${daily[2]:.4f}")

cb = conn.execute(
    "SELECT realized_pnl, unrealized_pnl FROM live_daily_pnl WHERE trade_date=?",
    (today,)
).fetchone()
if cb:
    print(f"  CB 기준: realized={cb[0]:+.2f}% unrealized={cb[1]:+.2f}%")

conn.close()
