"""매매 실행 상태 확인"""
import sys, os, time
from datetime import datetime, timezone
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
from db import get_connection
from engines.binance_executor import BinanceExecutor

conn = get_connection()
ex = BinanceExecutor(use_testnet=False)
now = time.time()

print("=" * 60)
print("  매매 실행 상태 확인")
print("=" * 60)

# 1. Binance 실시간
print(f"\n[Binance]")
total = ex.get_total_balance()
avail = ex.get_account_balance()
print(f"  잔고: ${total:.2f} (가용 ${avail:.2f})")

positions = ex.get_positions()
open_pos = [p for p in positions if float(p.get("positionAmt", 0)) != 0]
if open_pos:
    for p in open_pos:
        amt = float(p["positionAmt"])
        upnl = float(p.get("unRealizedProfit", 0))
        entry = float(p.get("entryPrice", 0))
        print(f"  포지션: {p['symbol']} {amt:+.4f} @ ${entry:.2f} | PnL ${upnl:+.2f}")
else:
    print(f"  포지션: 없음")

orders = ex.get_open_orders("SOLUSDT")
if orders:
    print(f"  미체결 주문: {len(orders)}건")
    for o in orders:
        print(f"    {o['side']} ${float(o['price']):.2f} qty={o['origQty']} | {o.get('type')} | {o.get('status')}")
else:
    print(f"  미체결 주문: 0건")

# 2. DB grid_positions
print(f"\n[DB grid_positions]")
stats = conn.execute(
    "SELECT status, COUNT(*), COALESCE(SUM(quantity),0) FROM grid_positions "
    "WHERE symbol='SOLUSDT' GROUP BY status ORDER BY status"
).fetchall()
for s in stats:
    print(f"  {s[0]}: {s[1]}건 (qty={s[2]:.1f})")

# 상세
non_empty = conn.execute(
    "SELECT grid_price, status, direction, quantity, buy_order_id, sell_order_id "
    "FROM grid_positions WHERE symbol='SOLUSDT' AND status != 'EMPTY' "
    "ORDER BY grid_price"
).fetchall()
if non_empty:
    print(f"  활성 레벨:")
    for r in non_empty:
        print(f"    ${r[0]:.2f} | {r[1]} {r[2] or ''} qty={r[3]:.1f} | buy_oid={r[4]} sell_oid={r[5]}")

# 3. 최근 주문 로그
print(f"\n[최근 주문 로그]")
recent = conn.execute(
    "SELECT id, symbol, side, direction, grid_price, quantity, limit_price, "
    "fill_price, status, created_at "
    "FROM grid_order_log ORDER BY id DESC LIMIT 10"
).fetchall()
if recent:
    for r in recent:
        fp = f"${r[7]:.2f}" if r[7] else "N/A"
        print(f"  #{r[0]} {r[9]} | {r[1]} {r[2]}({r[3]}) @ ${r[4]:.2f} "
              f"limit=${r[6]:.2f} fill={fp} | {r[8]}")
else:
    print(f"  주문 로그 없음")

# 4. CB 상태
from datetime import date
today = date.today().isoformat()
cb = conn.execute(
    "SELECT realized_pnl, unrealized_pnl, circuit_breaker_hit FROM live_daily_pnl WHERE trade_date=?",
    (today,)
).fetchone()
if cb:
    print(f"\n[CB] {'TRIGGERED' if cb[2] else 'OK'} | realized={cb[0]:+.2f}% unrealized={cb[1]:+.2f}%")
else:
    print(f"\n[CB] OK (기록 없음)")

# 5. 현재 그리드 범위 vs 현재가
mark = ex.get_mark_price("SOLUSDT")
grid = conn.execute(
    "SELECT lower_bound, upper_bound, grid_spacing FROM grid_configs "
    "WHERE symbol='SOLUSDT' ORDER BY id DESC LIMIT 1"
).fetchone()
if grid and mark:
    in_range = "IN" if grid[0] <= mark <= grid[1] else "OOB"
    print(f"\n[Grid] ${grid[0]:.2f}~${grid[1]:.2f} (spacing=${grid[2]:.2f}) | mark=${mark:.2f} [{in_range}]")

conn.close()
