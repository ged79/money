"""시스템 점검 스크립트"""
import sqlite3, sys, os
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

conn = sqlite3.connect("data/trades.db")

print("=" * 60)
print("  SYSTEM HEALTH CHECK")
print("=" * 60)

# 1. Circuit Breaker
print("\n[1] CIRCUIT BREAKER")
rows = conn.execute("SELECT * FROM live_daily_pnl ORDER BY id DESC LIMIT 5").fetchall()
for r in rows:
    cb = "*** HIT ***" if r[5] else "OK"
    print(f"  date={r[1]} real_pnl={r[2]:.4f} unreal_pnl={r[3]:.4f} orders={r[4]} CB={cb}")

# 2. Grid Positions (방향 포함)
print("\n[2] GRID POSITIONS")
rows = conn.execute("SELECT symbol, grid_price, status, quantity, buy_fill_price, direction, entry_fill_price, updated_at FROM grid_positions ORDER BY grid_price").fetchall()
status_counts = {}
for r in rows:
    key = f"{r[2]}({r[5]})" if r[5] else r[2]
    status_counts[key] = status_counts.get(key, 0) + 1
    dir_str = f" dir={r[5]}" if r[5] else ""
    entry = r[6] or r[4] or 0
    print(f"  ${r[1]:.2f} | {r[2]:10s}{dir_str:8s} | qty={r[3]:.4f} | entry=${entry:.2f}")
print(f"  Summary: {status_counts}")

# 3. Filled Orders (방향 포함)
print("\n[3] RECENT FILLED ORDERS")
rows = conn.execute("SELECT symbol, side, grid_price, limit_price, fill_price, pnl_usd, created_at, direction FROM grid_order_log WHERE status='FILLED' ORDER BY id DESC LIMIT 10").fetchall()
if not rows:
    print("  No filled orders")
for r in rows:
    dir_str = f"({r[7]})" if r[7] else ""
    print(f"  {r[0]} {r[1]:4s}{dir_str:8s} grid=${r[2]:.2f} limit=${r[3]:.2f} fill=${r[4] or 0:.2f} pnl=${r[5]:.4f} {r[6]}")

# 4. Strategy State (latest per symbol)
print("\n[4] STRATEGY STATE (latest)")
cnt = conn.execute("SELECT COUNT(*) FROM strategy_state").fetchone()[0]
expected = "OK" if cnt <= 5 else f"WARNING: should be ~3, not {cnt}"
print(f"  Total rows: {cnt} ({expected})")
for sym in ["SOLUSDT", "BTCUSDT", "ETHUSDT"]:
    row = conn.execute("SELECT id, symbol, state, l4_active, l4_grid_config_id, l2_active, l2_direction, updated_at FROM strategy_state WHERE symbol=? ORDER BY id DESC LIMIT 1", (sym,)).fetchone()
    if row:
        print(f"  {row[1]} state={row[2]} l4={row[3]} grid_id={row[4]} l2={row[5]} dir={row[6]} updated={row[7]}")

# 5. Grid Config
print("\n[5] LATEST GRID CONFIG")
rows = conn.execute("SELECT symbol, lower_bound, upper_bound, grid_count, grid_spacing, grid_spacing_pct, calculated_at FROM grid_configs ORDER BY id DESC LIMIT 3").fetchall()
for r in rows:
    print(f"  {r[0]} ${r[1]:.2f}-${r[2]:.2f} | {r[3]} grids @ ${r[4]:.2f} ({r[5]:.4f}%) | {r[6]}")

# 6. DB Size
size = os.path.getsize("data/trades.db")
print(f"\n[6] DB SIZE: {size/1024/1024:.1f} MB")

# 7. Binance Live
print("\n[7] BINANCE LIVE STATE")
try:
    from engines.binance_executor import BinanceExecutor
    ex = BinanceExecutor()

    bal = ex.get_account_balance()
    print(f"  Balance: {bal}")

    orders = ex.get_open_orders("SOLUSDT")
    print(f"  Open orders: {len(orders)}")
    for o in orders:
        print(f"    {o.get('side')} {o.get('type')} qty={o.get('origQty')} price={o.get('price')}")

    positions = ex.get_positions()
    for p in positions:
        amt = float(p.get("positionAmt", 0))
        if amt != 0:
            print(f"  Position: {p.get('symbol')} amt={amt} entry={p.get('entryPrice')} pnl={p.get('unRealizedProfit')}")

    mp = ex.get_mark_price("SOLUSDT")
    print(f"  Mark Price: ${mp:.4f}")
except Exception as e:
    print(f"  ERROR: {e}")

conn.close()
print("\n" + "=" * 60)
