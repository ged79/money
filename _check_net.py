"""넷포지션 한도 작동 확인"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
from db import get_connection
from config import GRID_V2_MAX_NET_LEVELS

conn = get_connection()

print("=" * 60)
print(f"  넷포지션 한도 확인 (MAX_NET_LEVELS = {GRID_V2_MAX_NET_LEVELS})")
print("=" * 60)

# 1. 현재 보유 현황
print(f"\n[보유 현황]")
holdings = conn.execute(
    "SELECT grid_price, status, direction, quantity FROM grid_positions "
    "WHERE symbol='SOLUSDT' AND quantity > 0 "
    "AND status IN ('HOLDING', 'SELL_OPEN', 'BUY_OPEN') "
    "ORDER BY grid_price"
).fetchall()

long_count = 0
short_count = 0
for r in holdings:
    side = r[2] or "?"
    print(f"  ${r[0]:.2f} | {r[1]} {side} qty={r[3]:.1f}")
    if side == "LONG":
        long_count += 1
    elif side == "SHORT":
        short_count += 1

net_level = long_count - short_count
print(f"\n  LONG: {long_count}건 | SHORT: {short_count}건")
print(f"  넷포지션: {net_level:+d} (한도: +/-{GRID_V2_MAX_NET_LEVELS})")

block_long = net_level >= GRID_V2_MAX_NET_LEVELS
block_short = net_level <= -GRID_V2_MAX_NET_LEVELS
print(f"  LONG 진입 차단: {'YES' if block_long else 'NO'}")
print(f"  SHORT 진입 차단: {'YES' if block_short else 'NO'}")

# 2. 전체 레벨 상태
print(f"\n[전체 그리드 레벨]")
all_levels = conn.execute(
    "SELECT grid_price, status, direction, quantity FROM grid_positions "
    "WHERE symbol='SOLUSDT' ORDER BY grid_price"
).fetchall()
for r in all_levels:
    marker = ""
    if r[1] == "EMPTY":
        marker = "(빈 슬롯)"
    elif r[2] == "LONG":
        marker = "<<< LONG"
    elif r[2] == "SHORT":
        marker = ">>> SHORT"
    print(f"  ${r[0]:.2f} | {r[1]:10s} {r[2] or '':6s} qty={r[3]:.1f} {marker}")

# 3. Binance 실제 넷포지션 대조
from engines.binance_executor import BinanceExecutor
ex = BinanceExecutor(use_testnet=False)
positions = ex.get_positions()
binance_net = 0.0
for p in positions:
    if p["symbol"] == "SOLUSDT":
        binance_net = float(p.get("positionAmt", 0))
        break

db_net_qty = sum(r[3] for r in holdings if r[2] == "LONG") - sum(r[3] for r in holdings if r[2] == "SHORT")
print(f"\n[DB vs Binance 대조]")
print(f"  DB 넷수량: {db_net_qty:+.1f}")
print(f"  Binance 넷수량: {binance_net:+.1f}")
print(f"  일치: {'YES' if abs(db_net_qty - binance_net) < 0.05 else 'NO - 불일치!'}")

conn.close()
