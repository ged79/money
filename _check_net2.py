"""넷포지션 한도 — 실제 포지션 기준 확인"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
from db import get_connection
from config import GRID_V2_MAX_NET_LEVELS
from engines.binance_executor import BinanceExecutor

conn = get_connection()
ex = BinanceExecutor(use_testnet=False)

print("=" * 60)
print(f"  넷포지션 한도 확인 (MAX_NET_LEVELS = {GRID_V2_MAX_NET_LEVELS})")
print("=" * 60)

# 전체 레벨 표시
print(f"\n[전체 그리드 레벨]")
all_levels = conn.execute(
    "SELECT grid_price, status, direction, quantity FROM grid_positions "
    "WHERE symbol='SOLUSDT' ORDER BY grid_price"
).fetchall()

for r in all_levels:
    gp, st, d, q = r
    # 실제 포지션 여부 판단
    is_real = (st == "HOLDING"
               or (st == "SELL_OPEN" and d == "LONG")
               or (st == "BUY_OPEN" and d == "SHORT"))
    is_pending = (st == "BUY_OPEN" and d == "LONG") or (st == "SELL_OPEN" and d == "SHORT")
    
    if st == "EMPTY":
        tag = "(빈 슬롯)"
    elif is_real:
        tag = f"*** 실제 {d} 포지션"
    elif is_pending:
        tag = f"    {d} 진입 대기 (미체결)"
    else:
        tag = ""
    print(f"  ${gp:.2f} | {st:10s} {d or '':6s} qty={q:.1f} {tag}")

# 실제 포지션만 카운트 (코드와 동일 로직)
real_positions = conn.execute(
    "SELECT direction, COUNT(*) FROM grid_positions "
    "WHERE symbol = 'SOLUSDT' AND quantity > 0 "
    "AND (status = 'HOLDING' "
    "  OR (status = 'SELL_OPEN' AND direction = 'LONG') "
    "  OR (status = 'BUY_OPEN' AND direction = 'SHORT')) "
    "GROUP BY direction"
).fetchall()
counts = {d: c for d, c in real_positions}
long_count = counts.get("LONG", 0)
short_count = counts.get("SHORT", 0)
net_level = long_count - short_count

print(f"\n[넷포지션 (실제 포지션만)]")
print(f"  실제 LONG: {long_count}건 | 실제 SHORT: {short_count}건")
print(f"  넷포지션: {net_level:+d} (한도: +/-{GRID_V2_MAX_NET_LEVELS})")
print(f"  LONG 진입 차단: {'YES' if net_level >= GRID_V2_MAX_NET_LEVELS else 'NO'}")
print(f"  SHORT 진입 차단: {'YES' if net_level <= -GRID_V2_MAX_NET_LEVELS else 'NO'}")

# Binance 대조
positions = ex.get_positions()
binance_net = 0.0
for p in positions:
    if p["symbol"] == "SOLUSDT":
        binance_net = float(p.get("positionAmt", 0))
        break

# 실제 포지션 수량 합산
real_holdings = conn.execute(
    "SELECT direction, quantity FROM grid_positions "
    "WHERE symbol = 'SOLUSDT' AND quantity > 0 "
    "AND (status = 'HOLDING' "
    "  OR (status = 'SELL_OPEN' AND direction = 'LONG') "
    "  OR (status = 'BUY_OPEN' AND direction = 'SHORT'))"
).fetchall()
db_net_qty = sum(q for d, q in real_holdings if d == "LONG") - sum(q for d, q in real_holdings if d == "SHORT")

print(f"\n[DB vs Binance 대조 (실제 포지션만)]")
print(f"  DB 넷수량: {db_net_qty:+.1f}")
print(f"  Binance 넷수량: {binance_net:+.1f}")
print(f"  일치: {'YES' if abs(db_net_qty - binance_net) < 0.05 else 'NO (차이=' + f'{abs(db_net_qty - binance_net):.1f})'}")

conn.close()
