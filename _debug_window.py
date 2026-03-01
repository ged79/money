"""Working Window 주문 배치 안 되는 원인 진단"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from db import get_connection
from engines.binance_executor import BinanceExecutor
from config import GRID_V2_WORKING_LEVELS, GRID_V2_MAX_POSITION_PCT, LIVE_LEVERAGE

conn = get_connection()
ex = BinanceExecutor(use_testnet=False)

# 1. 그리드 설정
grid = conn.execute(
    "SELECT lower_bound, upper_bound, grid_count, grid_spacing FROM grid_configs "
    "WHERE symbol='SOLUSDT' ORDER BY id DESC LIMIT 1"
).fetchone()
print(f"[Grid Config] low={grid[0]}, high={grid[1]}, count={grid[2]}, spacing={grid[3]}")

levels = []
if grid:
    low, high, count = grid[0], grid[1], grid[2]
    spacing = grid[3]
    step = (high - low) / max(count - 1, 1) if count > 1 else 0
    levels = [round(low + i * step, 2) for i in range(count)]
    print(f"[Levels] {levels}")

# 2. 현재가 & 가장 가까운 레벨
mark = ex.get_mark_price("SOLUSDT")
print(f"\n[Mark Price] ${mark:.2f}")

closest_idx = 0
min_dist = float("inf")
for i, lv in enumerate(levels):
    dist = abs(lv - mark)
    if dist < min_dist:
        min_dist = dist
        closest_idx = i
print(f"[Closest] idx={closest_idx}, price=${levels[closest_idx]:.2f}")

# 3. 방향 편향 & window 범위
wl = GRID_V2_WORKING_LEVELS
# bias 계산 단순화
long_levels = max(1, wl - 1)  # BEARISH
short_levels = wl + 1
window_low = max(0, closest_idx - long_levels)
window_high = min(len(levels) - 1, closest_idx + short_levels)
print(f"[Window] [{window_low}..{window_high}] L={long_levels} S={short_levels}")

# 4. active_count vs max_positions
active_count = conn.execute(
    "SELECT COUNT(*) FROM grid_positions WHERE symbol = 'SOLUSDT' "
    "AND status NOT IN ('EMPTY')"
).fetchone()[0]
max_positions = (long_levels + short_levels) * 2
print(f"\n[Active] active={active_count}, max={max_positions} → {'BLOCKED' if active_count >= max_positions else 'OK'}")

# 5. balance
balance = ex.get_account_balance()
print(f"[Balance] ${balance:.4f} → {'BLOCKED (<=0)' if balance <= 0 else 'OK'}")

# 6. order_qty vs min_qty
if balance > 0:
    remaining_slots = max(1, max_positions - active_count)
    per_grid_usdt = (balance * GRID_V2_MAX_POSITION_PCT / remaining_slots) * LIVE_LEVERAGE
    order_qty = per_grid_usdt / mark

    # min_qty 확인
    info = ex._exchange_info.get("SOLUSDT", {})
    min_qty_val = 0.01  # default
    for f in info.get("filters", []):
        if f.get("filterType") == "LOT_SIZE":
            min_qty_val = float(f.get("minQty", 0.01))
            break

    print(f"[OrderQty] per_grid_usdt=${per_grid_usdt:.4f}, order_qty={order_qty:.4f}, min_qty={min_qty_val}")
    print(f"  → {'BLOCKED (qty < min)' if order_qty < min_qty_val else 'OK'}")

# 7. 각 레벨 상태 (window 내)
print(f"\n[Window Levels Detail]")
for i in range(window_low, window_high + 1):
    lv_price = round(levels[i], 2)
    row = conn.execute(
        "SELECT status, quantity, buy_order_id, sell_order_id, direction "
        "FROM grid_positions WHERE symbol = 'SOLUSDT' AND round(grid_price, 2) = ?",
        (lv_price,),
    ).fetchone()
    if row:
        print(f"  [{i}] ${lv_price:.2f} → status={row[0]}, qty={row[1]:.4f}, dir={row[4]}, buy_oid={row[2]}, sell_oid={row[3]}")
    else:
        print(f"  [{i}] ${lv_price:.2f} → NO ROW IN DB!")

conn.close()
