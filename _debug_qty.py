import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from config import GRID_V2_MAX_POSITION_PCT, LIVE_LEVERAGE

balance = 59.14
mark = 79.83
min_qty = 0.1

for max_pos in [12, 10, 8, 6, 4, 3, 2]:
    remaining_slots = max(1, max_pos)
    per_grid_usdt = (balance * GRID_V2_MAX_POSITION_PCT / remaining_slots) * LIVE_LEVERAGE
    order_qty = per_grid_usdt / mark
    ok = "OK" if order_qty >= min_qty else "FAIL"
    print(f"  slots={max_pos:>2}: per_grid=${per_grid_usdt:.2f}, qty={order_qty:.4f} vs min=0.1 → {ok}")

print(f"\nGRID_V2_MAX_POSITION_PCT = {GRID_V2_MAX_POSITION_PCT}")
print(f"LIVE_LEVERAGE = {LIVE_LEVERAGE}")
print(f"\n필요 최소 잔고 (12 slots): ${0.1 * mark / LIVE_LEVERAGE / GRID_V2_MAX_POSITION_PCT * 12:.2f}")
