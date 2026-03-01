import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
from db import get_connection
from engines.binance_executor import BinanceExecutor
from config import LIVE_USE_TESTNET

conn = get_connection()
row = conn.execute(
    "SELECT id, lower_bound, upper_bound, grid_count, grid_spacing, grid_spacing_pct, calculated_at "
    "FROM grid_configs WHERE symbol='SOLUSDT' ORDER BY id DESC LIMIT 1"
).fetchone()

ex = BinanceExecutor(use_testnet=LIVE_USE_TESTNET)
mark = ex.get_mark_price("SOLUSDT")

print(f"=== 활성 그리드 (SOLUSDT) ===")
print(f"  Grid ID: {row[0]}")
print(f"  하한: ${row[1]:,.2f}")
print(f"  상한: ${row[2]:,.2f}")
print(f"  칸수: {row[3]}개")
print(f"  간격: ${row[4]:,.2f} ({row[5]:.4f}%)")
print(f"  설정시각: {row[6]}")
print(f"  현재가: ${mark:,.2f}")
print(f"  중심가: ${(row[1]+row[2])/2:,.2f}")

levels = [round(row[1] + i * row[4], 2) for i in range(row[3] + 1)]
print(f"\n  레벨:")
for i, lv in enumerate(levels):
    marker = " ← 현재가" if i < len(levels)-1 and levels[i] <= mark <= levels[i+1] else ""
    print(f"    [{i:2d}] ${lv:>8.2f}{marker}")

conn.close()
