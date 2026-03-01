"""grid_order_log 정확한 컬럼별 값 확인"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
from db import get_connection

conn = get_connection()

print("최근 15건 (FILLED만):")
rows = conn.execute(
    "SELECT id, symbol, side, direction, grid_price, quantity, limit_price, "
    "fill_price, fee, pnl_usd, status, created_at, filled_at "
    "FROM grid_order_log WHERE status='FILLED' ORDER BY id DESC LIMIT 15"
).fetchall()
for r in rows:
    print(f"  id={r[0]} {r[1]} {r[2]}({r[3]}) | "
          f"grid=${r[4]:.2f} qty={r[5]} limit=${r[6]:.2f} fill=${r[7] or 0:.2f} | "
          f"fee=${r[8] or 0:.4f} pnl=${r[9] or 0:.4f} | {r[10]} | {r[11]}")

print("\n최근 15건 (PLACED만):")
rows2 = conn.execute(
    "SELECT id, symbol, side, direction, grid_price, quantity, limit_price, "
    "fill_price, status, created_at "
    "FROM grid_order_log WHERE status='PLACED' ORDER BY id DESC LIMIT 15"
).fetchall()
for r in rows2:
    print(f"  id={r[0]} {r[1]} {r[2]}({r[3]}) | "
          f"grid=${r[4]:.2f} qty={r[5]} limit=${r[6]:.2f} fill={r[7]} | {r[8]} | {r[9]}")

# fill_price NULL/0 통계 (FILLED만)
filled_null = conn.execute("SELECT COUNT(*) FROM grid_order_log WHERE status='FILLED' AND (fill_price IS NULL OR fill_price = 0)").fetchone()[0]
filled_ok = conn.execute("SELECT COUNT(*) FROM grid_order_log WHERE status='FILLED' AND fill_price > 0").fetchone()[0]
print(f"\nFILLED: fill_price 있음={filled_ok} | fill_price 없음={filled_null}")

conn.close()
