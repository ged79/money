import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
from db import get_connection

conn = get_connection()

# strategy_state
print("=== strategy_state ===")
cols = [c[1] for c in conn.execute("PRAGMA table_info(strategy_state)").fetchall()]
print(f"  columns: {cols}")
rows = conn.execute("SELECT * FROM strategy_state").fetchall()
for r in rows:
    for c, v in zip(cols, r):
        print(f"    {c} = {v}")
    print()

# grid_positions 존재 여부
count = conn.execute("SELECT COUNT(*) FROM grid_positions").fetchone()[0]
print(f"=== grid_positions: {count}건 ===")

# _grid_db_initialized 확인
print(f"\n=== _balance_ok check ===")
from engines.live_trader import _balance_ok
print(f"  _balance_ok = {_balance_ok}")

conn.close()
