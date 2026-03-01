import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db import get_connection
conn = get_connection()
print("grid_configs columns:", [col[1] for col in conn.execute("PRAGMA table_info(grid_configs)").fetchall()])
print("\nLatest row:")
row = conn.execute("SELECT * FROM grid_configs WHERE symbol='SOLUSDT' ORDER BY id DESC LIMIT 1").fetchone()
if row:
    cols = [col[1] for col in conn.execute("PRAGMA table_info(grid_configs)").fetchall()]
    for c, v in zip(cols, row):
        print(f"  {c} = {v}")
conn.close()
