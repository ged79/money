import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
from db import get_connection

conn = get_connection()
cols = [c[1] for c in conn.execute("PRAGMA table_info(live_daily_pnl)").fetchall()]
print(f"columns: {cols}")
rows = conn.execute("SELECT * FROM live_daily_pnl ORDER BY trade_date DESC").fetchall()
for r in rows:
    for c, v in zip(cols, r):
        print(f"  {c} = {v}")
    print()
conn.close()
