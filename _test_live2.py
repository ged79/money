import sys, os, traceback
os.chdir(r'C:\Users\lungg\.openclaw\workspace\money')
sys.path.insert(0, '.')
os.environ['LIVE_TRADING_ENABLED'] = 'true'
os.environ['LIVE_USE_TESTNET'] = 'false'
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from db import get_connection
from engines.live_trader import _get_executor, _process_live_l4_grid, _balance_ok
import engines.live_trader as lt

# Force balance ok
lt._balance_ok = True
lt._initialized_symbols.add("SOLUSDT")

print("=== Testing _process_live_l4_grid ===")
try:
    _process_live_l4_grid("SOLUSDT")
except Exception as e:
    print(f"ERROR: {e}")
    traceback.print_exc()

# Check live_orders
conn = get_connection()
rows = conn.execute("SELECT * FROM live_orders ORDER BY id DESC LIMIT 5").fetchall()
print(f"\nlive_orders: {len(rows)} rows")
for r in rows:
    print(f"  {r}")
conn.close()
