"""CB 리셋 + grid_positions 초기화"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
from db import get_connection
from datetime import date

conn = get_connection()
today = date.today().isoformat()

# 1. CB 리셋
cb = conn.execute("SELECT circuit_breaker_hit FROM live_daily_pnl WHERE trade_date=?", (today,)).fetchone()
print(f"CB before: {cb}")
conn.execute("UPDATE live_daily_pnl SET circuit_breaker_hit=0, unrealized_pnl=0 WHERE trade_date=?", (today,))
conn.commit()
cb2 = conn.execute("SELECT circuit_breaker_hit, unrealized_pnl FROM live_daily_pnl WHERE trade_date=?", (today,)).fetchone()
print(f"CB after: {cb2}")

# 2. grid_positions 초기화
before = conn.execute(
    "SELECT status, COUNT(*) FROM grid_positions WHERE symbol='SOLUSDT' GROUP BY status"
).fetchall()
print(f"\ngrid_positions before: {before}")

conn.execute(
    "UPDATE grid_positions SET status='EMPTY', direction=NULL, quantity=0, "
    "buy_fill_price=NULL, entry_fill_price=NULL, "
    "buy_order_id=NULL, sell_order_id=NULL, "
    "buy_client_order_id=NULL, sell_client_order_id=NULL "
    "WHERE symbol='SOLUSDT'"
)
conn.commit()

after = conn.execute(
    "SELECT status, COUNT(*) FROM grid_positions WHERE symbol='SOLUSDT' GROUP BY status"
).fetchall()
print(f"grid_positions after: {after}")

conn.close()
print("\nDone!")
