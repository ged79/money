"""Grid V2 클린 리스타트: grid_positions/order_log 초기화"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from db import init_db, get_connection

init_db()
conn = get_connection()
conn.execute("DELETE FROM grid_positions")
conn.execute("DELETE FROM grid_order_log")
conn.execute("DELETE FROM live_daily_pnl")
conn.commit()
conn.close()
print("Grid V2 테이블 초기화 완료")
