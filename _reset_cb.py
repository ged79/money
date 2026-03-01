"""오늘자 Circuit Breaker 리셋 — 포지션 이미 정리됨"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
from db import get_connection
from datetime import date

conn = get_connection()
today = date.today().isoformat()

# CB 리셋 + starting_balance도 수정
conn.execute(
    "UPDATE live_daily_pnl SET circuit_breaker_hit = 0, "
    "realized_pnl = 0, unrealized_pnl = 0, total_orders = 0 "
    "WHERE trade_date = ?",
    (today,),
)
conn.commit()
print(f"[CB 리셋] {today} circuit_breaker_hit → 0, PnL 초기화")

# 확인
row = conn.execute(
    "SELECT trade_date, realized_pnl, unrealized_pnl, circuit_breaker_hit, starting_balance "
    "FROM live_daily_pnl WHERE trade_date = ?",
    (today,),
).fetchone()
print(f"  확인: date={row[0]}, realized={row[1]}, unrealized={row[2]}, "
      f"cb_hit={row[3]}, starting_balance={row[4]}")
conn.close()
