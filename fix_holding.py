"""HOLDING/SELL_OPEN 레벨에서 stale buy_order_id 클리어"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db import get_connection

conn = get_connection()

# SELL_OPEN이나 HOLDING 상태에서 buy_order_id가 남아있으면 클리어
r = conn.execute(
    "UPDATE grid_positions SET buy_order_id = NULL, buy_client_order_id = NULL "
    "WHERE status IN ('HOLDING', 'SELL_OPEN') AND buy_order_id IS NOT NULL"
)
print(f"Cleared stale buy_order_id from {r.rowcount} rows")

# grid_order_log에서 해당 BUY도 FILLED로 마킹 (아직 PLACED면)
r2 = conn.execute(
    "UPDATE grid_order_log SET status = 'CANCELLED' "
    "WHERE status = 'PLACED' AND order_id NOT IN "
    "(SELECT buy_order_id FROM grid_positions WHERE buy_order_id IS NOT NULL "
    "UNION SELECT sell_order_id FROM grid_positions WHERE sell_order_id IS NOT NULL)"
)
print(f"Cleaned remaining stale PLACED logs: {r2.rowcount}")

conn.commit()

# 최종 상태 확인
print("\n=== 최종 grid_positions ===")
for row in conn.execute("SELECT grid_price, status, buy_order_id, sell_order_id FROM grid_positions WHERE symbol='SOLUSDT' ORDER BY grid_price").fetchall():
    print(f"  ${row[0]:>8.2f} | {row[1]:10s} | buy={row[2]} | sell={row[3]}")

print("\n=== PLACED 남은 로그 ===")
for row in conn.execute("SELECT id, order_id, side, grid_price FROM grid_order_log WHERE status='PLACED'").fetchall():
    print(f"  id={row[0]} oid={row[1]} {row[2]} @ ${row[3]}")

conn.close()
