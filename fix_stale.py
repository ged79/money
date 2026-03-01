"""Stale PLACED 로그 정리"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db import get_connection

conn = get_connection()

# 현재 활성 order_id 목록
active_oids = set()
rows = conn.execute(
    "SELECT buy_order_id, sell_order_id FROM grid_positions"
).fetchall()
for buy_oid, sell_oid in rows:
    if buy_oid:
        active_oids.add(buy_oid)
    if sell_oid:
        active_oids.add(sell_oid)

print(f"Active order IDs: {active_oids}")

# PLACED인데 활성이 아닌 로그 찾기
stale = conn.execute(
    "SELECT id, order_id, side, grid_price, status FROM grid_order_log WHERE status = 'PLACED'"
).fetchall()

cleaned = 0
for row_id, oid, side, gp, status in stale:
    if oid not in active_oids:
        conn.execute(
            "UPDATE grid_order_log SET status = 'CANCELLED' WHERE id = ?",
            (row_id,),
        )
        cleaned += 1
        print(f"  Cleaned: id={row_id} oid={oid} {side} @ ${gp}")

conn.commit()
conn.close()
print(f"\nTotal cleaned: {cleaned}")
