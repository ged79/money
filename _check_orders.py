import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from db import get_connection
from engines.binance_executor import BinanceExecutor

conn = get_connection()

# DB 그리드 상태
print("=== DB 그리드 포지션 ===")
rows = conn.execute(
    "SELECT grid_price, status, direction, quantity, buy_order_id, sell_order_id "
    "FROM grid_positions WHERE symbol='SOLUSDT' ORDER BY grid_price"
).fetchall()
for r in rows:
    oid = r[4] or r[5] or ""
    print(f"  ${r[0]:>7.2f} | {r[1]:>10} | {r[2] or '':>6} | qty={r[3]:.2f} | oid={oid}")

# 바이낸스 오픈 주문
print("\n=== 바이낸스 오픈 주문 ===")
ex = BinanceExecutor(use_testnet=False)
orders = ex.get_open_orders("SOLUSDT")
print(f"총 {len(orders)}개")
for o in orders:
    print(f"  {o['side']:>4} @ ${float(o['price']):>8.2f} x {o['origQty']} ({o['type']}) id={o['orderId']}")

# 현재가
mark = ex.get_mark_price("SOLUSDT")
print(f"\n현재가: ${mark:.2f}" if mark else "\n현재가 조회 실패")

conn.close()
