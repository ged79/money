"""grid_order_log 가격 데이터 확인"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
from db import get_connection

conn = get_connection()

# 스키마 확인
cols = [c[1] + "(" + c[2] + ")" for c in conn.execute("PRAGMA table_info(grid_order_log)").fetchall()]
print(f"grid_order_log schema: {cols}\n")

# 최근 10건 상세
print("최근 10건:")
rows = conn.execute(
    "SELECT id, symbol, side, grid_price, quantity, limit_price, fill_price, status, created_at "
    "FROM grid_order_log ORDER BY id DESC LIMIT 10"
).fetchall()
for r in rows:
    print(f"  id={r[0]} | {r[1]} {r[2]} | grid={r[3]} limit={r[4]} fill={r[5]} | qty={r[6]} | {r[7]} | {r[8]}")

# grid_price가 0인 건수 vs 0이 아닌 건수
zero = conn.execute("SELECT COUNT(*) FROM grid_order_log WHERE grid_price = 0 OR grid_price IS NULL").fetchone()[0]
nonzero = conn.execute("SELECT COUNT(*) FROM grid_order_log WHERE grid_price > 0").fetchone()[0]
print(f"\ngrid_price=0: {zero}건 | grid_price>0: {nonzero}건")

# limit_price가 0인 건수
zero_lp = conn.execute("SELECT COUNT(*) FROM grid_order_log WHERE limit_price = 0 OR limit_price IS NULL").fetchone()[0]
nonzero_lp = conn.execute("SELECT COUNT(*) FROM grid_order_log WHERE limit_price > 0").fetchone()[0]
print(f"limit_price=0: {zero_lp}건 | limit_price>0: {nonzero_lp}건")

# fill_price가 0인 건수
zero_fp = conn.execute("SELECT COUNT(*) FROM grid_order_log WHERE fill_price = 0 OR fill_price IS NULL").fetchone()[0]
nonzero_fp = conn.execute("SELECT COUNT(*) FROM grid_order_log WHERE fill_price > 0").fetchone()[0]
print(f"fill_price=0: {zero_fp}건 | fill_price>0: {nonzero_fp}건")

# grid_price > 0 인 PLACED 건 샘플
print("\ngrid_price > 0 샘플:")
rows2 = conn.execute(
    "SELECT id, side, grid_price, limit_price, fill_price, status "
    "FROM grid_order_log WHERE grid_price > 0 ORDER BY id DESC LIMIT 5"
).fetchall()
for r in rows2:
    print(f"  id={r[0]} {r[1]} grid={r[2]} limit={r[3]} fill={r[4]} {r[5]}")

conn.close()
