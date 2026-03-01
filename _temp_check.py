import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db import get_connection

conn = get_connection()

print("=== SOLUSDT klines 데이터 현황 ===")
rows = conn.execute(
    "SELECT interval, COUNT(*), MIN(open_time), MAX(open_time) FROM klines WHERE symbol='SOLUSDT' GROUP BY interval"
).fetchall()
for r in rows:
    print(f"  {r[0]:6s}: {r[1]:5d}건")

print("\n=== Volume Profile 테스트 (4h, 180캔들) ===")
from engines.volume_profile import build_volume_profile, find_hvn_lvn
profile = build_volume_profile("SOLUSDT", "4h", 180)
print(f"  POC: ${profile['poc']}")
print(f"  VA High: ${profile['value_area_high']}")
print(f"  VA Low: ${profile['value_area_low']}")
print(f"  Buckets: {len(profile['buckets'])}개")

nodes = find_hvn_lvn(profile)
print(f"\n  HVN (고거래량): {[f'${p}' for p in nodes['hvn'][:10]]}")
print(f"  LVN (저거래량): {[f'${p}' for p in nodes['lvn'][:10]]}")

# 현재 그리드 설정 확인
print("\n=== 현재 그리드 설정 ===")
grid = conn.execute(
    "SELECT range_low, range_high, grid_count, grid_spacing_pct FROM grid_configs WHERE symbol='SOLUSDT' ORDER BY calculated_at DESC LIMIT 1"
).fetchone()
if grid:
    print(f"  Range: ${grid[0]:.2f} ~ ${grid[1]:.2f}")
    print(f"  Grid Count: {grid[2]}, Spacing: {grid[3]:.4f}%")

# 현재가
price = conn.execute(
    "SELECT close FROM klines WHERE symbol='SOLUSDT' AND interval='5m' ORDER BY open_time DESC LIMIT 1"
).fetchone()
if price:
    print(f"\n  현재가: ${price[0]:.2f}")

conn.close()
