import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from db import get_connection
from engines.volume_profile import build_volume_profile, find_hvn_lvn

conn = get_connection()

# 현재가
price_row = conn.execute(
    "SELECT close FROM klines WHERE symbol='SOLUSDT' AND interval='5m' ORDER BY open_time DESC LIMIT 1"
).fetchone()
current_price = price_row[0] if price_row else 0
print(f"현재가: ${current_price:.2f}")

# 현재 그리드 설정
grid = conn.execute(
    "SELECT range_low, range_high, grid_count, grid_spacing_pct FROM grid_configs WHERE symbol='SOLUSDT' ORDER BY calculated_at DESC LIMIT 1"
).fetchone()
if grid:
    print(f"현재 그리드: ${grid[0]:.2f} ~ ${grid[1]:.2f} ({grid[2]}레벨, {grid[3]:.4f}%)")
else:
    print("그리드 설정 없음")

# Volume Profile - 여러 타임프레임
print("\n=== Volume Profile (4h, 180캔들 = ~30일) ===")
profile = build_volume_profile("SOLUSDT", "4h", 180, 50)
print(f"  POC: ${profile['poc']:.2f}")
print(f"  VA: ${profile['value_area_low']:.2f} ~ ${profile['value_area_high']:.2f}")

nodes = find_hvn_lvn(profile)
print(f"\n  HVN (고거래량 = 강한 지지/저항):")
for p in sorted(nodes['hvn']):
    marker = " <-- 현재가 근처" if abs(p - current_price) / current_price < 0.02 else ""
    print(f"    ${p:.2f}{marker}")

print(f"\n  LVN (저거래량 = 빠른 이동 구간):")
for p in sorted(nodes['lvn']):
    if 60 < p < 120:  # 관련 범위만
        print(f"    ${p:.2f}")

# 1시간봉 기반 (더 최근 데이터)
print("\n=== Volume Profile (1h, 168캔들 = ~7일) ===")
profile_1h = build_volume_profile("SOLUSDT", "1h", 168, 30)
print(f"  POC: ${profile_1h['poc']:.2f}")
print(f"  VA: ${profile_1h['value_area_low']:.2f} ~ ${profile_1h['value_area_high']:.2f}")

nodes_1h = find_hvn_lvn(profile_1h)
print(f"\n  HVN (7일):")
for p in sorted(nodes_1h['hvn']):
    print(f"    ${p:.2f}")

# 일봉 기반 (장기)
print("\n=== Volume Profile (1d, 90캔들 = ~90일) ===")
profile_1d = build_volume_profile("SOLUSDT", "1d", 90, 50)
print(f"  POC: ${profile_1d['poc']:.2f}")
print(f"  VA: ${profile_1d['value_area_low']:.2f} ~ ${profile_1d['value_area_high']:.2f}")

conn.close()
