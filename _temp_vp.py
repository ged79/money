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
    "SELECT lower_bound, upper_bound, grid_count, grid_spacing_pct FROM grid_configs WHERE symbol='SOLUSDT' ORDER BY id DESC LIMIT 1"
).fetchone()
if grid:
    print(f"현재 그리드: ${grid[0]:.2f} ~ ${grid[1]:.2f} ({grid[2]}레벨, {grid[3]:.4f}%)")
else:
    print("그리드 설정 없음")

# Volume Profile - 4h (30일)
print("\n=== Volume Profile: 4h x 180 (~30일) ===")
profile = build_volume_profile("SOLUSDT", "4h", 180, 50)
print(f"  POC: ${profile['poc']:.2f}")
print(f"  VA: ${profile['value_area_low']:.2f} ~ ${profile['value_area_high']:.2f}")

nodes = find_hvn_lvn(profile)
hvn_sorted = sorted(nodes['hvn'])
lvn_sorted = sorted([p for p in nodes['lvn'] if 70 < p < 110])
print(f"\n  HVN (고거래량 = 강한 지지/저항):")
for p in hvn_sorted:
    dist = (p - current_price) / current_price * 100
    marker = " <<< 현재가 근처" if abs(dist) < 2 else ""
    print(f"    ${p:>8.2f}  ({dist:+.1f}%){marker}")

print(f"\n  LVN (저거래량 = 빠른 이동 구간):")
for p in lvn_sorted:
    dist = (p - current_price) / current_price * 100
    print(f"    ${p:>8.2f}  ({dist:+.1f}%)")

# 1h (7일)
print("\n=== Volume Profile: 1h x 168 (~7일) ===")
profile_1h = build_volume_profile("SOLUSDT", "1h", 168, 30)
print(f"  POC: ${profile_1h['poc']:.2f}")
print(f"  VA: ${profile_1h['value_area_low']:.2f} ~ ${profile_1h['value_area_high']:.2f}")

nodes_1h = find_hvn_lvn(profile_1h)
print(f"\n  HVN (7일):")
for p in sorted(nodes_1h['hvn']):
    dist = (p - current_price) / current_price * 100
    print(f"    ${p:>8.2f}  ({dist:+.1f}%)")

# 일봉 (90일)
print("\n=== Volume Profile: 1d x 90 (~90일) ===")
profile_1d = build_volume_profile("SOLUSDT", "1d", 90, 50)
print(f"  POC: ${profile_1d['poc']:.2f}")
print(f"  VA: ${profile_1d['value_area_low']:.2f} ~ ${profile_1d['value_area_high']:.2f}")

# 버킷 분포 시각화 (현재가 ±10% 범위)
print("\n=== 현재가 주변 거래량 분포 (4h 기준) ===")
for b in profile['buckets']:
    mid = (b['price_low'] + b['price_high']) / 2
    if abs(mid - current_price) / current_price < 0.10:
        bar_len = int(b['volume'] / max(bb['volume'] for bb in profile['buckets']) * 40)
        bar = '#' * bar_len
        marker = " <<<" if abs(mid - current_price) / current_price < 0.01 else ""
        print(f"  ${mid:>8.2f} | {bar}{marker}")

conn.close()
