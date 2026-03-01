import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from db import get_connection
from datetime import datetime

conn = get_connection()

# === 1. 일봉 분석: 횡보 구간 확인 ===
print("=" * 60)
print("  일봉 (1d) - 최근 90일")
print("=" * 60)

rows_1d = conn.execute(
    "SELECT open_time, open, high, low, close, volume FROM klines "
    "WHERE symbol='SOLUSDT' AND interval='1d' ORDER BY open_time DESC LIMIT 90"
).fetchall()

print(f"\n  총 {len(rows_1d)}캔들")
print(f"\n  최근 30일 일봉:")
for r in rows_1d[:30]:
    ts = datetime.fromtimestamp(r[0]/1000).strftime('%m/%d')
    o, h, l, c, v = r[1], r[2], r[3], r[4], r[5]
    change = (c - o) / o * 100
    bar_range = (h - l) / l * 100
    in_range = "■" if 77 <= l and h <= 88 else ("▲" if h > 88 else "▼")
    bar = '#' * int(v / max(rr[5] for rr in rows_1d[:30]) * 30)
    print(f"  {ts} | ${l:>6.1f}-${h:>6.1f} | C=${c:>6.1f} ({change:+.1f}%) | {in_range} | {bar}")

# $77-$88 범위 안에 있었던 일수
in_range_count = sum(1 for r in rows_1d[:30] if r[3] >= 75 and r[2] <= 90)
print(f"\n  $77-$88 범위 내 일수: {in_range_count}/{min(30, len(rows_1d))}일")

# === 2. 4시간봉 분석 ===
print("\n" + "=" * 60)
print("  4시간봉 (4h) - 최근 7일 (42캔들)")
print("=" * 60)

rows_4h = conn.execute(
    "SELECT open_time, open, high, low, close, volume FROM klines "
    "WHERE symbol='SOLUSDT' AND interval='4h' ORDER BY open_time DESC LIMIT 42"
).fetchall()

print(f"\n  최근 7일 4시간봉:")
for r in rows_4h[:42]:
    ts = datetime.fromtimestamp(r[0]/1000).strftime('%m/%d %H:%M')
    o, h, l, c, v = r[1], r[2], r[3], r[4], r[5]
    change = (c - o) / o * 100
    bar = '#' * int(v / max(rr[5] for rr in rows_4h[:42]) * 25)
    print(f"  {ts} | ${l:>6.1f}-${h:>6.1f} | C=${c:>6.1f} ({change:+.1f}%) | {bar}")

# === 3. 1시간봉 분석 - 최근 48시간 ===
print("\n" + "=" * 60)
print("  1시간봉 (1h) - 최근 48시간")
print("=" * 60)

rows_1h = conn.execute(
    "SELECT open_time, open, high, low, close, volume FROM klines "
    "WHERE symbol='SOLUSDT' AND interval='1h' ORDER BY open_time DESC LIMIT 48"
).fetchall()

# 1시간봉에서 가격 변동폭 통계
changes_1h = [(r[4] - r[1]) / r[1] * 100 for r in rows_1h]
ranges_1h = [(r[2] - r[3]) / r[3] * 100 for r in rows_1h]

print(f"\n  48시간 통계:")
print(f"  가격 변동: avg={sum(abs(c) for c in changes_1h)/len(changes_1h):.2f}%, max={max(abs(c) for c in changes_1h):.2f}%")
print(f"  캔들 범위: avg={sum(ranges_1h)/len(ranges_1h):.2f}%, max={max(ranges_1h):.2f}%")
print(f"  고가: ${max(r[2] for r in rows_1h):.2f}")
print(f"  저가: ${min(r[3] for r in rows_1h):.2f}")
print(f"  총 변동폭: {(max(r[2] for r in rows_1h) - min(r[3] for r in rows_1h)) / min(r[3] for r in rows_1h) * 100:.1f}%")

# === 4. 횡보 vs 변동 구분 ===
print("\n" + "=" * 60)
print("  횡보 vs 변동 분석")
print("=" * 60)

# 일봉: 최근 2주 범위
recent_14d = rows_1d[:14]
d_high = max(r[2] for r in recent_14d)
d_low = min(r[3] for r in recent_14d)
d_range_pct = (d_high - d_low) / d_low * 100
print(f"\n  일봉 14일 범위: ${d_low:.1f} ~ ${d_high:.1f} ({d_range_pct:.1f}%)")
print(f"  → {'횡보' if d_range_pct < 15 else '변동'} (기준: 15%)")

# 4시간: 최근 3일
recent_3d_4h = rows_4h[:18]
h4_high = max(r[2] for r in recent_3d_4h)
h4_low = min(r[3] for r in recent_3d_4h)
h4_range_pct = (h4_high - h4_low) / h4_low * 100
print(f"\n  4시간봉 3일 범위: ${h4_low:.1f} ~ ${h4_high:.1f} ({h4_range_pct:.1f}%)")
print(f"  → {'횡보' if h4_range_pct < 8 else '변동'} (기준: 8%)")

# 1시간: 최근 24시간
recent_24h = rows_1h[:24]
h1_high = max(r[2] for r in recent_24h)
h1_low = min(r[3] for r in recent_24h)
h1_range_pct = (h1_high - h1_low) / h1_low * 100
print(f"\n  1시간봉 24시간 범위: ${h1_low:.1f} ~ ${h1_high:.1f} ({h1_range_pct:.1f}%)")
print(f"  → {'횡보' if h1_range_pct < 5 else '변동'} (기준: 5%)")

conn.close()
