import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from db import get_connection
from datetime import datetime

conn = get_connection()

# 일봉 전체 조회
rows = conn.execute(
    "SELECT open_time, open, high, low, close, volume FROM klines "
    "WHERE symbol='SOLUSDT' AND interval='1d' ORDER BY open_time ASC"
).fetchall()

print(f"=== SOLUSDT 일봉 전체: {len(rows)}캔들 ===")
print(f"기간: {datetime.fromtimestamp(rows[0][0]/1000).strftime('%Y-%m-%d')} ~ {datetime.fromtimestamp(rows[-1][0]/1000).strftime('%Y-%m-%d')}")

# 전체 일봉 출력
print(f"\n{'날짜':>10} | {'저가':>8} - {'고가':>8} | {'종가':>8} | {'변동':>6} | 차트")
print("-" * 80)

for r in rows:
    ts = datetime.fromtimestamp(r[0]/1000).strftime('%m/%d')
    o, h, l, c, v = r[1], r[2], r[3], r[4], r[5]
    change = (c - o) / o * 100
    
    # 가격 범위 시각화 (간단한 바 차트)
    # $60~$300 범위를 40칸으로
    chart_min, chart_max = 60, 300
    chart_width = 50
    
    low_pos = int((l - chart_min) / (chart_max - chart_min) * chart_width)
    high_pos = int((h - chart_min) / (chart_max - chart_min) * chart_width)
    close_pos = int((c - chart_min) / (chart_max - chart_min) * chart_width)
    
    low_pos = max(0, min(chart_width-1, low_pos))
    high_pos = max(0, min(chart_width-1, high_pos))
    close_pos = max(0, min(chart_width-1, close_pos))
    
    bar = [' '] * chart_width
    for i in range(low_pos, high_pos + 1):
        bar[i] = '-'
    bar[close_pos] = '█'
    
    print(f"  {ts} | ${l:>7.1f} - ${h:>7.1f} | ${c:>7.1f} | {change:+5.1f}% | {''.join(bar)}")

# 횡보 구간 자동 감지
print(f"\n\n=== 횡보 구간 감지 (7일 윈도우, 변동폭 <15%) ===")
window = 7
for i in range(len(rows) - window):
    segment = rows[i:i+window]
    seg_high = max(r[2] for r in segment)
    seg_low = min(r[3] for r in segment)
    seg_range = (seg_high - seg_low) / seg_low * 100
    
    start_date = datetime.fromtimestamp(segment[0][0]/1000).strftime('%m/%d')
    end_date = datetime.fromtimestamp(segment[-1][0]/1000).strftime('%m/%d')
    
    if seg_range < 12:
        print(f"  {start_date}~{end_date}: ${seg_low:.0f}~${seg_high:.0f} (변동 {seg_range:.1f}%)")

# 더 긴 횡보 감지 (14일)
print(f"\n=== 장기 횡보 구간 (14일 윈도우, 변동폭 <20%) ===")
window = 14
for i in range(len(rows) - window):
    segment = rows[i:i+window]
    seg_high = max(r[2] for r in segment)
    seg_low = min(r[3] for r in segment)
    seg_range = (seg_high - seg_low) / seg_low * 100
    
    start_date = datetime.fromtimestamp(segment[0][0]/1000).strftime('%m/%d')
    end_date = datetime.fromtimestamp(segment[-1][0]/1000).strftime('%m/%d')
    
    if seg_range < 20:
        print(f"  {start_date}~{end_date}: ${seg_low:.0f}~${seg_high:.0f} (변동 {seg_range:.1f}%)")

conn.close()
