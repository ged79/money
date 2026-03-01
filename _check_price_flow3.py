"""SOLUSDT 가격 흐름 분석 — L2 진입 타당성 판단"""
import sys, os, time
from datetime import datetime, timezone, timedelta
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
from db import get_connection

conn = get_connection()

def ts(dt_str):
    """날짜 문자열 → 밀리초 타임스탬프"""
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

def fmt(ms):
    """밀리초 → 읽기 좋은 시간"""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%m/%d %H:%M")

# 일봉으로 전체 흐름
print("=" * 70)
print("  SOLUSDT 일봉 (최근 5일)")
print("=" * 70)
daily = conn.execute(
    "SELECT open_time, open, high, low, close FROM klines "
    "WHERE symbol='SOLUSDT' AND interval='1d' ORDER BY open_time DESC LIMIT 5"
).fetchall()
for d in reversed(daily):
    chg = (d[4] - d[1]) / d[1] * 100
    rng = (d[2] - d[3]) / d[3] * 100
    print(f"  {fmt(d[0])} | O=${d[1]:.2f} H=${d[2]:.2f} L=${d[3]:.2f} C=${d[4]:.2f} "
          f"| {chg:+.2f}% | 레인지 {rng:.1f}%")

# 2/28 = 1772150400000, 3/1 = 1772236800000
day_28_start = 1772150400000
day_28_end = 1772236800000
day_01_start = 1772236800000
day_01_end = day_01_start + 86400000

# 5분봉 4시간 단위 요약
print(f"\n{'=' * 70}")
print("  2/28~3/1 시간대별 흐름 (4시간)")
print("=" * 70)
rows = conn.execute(
    "SELECT open_time, close, volume FROM klines "
    "WHERE symbol='SOLUSDT' AND interval='5m' AND open_time >= ? AND open_time < ? "
    "ORDER BY open_time",
    (day_28_start, day_01_end),
).fetchall()
print(f"  총 {len(rows)}봉")

for i in range(0, len(rows), 48):
    chunk = rows[i:i+48]
    cp = [r[1] for r in chunk]
    cv = [r[2] for r in chunk]
    print(f"  {fmt(chunk[0][0])} ~ {fmt(chunk[-1][0])}")
    print(f"    ${cp[0]:.2f}→${cp[-1]:.2f} ({(cp[-1]-cp[0])/cp[0]*100:+.2f}%) "
          f"H=${max(cp):.2f} L=${min(cp):.2f} | vol={sum(cv):,.0f}")

# 그리드 범위 히스토리
print(f"\n{'=' * 70}")
print("  그리드 범위 변경 이력 (2/28)")
print("=" * 70)
grids = conn.execute(
    "SELECT lower_bound, upper_bound, grid_count, calculated_at FROM grid_configs "
    "WHERE symbol='SOLUSDT' AND calculated_at >= '2026-02-28' AND calculated_at < '2026-03-01' "
    "ORDER BY id"
).fetchall()
# 2/28 시작 직전 설정도
prev_grid = conn.execute(
    "SELECT lower_bound, upper_bound, grid_count, calculated_at FROM grid_configs "
    "WHERE symbol='SOLUSDT' AND calculated_at < '2026-02-28' "
    "ORDER BY id DESC LIMIT 1"
).fetchone()
if prev_grid:
    print(f"  [이전] {prev_grid[3]} | ${prev_grid[0]:.2f} ~ ${prev_grid[1]:.2f} ({prev_grid[2]} grids)")
for g in grids:
    print(f"  {g[3]} | ${g[0]:.2f} ~ ${g[1]:.2f} ({g[2]} grids)")

# OOB 분석 — 가격이 그리드 범위 밖인 구간
print(f"\n{'=' * 70}")
print("  OOB 분석 (그리드 범위 이탈)")
print("=" * 70)

# 시간순으로 각 5분봉마다 그리드 범위와 비교
all_grids = conn.execute(
    "SELECT lower_bound, upper_bound, calculated_at FROM grid_configs "
    "WHERE symbol='SOLUSDT' ORDER BY id"
).fetchall()

# 각 시점의 유효 그리드 찾기 (calculated_at 기준)
from datetime import datetime as DT
grid_ts_list = []
for g in all_grids:
    try:
        gt = DT.strptime(g[2], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        grid_ts_list.append((int(gt.timestamp()*1000), g[0], g[1]))
    except:
        pass

oob_count = 0
in_count = 0
first_oob = None
oob_segments = []  # (start_time, end_time, direction, min_price, max_price)
current_oob_start = None
current_oob_dir = None
current_oob_prices = []

for r in rows:
    candle_ts, price, vol = r
    # 해당 시점의 유효 그리드 찾기
    active_grid = None
    for gts, lb, ub in reversed(grid_ts_list):
        if gts <= candle_ts:
            active_grid = (lb, ub)
            break
    if not active_grid:
        continue
    
    lb, ub = active_grid
    if price < lb:
        oob_count += 1
        direction = "DOWN"
        if not first_oob:
            first_oob = (candle_ts, price, lb)
        if current_oob_dir != "DOWN":
            if current_oob_start:
                oob_segments.append((current_oob_start, candle_ts, current_oob_dir, current_oob_prices[:]))
            current_oob_start = candle_ts
            current_oob_dir = "DOWN"
            current_oob_prices = [price]
        else:
            current_oob_prices.append(price)
    elif price > ub:
        oob_count += 1
        direction = "UP"
        if current_oob_dir != "UP":
            if current_oob_start:
                oob_segments.append((current_oob_start, candle_ts, current_oob_dir, current_oob_prices[:]))
            current_oob_start = candle_ts
            current_oob_dir = "UP"
            current_oob_prices = [price]
        else:
            current_oob_prices.append(price)
    else:
        in_count += 1
        if current_oob_start:
            oob_segments.append((current_oob_start, candle_ts, current_oob_dir, current_oob_prices[:]))
            current_oob_start = None
            current_oob_dir = None
            current_oob_prices = []

if current_oob_start:
    oob_segments.append((current_oob_start, rows[-1][0], current_oob_dir, current_oob_prices[:]))

print(f"  범위 내: {in_count}봉 | 범위 밖: {oob_count}봉")
if first_oob:
    print(f"  첫 OOB: {fmt(first_oob[0])} @ ${first_oob[1]:.2f} (하한 ${first_oob[2]:.2f})")

print(f"\n  OOB 구간 목록:")
for seg in oob_segments:
    start, end, direction, prices = seg
    duration_min = (end - start) / 60000
    if duration_min < 5:
        continue  # 1봉 이하 무시
    print(f"    {fmt(start)} ~ {fmt(end)} ({duration_min:.0f}분) | "
          f"{direction} | ${min(prices):.2f}~${max(prices):.2f}")

# L2 SHORT/LONG 시뮬레이션
print(f"\n{'=' * 70}")
print("  L2 진입 시뮬레이션")
print("=" * 70)

# 주요 OOB 시점에서 진입했다면?
for seg in oob_segments:
    start, end, direction, prices = seg
    duration_min = (end - start) / 60000
    if duration_min < 30:  # 30분 이상 OOB만
        continue
    
    entry_price = prices[0]
    # 진입 이후 모든 가격
    after = [r for r in rows if r[0] >= start]
    if not after:
        continue
    
    after_prices = [r[1] for r in after]
    lowest = min(after_prices)
    highest = max(after_prices)
    final = after_prices[-1]
    
    print(f"\n  OOB 시작: {fmt(start)} @ ${entry_price:.2f} ({direction})")
    print(f"  지속: {duration_min:.0f}분 | OOB 내 가격범위: ${min(prices):.2f}~${max(prices):.2f}")
    
    if direction == "DOWN":
        # SHORT 시나리오
        max_profit = (entry_price - lowest) / entry_price * 100
        final_pnl = (entry_price - final) / entry_price * 100
        print(f"  → SHORT 진입시:")
        print(f"    최대 수익: {max_profit:+.2f}% (저점 ${lowest:.2f})")
        print(f"    최종(현재): {final_pnl:+.2f}% (${final:.2f})")
        # LONG 시나리오 (역방향)
        max_loss = (lowest - entry_price) / entry_price * 100
        long_final = (final - entry_price) / entry_price * 100
        print(f"  → LONG 진입시:")
        print(f"    최대 손실: {max_loss:+.2f}%")
        print(f"    최종(현재): {long_final:+.2f}%")
    else:
        # LONG 시나리오
        max_profit = (highest - entry_price) / entry_price * 100
        final_pnl = (final - entry_price) / entry_price * 100
        print(f"  → LONG 진입시:")
        print(f"    최대 수익: {max_profit:+.2f}% (고점 ${highest:.2f})")
        print(f"    최종(현재): {final_pnl:+.2f}%")

# SSM 방향 vs 실제 가격
print(f"\n{'=' * 70}")
print("  SSM 방향 vs 실제 가격 방향")
print("=" * 70)
ssm = conn.execute(
    "SELECT direction, total_score, calculated_at FROM ssm_scores "
    "WHERE symbol='SOLUSDT' AND calculated_at >= '2026-02-28' AND calculated_at < '2026-03-01' "
    "ORDER BY id"
).fetchall()
if ssm:
    directions = {}
    for s in ssm:
        directions[s[0]] = directions.get(s[0], 0) + 1
    print(f"  SSM 방향 분포: {directions}")
    print(f"  SSM 점수 범위: {min(s[1] for s in ssm):.2f} ~ {max(s[1] for s in ssm):.2f}")
    print(f"  실제 가격: 2/28 시가 $85.86 → 종가 $81.80 (하락)")
    print(f"  → SSM BULLISH vs 실제 BEARISH = 불일치")

conn.close()
