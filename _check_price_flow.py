"""어제~오늘 SOLUSDT 가격 흐름 분석 — L2 진입 타당성 판단"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
from db import get_connection

conn = get_connection()

# 1. 어제~오늘 5분봉 (2/27 ~ 3/1)
print("=" * 70)
print("  SOLUSDT 가격 흐름 (2/27 ~ 3/1)")
print("=" * 70)

for day in ["2026-02-27", "2026-02-28", "2026-03-01"]:
    rows = conn.execute(
        "SELECT close, volume, open_time FROM klines "
        "WHERE symbol='SOLUSDT' AND interval='5m' AND open_time LIKE ? "
        "ORDER BY open_time",
        (day + "%",),
    ).fetchall()
    if not rows:
        print(f"\n{day}: 데이터 없음")
        continue
    prices = [r[0] for r in rows]
    vols = [r[1] for r in rows]
    print(f"\n{day}: {len(rows)}봉 | 시가=${prices[0]:.2f} 고가=${max(prices):.2f} "
          f"저가=${min(prices):.2f} 종가=${prices[-1]:.2f} | "
          f"변동: {(prices[-1]-prices[0])/prices[0]*100:+.2f}%")

# 2. 그리드 범위 히스토리
print(f"\n{'=' * 70}")
print("  그리드 범위 히스토리")
print("=" * 70)
grids = conn.execute(
    "SELECT lower_bound, upper_bound, grid_count, calculated_at FROM grid_configs "
    "WHERE symbol='SOLUSDT' ORDER BY id DESC LIMIT 15"
).fetchall()
for g in reversed(grids):
    print(f"  {g[3]} | ${g[0]:.2f} ~ ${g[1]:.2f} ({g[2]} grids)")

# 3. 2/28 시간대별 가격 (4시간 단위 요약)
print(f"\n{'=' * 70}")
print("  2/28 시간대별 가격 흐름 (4시간)")
print("=" * 70)
rows_28 = conn.execute(
    "SELECT close, volume, open_time FROM klines "
    "WHERE symbol='SOLUSDT' AND interval='5m' AND open_time LIKE '2026-02-28%' "
    "ORDER BY open_time"
).fetchall()

if rows_28:
    # 4시간(48봉)씩 묶기
    for i in range(0, len(rows_28), 48):
        chunk = rows_28[i:i+48]
        cp = [r[0] for r in chunk]
        cv = [r[1] for r in chunk]
        t_start = chunk[0][2][:16]
        t_end = chunk[-1][2][:16]
        print(f"  {t_start} ~ {t_end}")
        print(f"    시가=${cp[0]:.2f} 고가=${max(cp):.2f} 저가=${min(cp):.2f} 종가=${cp[-1]:.2f} "
              f"({(cp[-1]-cp[0])/cp[0]*100:+.2f}%) | 거래량={sum(cv):,.0f}")

# 4. 2/28 급락 구간 상세 (15:00~17:00)
print(f"\n{'=' * 70}")
print("  2/28 급락 구간 (14:00~17:00 UTC) 15분 단위")
print("=" * 70)
crash = conn.execute(
    "SELECT close, volume, open_time FROM klines "
    "WHERE symbol='SOLUSDT' AND interval='5m' "
    "AND open_time BETWEEN '2026-02-28 14:00' AND '2026-02-28 17:00' "
    "ORDER BY open_time"
).fetchall()
if crash:
    for i in range(0, len(crash), 3):
        chunk = crash[i:i+3]
        cp = [r[0] for r in chunk]
        cv = [r[1] for r in chunk]
        print(f"  {chunk[0][2][:16]} | ${cp[0]:.2f}→${cp[-1]:.2f} ({(cp[-1]-cp[0])/cp[0]*100:+.2f}%) "
              f"vol={sum(cv):,.0f}")

# 5. OOB 이후 가격이 어디로 갔는지 — L2 SHORT 했으면 수익이었나?
print(f"\n{'=' * 70}")
print("  L2 SHORT 진입 시뮬레이션")
print("=" * 70)

# 그리드 하한 이탈 시점 찾기
grid_lower = None
g_at_28 = conn.execute(
    "SELECT lower_bound, upper_bound, calculated_at FROM grid_configs "
    "WHERE symbol='SOLUSDT' AND calculated_at < '2026-02-28 02:00' "
    "ORDER BY id DESC LIMIT 1"
).fetchone()
if g_at_28:
    grid_lower = g_at_28[0]
    print(f"  기준 그리드 하한: ${grid_lower:.2f} (설정: {g_at_28[2]})")

if rows_28 and grid_lower:
    # OOB 시작점 찾기
    oob_start = None
    for r in rows_28:
        if r[0] < grid_lower:
            oob_start = r
            break
    
    if oob_start:
        entry_price = oob_start[0]
        entry_time = oob_start[2]
        print(f"  OOB 시작: {entry_time} @ ${entry_price:.2f}")
        
        # 이후 가격 흐름
        after_oob = [r for r in rows_28 if r[2] >= entry_time]
        if after_oob:
            lowest = min(r[0] for r in after_oob)
            lowest_time = [r[2] for r in after_oob if r[0] == lowest][0]
            final = after_oob[-1][0]
            
            # L2 SHORT 시나리오
            short_entry = entry_price
            max_profit_pct = (short_entry - lowest) / short_entry * 100
            final_pnl_pct = (short_entry - final) / short_entry * 100
            
            print(f"  이후 저점: ${lowest:.2f} @ {lowest_time}")
            print(f"  2/28 종가: ${final:.2f}")
            print(f"  SHORT 진입가: ${short_entry:.2f}")
            print(f"  최대 수익: {max_profit_pct:+.2f}%")
            print(f"  종가 기준: {final_pnl_pct:+.2f}%")
            
            # 3/1 현재가도
            now_rows = conn.execute(
                "SELECT close, open_time FROM klines "
                "WHERE symbol='SOLUSDT' AND interval='5m' ORDER BY open_time DESC LIMIT 1"
            ).fetchone()
            if now_rows:
                now_pnl = (short_entry - now_rows[0]) / short_entry * 100
                print(f"  현재가: ${now_rows[0]:.2f} ({now_rows[1]})")
                print(f"  현재 기준: {now_pnl:+.2f}%")

# 6. L2 LONG 시나리오 (SSM이 BULLISH였으니)
print(f"\n{'=' * 70}")
print("  L2 LONG 진입 시뮬레이션 (SSM=BULLISH 기준)")
print("=" * 70)
if rows_28 and grid_lower:
    if oob_start:
        long_entry = entry_price
        max_loss_pct = (lowest - long_entry) / long_entry * 100
        final_pnl_long = (final - long_entry) / long_entry * 100
        print(f"  LONG 진입가: ${long_entry:.2f}")
        print(f"  최대 손실: {max_loss_pct:+.2f}%")
        print(f"  종가 기준: {final_pnl_long:+.2f}%")
        if now_rows:
            now_pnl_long = (now_rows[0] - long_entry) / long_entry * 100
            print(f"  현재 기준: {now_pnl_long:+.2f}%")

conn.close()
