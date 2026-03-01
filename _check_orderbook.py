"""오더북 데이터 수집 상태 확인"""
import sys, os
from datetime import datetime, timezone
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
from db import get_connection
import time

conn = get_connection()
now = time.time()

print("=" * 60)
print("  오더북 데이터 수집 상태")
print("=" * 60)

# 1. 전체 통계
total = conn.execute("SELECT COUNT(*) FROM orderbook_walls").fetchone()[0]
print(f"\n  총 데이터: {total:,}건")

# 2. 심볼별 최신 데이터
print(f"\n[심볼별 최신]")
for symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
    row = conn.execute(
        "SELECT side, price, quantity, scan_id, collected_at FROM orderbook_walls "
        "WHERE symbol=? ORDER BY id DESC LIMIT 1",
        (symbol,)
    ).fetchone()
    if row:
        try:
            dt = datetime.strptime(row[4], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            age_min = (now - dt.timestamp()) / 60
        except:
            age_min = -1
        cnt = conn.execute("SELECT COUNT(*) FROM orderbook_walls WHERE symbol=?", (symbol,)).fetchone()[0]
        status = "OK" if age_min < 60 else "WARN" if age_min < 240 else "FAIL"
        print(f"  [{status}] {symbol}: {cnt:,}건 | 최신: {row[4]} ({age_min:.0f}분 전)")
    else:
        print(f"  [FAIL] {symbol}: 데이터 없음")

# 3. 수집 간격 (scan_id 기준)
print(f"\n[수집 간격]")
for symbol in ["SOLUSDT"]:
    scans = conn.execute(
        "SELECT DISTINCT scan_id, collected_at FROM orderbook_walls "
        "WHERE symbol=? ORDER BY scan_id DESC LIMIT 10",
        (symbol,)
    ).fetchall()
    if len(scans) >= 2:
        intervals = []
        for i in range(len(scans)-1):
            try:
                t1 = datetime.strptime(scans[i][1], "%Y-%m-%d %H:%M:%S")
                t2 = datetime.strptime(scans[i+1][1], "%Y-%m-%d %H:%M:%S")
                intervals.append((t1-t2).total_seconds()/60)
            except:
                pass
        if intervals:
            print(f"  {symbol}: 평균 {sum(intervals)/len(intervals):.0f}분 간격 (최근 {len(scans)}회 스캔)")
        for s in scans[:5]:
            cnt = conn.execute("SELECT COUNT(*) FROM orderbook_walls WHERE scan_id=? AND symbol=?", (s[0], symbol)).fetchone()[0]
            print(f"    scan#{s[0]} | {s[1]} | {cnt}건")

# 4. 최신 스캔 상세 (SOLUSDT)
print(f"\n[SOLUSDT 최신 스캔 상세]")
latest_scan = conn.execute(
    "SELECT scan_id FROM orderbook_walls WHERE symbol='SOLUSDT' ORDER BY id DESC LIMIT 1"
).fetchone()
if latest_scan:
    walls = conn.execute(
        "SELECT side, price, quantity FROM orderbook_walls "
        "WHERE symbol='SOLUSDT' AND scan_id=? ORDER BY side, price",
        (latest_scan[0],)
    ).fetchall()
    
    bids = [(r[1], r[2]) for r in walls if r[0] == "BID"]
    asks = [(r[1], r[2]) for r in walls if r[0] == "ASK"]
    
    print(f"  scan_id: {latest_scan[0]}")
    print(f"  BID 벽 (지지): {len(bids)}건")
    for p, q in sorted(bids, key=lambda x: -x[1])[:5]:
        print(f"    ${p:.2f} | qty={q:,.1f}")
    print(f"  ASK 벽 (저항): {len(asks)}건")
    for p, q in sorted(asks, key=lambda x: -x[1])[:5]:
        print(f"    ${p:.2f} | qty={q:,.1f}")

# 5. 그리드 범위와 오더북 벽 비교
print(f"\n[그리드 범위 vs 오더북 벽]")
grid = conn.execute(
    "SELECT lower_bound, upper_bound, calculated_at FROM grid_configs "
    "WHERE symbol='SOLUSDT' ORDER BY id DESC LIMIT 1"
).fetchone()
if grid:
    print(f"  그리드: ${grid[0]:.2f} ~ ${grid[1]:.2f} | {grid[2]}")
    if bids:
        top_bid = max(bids, key=lambda x: x[1])
        print(f"  최대 BID 벽: ${top_bid[0]:.2f} (qty={top_bid[1]:,.1f})")
    if asks:
        top_ask = max(asks, key=lambda x: x[1])
        print(f"  최대 ASK 벽: ${top_ask[0]:.2f} (qty={top_ask[1]:,.1f})")

conn.close()
