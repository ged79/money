"""klines 날짜 포맷 확인 후 가격 흐름 분석"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
from db import get_connection

conn = get_connection()

# 날짜 포맷 확인
sample = conn.execute(
    "SELECT open_time, close, volume FROM klines "
    "WHERE symbol='SOLUSDT' AND interval='5m' ORDER BY open_time DESC LIMIT 3"
).fetchall()
print("최근 klines 샘플:")
for s in sample:
    print(f"  open_time={s[0]} (type={type(s[0]).__name__}) close={s[1]} vol={s[2]}")

# 전체 날짜 범위
minmax = conn.execute(
    "SELECT MIN(open_time), MAX(open_time), COUNT(*) FROM klines "
    "WHERE symbol='SOLUSDT' AND interval='5m'"
).fetchone()
print(f"\n전체 범위: {minmax[0]} ~ {minmax[1]} ({minmax[2]}건)")

# 날짜별 건수
daily = conn.execute(
    "SELECT SUBSTR(open_time, 1, 10) as d, COUNT(*), MIN(close), MAX(close) FROM klines "
    "WHERE symbol='SOLUSDT' AND interval='5m' "
    "GROUP BY d ORDER BY d DESC LIMIT 5"
).fetchall()
print("\n날짜별 건수:")
for d in daily:
    print(f"  {d[0]}: {d[1]}건 | ${d[2]:.2f} ~ ${d[3]:.2f}")

# 일봉도 확인
daily_candles = conn.execute(
    "SELECT open_time, open, high, low, close FROM klines "
    "WHERE symbol='SOLUSDT' AND interval='1d' ORDER BY open_time DESC LIMIT 5"
).fetchall()
print("\n일봉:")
for d in daily_candles:
    chg = (d[4] - d[1]) / d[1] * 100
    print(f"  {d[0]} O=${d[1]:.2f} H=${d[2]:.2f} L=${d[3]:.2f} C=${d[4]:.2f} ({chg:+.2f}%)")

conn.close()
