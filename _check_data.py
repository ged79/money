"""데이터 수집 현황 확인"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from db import get_connection

conn = get_connection()

tables = [
    ("open_interest", "collected_at"),
    ("funding_rate", "collected_at"),
    ("long_short_ratio", "collected_at"),
    ("orderbook_walls", "collected_at"),
    ("klines", "collected_at"),
    ("klines_5m", "collected_at"),
    ("fear_greed", "collected_at"),
    ("liquidations", "timestamp"),
    ("whale_transactions", "collected_at"),
    ("onchain_metrics", "collected_at"),
    ("macro_events", "collected_at"),
    ("atr_data", "calculated_at"),
    ("dynamic_threshold", "calculated_at"),
    ("grid_configs", "calculated_at"),
    ("ssm_scores", "calculated_at"),
    ("strategy_decisions", "decided_at"),
]

print("=" * 70)
print(f"{'테이블':<25} {'총 건수':>8} {'최초 수집':>20} {'최근 수집':>20}")
print("=" * 70)

for table, ts_col in tables:
    try:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        if count > 0:
            first = conn.execute(f"SELECT MIN({ts_col}) FROM {table}").fetchone()[0]
            last = conn.execute(f"SELECT MAX({ts_col}) FROM {table}").fetchone()[0]
        else:
            first = last = "-"
        print(f"{table:<25} {count:>8,} {str(first):>20} {str(last):>20}")
    except Exception as e:
        print(f"{table:<25} {'ERROR':>8} {str(e)[:40]}")

# 심볼별 5분봉 데이터 현황
print("\n--- 5분봉 심볼별 현황 ---")
for sym in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
    row = conn.execute(
        "SELECT COUNT(*), MIN(collected_at), MAX(collected_at) FROM klines_5m WHERE symbol=?",
        (sym,)
    ).fetchone()
    print(f"  {sym}: {row[0]:,}건 | {row[1]} ~ {row[2]}")

# 청산 데이터
print("\n--- 청산 데이터 (최근 10건) ---")
liqs = conn.execute(
    "SELECT symbol, side, price, quantity, timestamp FROM liquidations ORDER BY timestamp DESC LIMIT 10"
).fetchall()
for l in liqs:
    print(f"  {l[0]} {l[1]:>5} @ ${float(l[2]):>10,.2f} x {l[3]} | {l[4]}")

conn.close()
