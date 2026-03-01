"""데이터 수집 현황 확인 v2 — 실제 테이블 기반"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from db import get_connection

conn = get_connection()

# 1. 모든 테이블 목록
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
print("=== DB 테이블 목록 ===")
for t in tables:
    print(f"  {t[0]}")

print("\n" + "=" * 80)
print(f"{'테이블':<30} {'건수':>8} {'최초':>22} {'최근':>22}")
print("=" * 80)

for (table,) in tables:
    try:
        count = conn.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]
        # 타임스탬프 컬럼 자동 탐지
        cols = [c[1] for c in conn.execute(f"PRAGMA table_info([{table}])").fetchall()]
        ts_col = None
        for candidate in ["collected_at", "calculated_at", "decided_at", "timestamp", "created_at", "time", "ts", "open_time"]:
            if candidate in cols:
                ts_col = candidate
                break
        if count > 0 and ts_col:
            first = conn.execute(f"SELECT MIN([{ts_col}]) FROM [{table}]").fetchone()[0]
            last = conn.execute(f"SELECT MAX([{ts_col}]) FROM [{table}]").fetchone()[0]
        else:
            first = last = "-"
        print(f"{table:<30} {count:>8,} {str(first)[:22]:>22} {str(last)[:22]:>22}")
    except Exception as e:
        print(f"{table:<30} {'ERR':>8} {str(e)[:44]}")

# 심볼별 세부 현황 (kline 관련)
print("\n--- 캔들 데이터 심볼별 ---")
for table in [t[0] for t in tables]:
    if "kline" in table.lower() or "candle" in table.lower() or "5m" in table.lower():
        cols = [c[1] for c in conn.execute(f"PRAGMA table_info([{table}])").fetchall()]
        if "symbol" in cols:
            syms = conn.execute(f"SELECT symbol, COUNT(*) FROM [{table}] GROUP BY symbol").fetchall()
            for s, c in syms:
                print(f"  {table}.{s}: {c:,}건")

conn.close()
