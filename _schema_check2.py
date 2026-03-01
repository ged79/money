import sqlite3
conn = sqlite3.connect(r"C:\Users\lungg\.openclaw\workspace\money\trading.db")
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
print("전체 테이블:")
for t in tables:
    cnt = conn.execute(f"SELECT COUNT(*) FROM [{t[0]}]").fetchone()[0]
    print(f"  {t[0]}: {cnt}건")
conn.close()
