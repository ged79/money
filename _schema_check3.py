import sys, sqlite3
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
conn = sqlite3.connect(r"C:\Users\lungg\.openclaw\workspace\money\data\trades.db")
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
print("ALL TABLES:")
for t in tables:
    cnt = conn.execute(f"SELECT COUNT(*) FROM [{t[0]}]").fetchone()[0]
    print(f"  {t[0]}: {cnt} rows")

# onchain_metrics schema
for t_name in ["onchain_metrics", "liquidations", "orderbook_walls", "whale_trades"]:
    cols = conn.execute(f"PRAGMA table_info({t_name})").fetchall()
    if cols:
        print(f"\n{t_name} schema: {[c[1] for c in cols]}")
    else:
        print(f"\n{t_name}: TABLE NOT FOUND")
conn.close()
