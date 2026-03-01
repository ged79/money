import sys, sqlite3
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
conn = sqlite3.connect(r"C:\Users\lungg\.openclaw\workspace\money\data\trades.db")
for t in ["atr_values", "threshold_signals"]:
    cols = conn.execute(f"PRAGMA table_info({t})").fetchall()
    print(f"{t}: {[c[1] for c in cols]}")
    row = conn.execute(f"SELECT * FROM {t} ORDER BY id DESC LIMIT 1").fetchone()
    if row: print(f"  sample: {row}")
conn.close()
