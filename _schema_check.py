import sqlite3
conn = sqlite3.connect(r"C:\Users\lungg\.openclaw\workspace\money\trading.db")
for t in ["onchain_metrics", "liquidations", "orderbook_walls", "whale_trades"]:
    cols = conn.execute(f"PRAGMA table_info({t})").fetchall()
    print(f"{t}: {[c[1]+':'+c[2] for c in cols]}")
conn.close()
