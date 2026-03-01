import sqlite3

conn = sqlite3.connect('data/trades.db')
c = conn.cursor()

c.execute('SELECT SUM(pnl_pct) FROM paper_l4_grid WHERE created_at >= date(\'now\')')
today_profit = c.fetchone()[0] or 0

c.execute('SELECT SUM(pnl_pct) FROM paper_l4_grid')
total_profit = c.fetchone()[0] or 0

print(f'오늘 L4 수익: {today_profit:.4f}%')
print(f'전체 누적 수익: {total_profit:.4f}%')

conn.close()
