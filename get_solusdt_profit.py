import sqlite3

conn = sqlite3.connect('data/trades.db')
c = conn.cursor()

symbol = "SOLUSDT" # 실시간 거래 대상 심볼로 가정

c.execute('SELECT SUM(pnl_pct) FROM paper_l4_grid WHERE symbol = ? AND created_at >= date(\'now\')', (symbol,))
today_profit = c.fetchone()[0] or 0

c.execute('SELECT SUM(pnl_pct) FROM paper_l4_grid WHERE symbol = ?', (symbol,))
total_profit = c.fetchone()[0] or 0

print(f'오늘 L4 수익 ({symbol}): {today_profit:.4f}%')
print(f'전체 누적 수익 ({symbol}): {total_profit:.4f}%')

conn.close()
