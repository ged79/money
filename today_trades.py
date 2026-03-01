import sqlite3
from datetime import datetime, timedelta

from config import DB_PATH
conn = sqlite3.connect(str(DB_PATH))
c = conn.cursor()

# '실거래 시작' 시점으로 가정하는 한국 시간 2026-02-27 06:20:00 (UTC 2026-02-26 21:20:00)
start_time_utc = datetime(2026, 2, 26, 21, 20, 0)

c.execute('SELECT created_at, symbol, side, price, pnl_pct, status FROM live_orders WHERE created_at >= ? ORDER BY created_at DESC LIMIT 10', (start_time_utc.isoformat(),))
trades = c.fetchall()

print(f'오늘 2026-02-27 06:20 (한국 시간) 이후 라이브 거래 내역 (최근 10건):')
if not trades:
    print('  거래 내역이 없습니다.')
for trade in trades:
    utc_time_str = trade[0]
    utc_time = datetime.fromisoformat(utc_time_str.replace("Z", "+00:00"))
    kst_time = utc_time + timedelta(hours=9)
    kst_time_str = kst_time.strftime("%Y-%m-%d %H:%M:%S")

    print(f'- 시간: {kst_time_str} (한국 시간), 종목: {trade[1]}, 유형: {trade[2]}, 가격: {trade[3]}, 수익률: {trade[4]:.2f}%, 상태: {trade[5]}')

conn.close()
