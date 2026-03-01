import sqlite3

conn = sqlite3.connect('data/trades.db')
c = conn.cursor()

c.execute('SELECT grid_count FROM grid_configs ORDER BY id DESC LIMIT 1')
grid_count = c.fetchone()

if grid_count:
    print(f'현재 그리드 개수: {grid_count[0]}개')
else:
    print('현재 활성화된 그리드 설정을 찾을 수 없습니다.')

conn.close()
