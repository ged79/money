import sqlite3

conn = sqlite3.connect('data/trades.db')
c = conn.cursor()

c.execute("PRAGMA table_info(paper_l4_grid)")
columns = c.fetchall()

print('paper_l4_grid 테이블 컬럼 정보:')
for col in columns:
    print(col)

conn.close()
