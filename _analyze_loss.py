"""손실 원인 분석 — 매매 내역 + 수집 데이터 대조"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from db import get_connection
from datetime import datetime

conn = get_connection()

# ========================================
# 1. 매매 로그 전체 (grid_order_log)
# ========================================
print("=" * 70)
print("  1. 그리드 주문 로그 (grid_order_log)")
print("=" * 70)

# 테이블 존재 확인
tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
print(f"  DB 테이블: {', '.join(sorted(tables))}\n")

if "grid_order_log" in tables:
    cols = [c[1] for c in conn.execute("PRAGMA table_info(grid_order_log)").fetchall()]
    print(f"  columns: {cols}")
    rows = conn.execute("SELECT * FROM grid_order_log ORDER BY id DESC").fetchall()
    print(f"  총 {len(rows)}건\n")
    for r in rows:
        row_dict = dict(zip(cols, r))
        print(f"  [{row_dict.get('created_at','')}] {row_dict.get('symbol','')} "
              f"{row_dict.get('side','')} {row_dict.get('order_type','')} "
              f"qty={row_dict.get('quantity','')} @ ${float(row_dict.get('price',0)):,.2f} "
              f"grid_idx={row_dict.get('grid_index','')} "
              f"orderId={row_dict.get('order_id','')} "
              f"status={row_dict.get('status','')}")
else:
    print("  grid_order_log 테이블 없음")

# ========================================
# 2. live_grid_state (포지션 추적)
# ========================================
print("\n" + "=" * 70)
print("  2. 라이브 그리드 상태 (live_grid_state)")
print("=" * 70)

if "live_grid_state" in tables:
    cols = [c[1] for c in conn.execute("PRAGMA table_info(live_grid_state)").fetchall()]
    print(f"  columns: {cols}")
    rows = conn.execute("SELECT * FROM live_grid_state ORDER BY id").fetchall()
    print(f"  총 {len(rows)}건\n")
    for r in rows:
        row_dict = dict(zip(cols, r))
        print(f"  {row_dict}")
else:
    print("  live_grid_state 테이블 없음")

# ========================================
# 3. live_daily_pnl (일일 손익)
# ========================================
print("\n" + "=" * 70)
print("  3. 일일 손익 (live_daily_pnl)")
print("=" * 70)

if "live_daily_pnl" in tables:
    rows = conn.execute("SELECT * FROM live_daily_pnl ORDER BY trade_date").fetchall()
    cols = [c[1] for c in conn.execute("PRAGMA table_info(live_daily_pnl)").fetchall()]
    for r in rows:
        row_dict = dict(zip(cols, r))
        print(f"  {row_dict.get('trade_date')} | realized={row_dict.get('realized_pnl',0):+.4f}% "
              f"| unrealized={row_dict.get('unrealized_pnl',0):+.4f}% "
              f"| orders={row_dict.get('total_orders',0)} "
              f"| CB={'HIT' if row_dict.get('circuit_breaker_hit') else 'OK'} "
              f"| start_bal={row_dict.get('starting_balance',0)}")

# ========================================
# 4. signal_log (시그널 이력)
# ========================================
print("\n" + "=" * 70)
print("  4. 시그널 로그")
print("=" * 70)

rows = conn.execute(
    "SELECT created_at, signal_type, symbol, direction, ssm_score "
    "FROM signal_log ORDER BY id DESC LIMIT 20"
).fetchall()
for r in rows:
    score = f" score={r[4]:.2f}" if r[4] is not None else ""
    print(f"  {r[0]} | {r[1]} | {r[2]} {r[3]}{score}")

# ========================================
# 5. paper_trades (페이퍼 매매)
# ========================================
print("\n" + "=" * 70)
print("  5. 페이퍼 트레이드")
print("=" * 70)

if "paper_trades" in tables:
    cols = [c[1] for c in conn.execute("PRAGMA table_info(paper_trades)").fetchall()]
    rows = conn.execute("SELECT * FROM paper_trades ORDER BY id DESC LIMIT 20").fetchall()
    print(f"  총 {len(rows)}건 | columns: {cols}\n")
    for r in rows:
        row_dict = dict(zip(cols, r))
        pnl = row_dict.get('pnl_pct', 0)
        pnl_str = f" PnL={pnl:+.4f}%" if pnl else ""
        print(f"  [{row_dict.get('created_at','')}] {row_dict.get('symbol','')} "
              f"{row_dict.get('side','')} @ ${float(row_dict.get('price',0)):,.2f} "
              f"grid#{row_dict.get('grid_id','')}{pnl_str}")

# ========================================
# 6. SOLUSDT 가격 추이 (5분봉 최근 24시간)
# ========================================
print("\n" + "=" * 70)
print("  6. SOLUSDT 5분봉 가격 추이 (최근 24시간)")
print("=" * 70)

rows = conn.execute(
    "SELECT open_time, open, high, low, close, volume "
    "FROM klines WHERE symbol='SOLUSDT' AND interval='5m' "
    "ORDER BY open_time DESC LIMIT 288"
).fetchall()
if rows:
    prices = [r[4] for r in rows]
    print(f"  최신: ${prices[0]:,.2f} | 최고: ${max(prices):,.2f} | 최저: ${min(prices):,.2f}")
    print(f"  24h전: ${prices[-1]:,.2f} → 현재: ${prices[0]:,.2f} ({(prices[0]-prices[-1])/prices[-1]*100:+.2f}%)")
    # 시간대별 가격 (3시간 간격으로 샘플링)
    print(f"\n  시간대별 가격:")
    for i in range(0, len(rows), 36):  # 36 * 5분 = 3시간
        r = rows[i]
        ts = datetime.fromtimestamp(r[0]/1000).strftime("%m/%d %H:%M")
        print(f"    {ts} | O=${r[1]:,.2f} H=${r[2]:,.2f} L=${r[3]:,.2f} C=${r[4]:,.2f} | Vol={r[5]:,.0f}")

# ========================================
# 7. 그리드 설정 이력 (SOLUSDT)
# ========================================
print("\n" + "=" * 70)
print("  7. SOLUSDT 그리드 설정 이력")
print("=" * 70)

rows = conn.execute(
    "SELECT id, lower_bound, upper_bound, grid_count, grid_spacing_pct, calculated_at "
    "FROM grid_configs WHERE symbol='SOLUSDT' ORDER BY id DESC LIMIT 10"
).fetchall()
for r in rows:
    print(f"  [{r[5]}] ${r[1]:,.2f} - ${r[2]:,.2f} | {r[3]} grids ({r[4]:.2f}%)")

# ========================================
# 8. ATR 이력 (SOLUSDT)
# ========================================
print("\n" + "=" * 70)
print("  8. SOLUSDT ATR 이력")
print("=" * 70)

rows = conn.execute(
    "SELECT atr, atr_pct, stop_loss_pct, current_price, calculated_at "
    "FROM atr_values WHERE symbol='SOLUSDT' ORDER BY id DESC LIMIT 5"
).fetchall()
for r in rows:
    print(f"  [{r[4]}] ATR=${r[0]:,.2f} ({r[1]:.2f}%) SL={r[2]:.2f}% 현재가=${r[3]:,.2f}")

# ========================================
# 9. Threshold 이력 (SOLUSDT)
# ========================================
print("\n" + "=" * 70)
print("  9. SOLUSDT Threshold 이력 (최근 20건)")
print("=" * 70)

rows = conn.execute(
    "SELECT trigger_active, liq_amount_1h, threshold_value, direction, calculated_at "
    "FROM threshold_signals WHERE symbol='SOLUSDT' ORDER BY id DESC LIMIT 20"
).fetchall()
trigger_count = sum(1 for r in rows if r[0])
print(f"  최근 20건 중 trigger ON: {trigger_count}건")
for r in rows[:10]:
    print(f"  [{r[4]}] trigger={'ON' if r[0] else 'OFF'} | 1h청산=${r[1]:,.0f} | thresh={r[2]:.6f} | {r[3] or '-'}")

conn.close()
