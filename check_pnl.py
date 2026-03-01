"""Grid V2 수익률 확인 스크립트"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from db import get_connection

conn = get_connection()

print("=== Grid V2 주문 로그 (최근 20건) ===")
rows = conn.execute(
    "SELECT side, grid_price, quantity, limit_price, status, fill_price, pnl_usd, created_at "
    "FROM grid_order_log ORDER BY id DESC LIMIT 20"
).fetchall()
for r in rows:
    ts = r[7][:16] if r[7] else "N/A"
    print(f"  {ts} | {r[0]:4s} | grid=${r[1]} | qty={r[2]} | limit=${r[3]} | "
          f"status={r[4]} | fill={r[5]} | pnl=${r[6]}")

print()
print("=== Grid 레벨 상태 (SOLUSDT) ===")
rows2 = conn.execute(
    "SELECT grid_price, status, quantity, buy_fill_price, buy_order_id, sell_order_id "
    "FROM grid_positions WHERE symbol='SOLUSDT' ORDER BY grid_price"
).fetchall()
for r in rows2:
    print(f"  ${r[0]:>8.2f} | {r[1]:10s} | qty={r[2]} | buy_fill={r[3]} | "
          f"buy_oid={r[4]} | sell_oid={r[5]}")

print()
print("=== 일일 PnL ===")
rows3 = conn.execute(
    "SELECT trade_date, realized_pnl, total_orders, circuit_breaker_hit "
    "FROM live_daily_pnl"
).fetchall()
if rows3:
    for r in rows3:
        print(f"  {r[0]} | realized={r[1]:+.4f}% | orders={r[2]} | cb={r[3]}")
else:
    print("  (아직 PnL 기록 없음)")

print()
print("=== SELL 체결 합계 ===")
total = conn.execute(
    "SELECT COUNT(*), SUM(pnl_usd) FROM grid_order_log "
    "WHERE side='SELL' AND status='FILLED'"
).fetchone()
print(f"  SELL 체결: {total[0]}건 | 합계 PnL: ${total[1] or 0:+.4f}")

print()
print("=== Binance 오픈 주문 ===")
try:
    from engines.binance_executor import BinanceExecutor
    from config import LIVE_USE_TESTNET
    ex = BinanceExecutor(use_testnet=LIVE_USE_TESTNET)
    orders = ex.get_open_orders("SOLUSDT")
    print(f"  오픈 주문 {len(orders)}건")
    for o in orders:
        print(f"  orderId={o['orderId']} | {o['side']} | price={o['price']} | "
              f"qty={o['origQty']} | status={o['status']} | cid={o.get('clientOrderId','')[:30]}")

    positions = ex.get_positions("SOLUSDT")
    print(f"\n=== 오픈 포지션 ===")
    if positions:
        for p in positions:
            print(f"  {p['symbol']}: {float(p['positionAmt']):.4f} @ ${float(p['entryPrice']):,.2f} "
                  f"| PnL ${float(p['unRealizedProfit']):,.4f}")
    else:
        print("  포지션 없음")

    balance = ex.get_account_balance()
    print(f"\n  잔고: ${balance:,.2f}")
except Exception as e:
    print(f"  Binance 조회 실패: {e}")

conn.close()
