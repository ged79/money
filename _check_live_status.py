"""현재 라이브 포지션/잔고/미체결 주문 조회"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from engines.binance_executor import BinanceExecutor

ex = BinanceExecutor(use_testnet=False)

# 총 잔고
total = ex.get_total_balance()
avail = ex.get_account_balance()
print(f"총 잔고: ${total:,.2f}")
print(f"가용 잔고: ${avail:,.2f}")
print(f"마진 사용: ${total - avail:,.2f}")

# 포지션
positions = ex.get_positions()
print(f"\n오픈 포지션: {len(positions)}건")
total_pnl = 0
for p in positions:
    sym = p["symbol"]
    amt = float(p["positionAmt"])
    entry = float(p["entryPrice"])
    mark = float(p.get("markPrice", 0))
    pnl = float(p["unRealizedProfit"])
    lev = p.get("leverage", "?")
    side = "LONG" if amt > 0 else "SHORT"
    notional = abs(amt) * entry
    pnl_pct = (pnl / notional * 100) if notional > 0 else 0
    total_pnl += pnl
    print(f"  {sym} {side} | qty: {abs(amt)} | entry: ${entry:,.2f} | mark: ${mark:,.2f} | PnL: ${pnl:,.2f} ({pnl_pct:+.2f}%) | {lev}x")

print(f"\n미실현 총 PnL: ${total_pnl:,.2f}")
if total > 0:
    print(f"총 수익률: {total_pnl/total*100:+.2f}%")

# 오픈 주문
for sym in ["SOLUSDT", "BTCUSDT", "ETHUSDT"]:
    orders = ex.get_open_orders(sym)
    if orders:
        print(f"\n[{sym} 미체결 주문] {len(orders)}건")
        for o in orders:
            print(f"  id={o['orderId']} | {o['side']} {o['type']} | qty={o['origQty']} @ ${float(o['price']):,.2f} | {o['status']}")
    else:
        print(f"\n[{sym} 미체결 주문] 없음")
