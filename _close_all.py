"""모든 포지션 청산 + 미체결 주문 취소"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from engines.binance_executor import BinanceExecutor

ex = BinanceExecutor(use_testnet=False)

# 1. 미체결 주문 전부 취소
for sym in ["SOLUSDT", "BTCUSDT", "ETHUSDT"]:
    orders = ex.get_open_orders(sym)
    if orders:
        print(f"\n[{sym}] 미체결 주문 {len(orders)}건 일괄 취소")
        ex.cancel_all_orders(sym)
    else:
        print(f"[{sym}] 미체결 주문 없음")

# 2. 오픈 포지션 시장가 청산
positions = ex.get_positions()
if positions:
    print(f"\n오픈 포지션 {len(positions)}건 청산")
    for p in positions:
        sym = p["symbol"]
        amt = float(p["positionAmt"])
        entry = float(p["entryPrice"])
        pnl = float(p["unRealizedProfit"])
        if amt > 0:
            # LONG → SELL로 청산
            print(f"  {sym} LONG {amt} @ ${entry:,.2f} (PnL: ${pnl:,.2f}) → SELL 청산")
            result = ex.place_market_order(sym, "SELL", abs(amt))
        elif amt < 0:
            # SHORT → BUY로 청산
            print(f"  {sym} SHORT {abs(amt)} @ ${entry:,.2f} (PnL: ${pnl:,.2f}) → BUY 청산")
            result = ex.place_market_order(sym, "BUY", abs(amt))
        if result:
            print(f"  체결 완료: {result.get('status', 'N/A')}")
        else:
            print(f"  체결 실패!")
else:
    print("\n오픈 포지션 없음")

# 3. 최종 확인
print("\n--- 정리 후 상태 ---")
total = ex.get_total_balance()
avail = ex.get_account_balance()
print(f"총 잔고: ${total:,.2f} | 가용: ${avail:,.2f}")
remaining = ex.get_positions()
print(f"잔여 포지션: {len(remaining)}건")
for sym in ["SOLUSDT", "BTCUSDT", "ETHUSDT"]:
    o = ex.get_open_orders(sym)
    if o:
        print(f"  {sym} 잔여 주문: {len(o)}건")
