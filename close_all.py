"""포지션 전량 청산 + 오픈 주문 취소"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from engines.binance_executor import BinanceExecutor
from config import LIVE_USE_TESTNET

ex = BinanceExecutor(use_testnet=LIVE_USE_TESTNET)

symbol = "SOLUSDT"

# 1. 오픈 주문 전량 취소
print("=== 오픈 주문 취소 ===")
result = ex.cancel_all_orders(symbol)
print(f"  취소 결과: {result}")

# 2. 오픈 포지션 청산
print("\n=== 포지션 청산 ===")
positions = ex.get_positions(symbol)
for p in positions:
    amt = float(p["positionAmt"])
    if amt == 0:
        continue
    side = "SELL" if amt > 0 else "BUY"
    qty = abs(amt)
    print(f"  {symbol}: {amt:+.4f} → {side} {qty} MARKET")
    result = ex.place_market_order(symbol, side, qty)
    if result:
        avg_price = float(result.get("avgPrice", 0))
        print(f"  체결: ${avg_price:,.2f}")
    else:
        print(f"  실패!")

if not positions:
    print("  포지션 없음")

# 3. 최종 잔고
print("\n=== 최종 상태 ===")
balance = ex.get_account_balance()
print(f"  잔고: ${balance:,.2f}")
remaining_orders = ex.get_open_orders(symbol)
print(f"  남은 주문: {len(remaining_orders)}건")
remaining_pos = ex.get_positions(symbol)
if remaining_pos:
    for p in remaining_pos:
        print(f"  남은 포지션: {p['symbol']} {float(p['positionAmt']):+.4f}")
else:
    print("  남은 포지션: 없음")
