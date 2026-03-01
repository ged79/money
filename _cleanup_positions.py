"""기존 주문/포지션 전부 정리 → 깨끗한 상태로 재시작"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from engines.binance_executor import BinanceExecutor
from db import get_connection

ex = BinanceExecutor(use_testnet=False)
symbol = "SOLUSDT"

# 1. 모든 오픈 주문 취소
print("=== 1. 오픈 주문 전체 취소 ===")
ex.cancel_all_orders(symbol)
print("완료")

# 2. 포지션 시장가 청산
print("\n=== 2. 포지션 청산 ===")
positions = ex.get_positions()
for p in positions:
    if p["symbol"] == symbol:
        amt = float(p.get("positionAmt", 0))
        if amt != 0:
            side = "BUY" if amt < 0 else "SELL"
            qty = abs(amt)
            print(f"  {symbol}: {amt:+.4f} → {side} {qty} 시장가 청산")
            result = ex.place_market_order(symbol, side, qty)
            print(f"  결과: {result}")
        else:
            print(f"  {symbol}: 포지션 없음")

# 3. DB 그리드 초기화
print("\n=== 3. DB 그리드 초기화 ===")
conn = get_connection()
conn.execute("DELETE FROM grid_positions WHERE symbol = ?", (symbol,))
conn.commit()
print(f"  grid_positions 삭제 완료")
conn.close()

# 4. 최종 상태 확인
print("\n=== 4. 최종 상태 ===")
balance = ex.get_account_balance()
print(f"  가용 잔고: ${balance:.4f}")
orders = ex.get_open_orders(symbol)
print(f"  오픈 주문: {len(orders)}개")
