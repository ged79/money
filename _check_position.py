import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
from engines.binance_executor import BinanceExecutor

ex = BinanceExecutor(use_testnet=False)

# 전체 잔고
balance = ex.get_account_balance()
print(f"Available Balance: ${balance:.4f}")

# 포지션
positions = ex.get_positions()
for p in positions:
    amt = float(p.get("positionAmt", 0))
    if amt != 0:
        pnl = float(p.get("unRealizedProfit", 0))
        entry = float(p.get("entryPrice", 0))
        leverage = p.get("leverage", "?")
        margin = float(p.get("initialMargin", 0))
        print(f"\n{p['symbol']}:")
        print(f"  포지션: {amt:+.4f} ({'LONG' if amt > 0 else 'SHORT'})")
        print(f"  진입가: ${entry:.2f}")
        print(f"  미실현PnL: ${pnl:+.4f}")
        print(f"  레버리지: {leverage}x")
        print(f"  마진: ${margin:.4f}")

# 계좌 총 자산
try:
    import requests, time, hashlib, hmac
    from config import BINANCE_API_KEY, BINANCE_SECRET_KEY
    ts = int(time.time() * 1000)
    qs = f"timestamp={ts}"
    sig = hmac.new(BINANCE_SECRET_KEY.encode(), qs.encode(), hashlib.sha256).hexdigest()
    r = requests.get(f"https://fapi.binance.com/fapi/v2/account?{qs}&signature={sig}",
                     headers={"X-MBX-APIKEY": BINANCE_API_KEY})
    data = r.json()
    print(f"\n=== 계좌 요약 ===")
    print(f"  총 자산: ${float(data.get('totalWalletBalance', 0)):.4f}")
    print(f"  미실현PnL: ${float(data.get('totalUnrealizedProfit', 0)):.4f}")
    print(f"  가용 잔고: ${float(data.get('availableBalance', 0)):.4f}")
    print(f"  총 마진: ${float(data.get('totalInitialMargin', 0)):.4f}")
except Exception as e:
    print(f"계좌 조회 오류: {e}")
