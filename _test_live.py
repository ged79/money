import sys, os, traceback
os.chdir(r'C:\Users\lungg\.openclaw\workspace\money')
sys.path.insert(0, '.')
os.environ['LIVE_TRADING_ENABLED'] = 'true'
os.environ['LIVE_USE_TESTNET'] = 'false'

try:
    from engines.live_trader import run_live_trader
    print("Calling run_live_trader()...")
    run_live_trader()
    print("Done.")
except Exception as e:
    print(f"ERROR: {e}")
    traceback.print_exc()
