import sys, os
import time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import check_data_freshness
from config import SYMBOLS

print("=== 데이터 신선도 확인 (Data Freshness Check) ===")
for symbol in SYMBOLS:
    freshness = check_data_freshness(symbol)
    print(f"\n[{symbol}] 데이터 신선도:")
    for key, data in freshness.items():
        status = "오래됨" if data['stale'] else "최신"
        age_str = f"{data['age_seconds']}초 전" if data['age_seconds'] is not None else "데이터 없음"
        display_key = key.replace("_", " ").title()
        print(f"  - {display_key}: {status} ({age_str})")
