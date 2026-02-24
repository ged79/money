"""백테스트 설정 파라미터"""
from pathlib import Path

BT_DAYS = 90                    # 90일 백테스트 윈도우
BT_STEP_SECONDS = 300           # 5분 단위 시간 스텝 (= 5m 캔들 간격)
BT_DB_PATH = Path(__file__).parent.parent / "data" / "backtest.db"
BT_SYMBOLS = ["BTCUSDT"]       # BTC만 (속도 우선)
BT_INITIAL_CAPITAL = 10000     # $10,000 가상 자본
BT_LOG_INTERVAL = 86400        # 24시간 시뮬레이션마다 일별 요약 출력
