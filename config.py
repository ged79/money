"""설정 모듈 v8.1 — SSM+T 추세 트레이딩 (진짜 돈 데이터만)"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

# === 바이낸스 API ===
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY", "")
BINANCE_FUTURES_BASE = "https://fapi.binance.com"
BINANCE_WS_BASE = "wss://fstream.binance.com/ws"

# === 외부 API ===
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# === 운용 심볼 ===
SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT",       # 대장 + L1
    "AVAXUSDT", "SUIUSDT",                  # L1
    "DOGEUSDT", "1000PEPEUSDT",              # 밈
    "AAVEUSDT", "LINKUSDT",                 # DeFi
    "ARBUSDT",                              # L2
]

# === DB ===
DB_PATH = Path(__file__).parent / "data" / "trades.db"

# === 수집 주기 (초) ===
OI_INTERVAL = 300
FUNDING_INTERVAL = 28800
LONG_SHORT_INTERVAL = 3600
ORDERBOOK_INTERVAL = 3600
KLINES_DAILY_INTERVAL = 86400
KLINES_5M_INTERVAL = 300
KLINES_4H_INTERVAL = 14400
KLINES_1H_INTERVAL = 3600
TAKER_RATIO_INTERVAL = 300
TOP_LS_RATIO_INTERVAL = 300

# === WebSocket ===
WS_RECONNECT_ATTEMPTS = 3
WS_RECONNECT_DELAY = 10

# === 오더북 ===
ORDERBOOK_DEPTH_LIMIT = 1000
ORDERBOOK_WALL_PERCENTILE = 90

# === 수수료 (Binance Futures) ===
MAKER_FEE_RATE = 0.0002    # 0.02%
TAKER_FEE_RATE = 0.0004    # 0.04%

# === Volume Profile 설정 ===
VP_DAILY_LOOKBACK = 45
VP_4H_LOOKBACK = 42
VP_1H_LOOKBACK = 72
VP_5M_LOOKBACK = 36
VP_BUCKETS = 50
VP_VALUE_AREA_PCT = 0.70
VP_COMPOSITE_WEIGHTS = {
    "daily": 0.35,
    "4h": 0.30,
    "1h": 0.25,
    "5m": 0.10,
}
VP_VOLUME_BREAK_RATIO = 1.5

# === SSM+T 트레이딩 (v8.1) ===
TRADE_CYCLE_INTERVAL = 30          # 매매 루프 주기 (초)
MAX_CONCURRENT_POSITIONS = 2       # 동시 포지션 수 (상위 2개 진입)
POSITION_PCT = 0.80                # 포지션 크기: 자본의 80% (롱 집중, 20% 숏 여유)
COOLDOWN_AFTER_STOP = 600          # 손절 후 쿨다운 10분
MAX_HOLD_DURATION = 86400          # 최대 보유 24시간

# SSM 방향 판정
SSM_VETO_CONFIDENCE = 4            # SSM 반대 conf≥4 → 진입 불가
SSM_MIN_CONFIDENCE = 3             # SSM 방향 확인 최소 확신도

# 펀딩비 극단 (Sentiment)
FUNDING_EXTREME_HIGH = 0.0005      # 0.05% 이상 → 과열 (숏 유리)
FUNDING_EXTREME_LOW = -0.0001      # -0.01% 이하 → 과매도 (롱 유리)

# VP 매물대 진입/손절
VP_SUPPORT_ENTRY_PCT = 0.01        # VA low/HVN ±1.0% 이내 → 진입 고려
VP_STOP_BUFFER_PCT = 0.015         # 매물대 이탈 +1.5% → 손절 (SOL 변동성 감안)
TRAIL_PCT = 0.015                  # 피크에서 1.5% 후퇴 → 트레일링 청산

# === Profit Ratchet (수익 래칫) ===
RATCHET_THRESHOLDS = [
    (200, 0.50),
    (500, 0.70),
    (1000, 0.80),
]

# === 미실현 수익 래칫 (ROI% 기준) ===
UNREALIZED_RATCHET = [
    (8.0,  4.0),
    (12.0, 8.0),
    (18.0, 12.0),
]
UNREALIZED_RATCHET_CLOSE_PCT = 0.5
UNREALIZED_STOP_LOSS = -8.0

# === Circuit Breaker ===
DAILY_LOSS_LIMIT = -3.0
UNREALIZED_LOSS_WARN = -2.0
UNREALIZED_LOSS_REDUCE = -4.0

# === 라이브 트레이딩 ===
LIVE_TRADING_ENABLED = os.getenv("LIVE_TRADING_ENABLED", "true").lower() == "true"
LIVE_USE_TESTNET = os.getenv("LIVE_USE_TESTNET", "false").lower() == "true"
LIVE_LEVERAGE = 3

# === Whale Alert API ===
WHALE_ALERT_API_KEY = os.getenv("WHALE_ALERT_API_KEY", "")

# === Gemini API (SSM 고래/매크로) ===
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = "gemini-2.0-flash"
GEMINI_DAILY_LIMIT = 50
SSM_INTERVAL = 14400               # 4시간마다

# === ATR (레거시 호환) ===
ATR_STOP_LOSS_MULTIPLIER = 1.5

# 테스트넷
BINANCE_TESTNET_BASE = "https://testnet.binancefuture.com"
BINANCE_TESTNET_API_KEY = os.getenv("BINANCE_TESTNET_API_KEY", "")
BINANCE_TESTNET_SECRET_KEY = os.getenv("BINANCE_TESTNET_SECRET_KEY", "")
