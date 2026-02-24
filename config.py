"""설정 모듈 — .env 로드 + 상수 정의"""
import os
from pathlib import Path
from dotenv import load_dotenv

# .env 로드
load_dotenv(Path(__file__).parent / ".env")

# === 바이낸스 API ===
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY", "")
BINANCE_FUTURES_BASE = "https://fapi.binance.com"
BINANCE_WS_BASE = "wss://fstream.binance.com/ws"

# === 외부 API ===
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
WHALE_ALERT_API_KEY = os.getenv("WHALE_ALERT_API_KEY", "")

# === 운용 자산 ===
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

# === 수집 주기 (초) ===
OI_INTERVAL = 3600          # OI: 매 1시간
FUNDING_INTERVAL = 28800    # 펀딩비: 매 8시간
LONG_SHORT_INTERVAL = 3600  # 롱숏비율: 매 1시간
ORDERBOOK_INTERVAL = 14400  # 오더북: 매 4시간
KLINES_DAILY_INTERVAL = 86400   # 일봉(ATR): 매일 1회
KLINES_5M_INTERVAL = 300        # 5분봉(실시간 가격): 5분마다
FEAR_GREED_INTERVAL = 21600 # 공포/탐욕: 매 6시간
MACRO_CHECK_INTERVAL = 3600 # 매크로 이벤트 체크: 매 1시간

# === WebSocket 재연결 ===
WS_RECONNECT_ATTEMPTS = 3
WS_RECONNECT_DELAY = 10  # 초

# === DB 경로 ===
DB_PATH = Path(__file__).parent / "data" / "trades.db"

# === 오더북 설정 ===
ORDERBOOK_DEPTH_LIMIT = 1000  # API weight 50 (500과 동일)
ORDERBOOK_WALL_PERCENTILE = 90  # 상위 10% = 벽 후보

# ============================
# Phase 2: 엔진 설정
# ============================

# === 엔진 실행 주기 (초) ===
ATR_INTERVAL = 86400             # ATR: 매일 1회
THRESHOLD_INTERVAL = 300         # 동적 임계점: 5분
GRID_INTERVAL = 14400            # 그리드 범위: 4시간
SSM_SCORE_INTERVAL = 600         # SSM+V+T 스코어: 10분
STRATEGY_INTERVAL = 60           # 전략 매니저: 1분
MACRO_GUARD_INTERVAL = 300       # 매크로 가드: 5분

# === Gemini LLM 설정 ===
GEMINI_DAILY_LIMIT = 25          # 일일 호출 한도 (250 무료 중 10%)
GEMINI_MODEL = "gemini-2.5-flash"

# === L1 델타 뉴트럴 ===
L1_FUNDING_THRESHOLD = 0.0005   # 0.05% 이상 시 진입
L1_LS_RATIO_THRESHOLD = 0.65    # 롱비율 65% 이상 시 진입
L1_FUNDING_EXIT = 0.0001        # 0.01% 이하 시 청산 검토

# === L2 방향성 트레이딩 ===
L2_MAX_DIRECTION_CHANGES = 2     # 하루 최대 방향 전환
L2_STEP1_PCT = 0.30             # 1단계: 30%
L2_STEP2_PCT = 0.30             # 2단계: +30%
L2_STEP3_PCT = 0.40             # 3단계: +40%
L2_STEP2_DELAY = 900            # 15분 (초)
L2_STEP3_DELAY = 1800           # 30분 (초)
L2_TRIGGER_THRESHOLD_PCT = 0.01 # 청산 > OI×1% 시 트리거

# === L4 그리드 봇 ===
GRID_COUNT_MIN = 10
GRID_COUNT_MAX = 15
GRID_LEVERAGE_MAX = 2

# === 박스권 형성 감지 ===
BOX_PRICE_TOLERANCE = 0.02      # ±2%
BOX_DURATION_MIN = 14400        # 4시간 (초)
OI_RECOVERY_THRESHOLD = 0.80    # OI 80% 이상 회복

# === ATR 스톱로스 ===
ATR_STOP_LOSS_MULTIPLIER = 1.5  # ATR × 1.5
