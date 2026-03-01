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
KLINES_1W_INTERVAL = 86400      # 주봉: 매일 1회
KLINES_4H_INTERVAL = 14400      # 4시간봉: 매 4시간
KLINES_1H_INTERVAL = 3600       # 1시간봉: 매 1시간
MTF_ANALYSIS_INTERVAL = 3600    # MTF 분석: 매 1시간
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
L1_FUNDING_THRESHOLD = 0.0003   # 0.03% 이상 시 진입 (완화: 0.05→0.03)
L1_LS_RATIO_THRESHOLD = 0.60    # 롱비율 60% 이상 시 진입 (완화: 65→60)
L1_FUNDING_EXIT = 0.0001        # 0.01% 이하 시 청산 검토

# === L2 방향성 트레이딩 ===
L2_MAX_DIRECTION_CHANGES = 1     # 하루 최대 방향 전환 (축소: 2→1)
L2_STEP1_PCT = 0.15             # 1단계: 15% probe (축소: 30→15)
L2_STEP2_PCT = 0.25             # 2단계: +25%
L2_STEP3_PCT = 0.30             # 3단계: +30%
L2_STEP2_DELAY = 1800           # 30분 (초) (증가: 15→30분)
L2_STEP3_DELAY = 3600           # 60분 (초) (증가: 30→60분)
L2_TRIGGER_THRESHOLD_PCT = 0.001 # 청산 > OI×0.1% 시 트리거 (하향: 1%→0.1%)
L2_MIN_SSM_SCORE = 1.5          # L2 진입 최소 SSM 점수 (하향: 2.0→1.5)
L2_BREAKOUT_CONFIRM_CANDLES = 3  # breakout 확인 캔들 수 (5분×3=15분)
L2_TRAILING_STOP_ACTIVATE = 0.02  # trailing stop 활성화 수익률 (+2%)
L2_TRAILING_STOP_DISTANCE = 0.01  # trailing stop 거리 (1%)

# === L4 그리드 봇 ===
GRID_COUNT_MIN = 10
GRID_COUNT_MAX = 12
GRID_LEVERAGE_MAX = 2

# === 박스권 형성 감지 ===
BOX_PRICE_TOLERANCE = 0.02      # ±2%
BOX_DURATION_MIN = 14400        # 4시간 (초)
OI_RECOVERY_THRESHOLD = 0.80    # OI 80% 이상 회복

# === ATR 스톱로스 ===
ATR_STOP_LOSS_MULTIPLIER = 1.5  # ATR × 1.5

# === 수수료 설정 (Binance Futures) ===
MAKER_FEE_RATE = 0.0002   # 0.02%
TAKER_FEE_RATE = 0.0004   # 0.04%
L4_FEE_RATE = MAKER_FEE_RATE  # 그리드는 Limit 주문 (Maker)
L2_FEE_RATE = TAKER_FEE_RATE  # 방향성은 Market 주문 (Taker)
# 그리드 최소 간격: 왕복 수수료의 2배 (수수료 제하고도 수익 보장)
MIN_GRID_SPACING_PCT = L4_FEE_RATE * 2 * 100 * 4  # 0.16% (수수료 3배 보장, 기존 0.08%)

# ============================
# Phase 3: 라이브 트레이딩
# ============================
LIVE_TRADING_ENABLED = os.getenv("LIVE_TRADING_ENABLED", "true").lower() == "true"
LIVE_USE_TESTNET = os.getenv("LIVE_USE_TESTNET", "true").lower() == "true"
LIVE_SYMBOLS = ["SOLUSDT"]       # 소액이므로 SOL만
LIVE_LEVERAGE = 3                # 고정 3x
LIVE_DAILY_LOSS_LIMIT = -3.0     # -3% circuit breaker
LIVE_MAX_POSITION_PCT = 1.0      # 심볼당 자본 100%

# === Grid V2: LIMIT 주문 기반 그리드 ===
GRID_V2_CYCLE_INTERVAL = 30        # 메인 루프 주기 (초)
GRID_V2_WORKING_LEVELS = 3         # 현재가 ±N레벨에만 주문
GRID_V2_ORDER_TIMEOUT = 3600       # 미체결 주문 취소 (1시간)
GRID_V2_PRICE_TOLERANCE = 0.005    # 그리드 전환시 가격 매핑 허용오차 (0.5%)
GRID_V2_MAX_POSITION_PCT = 0.8     # 잔고의 80% 사용 (양방향 상쇄로 넷 노출 낮음)
GRID_V2_TREND_GUARD_PCT = 5.0      # 5% 이상 이동시 중단
GRID_V2_TREND_GUARD_WINDOW = 14400 # 4시간 (초)
GRID_V2_OOB_PAUSE_MINUTES = 30     # 범위 밖 30분 이상시 일시정지 (폴백)
GRID_V2_OOB_VOLUME_MULTIPLIER = 2.0  # 거래량 평균 대비 N배 이상이면 즉시 이탈 판정
GRID_V2_OOB_LIQ_THRESHOLD = 50000   # 청산금액 $N 이상이면 이탈 보강 신호 (USD)
GRID_V2_ENTRY_OFFSET_ATR_RATIO = 0.08  # 진입 오프셋: ATR%의 8% (체결률 향상, 기존 15%)
GRID_V2_ENTRY_OFFSET_MAX_SPACING_PCT = 0.5  # 오프셋 상한: 그리드 간격의 50%
GRID_V2_HOLDING_STOP_ATR_MULT = 1.5   # HOLDING 스톱로스: ATR × 1.5 (폴백: 진입가의 3%)
GRID_V2_MAX_NET_LEVELS = 1             # 넷포지션 한도: LONG-SHORT 레벨 차이 최대 ±1 (편향 최소화)

# === 하이브리드: Grid ↔ L2 전환 ===
HYBRID_L2_MIN_SSM = 1.5             # L2 진입 최소 SSM 점수 (하향: 2.0→1.5)
HYBRID_L2_STOP_LOSS_PCT = -1.5      # L2 스톱로스 (-1.5%)
HYBRID_L2_TRAILING_ACTIVATE = 1.0   # 트레일링 스탑 활성화 수익률 (1%)
HYBRID_L2_TRAILING_DISTANCE = 0.5   # 트레일링 스탑 간격 (최고점 - 0.5%)
HYBRID_L2_MAX_DURATION = 14400      # L2 최대 유지 시간 (4시간, 초)
HYBRID_L2_ENABLED = True            # 하이브리드 전환 활성화

# 테스트넷 API 키 (메인넷 키와 별도)
BINANCE_TESTNET_BASE = "https://testnet.binancefuture.com"
BINANCE_TESTNET_API_KEY = os.getenv("BINANCE_TESTNET_API_KEY", "")
BINANCE_TESTNET_SECRET_KEY = os.getenv("BINANCE_TESTNET_SECRET_KEY", "")
