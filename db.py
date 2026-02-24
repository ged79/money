"""SQLite 데이터베이스 초기화 + 헬퍼 함수"""
import sqlite3
from pathlib import Path
from config import DB_PATH


def get_connection() -> sqlite3.Connection:
    """동기 SQLite 연결 반환"""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")  # 동시 읽기 성능 향상
    return conn


def init_db():
    """모든 테이블 생성 (없으면 생성)"""
    conn = get_connection()
    cursor = conn.cursor()

    # ① 청산 이벤트 (WebSocket forceOrder)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS liquidations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            price REAL NOT NULL,
            qty REAL NOT NULL,
            trade_time INTEGER NOT NULL,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_liq_symbol_time
        ON liquidations(symbol, trade_time)
    """)

    # ② OI 스냅샷
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS oi_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            open_interest REAL NOT NULL,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ② 펀딩비
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS funding_rates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            funding_rate REAL NOT NULL,
            funding_time INTEGER NOT NULL,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ② 롱/숏 비율
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS long_short_ratios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            long_short_ratio REAL NOT NULL,
            long_account REAL NOT NULL,
            short_account REAL NOT NULL,
            timestamp INTEGER NOT NULL,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ② 오더북 벽 (상위 10% 매수/매도벽)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orderbook_walls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            price REAL NOT NULL,
            quantity REAL NOT NULL,
            scan_id INTEGER NOT NULL,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_ob_symbol_scan
        ON orderbook_walls(symbol, scan_id)
    """)

    # ② 캔들 데이터 (ATR 계산용)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS klines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            interval TEXT NOT NULL,
            open_time INTEGER NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, interval, open_time)
        )
    """)

    # ③ 고래 거래 (Arkham)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS whale_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tx_hash TEXT,
            from_address TEXT,
            to_address TEXT,
            from_label TEXT,
            to_label TEXT,
            asset TEXT NOT NULL,
            amount REAL NOT NULL,
            usd_value REAL,
            block_time INTEGER,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ④ 거래소 넷플로우 (CryptoQuant)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS exchange_netflow (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset TEXT NOT NULL,
            netflow REAL NOT NULL,
            timestamp INTEGER NOT NULL,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ④-2 온체인 메트릭 (BGeometrics: MVRV, SOPR 등)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS onchain_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            metric TEXT NOT NULL,
            value REAL NOT NULL,
            timestamp INTEGER NOT NULL,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(metric, timestamp)
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_onchain_metric
        ON onchain_metrics(metric, timestamp)
    """)

    # ④-3 Taker Buy/Sell Ratio (Binance, 실시간)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS taker_ratio (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            buy_sell_ratio REAL NOT NULL,
            buy_vol REAL NOT NULL,
            sell_vol REAL NOT NULL,
            timestamp INTEGER NOT NULL,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, timestamp)
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_taker_symbol
        ON taker_ratio(symbol, timestamp)
    """)

    # ⑤ 공포/탐욕 지수
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fear_greed (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            value INTEGER NOT NULL,
            classification TEXT NOT NULL,
            fg_timestamp INTEGER NOT NULL,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ============================
    # Phase 2: 엔진 출력 테이블
    # ============================

    # ATR 계산 결과
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS atr_values (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            atr REAL NOT NULL,
            atr_pct REAL NOT NULL,
            stop_loss_pct REAL NOT NULL,
            current_price REAL NOT NULL,
            calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_atr_symbol
        ON atr_values(symbol, calculated_at)
    """)

    # 동적 임계점 시그널
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS threshold_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            threshold_value REAL NOT NULL,
            liq_amount_1h REAL NOT NULL,
            current_oi REAL NOT NULL,
            liquidity_coeff REAL NOT NULL,
            trigger_active INTEGER NOT NULL DEFAULT 0,
            direction TEXT,
            calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_threshold_symbol
        ON threshold_signals(symbol, calculated_at)
    """)

    # 그리드 설정
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS grid_configs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            lower_bound REAL NOT NULL,
            upper_bound REAL NOT NULL,
            grid_count INTEGER NOT NULL,
            grid_spacing REAL NOT NULL,
            grid_spacing_pct REAL NOT NULL,
            spoofing_filtered INTEGER NOT NULL DEFAULT 0,
            calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_grid_symbol
        ON grid_configs(symbol, calculated_at)
    """)

    # SSM+V+T 스코어
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ssm_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            trigger_active INTEGER NOT NULL DEFAULT 0,
            momentum_score REAL NOT NULL DEFAULT 0,
            sentiment_score REAL NOT NULL DEFAULT 0,
            story_score REAL NOT NULL DEFAULT 0,
            value_score REAL NOT NULL DEFAULT 0,
            total_score REAL NOT NULL DEFAULT 0,
            direction TEXT,
            score_detail TEXT,
            gemini_calls_used INTEGER NOT NULL DEFAULT 0,
            calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_ssm_symbol
        ON ssm_scores(symbol, calculated_at)
    """)

    # 전략 상태 (심볼당 최신 1행)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS strategy_state (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'A',
            l1_active INTEGER NOT NULL DEFAULT 0,
            l1_entry_reason TEXT,
            l2_active INTEGER NOT NULL DEFAULT 0,
            l2_direction TEXT,
            l2_step INTEGER NOT NULL DEFAULT 0,
            l2_entry_pct REAL NOT NULL DEFAULT 0,
            l2_avg_entry_price REAL,
            l2_step1_time TEXT,
            l2_score_at_entry REAL,
            l2_direction_changes_today INTEGER NOT NULL DEFAULT 0,
            l2_last_reset_date TEXT,
            l4_active INTEGER NOT NULL DEFAULT 0,
            l4_grid_config_id INTEGER,
            macro_blocked INTEGER NOT NULL DEFAULT 0,
            macro_block_reason TEXT,
            pending_signal TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_strategy_symbol
        ON strategy_state(symbol, updated_at)
    """)

    # 시그널 로그 (append-only)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS signal_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            signal_type TEXT NOT NULL,
            direction TEXT,
            details TEXT,
            ssm_score REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_signal_symbol
        ON signal_log(symbol, created_at)
    """)

    # Gemini 일일 사용량 추적
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gemini_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            call_date TEXT NOT NULL,
            calls_used INTEGER NOT NULL DEFAULT 0,
            daily_limit INTEGER NOT NULL DEFAULT 25,
            UNIQUE(call_date)
        )
    """)

    # ============================
    # Phase 2.5: 페이퍼 트레이딩
    # ============================

    # 가상 포지션 이력
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS paper_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'OPEN',
            entry_price REAL NOT NULL,
            exit_price REAL,
            entry_pct REAL NOT NULL DEFAULT 0.30,
            l2_step INTEGER NOT NULL DEFAULT 1,
            stop_loss REAL,
            pnl_pct REAL,
            pnl_weighted REAL,
            exit_reason TEXT,
            entry_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            exit_time TIMESTAMP,
            last_signal_id INTEGER NOT NULL
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_paper_symbol_status
        ON paper_trades(symbol, status)
    """)

    # L1 펀딩비 수익 이력
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS paper_l1_funding (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            funding_rate REAL NOT NULL,
            funding_pnl_pct REAL NOT NULL,
            l1_effective REAL NOT NULL DEFAULT 1.0,
            l2_conflict INTEGER NOT NULL DEFAULT 0,
            collected_at TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_l1_funding_symbol
        ON paper_l1_funding(symbol, created_at)
    """)

    # L4 그리드 매매 이력
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS paper_l4_grid (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            grid_level INTEGER NOT NULL,
            grid_price REAL NOT NULL,
            side TEXT NOT NULL,
            pnl_pct REAL NOT NULL DEFAULT 0,
            grid_config_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_l4_grid_symbol
        ON paper_l4_grid(symbol, created_at)
    """)

    # 일별 성과 요약
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS paper_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            summary_date TEXT NOT NULL,
            total_trades INTEGER NOT NULL DEFAULT 0,
            wins INTEGER NOT NULL DEFAULT 0,
            losses INTEGER NOT NULL DEFAULT 0,
            total_pnl_pct REAL NOT NULL DEFAULT 0,
            best_trade_pct REAL,
            worst_trade_pct REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, summary_date)
        )
    """)

    conn.commit()
    conn.close()
    print("[DB] 테이블 초기화 완료")


if __name__ == "__main__":
    init_db()
    print(f"[DB] 경로: {DB_PATH}")
