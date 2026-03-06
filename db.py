"""SQLite 데이터베이스 초기화 + 헬퍼 함수"""
import sqlite3
from pathlib import Path
from config import DB_PATH


def get_connection() -> sqlite3.Connection:
    """동기 SQLite 연결 반환"""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")   # WAL 모드에서 안전 + 빠름
    conn.execute("PRAGMA wal_autocheckpoint=1000")  # 체크포인트 빈도 줄임
    return conn


def init_db():
    """모든 테이블 생성 (없으면 생성)"""
    conn = get_connection()
    cursor = conn.cursor()

    # ============================
    # Phase 1: 수집기 테이블 (유지)
    # ============================

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

    # ② 캔들 데이터
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

    # ④-4 Top Trader Long/Short Ratio (Position + Account)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS top_trader_ratios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            ratio_type TEXT NOT NULL,
            long_short_ratio REAL NOT NULL,
            long_account REAL NOT NULL,
            short_account REAL NOT NULL,
            timestamp INTEGER NOT NULL,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, ratio_type, timestamp)
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_top_trader_symbol
        ON top_trader_ratios(symbol, ratio_type, timestamp)
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
    # Phase 2: 엔진 출력 테이블 (유지)
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

    # 그리드 설정 (레거시)
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

    # 전략 상태 (레거시)
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
            l2_trailing_stop_price REAL,
            pending_signal TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_strategy_symbol
        ON strategy_state(symbol, updated_at)
    """)
    # strategy_state symbol UNIQUE 보장 (심볼당 1행)
    cursor.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_symbol_unique
        ON strategy_state(symbol)
    """)

    # Migration: trailing stop 컬럼 추가
    try:
        cursor.execute("ALTER TABLE strategy_state ADD COLUMN l2_trailing_stop_price REAL")
    except sqlite3.OperationalError as e:
        if "duplicate column" not in str(e).lower() and "already exists" not in str(e).lower():
            raise

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
    # Phase 2.5: 페이퍼 트레이딩 (유지)
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

    # ============================
    # Phase 3: 라이브 트레이딩 (유지)
    # ============================

    # 실주문 이력
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS live_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            order_type TEXT NOT NULL,
            quantity REAL NOT NULL,
            price REAL,
            order_id TEXT,
            status TEXT NOT NULL,
            pnl_pct REAL DEFAULT 0,
            grid_level INTEGER,
            error_msg TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_live_orders_symbol
        ON live_orders(symbol, created_at)
    """)

    # 일일 실현 손익
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS live_daily_pnl (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT NOT NULL UNIQUE,
            realized_pnl REAL NOT NULL DEFAULT 0,
            unrealized_pnl REAL NOT NULL DEFAULT 0,
            total_orders INTEGER NOT NULL DEFAULT 0,
            circuit_breaker_hit INTEGER NOT NULL DEFAULT 0,
            starting_balance REAL NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Grid V1 레거시 (유지)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS grid_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            grid_price REAL NOT NULL,
            status TEXT NOT NULL DEFAULT 'EMPTY',
            direction TEXT DEFAULT NULL,
            quantity REAL DEFAULT 0,
            buy_fill_price REAL,
            entry_fill_price REAL,
            buy_order_id TEXT,
            sell_order_id TEXT,
            buy_client_order_id TEXT,
            sell_client_order_id TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, grid_price)
        )
    """)
    # Migration: 기존 DB에 새 컬럼 추가
    try:
        cursor.execute("ALTER TABLE grid_positions ADD COLUMN direction TEXT DEFAULT NULL")
    except sqlite3.OperationalError as e:
        if "duplicate column" not in str(e).lower() and "already exists" not in str(e).lower():
            raise
    try:
        cursor.execute("ALTER TABLE grid_positions ADD COLUMN entry_fill_price REAL")
    except sqlite3.OperationalError as e:
        if "duplicate column" not in str(e).lower() and "already exists" not in str(e).lower():
            raise
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_grid_pos_symbol
        ON grid_positions(symbol, status)
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS grid_order_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            direction TEXT DEFAULT NULL,
            grid_price REAL NOT NULL,
            quantity REAL NOT NULL,
            limit_price REAL NOT NULL,
            order_id TEXT,
            client_order_id TEXT,
            status TEXT NOT NULL,
            fill_price REAL,
            fee REAL DEFAULT 0,
            pnl_usd REAL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            filled_at TIMESTAMP
        )
    """)
    # Migration: 기존 DB에 direction 컬럼 추가
    try:
        cursor.execute("ALTER TABLE grid_order_log ADD COLUMN direction TEXT DEFAULT NULL")
    except sqlite3.OperationalError as e:
        if "duplicate column" not in str(e).lower() and "already exists" not in str(e).lower():
            raise
    # Migration: live_daily_pnl에 starting_balance 컬럼 추가
    try:
        cursor.execute("ALTER TABLE live_daily_pnl ADD COLUMN starting_balance REAL NOT NULL DEFAULT 0")
    except sqlite3.OperationalError as e:
        if "duplicate column" not in str(e).lower() and "already exists" not in str(e).lower():
            raise
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_grid_log_symbol
        ON grid_order_log(symbol, created_at)
    """)

    # MTF 분석 결과
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS mtf_analysis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            alignment_score REAL NOT NULL DEFAULT 0,
            bias TEXT,
            pattern_1d TEXT,
            pattern_4h TEXT,
            nearest_support REAL,
            nearest_resistance REAL,
            detail_json TEXT,
            calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_mtf_symbol
        ON mtf_analysis(symbol, calculated_at)
    """)

    # ============================
    # V2: Grid + Trend 시스템
    # ============================

    # Grid V2 상태
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS grid_state (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'INACTIVE',
            va_high REAL,
            va_low REAL,
            poc REAL,
            grid_count INTEGER,
            net_position INTEGER DEFAULT 0,
            cumulative_pnl REAL DEFAULT 0,
            ratchet_floor REAL DEFAULT 0,
            last_vp_update TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol)
        )
    """)

    # Grid V2 레벨
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS grid_levels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            level_index INTEGER NOT NULL,
            price REAL NOT NULL,
            side TEXT,
            status TEXT NOT NULL DEFAULT 'EMPTY',
            order_id TEXT,
            entry_price REAL,
            quantity REAL,
            placed_at TIMESTAMP,
            filled_at TIMESTAMP,
            UNIQUE(symbol, level_index)
        )
    """)

    # Grid V2 거래 로그
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS grid_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            entry_price REAL NOT NULL,
            exit_price REAL NOT NULL,
            quantity REAL NOT NULL,
            pnl_usd REAL NOT NULL,
            fee_usd REAL NOT NULL,
            level_index INTEGER,
            traded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 추세 포지션
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trend_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            entry_price REAL NOT NULL,
            quantity REAL NOT NULL,
            trailing_stop REAL,
            highest_pnl REAL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'OPEN',
            opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            closed_at TIMESTAMP,
            pnl_usd REAL,
            close_reason TEXT
        )
    """)

    # 일일 PnL 추적
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_pnl (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            grid_pnl REAL DEFAULT 0,
            trend_pnl REAL DEFAULT 0,
            total_pnl REAL DEFAULT 0,
            total_trades INTEGER DEFAULT 0,
            circuit_breaker_hit INTEGER DEFAULT 0,
            UNIQUE(date, symbol)
        )
    """)

    # VP 캐시 (재계산 방지)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS vp_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            poc REAL NOT NULL,
            va_high REAL NOT NULL,
            va_low REAL NOT NULL,
            data_json TEXT,
            calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, timeframe)
        )
    """)

    # Smart Trade 방향 판정 기록 (5분봉 기준)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS smart_judgments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            candle_time INTEGER NOT NULL,
            mark_price REAL NOT NULL,
            direction TEXT NOT NULL,
            reason TEXT,
            blocked INTEGER NOT NULL DEFAULT 0,
            entered INTEGER NOT NULL DEFAULT 0,
            bias TEXT,
            trend_score REAL,
            patterns TEXT,
            nearest_support TEXT,
            nearest_resistance TEXT,
            up_vol REAL,
            down_vol REAL,
            defense_vol REAL,
            attack_ratio REAL,
            detail TEXT,
            oi_change REAL,
            liq_buy_vol REAL,
            liq_sell_vol REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, candle_time)
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_smart_judgments_symbol
        ON smart_judgments(symbol, candle_time)
    """)

    # smart_judgments 마이그레이션: v5.1 컬럼 추가
    for col in ["oi_change REAL", "liq_buy_vol REAL", "liq_sell_vol REAL"]:
        try:
            cursor.execute(f"ALTER TABLE smart_judgments ADD COLUMN {col}")
        except Exception:
            pass  # 이미 존재

    # trend_positions 마이그레이션: v8 컬럼 추가
    for col in ["size_pct REAL DEFAULT 1.0",
                "signal_strength REAL DEFAULT 0",
                "trigger_reason TEXT"]:
        try:
            cursor.execute(f"ALTER TABLE trend_positions ADD COLUMN {col}")
        except Exception:
            pass  # 이미 존재

    # V2 인덱스
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_grid_levels_symbol
        ON grid_levels(symbol, status)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_grid_trades_symbol
        ON grid_trades(symbol, traded_at)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_trend_pos_symbol
        ON trend_positions(symbol, status)
    """)

    conn.commit()
    conn.close()
    print("[DB] 테이블 초기화 완료")


def reset_grid_state(symbol: str):
    """Grid V2 상태 초기화 — grid_state, grid_levels 삭제"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM grid_state WHERE symbol = ?", (symbol,))
    cursor.execute("DELETE FROM grid_levels WHERE symbol = ?", (symbol,))
    conn.commit()
    conn.close()
    print(f"[DB] Grid 상태 초기화: {symbol}")


def check_data_freshness(symbol: str, max_age_seconds: int = 600) -> dict:
    """데이터 신선도 확인 — 소스별 개별 기준 적용"""
    import time
    conn = get_connection()
    now = time.time()
    # (테이블, 컬럼, WHERE 조건, 개별 max_age)
    tables = {
        "klines_5m": ("klines", "collected_at", f"symbol = '{symbol}' AND interval = '5m'", 600),
        "oi": ("oi_snapshots", "collected_at", f"symbol = '{symbol}'", 7200),         # 1시간 수집 → 2시간 허용
        "funding": ("funding_rates", "collected_at", f"symbol = '{symbol}'", 57600),   # 8시간 수집 → 16시간 허용
        "threshold": ("threshold_signals", "calculated_at", f"symbol = '{symbol}'", 600),
        "ssm_score": ("ssm_scores", "calculated_at", f"symbol = '{symbol}'", 1200),    # 10분 수집 → 20분 허용
    }
    result = {}
    for key, (table, col, where, src_max_age) in tables.items():
        row = conn.execute(
            f"SELECT {col} FROM {table} WHERE {where} ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if row and row[0]:
            from datetime import datetime
            try:
                ts = datetime.fromisoformat(row[0].replace("Z", "+00:00"))
                age = now - ts.timestamp()
            except Exception:
                age = float("inf")
            result[key] = {"age_seconds": round(age), "stale": age > src_max_age}
        else:
            result[key] = {"age_seconds": None, "stale": True}
    conn.close()
    return result


def purge_old_data(days_short: int = 30, days_long: int = 90):
    """오래된 데이터 자동 삭제 — 고빈도 테이블 30일, 저빈도 90일"""
    conn = get_connection()
    cursor = conn.cursor()

    # 고빈도 테이블 (30일)
    # strategy_state 제외: UNIQUE(symbol) 행이므로 삭제하면 트레이딩 중단됨
    short_tables = [
        ("liquidations", "collected_at"),
        ("klines", "collected_at"),
        ("threshold_signals", "calculated_at"),
        ("ssm_scores", "calculated_at"),
        ("signal_log", "created_at"),
        ("grid_trades", "traded_at"),
    ]
    for table, col in short_tables:
        cursor.execute(
            f"DELETE FROM {table} WHERE {col} < datetime('now', '-{days_short} days')"
        )
        deleted = cursor.rowcount
        if deleted > 0:
            print(f"[DB Purge] {table}: {deleted}건 삭제 ({days_short}일 이전)")

    # 저빈도 테이블 (90일) — 페이퍼 트레이딩 이력은 보존 (성과 집계용)
    long_tables = [
        ("oi_snapshots", "collected_at"),
        ("funding_rates", "collected_at"),
        ("long_short_ratios", "collected_at"),
        ("orderbook_walls", "collected_at"),
        ("fear_greed", "collected_at"),
        ("trend_positions", "opened_at"),
        ("daily_pnl", "date"),
    ]
    for table, col in long_tables:
        cursor.execute(
            f"DELETE FROM {table} WHERE {col} < datetime('now', '-{days_long} days')"
        )
        deleted = cursor.rowcount
        if deleted > 0:
            print(f"[DB Purge] {table}: {deleted}건 삭제 ({days_long}일 이전)")

    conn.commit()
    conn.close()
    print("[DB Purge] 완료")


if __name__ == "__main__":
    init_db()
    print(f"[DB] 경로: {DB_PATH}")
