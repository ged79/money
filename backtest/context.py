"""백테스트 컨텍스트 — 라이브 시스템 의존성을 가상 시간/DB로 교체

monkey-patch 대상:
1. db.get_connection()         → backtest.db 연결
2. time.time()                 → clock.time()  (dynamic_threshold, macro_guard, gemini_client 등)
3. datetime.now()              → clock.now()    (strategy_manager)
4. date.today()                → clock.today()  (strategy_manager, paper_trader, gemini_client, cryptoquant)
5. gemini_client.analyze_sentiment_majority() → neutral stub
6. arkham.get_whale_direction()               → neutral stub
7. cryptoquant.get_mvrv_signal()              → neutral stub
8. macro_events.load_calendar()               → empty list
"""
import sqlite3
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import date, datetime

from backtest.clock import VirtualClock


class _NoCloseConnection:
    """conn.close()를 무시하는 SQLite 연결 래퍼.

    프로덕션 엔진들이 매 호출마다 conn.close()를 호출하지만,
    백테스트에서는 공유 연결을 유지해야 하므로 close를 무시한다.
    """
    def __init__(self, real_conn: sqlite3.Connection):
        self._conn = real_conn

    def execute(self, *args, **kwargs):
        return self._conn.execute(*args, **kwargs)

    def executemany(self, *args, **kwargs):
        return self._conn.executemany(*args, **kwargs)

    def commit(self):
        pass  # 무시 — 메인 루프에서 주기적으로 실제 commit

    def close(self):
        pass  # 무시 — BacktestContext.__exit__에서 실제 close

    def cursor(self):
        return self._conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass  # close 무시


class BacktestContext:
    """라이브 엔진을 백테스트 모드로 전환하는 컨텍스트 매니저"""

    def __init__(self, clock: VirtualClock, bt_db_path: Path):
        self.clock = clock
        self.bt_db_path = bt_db_path
        self._stack = ExitStack()
        # 공유 DB 연결 (매번 open/close 대신 재사용 → 대폭 속도 향상)
        self._shared_conn = None

    def __enter__(self):
        # ============================
        # 0. 공유 DB 연결 초기화 (즉시 — lazy 아님)
        # ============================
        self._shared_conn = sqlite3.connect(str(self.bt_db_path))
        self._shared_conn.execute("PRAGMA journal_mode=WAL")
        self._shared_conn.execute("PRAGMA synchronous=NORMAL")
        self._shared_conn.execute("PRAGMA wal_autocheckpoint=500")

        # ============================
        # 1. DB 연결 패치
        # ============================
        # 핵심: `from db import get_connection` 패턴은 각 모듈에
        # 로컬 참조를 생성하므로, db 모듈 + 모든 사용 모듈을 개별 패치해야 함
        self._stack.enter_context(
            patch('db.get_connection', self._get_bt_connection)
        )
        # 각 엔진/컬렉터 모듈의 로컬 get_connection 참조 패치
        for mod in [
            'engines.atr',
            'engines.dynamic_threshold',
            'engines.grid_range',
            'engines.scorer',
            'engines.strategy_manager',
            'engines.paper_trader',
            'engines.gemini_client',
            'collectors.arkham',
            'collectors.cryptoquant',
        ]:
            self._stack.enter_context(
                patch(f'{mod}.get_connection', self._get_bt_connection)
            )

        # ============================
        # 2. time.time() 패치 — 각 모듈별로 패치
        # ============================
        mock_time_module = MagicMock()
        mock_time_module.time = self._mock_time
        mock_time_module.sleep = lambda s: None  # sleep 무시

        # dynamic_threshold.py: import time → time.time() 사용
        self._stack.enter_context(
            patch('engines.dynamic_threshold.time', mock_time_module)
        )

        # macro_guard.py: import time → time.time() 사용
        self._stack.enter_context(
            patch('engines.macro_guard.time', mock_time_module)
        )

        # gemini_client.py: import time → time.time() 사용
        self._stack.enter_context(
            patch('engines.gemini_client.time', mock_time_module)
        )

        # strategy_manager.py: import time → time.time() 사용
        self._stack.enter_context(
            patch('engines.strategy_manager.time', mock_time_module)
        )

        # arkham.py: import time → time.time() 사용
        self._stack.enter_context(
            patch('collectors.arkham.time', mock_time_module)
        )

        # ============================
        # 3. datetime 패치 — strategy_manager, gemini_client
        # ============================
        # strategy_manager.py: from datetime import date, datetime
        # 사용: date.today(), datetime.now(), datetime.fromisoformat()
        mock_datetime = MagicMock(wraps=datetime)
        mock_datetime.now = self._mock_datetime_now
        mock_datetime.fromisoformat = datetime.fromisoformat
        mock_datetime.fromtimestamp = datetime.fromtimestamp

        self._stack.enter_context(
            patch('engines.strategy_manager.datetime', mock_datetime)
        )

        # ============================
        # 4. date 패치 — strategy_manager, paper_trader, gemini_client, cryptoquant
        # ============================
        mock_date = MagicMock(wraps=date)
        mock_date.today = self._mock_date_today
        mock_date.fromisoformat = date.fromisoformat

        self._stack.enter_context(
            patch('engines.strategy_manager.date', mock_date)
        )
        self._stack.enter_context(
            patch('engines.paper_trader.date', mock_date)
        )
        self._stack.enter_context(
            patch('engines.gemini_client.date', mock_date)
        )
        self._stack.enter_context(
            patch('collectors.cryptoquant.date', mock_date)
        )

        # ============================
        # 5. 외부 API stub — Gemini
        # ============================
        self._stack.enter_context(
            patch('engines.gemini_client.analyze_sentiment_majority',
                  self._stub_gemini)
        )
        # scorer.py에서 직접 import: from engines.gemini_client import analyze_sentiment_majority
        self._stack.enter_context(
            patch('engines.scorer.analyze_sentiment_majority',
                  self._stub_gemini)
        )

        # ============================
        # 6. 외부 API stub — Whale (Arkham)
        # ============================
        self._stack.enter_context(
            patch('collectors.arkham.get_whale_direction',
                  self._stub_whale)
        )
        # scorer.py에서 직접 import: from collectors.arkham import get_whale_direction
        self._stack.enter_context(
            patch('engines.scorer.get_whale_direction',
                  self._stub_whale)
        )

        # ============================
        # 7. 외부 API stub — MVRV
        # ============================
        self._stack.enter_context(
            patch('collectors.cryptoquant.get_mvrv_signal',
                  self._stub_mvrv)
        )
        # scorer.py에서 직접 import: from collectors.cryptoquant import get_mvrv_signal
        self._stack.enter_context(
            patch('engines.scorer.get_mvrv_signal',
                  self._stub_mvrv)
        )

        # ============================
        # 8. 매크로 이벤트 stub — 빈 캘린더
        # ============================
        self._stack.enter_context(
            patch('collectors.macro_events.load_calendar',
                  lambda: [])
        )
        # macro_guard.py에서 직접 import: from collectors.macro_events import load_calendar
        self._stack.enter_context(
            patch('engines.macro_guard.load_calendar',
                  lambda: [])
        )

        return self

    def __exit__(self, *args):
        self._stack.__exit__(*args)
        if self._shared_conn:
            try:
                self._shared_conn.commit()
                self._shared_conn.close()
            except Exception:
                pass
            self._shared_conn = None

    # ---- Patch implementations ----

    def _get_bt_connection(self) -> sqlite3.Connection:
        """공유 backtest.db 연결 반환 (conn.close() 호출을 무시하는 래퍼)"""
        if self._shared_conn is None:
            self._shared_conn = sqlite3.connect(str(self.bt_db_path))
            self._shared_conn.execute("PRAGMA journal_mode=WAL")
            self._shared_conn.execute("PRAGMA synchronous=NORMAL")
            self._shared_conn.execute("PRAGMA wal_autocheckpoint=500")
        return _NoCloseConnection(self._shared_conn)

    def _mock_time(self) -> float:
        """가상 시계의 Unix timestamp 반환"""
        return self.clock.time()

    def _mock_datetime_now(self, tz=None) -> datetime:
        """가상 시계의 datetime 반환"""
        return self.clock.now()

    def _mock_date_today(self) -> date:
        """가상 시계의 date 반환"""
        return self.clock.today()

    def _stub_gemini(self, symbol: str = None, calls: int = 3) -> dict:
        """Gemini API stub — neutral 반환"""
        return {
            "sentiment": "neutral",
            "confidence": 0.0,
            "agreement": 0.33,
            "calls_used": 0,
            "votes": {"neutral": 3},
        }

    def _stub_whale(self, asset: str = "bitcoin", hours: int = 6) -> dict:
        """Whale Alert stub — neutral 반환"""
        return {
            "direction": "neutral",
            "inflow_usd": 0,
            "outflow_usd": 0,
            "net_flow_usd": 0,
            "tx_count": 0,
            "score": 0.0,
        }

    def _stub_mvrv(self) -> dict:
        """MVRV stub — neutral 반환"""
        return {
            "mvrv": 1.5,
            "signal": "neutral",
            "score": 0.0,
        }
