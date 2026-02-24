"""백테스트 러너 — 메인 시뮬레이션 루프"""
import io
import os
import sys
import time as real_time
from datetime import datetime, timedelta

from backtest.config_bt import (
    BT_DAYS, BT_STEP_SECONDS, BT_DB_PATH,
    BT_SYMBOLS, BT_LOG_INTERVAL,
)
from backtest.clock import VirtualClock
from backtest.context import BacktestContext


class _SuppressPrint:
    """엔진 print 출력 억제 (백테스트 속도 최적화)"""
    def __init__(self, real_stdout):
        self._real = real_stdout
    def write(self, text):
        pass
    def flush(self):
        pass
    def reconfigure(self, **kwargs):
        pass


class _DataFeeder:
    """In-memory drip-feed: DB 데이터를 메모리로 로드 → 가상 시간에 맞춰 DB에 삽입.

    SQLite staging 테이블 대신 Python 리스트를 사용하여
    Windows SQLite 호환성 문제를 완전히 회피.
    모든 시간값을 Unix timestamp (float)로 정규화하여 비교.
    """

    # (테이블명, 시간컬럼, 시간단위, 컬럼인덱스)
    # 시간단위: ms=밀리초, s=초, iso=ISO문자열
    TABLE_SPECS = [
        ("liquidations", "trade_time", "ms", 5),
        ("oi_snapshots", "collected_at", "iso", 3),
        ("funding_rates", "collected_at", "iso", 4),
        ("long_short_ratios", "collected_at", "iso", 6),
        ("taker_ratio", "timestamp", "ms", 5),
        ("fear_greed", "fg_timestamp", "s", 3),
    ]

    def __init__(self, conn):
        self._conn = conn
        # table -> list of (unix_ts, row_tuple)
        self._buffers = {}
        self._cursors = {}

    def _to_unix_ts(self, value, unit: str) -> float:
        """시간값을 Unix timestamp(초)로 변환"""
        if unit == "ms":
            return value / 1000.0
        elif unit == "s":
            return float(value)
        elif unit == "iso":
            try:
                return datetime.fromisoformat(
                    str(value).replace("Z", "+00:00")
                ).timestamp()
            except Exception:
                return 0.0
        return 0.0

    def load_and_clear(self):
        """모든 시계열 데이터를 메모리로 로드 후 DB 테이블 비움"""
        for table, time_col, unit, col_idx in self.TABLE_SPECS:
            rows = self._conn.execute(
                f"SELECT * FROM {table} ORDER BY {time_col}"
            ).fetchall()

            # (unix_ts, row) 쌍으로 저장
            items = []
            for row in rows:
                ts = self._to_unix_ts(row[col_idx], unit)
                items.append((ts, row))
            items.sort(key=lambda x: x[0])

            self._buffers[table] = items
            self._cursors[table] = 0
            self._conn.execute(f"DELETE FROM {table}")

        # 5m klines 별도 처리 (open_time = ms, index 3)
        rows = self._conn.execute(
            "SELECT * FROM klines WHERE interval = '5m' ORDER BY open_time"
        ).fetchall()
        items = [(row[3] / 1000.0, row) for row in rows]  # open_time ms → unix_ts
        items.sort(key=lambda x: x[0])

        self._buffers["klines_5m"] = items
        self._cursors["klines_5m"] = 0
        self._conn.execute("DELETE FROM klines WHERE interval = '5m'")
        # 1d klines은 유지 (ATR 계산에 항상 필요)

        self._conn.commit()

        total = sum(len(v) for v in self._buffers.values())
        print(f"[BT] 데이터 로드 완료: {total:,}건 → 메모리")

    def drip(self, current_ts: float):
        """현재 시뮬레이션 시간까지의 데이터를 DB에 삽입"""
        # 모든 테이블을 일괄 처리
        for table, _, _, _ in self.TABLE_SPECS:
            self._drip_table(table, current_ts)
        # klines 5m
        self._drip_table("klines_5m", current_ts, insert_cmd="INSERT OR IGNORE INTO klines")

    def _drip_table(self, key: str, current_ts: float, insert_cmd: str = None):
        """단일 테이블의 데이터를 current_ts까지 삽입"""
        buf = self._buffers[key]
        cursor = self._cursors[key]

        if cursor >= len(buf):
            return  # 이미 전부 삽입됨

        batch = []
        while cursor < len(buf):
            ts, row = buf[cursor]
            if ts > current_ts:
                break
            batch.append(row)
            cursor += 1
        self._cursors[key] = cursor

        if batch:
            table_name = key if insert_cmd is None else None
            if insert_cmd is None:
                insert_cmd = f"INSERT INTO {key}"
            placeholders = ",".join(["?"] * len(batch[0]))
            self._conn.executemany(
                f"{insert_cmd} VALUES ({placeholders})", batch
            )


def run_backtest(days: int = None, symbols: list = None):
    """백테스트 메인 루프 실행

    Returns:
        dict: {symbol: {equity_curve: [...], signals: [...], ...}}
    """
    days = days or BT_DAYS
    symbols = symbols or BT_SYMBOLS

    # 시간 범위 설정
    end_ts = real_time.time()
    start_ts = end_ts - (days * 86400)

    total_steps = int((end_ts - start_ts) / BT_STEP_SECONDS)

    print(f"\n{'='*60}")
    print(f"  BACKTEST START")
    print(f"  Period: {datetime.fromtimestamp(start_ts).strftime('%Y-%m-%d')} ~ "
          f"{datetime.fromtimestamp(end_ts).strftime('%Y-%m-%d')}")
    print(f"  Symbols: {', '.join(symbols)}")
    print(f"  Steps: {total_steps:,} ({BT_STEP_SECONDS}s each)")
    print(f"{'='*60}\n")

    # 가상 시계 초기화
    clock = VirtualClock(start_ts)

    # 엔진 임포트 (context 내부에서 패치 적용됨)
    from engines.atr import calculate_atr
    from engines.dynamic_threshold import calculate_threshold
    from engines.grid_range import calculate_grid_range
    from engines.scorer import calculate_score
    from engines.strategy_manager import run_strategy
    from engines.paper_trader import run_paper_trader

    # 엔진 실행 간격 (초)
    intervals = {
        "atr": 86400,           # 매일
        "threshold": 300,       # 5분
        "grid": 14400,          # 4시간
        "score": 600,           # 10분
        "strategy": 60,         # 1분 → step 단위로 매번
        "paper_trader": 60,     # 1분 → step 단위로 매번
    }

    # 마지막 실행 시간 추적
    last_run = {key: 0.0 for key in intervals}
    last_log = 0.0

    # 결과 수집
    results = {sym: {"equity_snapshots": []} for sym in symbols}

    wall_start = real_time.time()
    steps_done = 0
    print_interval = max(1, total_steps // 20)  # 5% 단위 진행률

    # 엔진 출력 억제용
    real_stdout = sys.stdout
    suppress = _SuppressPrint(real_stdout)

    with BacktestContext(clock, BT_DB_PATH) as ctx:
        # In-memory drip-feed 초기화
        print("[BT] 데이터 로드 (look-ahead bias 방지)...")
        feeder = _DataFeeder(ctx._shared_conn)
        feeder.load_and_clear()

        for step in range(total_steps):
            clock.advance(BT_STEP_SECONDS)
            current_ts = clock.timestamp
            steps_done += 1

            # Drip-feed: 현재 시뮬레이션 시간까지의 데이터만 DB에 삽입
            feeder.drip(current_ts)

            # 주기적 commit (매 스텝이 아닌 200 스텝마다)
            if steps_done % 200 == 0:
                ctx._shared_conn.commit()

            # 엔진 print 억제
            sys.stdout = suppress

            # ---- 엔진 실행 (간격 체크) ----

            # ATR: 매일
            if current_ts - last_run["atr"] >= intervals["atr"]:
                for sym in symbols:
                    try:
                        calculate_atr(sym)
                    except Exception:
                        pass
                last_run["atr"] = current_ts

            # Threshold: 5분
            if current_ts - last_run["threshold"] >= intervals["threshold"]:
                for sym in symbols:
                    try:
                        calculate_threshold(sym)
                    except Exception:
                        pass
                last_run["threshold"] = current_ts

            # Grid: 4시간
            if current_ts - last_run["grid"] >= intervals["grid"]:
                for sym in symbols:
                    try:
                        calculate_grid_range(sym)
                    except Exception:
                        pass
                last_run["grid"] = current_ts

            # Score: 10분
            if current_ts - last_run["score"] >= intervals["score"]:
                for sym in symbols:
                    try:
                        calculate_score(sym)
                    except Exception:
                        pass
                last_run["score"] = current_ts

            # Strategy: 매 스텝
            if current_ts - last_run["strategy"] >= intervals["strategy"]:
                for sym in symbols:
                    try:
                        run_strategy(sym)
                    except Exception:
                        pass
                last_run["strategy"] = current_ts

            # Paper Trader: 매 스텝
            if current_ts - last_run["paper_trader"] >= intervals["paper_trader"]:
                for sym in symbols:
                    try:
                        run_paper_trader(sym)
                    except Exception:
                        pass
                last_run["paper_trader"] = current_ts

            # 엔진 print 복원
            sys.stdout = real_stdout

            # ---- 일별 로그 ----
            if current_ts - last_log >= BT_LOG_INTERVAL:
                sim_date = clock.now().strftime("%Y-%m-%d")
                elapsed_wall = real_time.time() - wall_start
                progress = steps_done / total_steps * 100

                # 간단한 equity 스냅샷 수집
                for sym in symbols:
                    try:
                        equity = _get_equity_snapshot(sym)
                        results[sym]["equity_snapshots"].append({
                            "date": sim_date,
                            "timestamp": current_ts,
                            **equity,
                        })
                    except Exception:
                        pass

                print(f"[BT] {sim_date} | progress={progress:.1f}% | "
                      f"wall_time={elapsed_wall:.0f}s")
                last_log = current_ts

            # 진행률 표시 (5% 단위)
            elif steps_done % print_interval == 0:
                progress = steps_done / total_steps * 100
                sim_date = clock.now().strftime("%Y-%m-%d")
                elapsed_wall = real_time.time() - wall_start
                print(f"[BT] {sim_date} | progress={progress:.0f}% | "
                      f"wall_time={elapsed_wall:.0f}s", end="\r")

        # 루프 종료 후 최종 commit
        ctx._shared_conn.commit()

    wall_total = real_time.time() - wall_start
    print(f"\n\n[BT] 백테스트 완료! ({wall_total:.1f}초 소요)")

    return results


def _get_equity_snapshot(symbol: str) -> dict:
    """현재 시점의 PnL 스냅샷"""
    from db import get_connection
    conn = get_connection()

    # L2 실현 PnL
    l2_row = conn.execute(
        "SELECT COALESCE(SUM(pnl_weighted), 0) FROM paper_trades "
        "WHERE symbol = ? AND status = 'CLOSED'",
        (symbol,),
    ).fetchone()
    l2_realized = l2_row[0] if l2_row else 0

    # L2 미실현 PnL
    open_trade = conn.execute(
        "SELECT direction, entry_price, entry_pct FROM paper_trades "
        "WHERE symbol = ? AND status = 'OPEN' ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()

    l2_unrealized = 0.0
    if open_trade:
        price_row = conn.execute(
            "SELECT close FROM klines WHERE symbol = ? AND interval = '5m' "
            "ORDER BY open_time DESC LIMIT 1",
            (symbol,),
        ).fetchone()
        if price_row and open_trade[1]:
            current = price_row[0]
            entry = open_trade[1]
            direction = open_trade[0]
            pct = open_trade[2]
            if direction == "LONG":
                l2_unrealized = ((current - entry) / entry * 100) * pct
            else:
                l2_unrealized = ((entry - current) / entry * 100) * pct

    # L1 펀딩비 PnL
    l1_row = conn.execute(
        "SELECT COALESCE(SUM(funding_pnl_pct), 0) FROM paper_l1_funding WHERE symbol = ?",
        (symbol,),
    ).fetchone()
    l1_pnl = l1_row[0] if l1_row else 0

    # L4 그리드 PnL
    l4_row = conn.execute(
        "SELECT COALESCE(SUM(pnl_pct), 0) FROM paper_l4_grid "
        "WHERE symbol = ? AND side = 'SELL'",
        (symbol,),
    ).fetchone()
    l4_pnl = l4_row[0] if l4_row else 0

    conn.close()

    total = l2_realized + l2_unrealized + l1_pnl + l4_pnl

    return {
        "l2_realized": round(l2_realized, 4),
        "l2_unrealized": round(l2_unrealized, 4),
        "l1_pnl": round(l1_pnl, 4),
        "l4_pnl": round(l4_pnl, 4),
        "total_pnl": round(total, 4),
    }


if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8")
    results = run_backtest(days=7)
    print("\nResults:", results)
