"""Phase 1+2 통합 실행 - 데이터 수집 + 분석 엔진"""
import asyncio
import signal
import sys
import traceback
import os

# Windows 콘솔 UTF-8 출력
if sys.platform == "win32":
    os.system("")  # ANSI 활성화
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from db import init_db
from config import (
    OI_INTERVAL, FUNDING_INTERVAL, LONG_SHORT_INTERVAL,
    ORDERBOOK_INTERVAL, KLINES_DAILY_INTERVAL, KLINES_5M_INTERVAL,
    FEAR_GREED_INTERVAL, MACRO_CHECK_INTERVAL,
    KLINES_1W_INTERVAL, KLINES_4H_INTERVAL, KLINES_1H_INTERVAL,
    MTF_ANALYSIS_INTERVAL,
    ATR_INTERVAL, THRESHOLD_INTERVAL, GRID_INTERVAL,
    SSM_SCORE_INTERVAL, STRATEGY_INTERVAL, MACRO_GUARD_INTERVAL,
    GRID_V2_CYCLE_INTERVAL,
)

# Phase 1: 수집기
from collectors.ws_liquidation import run_liquidation_stream
from collectors.binance_rest import (
    collect_open_interest, collect_funding_rate,
    collect_long_short_ratio, collect_orderbook_walls,
    collect_klines, collect_klines_5m,
    collect_klines_1w, collect_klines_4h, collect_klines_1h,
)
from collectors.arkham import collect_whale_transactions
from collectors.cryptoquant import collect_all_onchain
from collectors.fear_greed import collect_fear_greed
from collectors.macro_events import check_upcoming_events

# Phase 2: 엔진
from engines.atr import calculate_atr
from engines.dynamic_threshold import calculate_threshold
from engines.grid_range import calculate_grid_range
from engines.scorer import calculate_score
from engines.strategy_manager import run_strategy
from engines.macro_guard import check_macro_block
from engines.paper_trader import run_paper_trader
from engines.live_trader import run_live_trader
from engines.mtf_analyzer import calculate_mtf
from db import purge_old_data
from config import LIVE_TRADING_ENABLED


def _run_sync(func):
    """동기 함수를 asyncio 루프에서 실행하기 위한 래퍼"""
    def wrapper():
        try:
            func()
        except Exception as e:
            print(f"[오류] {func.__name__}: {e}")
            traceback.print_exc()
    return wrapper


async def main():
    # DB 초기화
    init_db()
    print("=" * 60)
    print("  Auto Trading System - Phase 1+2")
    print("  데이터 수집 + 분석 엔진")
    from config import SYMBOLS
    print(f"  감시 대상: {', '.join(s.replace('USDT','') for s in SYMBOLS)}")
    print("=" * 60)

    # === Phase 1: 최초 데이터 수집 ===
    print("\n[Phase 1] 초기 수집 시작")
    collect_open_interest()
    collect_funding_rate()
    collect_long_short_ratio()
    collect_orderbook_walls()
    collect_klines()
    collect_klines_5m()
    collect_klines_1w()
    collect_klines_4h()
    collect_klines_1h()
    collect_fear_greed()
    collect_whale_transactions()
    collect_all_onchain()
    check_upcoming_events()
    print("[Phase 1] 초기 수집 완료\n")

    # === Phase 2: 엔진 초기 실행 (의존성 순서) ===
    print("[Phase 2] 엔진 초기 실행")
    calculate_atr()
    calculate_threshold()
    check_macro_block()
    calculate_grid_range()
    calculate_mtf()
    calculate_score()
    run_strategy()
    run_paper_trader()
    print("[Phase 2] 엔진 초기 실행 완료\n")

    # === Phase 3: 라이브 트레이딩 (선택적) ===
    if LIVE_TRADING_ENABLED:
        print("[Phase 3] 라이브 트레이딩 활성화")
        try:
            run_live_trader()
        except Exception as e:
            print(f"[Phase 3] 라이브 트레이더 초기화 실패: {e}")
            import traceback
            traceback.print_exc()

    # 스케줄러 설정
    scheduler = AsyncIOScheduler()

    # Phase 1: 수집 스케줄
    scheduler.add_job(_run_sync(collect_open_interest), "interval", seconds=OI_INTERVAL, id="oi")
    scheduler.add_job(_run_sync(collect_funding_rate), "interval", seconds=FUNDING_INTERVAL, id="funding")
    scheduler.add_job(_run_sync(collect_long_short_ratio), "interval", seconds=LONG_SHORT_INTERVAL, id="long_short")
    scheduler.add_job(_run_sync(collect_orderbook_walls), "interval", seconds=ORDERBOOK_INTERVAL, id="orderbook")
    scheduler.add_job(_run_sync(collect_klines), "interval", seconds=KLINES_DAILY_INTERVAL, id="klines_daily")
    scheduler.add_job(_run_sync(collect_klines_5m), "interval", seconds=KLINES_5M_INTERVAL, id="klines_5m")
    scheduler.add_job(_run_sync(collect_klines_1w), "interval", seconds=KLINES_1W_INTERVAL, id="klines_1w")
    scheduler.add_job(_run_sync(collect_klines_4h), "interval", seconds=KLINES_4H_INTERVAL, id="klines_4h")
    scheduler.add_job(_run_sync(collect_klines_1h), "interval", seconds=KLINES_1H_INTERVAL, id="klines_1h")
    scheduler.add_job(_run_sync(collect_fear_greed), "interval", seconds=FEAR_GREED_INTERVAL, id="fear_greed")
    scheduler.add_job(_run_sync(collect_whale_transactions), "interval", seconds=FEAR_GREED_INTERVAL, id="whale_alert")
    scheduler.add_job(_run_sync(collect_all_onchain), "interval", seconds=FEAR_GREED_INTERVAL, id="bgeometrics")
    scheduler.add_job(_run_sync(check_upcoming_events), "interval", seconds=MACRO_CHECK_INTERVAL, id="macro")

    # Phase 2: 엔진 스케줄
    scheduler.add_job(_run_sync(calculate_atr), "interval", seconds=ATR_INTERVAL, id="atr_engine")
    scheduler.add_job(_run_sync(calculate_threshold), "interval", seconds=THRESHOLD_INTERVAL, id="threshold_engine")
    scheduler.add_job(_run_sync(check_macro_block), "interval", seconds=MACRO_GUARD_INTERVAL, id="macro_guard")
    scheduler.add_job(_run_sync(calculate_grid_range), "interval", seconds=GRID_INTERVAL, id="grid_engine")
    scheduler.add_job(_run_sync(calculate_mtf), "interval", seconds=MTF_ANALYSIS_INTERVAL, id="mtf_engine")
    scheduler.add_job(_run_sync(calculate_score), "interval", seconds=SSM_SCORE_INTERVAL, id="scorer_engine")
    scheduler.add_job(_run_sync(run_strategy), "interval", seconds=STRATEGY_INTERVAL, id="strategy_engine")
    scheduler.add_job(_run_sync(run_paper_trader), "interval", seconds=STRATEGY_INTERVAL, id="paper_trader")
    if LIVE_TRADING_ENABLED:
        scheduler.add_job(_run_sync(run_live_trader), "interval", seconds=GRID_V2_CYCLE_INTERVAL, id="live_trader", max_instances=1, coalesce=True)
    scheduler.add_job(_run_sync(purge_old_data), "interval", seconds=86400, id="db_purge")

    scheduler.start()
    print("[스케줄러] 가동 중")
    print("  --- Phase 1 수집 ---")
    print(f"  OI: {OI_INTERVAL//3600}h | 펀딩비: {FUNDING_INTERVAL//3600}h | 롱숏: {LONG_SHORT_INTERVAL//3600}h")
    print(f"  오더북: {ORDERBOOK_INTERVAL//3600}h | 일봉: {KLINES_DAILY_INTERVAL//3600}h | 5분봉: {KLINES_5M_INTERVAL}s | F&G: {FEAR_GREED_INTERVAL//3600}h")
    print(f"  주봉: {KLINES_1W_INTERVAL//3600}h | 4시간봉: {KLINES_4H_INTERVAL//3600}h | 1시간봉: {KLINES_1H_INTERVAL//3600}h")
    print("  --- Phase 2 엔진 ---")
    print(f"  Threshold: {THRESHOLD_INTERVAL}s | Scorer: {SSM_SCORE_INTERVAL}s | Strategy: {STRATEGY_INTERVAL}s")
    print(f"  Grid: {GRID_INTERVAL//3600}h | MacroGuard: {MACRO_GUARD_INTERVAL}s | ATR: {ATR_INTERVAL//3600}h | MTF: {MTF_ANALYSIS_INTERVAL//3600}h")
    print(f"  Paper Trader: {STRATEGY_INTERVAL}s")
    if LIVE_TRADING_ENABLED:
        from config import LIVE_USE_TESTNET, LIVE_SYMBOLS
        net = "TESTNET" if LIVE_USE_TESTNET else "MAINNET"
        print(f"  Live Trader V2: {GRID_V2_CYCLE_INTERVAL}s ({net}, {','.join(LIVE_SYMBOLS)})")
    print(f"  DB Purge: 24h")
    print("\n[WebSocket] 청산 스트림 시작...")
    print("종료: Ctrl+C\n")

    # WebSocket 청산 스트림 (무한 루프)
    await run_liquidation_stream()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[종료] 사용자에 의해 중단됨")
        sys.exit(0)
