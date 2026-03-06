"""추세 중심 자동매매 시스템 v8 — TrendTrader + SSM"""
import asyncio
import sys
import os
import traceback

# Windows 콘솔 UTF-8
if sys.platform == "win32":
    os.system("")
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from db import init_db, purge_old_data
from config import (
    SYMBOLS, TRADE_CYCLE_INTERVAL, LIVE_TRADING_ENABLED, LIVE_USE_TESTNET,
    LIVE_LEVERAGE,
    OI_INTERVAL, FUNDING_INTERVAL, LONG_SHORT_INTERVAL,
    ORDERBOOK_INTERVAL, KLINES_DAILY_INTERVAL, KLINES_5M_INTERVAL,
    KLINES_4H_INTERVAL, KLINES_1H_INTERVAL,
    TAKER_RATIO_INTERVAL, TOP_LS_RATIO_INTERVAL,
    SSM_INTERVAL,
)

# Phase 1: 수집기
from collectors.binance_rest import (
    collect_open_interest, collect_funding_rate,
    collect_long_short_ratio, collect_orderbook_walls,
    collect_klines, collect_klines_5m, collect_klines_4h, collect_klines_1h,
    collect_taker_ratio, collect_top_trader_ratios,
)
from collectors.fear_greed import collect_fear_greed
from collectors.ws_liquidation import run_liquidation_stream

# Phase 2: 엔진
from engines.atr import calculate_atr
from engines.volume_profile import get_multi_tf_vp
from engines.trend_engine import TrendTrader
from engines.ssm_engine import SSMEngine
from engines.risk_manager import RiskManager
from engines.binance_executor import BinanceExecutor

# 전역 인스턴스
executor = None
trader = None
ssm = None
risk = None


def _wrap(func):
    """동기 함수 예외 래퍼"""
    def wrapper():
        try:
            func()
        except Exception as e:
            print(f"[오류] {func.__name__}: {e}")
            traceback.print_exc()
    return wrapper


def init_engines():
    """매매 엔진 초기화"""
    global executor, trader, ssm, risk
    executor = BinanceExecutor(use_testnet=LIVE_USE_TESTNET)
    ssm = SSMEngine()
    risk = RiskManager()
    trader = TrendTrader(executor, ssm=ssm, risk=risk)
    # 레버리지 + 마진타입 설정
    for sym in SYMBOLS:
        executor.set_leverage(sym, LIVE_LEVERAGE)
        executor.set_margin_type(sym, "CROSSED")
    print(f"[Main] 엔진 초기화 완료 ({'TESTNET' if LIVE_USE_TESTNET else 'MAINNET'})")


def trading_cycle():
    """30초 메인 트레이딩 루프 — 전체 스캔 후 최고 스코어 진입"""
    bal = executor.get_account_balance() if executor else 0
    try:
        trader.scan_and_enter(SYMBOLS, cached_balance=bal)
    except Exception as e:
        print(f"[Main] 트레이딩 사이클 오류: {e}")
        traceback.print_exc()


def ssm_update():
    """SSM 업데이트 (4시간마다) — Gemini grounding으로 고래/매크로 확인
    시장 전체 방향이므로 1회만 호출 (API 절약)"""
    if not ssm:
        return
    try:
        ssm.update()
    except Exception as e:
        print(f"[SSM] 업데이트 오류: {e}")


def vp_recalc():
    """VP 재계산 (1시간마다) — S/R 레벨 갱신"""
    for sym in SYMBOLS:
        try:
            trader.update_vp(sym)
        except Exception as e:
            print(f"[VP] {sym} 오류: {e}")


def position_reconcile():
    """Binance 실 포지션 vs DB 동기화 (30분마다)"""
    for sym in SYMBOLS:
        try:
            trader.reconcile(sym)
        except Exception as e:
            print(f"[Sync] {sym} 오류: {e}")


async def main():
    init_db()
    net = "TESTNET" if LIVE_USE_TESTNET else "MAINNET"

    print("=" * 60)
    print("  Trend Auto Trading System v8")
    print(f"  Network: {net} | Leverage: {LIVE_LEVERAGE}x")
    print(f"  Symbols: {', '.join(SYMBOLS)}")
    print(f"  Cycle: {TRADE_CYCLE_INTERVAL}s")
    print("=" * 60)

    # === 1단계: 데이터 수집 ===
    print("\n[1/3] 데이터 수집...")
    collect_klines()
    collect_klines_5m()
    collect_klines_4h()
    collect_klines_1h()
    collect_open_interest()
    collect_funding_rate()
    collect_long_short_ratio()
    collect_orderbook_walls()
    collect_taker_ratio()
    collect_top_trader_ratios()
    try:
        collect_fear_greed()
    except Exception:
        pass
    print("[1/3] 데이터 수집 완료")

    # === 2단계: VP + ATR 계산 ===
    print("\n[2/3] VP + ATR 계산...")
    calculate_atr()
    for sym in SYMBOLS:
        vp = get_multi_tf_vp(sym)
        d = vp.get("daily", {})
        if d.get("poc"):
            print(f"  {sym} Daily: POC=${d['poc']:,.2f} "
                  f"VA=${d['va_low']:,.2f}~${d['va_high']:,.2f}")
            for tf in ("4h", "1h", "5m"):
                t = vp.get(tf, {})
                if t.get("poc"):
                    print(f"  {sym} {tf:5s}: POC=${t['poc']:,.2f} "
                          f"VA=${t['va_low']:,.2f}~${t['va_high']:,.2f}")
            c = vp.get("composite", {})
            if c.get("poc"):
                print(f"  {sym} 합성:  POC=${c['poc']:,.2f} "
                      f"VA=${c['va_low']:,.2f}~${c['va_high']:,.2f}")
        else:
            print(f"  {sym} VP 데이터 부족")
    print("[2/3] 계산 완료")

    # === 3단계: 매매 엔진 ===
    print("\n[3/3] 매매 엔진 초기화...")
    init_engines()
    # SSM 초기 업데이트 (고래/매크로 방향 확인)
    print("[SSM] 초기 업데이트...")
    ssm_update()
    trading_cycle()  # 첫 사이클
    mode = "LIVE" if LIVE_TRADING_ENABLED else "DRY_RUN (판정만)"
    print(f"[3/3] 초기 사이클 완료 — {mode}")

    # === 스케줄러 ===
    scheduler = AsyncIOScheduler()

    # 수집 스케줄
    scheduler.add_job(_wrap(collect_klines), "interval", seconds=KLINES_DAILY_INTERVAL, id="klines_1d")
    scheduler.add_job(_wrap(collect_klines_5m), "interval", seconds=KLINES_5M_INTERVAL, id="klines_5m")
    scheduler.add_job(_wrap(collect_klines_4h), "interval", seconds=KLINES_4H_INTERVAL, id="klines_4h")
    scheduler.add_job(_wrap(collect_klines_1h), "interval", seconds=KLINES_1H_INTERVAL, id="klines_1h")
    scheduler.add_job(_wrap(collect_open_interest), "interval", seconds=OI_INTERVAL, id="oi")
    scheduler.add_job(_wrap(collect_funding_rate), "interval", seconds=FUNDING_INTERVAL, id="funding")
    scheduler.add_job(_wrap(collect_long_short_ratio), "interval", seconds=LONG_SHORT_INTERVAL, id="ls_ratio")
    scheduler.add_job(_wrap(collect_orderbook_walls), "interval", seconds=ORDERBOOK_INTERVAL, id="orderbook")
    scheduler.add_job(_wrap(collect_taker_ratio), "interval", seconds=TAKER_RATIO_INTERVAL, id="taker_ratio")
    scheduler.add_job(_wrap(collect_top_trader_ratios), "interval", seconds=TOP_LS_RATIO_INTERVAL, id="top_ls_ratio")
    scheduler.add_job(_wrap(collect_fear_greed), "interval", seconds=43200, id="fear_greed")

    # 엔진 스케줄
    scheduler.add_job(_wrap(calculate_atr), "interval", seconds=KLINES_DAILY_INTERVAL, id="atr")
    scheduler.add_job(_wrap(vp_recalc), "interval", seconds=KLINES_1H_INTERVAL, id="vp_recalc")
    scheduler.add_job(_wrap(ssm_update), "interval", seconds=SSM_INTERVAL, id="ssm")
    scheduler.add_job(_wrap(position_reconcile), "interval", seconds=1800, id="reconcile")
    scheduler.add_job(_wrap(purge_old_data), "interval", seconds=86400, id="db_purge")

    # 트레이딩 사이클 (30초)
    scheduler.add_job(_wrap(trading_cycle), "interval", seconds=TRADE_CYCLE_INTERVAL,
                      id="trading", max_instances=1, coalesce=True)

    scheduler.start()

    # 스케줄 요약
    print("\n[스케줄러] 가동 중")
    print(f"  수집: 5m={KLINES_5M_INTERVAL}s | 1h={KLINES_1H_INTERVAL}s | "
          f"4h={KLINES_4H_INTERVAL}s | 1d={KLINES_DAILY_INTERVAL}s")
    print(f"  OI={OI_INTERVAL}s | Funding={FUNDING_INTERVAL}s | "
          f"Orderbook={ORDERBOOK_INTERVAL}s | F&G=12h")
    print(f"  Taker={TAKER_RATIO_INTERVAL}s | TopLS={TOP_LS_RATIO_INTERVAL}s")
    print(f"  VP 재계산: {KLINES_1H_INTERVAL}s | ATR: {KLINES_DAILY_INTERVAL}s | SSM: {SSM_INTERVAL}s")
    print(f"  트레이딩: {TRADE_CYCLE_INTERVAL}s ({mode})")
    print("\n종료: Ctrl+C")

    # WebSocket 청산 스트림 (이벤트 루프 유지)
    await run_liquidation_stream()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[종료] 사용자에 의해 중단됨")
        sys.exit(0)
    except Exception as e:
        print(f"\n[FATAL] 치명적 오류: {e}")
        traceback.print_exc()
        sys.exit(1)
