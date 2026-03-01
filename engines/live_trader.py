"""Engine 8: Grid V2 — LIMIT 주문 기반 라이브 그리드 트레이더

V1 대비 변경:
- MARKET → LIMIT 주문 (maker 0.02%)
- 레벨 인덱스 → 가격 기반 추적
- 반응형 → 사전 배치 (BUY LIMIT 미리 걸어두기)
- 체결 감지 → 카운터 주문 자동 배치
- 포지션 동기화 (DB ↔ Binance 대조)
- 트렌드 가드, OOB 가드, 주문 타임아웃

안전장치 7층:
1. LIVE_TRADING_ENABLED 마스터 스위치
2. LIVE_USE_TESTNET 테스트넷 지원
3. 포지션 한도 (잔고 50%)
4. 일일 손실 한도 -3% circuit breaker (fail-safe)
5. 트렌드 가드 (4h 내 5% 이동시 중단)
6. OOB 가드 (범위 밖 30분 이상시 주문 철수)
7. grid_order_log 전수 감사 로깅
"""
import time
import threading
from datetime import date, datetime

import requests as _requests

from db import get_connection
from config import (
    LIVE_TRADING_ENABLED, LIVE_USE_TESTNET, LIVE_SYMBOLS,
    LIVE_LEVERAGE, LIVE_DAILY_LOSS_LIMIT, LIVE_MAX_POSITION_PCT,
    MAKER_FEE_RATE, TAKER_FEE_RATE, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
    GRID_V2_WORKING_LEVELS, GRID_V2_ORDER_TIMEOUT,
    GRID_V2_PRICE_TOLERANCE, GRID_V2_MAX_POSITION_PCT,
    GRID_V2_TREND_GUARD_PCT, GRID_V2_TREND_GUARD_WINDOW,
    GRID_V2_OOB_PAUSE_MINUTES,
    GRID_V2_OOB_VOLUME_MULTIPLIER, GRID_V2_OOB_LIQ_THRESHOLD,
    GRID_V2_ENTRY_OFFSET_ATR_RATIO, GRID_V2_ENTRY_OFFSET_MAX_SPACING_PCT,
    GRID_V2_HOLDING_STOP_ATR_MULT,
    GRID_V2_MAX_NET_LEVELS,
    HYBRID_L2_MIN_SSM, HYBRID_L2_STOP_LOSS_PCT,
    HYBRID_L2_TRAILING_ACTIVATE, HYBRID_L2_TRAILING_DISTANCE,
    HYBRID_L2_MAX_DURATION, HYBRID_L2_ENABLED,
)

# 동시 실행 방지 Lock
_trade_lock = threading.Lock()

# 글로벌 executor 인스턴스 (lazy init)
_executor = None
_initialized_symbols = set()
_balance_ok = False

# OOB 추적: {symbol: first_oob_timestamp}
_oob_since: dict[str, float] = {}

# 가격 히스토리 (트렌드 가드용): {symbol: [(timestamp, price), ...]}
_price_history: dict[str, list[tuple[float, float]]] = {}

# 활성 그리드 ID: {symbol: grid_config_id} — OOB 재계산 전까지 고정
_active_grid_id: dict[str, int] = {}

# 활성 그리드 레벨 캐시: {symbol: list[float]} — 메모리 고정, OOB 전환시만 갱신
_active_levels: dict[str, list[float]] = {}
_active_spacing: dict[str, float] = {}

# 그리드 DB 초기화 완료 여부: {symbol: True}
_grid_db_initialized: dict[str, bool] = {}

# 방향 편향 캐시: {symbol: ("BULLISH"|"BEARISH"|"NEUTRAL", timestamp)}
_direction_bias: dict[str, tuple[str, float]] = {}
DIRECTION_BIAS_TTL = 300  # 5분마다 재계산

# ============================
# 하이브리드 전략: Grid ↔ L2 전환
# ============================
# 현재 모드: {symbol: "GRID" | "L2"}
_current_mode: dict[str, str] = {}

# L2 상태 추적
_l2_entry_price: dict[str, float] = {}
_l2_direction: dict[str, str] = {}      # "LONG" | "SHORT"
_l2_entry_time: dict[str, float] = {}
_l2_highest_pnl: dict[str, float] = {}
_l2_quantity: dict[str, float] = {}
_reconcile_skip_count: dict[str, int] = {}  # 넷포지션 스킵 카운터
_reconcile_skip_time: dict[str, float] = {}  # 스킵 시작 시간
_RECONCILE_MAX_SKIPS = 3  # 최대 연속 스킵 횟수 (3회 = 90초)
_RECONCILE_SKIP_TIMEOUT = 300  # 5분 타임아웃 (초)


# ============================
# 방향 편향 판단 (EMA(48) 기울기 + 일봉)
# ============================

def _calc_ema(closes: list[float], period: int) -> list[float]:
    """EMA 계산 — closes는 오래된→최신 순서, 반환도 같은 순서"""
    if len(closes) < period:
        return []
    k = 2.0 / (period + 1)
    ema_values = [sum(closes[:period]) / period]  # 첫 SMA
    for price in closes[period:]:
        ema_values.append(price * k + ema_values[-1] * (1 - k))
    return ema_values


def _get_short_term_bias(symbol: str) -> tuple[str, float, float]:
    """5분봉 EMA(48) 기울기 + 가격 위치로 단기 방향 판단

    Returns:
        (bias, ema_slope_pct, current_price)
        bias: "BULLISH" | "BEARISH" | "NEUTRAL"
    """
    conn = get_connection()
    rows = conn.execute(
        "SELECT close FROM klines "
        "WHERE symbol = ? AND interval = '5m' "
        "ORDER BY open_time DESC LIMIT 60",
        (symbol,),
    ).fetchall()
    conn.close()

    if len(rows) < 50:
        return ("NEUTRAL", 0.0, 0.0)

    closes = [r[0] for r in reversed(rows)]  # 오래된→최신
    ema = _calc_ema(closes, 48)
    if len(ema) < 13:
        return ("NEUTRAL", 0.0, 0.0)

    ema_now = ema[-1]
    ema_1h_ago = ema[-13]  # 13 × 5분 = 1시간 전
    slope_pct = (ema_now - ema_1h_ago) / ema_1h_ago * 100
    current_price = closes[-1]

    if current_price < ema_now and slope_pct < -0.15:
        bias = "BEARISH"
    elif current_price > ema_now and slope_pct > 0.15:
        bias = "BULLISH"
    else:
        bias = "NEUTRAL"

    return (bias, slope_pct, current_price)


def _get_direction_bias(symbol: str) -> str:
    """MTF 종합 판단 + 단기 EMA 필터 기반 방향 편향

    단기 필터 (5분봉 EMA48):
      가격 < EMA AND 기울기 < -0.15% → 단기 BEARISH → MTF 무시, BEARISH 강제
      가격 > EMA AND 기울기 > +0.15% → 단기 BULLISH → MTF 판단 유지
      그 외 → NEUTRAL → MTF 판단 유지

    MTF 판단 (MA 정렬 + 스윙 패턴):
      MA 상승 + 스윙 동의/횡보 → BULLISH (L4:S2)
      MA 하락 + 스윙 동의/횡보 → BEARISH (L0:S4, 롱 차단)
      MA↔스윙 충돌, MA 중립 → NEUTRAL (L3:S3)
    """
    cached = _direction_bias.get(symbol)
    if cached and time.time() - cached[1] < DIRECTION_BIAS_TTL:
        return cached[0]

    # 0) 단기 EMA 필터
    short_term, slope_pct, cur_price = _get_short_term_bias(symbol)

    # 1) MTF 데이터 조회
    try:
        from engines.mtf_analyzer import get_latest_mtf
        mtf = get_latest_mtf(symbol)
    except Exception:
        mtf = None

    if not mtf:
        # MTF 없어도 단기 필터 적용
        bias = short_term if short_term == "BEARISH" else "NEUTRAL"
        _direction_bias[symbol] = (bias, time.time())
        print(f"[Grid][{symbol}] bias: {bias} (MTF 없음) | "
              f"short_term={short_term} slope={slope_pct:+.3f}%")
        return bias

    alignment = mtf["alignment_score"]  # -1.0 ~ +1.0
    pattern_1d = mtf.get("pattern_1d", "sideways")
    pattern_4h = mtf.get("pattern_4h", "sideways")

    # 2) MA 방향 (MTF alignment)
    if alignment >= 0.5:
        ma_dir = "BULLISH"
    elif alignment <= -0.5:
        ma_dir = "BEARISH"
    else:
        ma_dir = "NEUTRAL"

    # 3) 스윙 방향 (1D 우선, 4H 보조)
    bullish_patterns = ("uptrend", "ascending")
    bearish_patterns = ("downtrend", "descending")

    if pattern_1d in bullish_patterns:
        swing_dir = "BULLISH"
    elif pattern_1d in bearish_patterns:
        swing_dir = "BEARISH"
    elif pattern_4h in bullish_patterns:
        swing_dir = "BULLISH"
    elif pattern_4h in bearish_patterns:
        swing_dir = "BEARISH"
    else:
        swing_dir = "NEUTRAL"

    # 4) MTF 종합 판단
    if ma_dir == "BULLISH" and swing_dir != "BEARISH":
        mtf_bias = "BULLISH"
    elif ma_dir == "BEARISH" and swing_dir != "BULLISH":
        mtf_bias = "BEARISH"
    else:
        mtf_bias = "NEUTRAL"

    # 5) 단기 필터 override — 단기 하락이면 MTF 무시하고 BEARISH 강제
    if short_term == "BEARISH":
        bias = "BEARISH"
    else:
        bias = mtf_bias

    _direction_bias[symbol] = (bias, time.time())
    print(f"[Grid][{symbol}] bias: {bias} | "
          f"MA={alignment:+.2f}({ma_dir}) "
          f"Swing=1D:{pattern_1d}/4H:{pattern_4h}({swing_dir}) "
          f"short_term={short_term}(slope={slope_pct:+.3f}%)")
    return bias


# ============================
# Telegram 알림
# ============================

def _send_telegram(message: str):
    """Telegram 알림 전송"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        _requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message},
            timeout=5,
        )
    except Exception:
        pass


# ============================
# Executor 초기화
# ============================

def _get_executor():
    """BinanceExecutor lazy 초기화"""
    global _executor
    if _executor is None:
        from engines.binance_executor import BinanceExecutor
        _executor = BinanceExecutor(use_testnet=LIVE_USE_TESTNET)
    return _executor


def _init_symbol(symbol: str):
    """심볼별 최초 1회 초기화: 레버리지 + 마진타입 + 잔여 포지션 정리"""
    if symbol in _initialized_symbols:
        return
    ex = _get_executor()
    ex.set_margin_type(symbol, "CROSSED")
    ex.set_leverage(symbol, LIVE_LEVERAGE)

    # 시작시 Binance 잔여 포지션 체크 → DB에 없으면 청산
    try:
        positions = ex.get_positions()
        for p in positions:
            if p["symbol"] == symbol:
                amt = float(p.get("positionAmt", 0))
                if amt != 0:
                    conn = get_connection()
                    db_has_holdings = conn.execute(
                        "SELECT COUNT(*) FROM grid_positions "
                        "WHERE symbol = ? AND status IN ('HOLDING', 'SELL_OPEN', 'BUY_OPEN') "
                        "AND quantity > 0",
                        (symbol,),
                    ).fetchone()[0]
                    conn.close()
                    if db_has_holdings == 0:
                        close_side = "BUY" if amt < 0 else "SELL"
                        ex.cancel_all_orders(symbol)
                        ex.place_market_order(symbol, close_side, abs(amt))
                        print(f"[Live V2] {symbol}: 시작시 잔여 포지션 정리 "
                              f"{amt:+.4f} → {close_side} {abs(amt)}")
                break
    except Exception as e:
        print(f"[Live V2] {symbol}: 시작시 포지션 체크 실패 — {e}")

    _initialized_symbols.add(symbol)


# ============================
# 메인 루프
# ============================

def run_live_trader():
    """라이브 트레이더 메인 루프 (30초마다 호출)"""
    global _balance_ok

    if not LIVE_TRADING_ENABLED:
        return

    # 잔고 체크 — totalBalance 또는 오픈 포지션이 있으면 통과
    if not _balance_ok:
        try:
            ex = _get_executor()
            total_balance = ex.get_total_balance()
            avail_balance = ex.get_account_balance()
            has_positions = len(ex.get_positions()) > 0

            if total_balance >= 5.0 or has_positions:
                display_balance = total_balance if total_balance > avail_balance else avail_balance
                if has_positions and avail_balance < 5.0:
                    print(f"[Live] 포지션 보유 중 — 총 잔고 ${total_balance:.2f} (가용 ${avail_balance:.2f})")
                else:
                    print(f"[Live] Futures 잔고 확인: ${display_balance:.2f} — 트레이딩 시작!")
                _send_telegram(
                    f"Grid V2 시작!\n"
                    f"Futures 잔고: ${total_balance:.2f} (가용 ${avail_balance:.2f})\n"
                    f"심볼: {', '.join(LIVE_SYMBOLS)}\n"
                    f"레버리지: {LIVE_LEVERAGE}x\n"
                    f"모드: LIMIT (maker 0.02%)"
                )
                _balance_ok = True
            else:
                print(f"[Live] Futures 잔고 대기 중: ${total_balance:.2f} (최소 $5 필요)")
                return
        except Exception as e:
            print(f"[Live] 잔고 확인 실패: {e}")
            return

    if not _balance_ok:
        return

    # Circuit breaker 체크 (fail-safe: API 실패시 True → 거래 중단)
    if _is_circuit_breaker_hit():
        return

    for symbol in LIVE_SYMBOLS:
        _init_symbol(symbol)
        mode = _current_mode.get(symbol, "GRID")
        if mode == "L2":
            _run_l2_cycle(symbol)
        else:
            _run_grid_cycle(symbol)


def _run_grid_cycle(symbol: str):
    """Lock 보호 그리드 사이클"""
    if not _trade_lock.acquire(blocking=False):
        return
    try:
        _run_grid_cycle_inner(symbol)
    except Exception as e:
        print(f"[Live V2] {symbol}: 사이클 오류 — {e}")
        import traceback
        traceback.print_exc()
    finally:
        _trade_lock.release()


def _run_grid_cycle_inner(symbol: str):
    """그리드 V2 메인 사이클"""
    conn = get_connection()

    # Step 0: L4 활성 여부 확인
    state = conn.execute(
        "SELECT l4_active FROM strategy_state "
        "WHERE symbol = ? ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()

    if not state or not state[0]:
        conn.close()
        return

    # Step 1: 실시간 마크 프라이스 조회
    ex = _get_executor()
    mark_price = ex.get_mark_price(symbol)
    if not mark_price:
        print(f"[Live V2] {symbol}: 마크 프라이스 조회 실패 — 사이클 스킵")
        conn.close()
        return

    # Step 2: 그리드 레벨 로드 (메모리 캐시 우선)
    # — 메모리에 캐시된 레벨이 있으면 그대로 사용 (재계산 안 함)
    # — OOB PAUSE 시에만 캐시 무효화 → 다음 사이클에서 최신 grid_configs 사용
    if symbol in _active_levels:
        levels = _active_levels[symbol]
        spacing = _active_spacing[symbol]
        grid_id = _active_grid_id.get(symbol, 0)
    else:
        # 최초 또는 OOB 리셋 후 → 최신 그리드 사용
        grid = conn.execute(
            "SELECT id, lower_bound, upper_bound, grid_count, grid_spacing "
            "FROM grid_configs WHERE symbol = ? ORDER BY id DESC LIMIT 1",
            (symbol,),
        ).fetchone()

        if not grid:
            conn.close()
            return

        grid_id, lower, upper, count, spacing = grid
        levels = [round(lower + i * spacing, 2) for i in range(count + 1)]

        # 메모리에 고정 (OOB 전환 전까지 불변)
        _active_grid_id[symbol] = grid_id
        _active_levels[symbol] = levels
        _active_spacing[symbol] = spacing
        print(f"[Live V2][{symbol}] 그리드 고정 id={grid_id} "
              f"${levels[0]:,.2f}~${levels[-1]:,.2f} ({len(levels)}레벨, 간격=${spacing:.2f})")

    # Step 3: OOB (Out of Bounds) 체크 — 트렌드 가드보다 먼저 실행
    # (강한 추세 = OOB 발생 = L2가 필요한 상황이므로 트렌드 가드에 막히면 안 됨)
    if mark_price < levels[0] or mark_price > levels[-1]:
        print(f"[Live V2][{symbol}] OOB 감지: 현재가 ${mark_price:,.2f} / 범위 ${levels[0]:,.2f}~${levels[-1]:,.2f}")
        oob_action = _handle_oob(symbol, mark_price, levels, conn)
        if oob_action == "PAUSE":
            # 하이브리드: 거래량 동반 OOB → L2 전환 시도
            # ※ 캐시 클리어는 L2 전환 실패 후에만 — 현재 levels로 L2 판단해야 함
            l2_entered = False
            if HYBRID_L2_ENABLED:
                volume_signal = _check_volume_breakout(conn, symbol)
                if volume_signal:
                    l2_entered = _try_enter_l2(symbol, mark_price, levels, conn)
                    if l2_entered:
                        # L2 전환 성공 → 그리드 캐시 클리어 (L2 종료 후 최신 범위 사용)
                        _active_grid_id.pop(symbol, None)
                        _active_levels.pop(symbol, None)
                        _active_spacing.pop(symbol, None)
                        _grid_db_initialized.pop(symbol, None)
                        _direction_bias.pop(symbol, None)
                        conn.close()
                        return

            # L2 전환 안 됨 → HOLDING 포지션 시장가 청산 + 캐시 유지
            # ※ 캐시를 클리어하지 않음 → 다음 사이클에서도 같은 범위로 OOB 판단 유지
            # ※ 새 범위 로드하면 OOB가 사라져서 L2 기회 소멸
            if not l2_entered:
                _close_all_grid_holdings(symbol, conn)
                conn.commit()

            conn.close()
            return
        # oob_action == "WAIT": 아직 조건 미충족 → 사이클 스킵만
        conn.close()
        return
    else:
        # 범위 내 → OOB 타이머 리셋
        _oob_since.pop(symbol, None)

    # Step 4: 트렌드 가드 체크 (범위 내 그리드 주문에만 적용)
    if _is_trend_guard_active(symbol, mark_price):
        print(f"[Live V2][{symbol}] 트렌드 가드 발동 — 사이클 스킵, 캐시 클리어")
        _active_levels.pop(symbol, None)
        _active_spacing.pop(symbol, None)
        _active_grid_id.pop(symbol, None)
        _grid_db_initialized.pop(symbol, None)
        conn.close()
        return

    # Step 5~9: 예외 발생 시에도 conn.close() 보장
    try:
        # Step 5: 그리드 레벨 DB 동기화 (최초 1회만)
        if not _grid_db_initialized.get(symbol):
            _init_grid_positions(conn, symbol, levels)
            _grid_db_initialized[symbol] = True

        # Step 6: 체결 감지 + 카운터 주문 배치
        _detect_fills_and_place_counters(conn, symbol, mark_price, levels, spacing)

        # Step 6.5: HOLDING 개별 스톱로스 체크 (ATR 기반)
        _check_holding_stop_loss(conn, symbol, mark_price)

        # Step 7: Working window 관리 (±N레벨 BUY/SELL 배치)
        print(f"[Live V2][{symbol}] Working Window 관리 시작")
        _manage_working_window(conn, symbol, mark_price, levels, spacing)

        # Step 8: 주문 타임아웃 정리
        _cleanup_stale_orders(conn, symbol)

        # Step 9: 포지션 동기화 (DB vs Binance)
        _reconcile_positions(conn, symbol)
    finally:
        conn.close()


# ============================
# 그리드 레벨 관리
# ============================

def _init_grid_positions(conn, symbol: str, levels: list[float]):
    """그리드 DB 초기화: 기존 포지션 정리 후 새 레벨 생성 (방향 보존)

    - 기존 BUY_OPEN/SELL_OPEN 주문 취소
    - HOLDING 포지션은 가장 가까운 새 레벨에 매핑 (direction 보존)
    - 매핑 불가한 HOLDING은 시장가 청산 (LONG→SELL, SHORT→BUY)
    """
    ex = _get_executor()

    # 1. 기존 grid_positions 조회
    existing = conn.execute(
        "SELECT grid_price, status, quantity, buy_fill_price, buy_order_id, "
        "sell_order_id, direction, entry_fill_price "
        "FROM grid_positions WHERE symbol = ?",
        (symbol,),
    ).fetchall()

    existing_prices = {round(row[0], 2) for row in existing}
    new_prices = {round(p, 2) for p in levels}

    # 이미 동일하면 스킵
    if existing_prices == new_prices:
        print(f"[Live V2] {symbol}: 그리드 DB 확인 완료 ({len(levels)}레벨)")
        return

    # 2. 기존 주문 전량 취소
    if existing:
        ex.cancel_all_orders(symbol)
        for row in existing:
            price, status, qty, buy_fill, buy_oid, sell_oid, direction, entry_fill = row
            if status in ("BUY_OPEN", "SELL_OPEN"):
                side = "BUY" if status == "BUY_OPEN" else "SELL"
                _log_grid_order(conn, symbol, side,
                                price, qty or 0, price, None, None, "CANCELLED",
                                direction=direction)

    # 3. HOLDING 포지션 수집 (매핑용) — direction 포함
    holdings = [
        (row[0], row[2], row[3], row[6], row[7])  # price, qty, buy_fill, direction, entry_fill
        for row in existing if row[1] == "HOLDING"
    ]

    # 4. 기존 레벨 전부 삭제 → 새 레벨 생성
    conn.execute("DELETE FROM grid_positions WHERE symbol = ?", (symbol,))
    for price in levels:
        conn.execute(
            "INSERT INTO grid_positions "
            "(symbol, grid_price, status, quantity) "
            "VALUES (?, ?, 'EMPTY', 0)",
            (symbol, price),
        )

    # 5. HOLDING 매핑 (direction 보존, 중복 타겟 방지)
    used_targets = set()
    for old_price, qty, buy_fill, direction, entry_fill in holdings:
        best_match = None
        best_dist = float("inf")
        for new_price in levels:
            if new_price in used_targets:
                continue
            dist = abs(new_price - old_price) / old_price
            if dist < best_dist:
                best_dist = dist
                best_match = new_price

        if best_match and best_dist <= GRID_V2_PRICE_TOLERANCE:
            used_targets.add(best_match)
            conn.execute(
                "UPDATE grid_positions SET status = 'HOLDING', quantity = ?, "
                "buy_fill_price = ?, direction = ?, entry_fill_price = ? "
                "WHERE symbol = ? AND grid_price = ?",
                (qty, buy_fill, direction, entry_fill, symbol, best_match),
            )
            print(f"[Live V2] {symbol}: HOLDING({direction}) ${old_price:,.2f} → ${best_match:,.2f} 매핑")
        else:
            if qty and qty > 0:
                # LONG → SELL, SHORT → BUY
                close_side = "BUY" if direction == "SHORT" else "SELL"
                result = ex.place_market_order(symbol, close_side, qty)
                fill_price = _extract_fill_price(result)
                if fill_price:
                    _check_slippage(symbol, old_price, fill_price, close_side)
                pnl = 0.0
                ref_price = entry_fill or buy_fill
                if ref_price and fill_price:
                    fee = (ref_price + fill_price) * qty * TAKER_FEE_RATE
                    if direction == "SHORT":
                        pnl = (ref_price - fill_price) * qty - fee
                    else:
                        pnl = (fill_price - ref_price) * qty - fee
                _log_grid_order(conn, symbol, close_side, old_price, qty, old_price,
                                str(result.get("orderId", "")) if result else None,
                                None, "FILLED" if result else "FAILED",
                                fill_price=fill_price, pnl_usd=pnl, direction=direction)
                print(f"[Live V2] {symbol}: HOLDING({direction}) ${old_price:,.2f} 매핑 불가 → "
                      f"{close_side} 시장가 청산 PnL=${pnl:+.2f}")

    conn.commit()
    if existing:
        print(f"[Live V2] {symbol}: 그리드 전환 완료 — {len(levels)}개 레벨")
    else:
        print(f"[Live V2] {symbol}: 그리드 레벨 {len(levels)}개 초기화 "
              f"(${levels[0]:,.2f} ~ ${levels[-1]:,.2f})")


# ============================
# 체결 감지 + 카운터 주문
# ============================

def _detect_fills_and_place_counters(conn, symbol: str, mark_price: float,
                                       levels: list[float], spacing: float):
    """BUY_OPEN/SELL_OPEN 체결 감지 → 양방향 카운터 주문 배치"""
    ex = _get_executor()

    # ===== BUY_OPEN 체결 감지 =====
    buy_opens = conn.execute(
        "SELECT grid_price, buy_order_id, buy_client_order_id, quantity, "
        "direction, entry_fill_price "
        "FROM grid_positions WHERE symbol = ? AND status = 'BUY_OPEN'",
        (symbol,),
    ).fetchall()

    for row in buy_opens:
        grid_price, order_id, client_oid, qty, direction, entry_fill = row
        if not order_id:
            continue
        order_status = ex.get_order_status(symbol, int(order_id))
        if not order_status:
            continue
        status = order_status.get("status", "")

        if status in ("FILLED", "PARTIALLY_FILLED"):
            fill_price = float(order_status.get("avgPrice", grid_price))
            fill_qty = float(order_status.get("executedQty", qty or 0))

            # 부분 체결 → 잔량 취소 후 체결 수량만 처리
            if status == "PARTIALLY_FILLED":
                ex.cancel_order(symbol, order_id)
                if fill_qty < _get_min_qty(symbol):
                    continue  # 체결량이 최소 미만이면 무시

            if direction == "SHORT":
                # SHORT 커버 완료 → EMPTY + PnL
                pnl_usd = 0.0
                if entry_fill:
                    gross = (entry_fill - fill_price) * fill_qty
                    fee = (entry_fill * fill_qty + fill_price * fill_qty) * MAKER_FEE_RATE
                    pnl_usd = gross - fee
                conn.execute(
                    "UPDATE grid_positions SET status = 'EMPTY', direction = NULL, "
                    "quantity = 0, buy_fill_price = NULL, entry_fill_price = NULL, "
                    "buy_order_id = NULL, sell_order_id = NULL, "
                    "buy_client_order_id = NULL, sell_client_order_id = NULL "
                    "WHERE symbol = ? AND grid_price = ?",
                    (symbol, grid_price),
                )
                conn.execute(
                    "UPDATE grid_order_log SET status = 'FILLED', fill_price = ?, "
                    "fee = ?, pnl_usd = ?, filled_at = datetime('now') "
                    "WHERE order_id = ? AND status = 'PLACED'",
                    (fill_price, fill_price * fill_qty * MAKER_FEE_RATE,
                     pnl_usd, order_id),
                )
                _update_daily_pnl_usd(conn, pnl_usd)
                conn.commit()
                print(f"[Live V2] {symbol}: SHORT 커버 @ ${fill_price:,.2f} — PnL ${pnl_usd:+.4f}")
                _send_telegram(f"[LIVE] {symbol} SHORT 커버\n${fill_price:,.2f} PnL ${pnl_usd:+.4f}")
            else:
                # LONG 진입 체결 → HOLDING + SELL 카운터
                conn.execute(
                    "UPDATE grid_positions SET status = 'HOLDING', "
                    "quantity = ?, buy_fill_price = ?, entry_fill_price = ?, "
                    "buy_order_id = NULL, buy_client_order_id = NULL "
                    "WHERE symbol = ? AND grid_price = ?",
                    (fill_qty, fill_price, fill_price, symbol, grid_price),
                )
                conn.execute(
                    "UPDATE grid_order_log SET status = 'FILLED', fill_price = ?, "
                    "fee = ?, filled_at = datetime('now') "
                    "WHERE order_id = ? AND status = 'PLACED'",
                    (fill_price, fill_price * fill_qty * MAKER_FEE_RATE, order_id),
                )
                conn.commit()
                print(f"[Live V2] {symbol}: LONG BUY 체결 @ ${fill_price:,.2f}")
                _send_telegram(f"[LIVE] {symbol} LONG BUY 체결 ${fill_price:,.2f}")
                sell_price = round(grid_price + spacing, 2)
                _place_exit_sell_limit(conn, ex, symbol, grid_price, sell_price, fill_qty)

        elif status in ("CANCELED", "EXPIRED"):
            conn.execute(
                "UPDATE grid_order_log SET status = 'CANCELLED' "
                "WHERE order_id = ? AND status = 'PLACED'",
                (order_id,),
            )
            if direction == "SHORT":
                # SHORT 커버 취소 → 아직 HOLDING(SHORT)
                conn.execute(
                    "UPDATE grid_positions SET status = 'HOLDING', "
                    "buy_order_id = NULL, buy_client_order_id = NULL "
                    "WHERE symbol = ? AND grid_price = ?",
                    (symbol, grid_price),
                )
            else:
                # LONG 진입 취소 → EMPTY
                conn.execute(
                    "UPDATE grid_positions SET status = 'EMPTY', direction = NULL, "
                    "buy_order_id = NULL, buy_client_order_id = NULL, quantity = 0 "
                    "WHERE symbol = ? AND grid_price = ?",
                    (symbol, grid_price),
                )
            conn.commit()

    # ===== SELL_OPEN 체결 감지 =====
    sell_opens = conn.execute(
        "SELECT grid_price, sell_order_id, sell_client_order_id, quantity, "
        "buy_fill_price, direction, entry_fill_price "
        "FROM grid_positions WHERE symbol = ? AND status = 'SELL_OPEN'",
        (symbol,),
    ).fetchall()

    for row in sell_opens:
        grid_price, order_id, client_oid, qty, buy_fill, direction, entry_fill = row
        if not order_id:
            continue
        order_status = ex.get_order_status(symbol, int(order_id))
        if not order_status:
            continue
        status = order_status.get("status", "")

        if status in ("FILLED", "PARTIALLY_FILLED"):
            fill_price = float(order_status.get("avgPrice", grid_price))
            fill_qty = float(order_status.get("executedQty", qty or 0))

            # 부분 체결 → 잔량 취소 후 체결 수량만 처리
            if status == "PARTIALLY_FILLED":
                ex.cancel_order(symbol, order_id)
                if fill_qty < _get_min_qty(symbol):
                    continue

            if direction == "SHORT":
                # SHORT 진입 체결 → HOLDING + BUY 카운터
                conn.execute(
                    "UPDATE grid_positions SET status = 'HOLDING', "
                    "quantity = ?, entry_fill_price = ?, "
                    "sell_order_id = NULL, sell_client_order_id = NULL "
                    "WHERE symbol = ? AND grid_price = ?",
                    (fill_qty, fill_price, symbol, grid_price),
                )
                conn.execute(
                    "UPDATE grid_order_log SET status = 'FILLED', fill_price = ?, "
                    "fee = ?, filled_at = datetime('now') "
                    "WHERE order_id = ? AND status = 'PLACED'",
                    (fill_price, fill_price * fill_qty * MAKER_FEE_RATE, order_id),
                )
                conn.commit()
                print(f"[Live V2] {symbol}: SHORT SELL 체결 @ ${fill_price:,.2f}")
                _send_telegram(f"[LIVE] {symbol} SHORT SELL 체결 ${fill_price:,.2f}")
                buy_price = round(grid_price - spacing, 2)
                _place_exit_buy_limit(conn, ex, symbol, grid_price, buy_price, fill_qty)
            else:
                # LONG 익절 체결 → EMPTY + PnL
                pnl_usd = 0.0
                entry_p = entry_fill or buy_fill
                if entry_p:
                    gross = (fill_price - entry_p) * fill_qty
                    fee = (entry_p * fill_qty + fill_price * fill_qty) * MAKER_FEE_RATE
                    pnl_usd = gross - fee
                conn.execute(
                    "UPDATE grid_positions SET status = 'EMPTY', direction = NULL, "
                    "quantity = 0, buy_fill_price = NULL, entry_fill_price = NULL, "
                    "buy_order_id = NULL, sell_order_id = NULL, "
                    "buy_client_order_id = NULL, sell_client_order_id = NULL "
                    "WHERE symbol = ? AND grid_price = ?",
                    (symbol, grid_price),
                )
                conn.execute(
                    "UPDATE grid_order_log SET status = 'FILLED', fill_price = ?, "
                    "fee = ?, pnl_usd = ?, filled_at = datetime('now') "
                    "WHERE order_id = ? AND status = 'PLACED'",
                    (fill_price, fill_price * fill_qty * MAKER_FEE_RATE,
                     pnl_usd, order_id),
                )
                _update_daily_pnl_usd(conn, pnl_usd)
                conn.commit()
                print(f"[Live V2] {symbol}: LONG 익절 @ ${fill_price:,.2f} — PnL ${pnl_usd:+.4f}")
                _send_telegram(f"[LIVE] {symbol} LONG 익절 ${fill_price:,.2f} PnL ${pnl_usd:+.4f}")

        elif status in ("CANCELED", "EXPIRED"):
            conn.execute(
                "UPDATE grid_order_log SET status = 'CANCELLED' "
                "WHERE order_id = ? AND status = 'PLACED'",
                (order_id,),
            )
            if direction == "SHORT":
                # SHORT 진입 취소 → EMPTY
                conn.execute(
                    "UPDATE grid_positions SET status = 'EMPTY', direction = NULL, "
                    "sell_order_id = NULL, sell_client_order_id = NULL, quantity = 0 "
                    "WHERE symbol = ? AND grid_price = ?",
                    (symbol, grid_price),
                )
            else:
                # LONG 익절 취소 → HOLDING 유지
                conn.execute(
                    "UPDATE grid_positions SET status = 'HOLDING', "
                    "sell_order_id = NULL, sell_client_order_id = NULL "
                    "WHERE symbol = ? AND grid_price = ?",
                    (symbol, grid_price),
                )
            conn.commit()


# ============================
# Working Window 관리
# ============================

def _manage_working_window(conn, symbol: str, mark_price: float,
                            levels: list[float], spacing: float):
    """양방향 Working Window: 아래=LONG BUY, 위=SHORT SELL, 편향 비율 적용"""
    ex = _get_executor()

    # 현재가에 가장 가까운 레벨 인덱스
    closest_idx = 0
    min_dist = float("inf")
    for i, lv in enumerate(levels):
        dist = abs(lv - mark_price)
        if dist < min_dist:
            min_dist = dist
            closest_idx = i

    # 방향 편향: BEARISH=2L+4S, BULLISH=4L+2S, NEUTRAL=3+3
    bias = _get_direction_bias(symbol)
    wl = GRID_V2_WORKING_LEVELS

    if bias == "BEARISH":
        long_levels = max(1, wl - 1)    # 2 (소량 롱 허용, 급등 상쇄용)
        short_levels = wl + 1            # 4
    elif bias == "BULLISH":
        long_levels = wl + 1             # 4
        short_levels = max(1, wl - 1)    # 2
    else:
        long_levels = wl
        short_levels = wl

    window_low = max(0, closest_idx - long_levels)
    window_high = min(len(levels) - 1, closest_idx + short_levels)

    print(f"[Live V2][{symbol}] window [{window_low}..{window_high}] "
          f"bias={bias} L={long_levels} S={short_levels}")

    # 주문량 계산 (availableBalance = 전체잔고 - 사용중마진)
    balance = ex.get_account_balance()
    if balance <= 0:
        return

    # window 내 EMPTY 레벨 수 기준으로 분배 (실제 배치할 슬롯만 계산)
    window_size = window_high - window_low + 1
    empty_in_window = conn.execute(
        "SELECT COUNT(*) FROM grid_positions WHERE symbol = ? "
        "AND status = 'EMPTY' AND round(grid_price, 2) BETWEEN ? AND ?",
        (symbol, round(levels[window_low], 2), round(levels[window_high], 2)),
    ).fetchone()[0]

    if empty_in_window == 0:
        return

    remaining_slots = max(1, empty_in_window)
    per_grid_usdt = (balance * GRID_V2_MAX_POSITION_PCT / remaining_slots) * LIVE_LEVERAGE
    order_qty = per_grid_usdt / mark_price
    min_qty = _get_min_qty(symbol)

    min_qty_mode = False
    if order_qty < min_qty:
        # 최소 수량으로 배치 가능한 슬롯 수 계산
        max_affordable = int((balance * GRID_V2_MAX_POSITION_PCT * LIVE_LEVERAGE) / (min_qty * mark_price))
        if max_affordable <= 0:
            return
        order_qty = min_qty
        min_qty_mode = True

    # 넷포지션 한도 체크 — 편향 누적 방지
    # 실제 포지션 보유 중인 레벨만 카운트:
    #   HOLDING(LONG/SHORT), SELL_OPEN+LONG(익절 대기), BUY_OPEN+SHORT(커버 대기)
    # 진입 대기(BUY_OPEN+LONG, SELL_OPEN+SHORT)는 미체결이므로 제외
    net_positions = conn.execute(
        "SELECT direction, COUNT(*) FROM grid_positions "
        "WHERE symbol = ? AND quantity > 0 "
        "AND (status = 'HOLDING' "
        "  OR (status = 'SELL_OPEN' AND direction = 'LONG') "
        "  OR (status = 'BUY_OPEN' AND direction = 'SHORT')) "
        "GROUP BY direction",
        (symbol,),
    ).fetchall()
    net_counts = {d: c for d, c in net_positions}
    long_count = net_counts.get("LONG", 0)
    short_count = net_counts.get("SHORT", 0)
    net_level = long_count - short_count  # 양수=롱 편향, 음수=숏 편향

    # 수량 가중 넷포지션 체크
    qty_positions = conn.execute(
        "SELECT direction, SUM(quantity) FROM grid_positions "
        "WHERE symbol = ? AND quantity > 0 "
        "AND (status = 'HOLDING' "
        "  OR (status = 'SELL_OPEN' AND direction = 'LONG') "
        "  OR (status = 'BUY_OPEN' AND direction = 'SHORT')) "
        "GROUP BY direction",
        (symbol,),
    ).fetchall()
    qty_sums = {d: q for d, q in qty_positions}
    long_qty = qty_sums.get("LONG", 0.0)
    short_qty = qty_sums.get("SHORT", 0.0)
    net_qty = long_qty - short_qty
    max_net_qty = order_qty * GRID_V2_MAX_NET_LEVELS  # 수량 기준 한도

    block_long = net_level >= GRID_V2_MAX_NET_LEVELS or net_qty >= max_net_qty
    block_short = net_level <= -GRID_V2_MAX_NET_LEVELS or net_qty <= -max_net_qty
    if block_long or block_short:
        blocked = []
        if block_long:
            blocked.append("LONG")
        if block_short:
            blocked.append("SHORT")
        print(f"[Live V2][{symbol}] 넷포지션 한도: L={long_count} S={short_count} "
              f"net={net_level:+d} — {'/'.join(blocked)} 진입 차단")

    # window 내 레벨 순회
    placed_count = 0
    max_place = remaining_slots
    if min_qty_mode:
        max_place = min(remaining_slots, max_affordable)

    for i in range(window_low, window_high + 1):
        lv_price = round(levels[i], 2)

        row = conn.execute(
            "SELECT status, quantity, buy_order_id, sell_order_id, direction "
            "FROM grid_positions WHERE symbol = ? AND round(grid_price, 2) = ?",
            (symbol, lv_price),
        ).fetchone()

        if not row:
            continue

        status, qty, buy_oid, sell_oid, direction = row

        if status == "EMPTY":
            if placed_count >= max_place:
                continue
            if lv_price < mark_price:
                if block_long:
                    continue
                # 현재가 아래 → LONG 진입 BUY
                _place_entry_buy_limit(conn, ex, symbol, lv_price, order_qty, spacing)
                placed_count += 1
            elif lv_price > mark_price:
                if block_short:
                    continue
                # 현재가 위 → SHORT 진입 SELL
                _place_entry_sell_limit(conn, ex, symbol, lv_price, order_qty, spacing)
                placed_count += 1

        elif status == "HOLDING":
            if direction == "LONG" and not sell_oid:
                # LONG 익절 SELL
                sell_price = round(lv_price + spacing, 2)
                _place_exit_sell_limit(conn, ex, symbol, lv_price, sell_price, qty)
            elif direction == "SHORT" and not buy_oid:
                # SHORT 커버 BUY
                buy_price = round(lv_price - spacing, 2)
                if buy_price > 0:
                    _place_exit_buy_limit(conn, ex, symbol, lv_price, buy_price, qty)

    # window 밖 진입 주문 취소 (HOLDING 유지)
    _cancel_out_of_window(conn, ex, symbol, levels, window_low, window_high)


def _cancel_out_of_window(conn, ex, symbol: str, levels: list[float],
                           window_low: int, window_high: int):
    """window 밖 진입 주문 취소, 익절 주문은 유지"""
    low_price = levels[window_low]
    high_price = levels[window_high]

    # BUY_OPEN outside window
    outside_buys = conn.execute(
        "SELECT grid_price, buy_order_id, direction "
        "FROM grid_positions WHERE symbol = ? AND status = 'BUY_OPEN'",
        (symbol,),
    ).fetchall()
    for gp, oid, direction in outside_buys:
        if gp < low_price or gp > high_price:
            # SHORT 커버(익절)는 유지 — 취소하면 HOLDING이 출구 없이 방치됨
            if direction == "SHORT":
                continue
            # LONG 진입만 취소
            if oid:
                ex.cancel_order(symbol, oid)
                conn.execute(
                    "UPDATE grid_order_log SET status = 'CANCELLED' "
                    "WHERE order_id = ? AND status = 'PLACED'", (oid,))
            conn.execute(
                "UPDATE grid_positions SET status = 'EMPTY', direction = NULL, "
                "buy_order_id = NULL, buy_client_order_id = NULL, quantity = 0 "
                "WHERE symbol = ? AND grid_price = ?", (symbol, gp))
            conn.commit()

    # SELL_OPEN outside window
    outside_sells = conn.execute(
        "SELECT grid_price, sell_order_id, direction "
        "FROM grid_positions WHERE symbol = ? AND status = 'SELL_OPEN'",
        (symbol,),
    ).fetchall()
    for gp, oid, direction in outside_sells:
        if gp < low_price or gp > high_price:
            # LONG 익절은 유지 — 취소하면 HOLDING이 출구 없이 방치됨
            if direction == "LONG":
                continue
            # SHORT 진입만 취소
            if oid:
                ex.cancel_order(symbol, oid)
                conn.execute(
                    "UPDATE grid_order_log SET status = 'CANCELLED' "
                    "WHERE order_id = ? AND status = 'PLACED'", (oid,))
            conn.execute(
                "UPDATE grid_positions SET status = 'EMPTY', direction = NULL, "
                "sell_order_id = NULL, sell_client_order_id = NULL, quantity = 0 "
                "WHERE symbol = ? AND grid_price = ?", (symbol, gp))
            conn.commit()


# ============================
# 주문 배치 헬퍼 (양방향)
# ============================

def _get_entry_offset(symbol: str, grid_spacing: float) -> float:
    """ATR 기반 진입 오프셋 비율 계산 (0.0 ~ cap)

    오더북 벽 가격에서 안쪽으로 당길 비율을 반환.
    ATR 데이터 없으면 0 (기존 동작 유지).
    """
    from engines.atr import get_latest_atr
    atr_data = get_latest_atr(symbol)
    if not atr_data or atr_data["atr_pct"] <= 0:
        return 0.0

    # ATR%의 일정 비율을 오프셋으로 사용
    offset_pct = atr_data["atr_pct"] * GRID_V2_ENTRY_OFFSET_ATR_RATIO / 100

    # 안전장치: 그리드 간격의 50%를 넘지 않도록 cap
    if grid_spacing > 0:
        current_price = atr_data["current_price"]
        spacing_pct = grid_spacing / current_price if current_price > 0 else 0
        max_offset = spacing_pct * GRID_V2_ENTRY_OFFSET_MAX_SPACING_PCT
        offset_pct = min(offset_pct, max_offset)

    return offset_pct


def _place_entry_buy_limit(conn, ex, symbol: str, grid_price: float,
                           quantity: float, grid_spacing: float = 0):
    """LONG 진입: BUY LIMIT (벽 안쪽으로 오프셋 적용)"""
    offset = _get_entry_offset(symbol, grid_spacing)
    limit_price = grid_price * (1 + offset)  # 약간 높게 → 체결 확률 ↑

    client_oid = f"gv2_lb_{symbol}_{grid_price}_{int(time.time())}"
    result = ex.place_limit_order_with_id(symbol, "BUY", quantity, limit_price, client_oid)
    if result:
        order_id = str(result.get("orderId", ""))
        conn.execute(
            "UPDATE grid_positions SET status = 'BUY_OPEN', direction = 'LONG', "
            "buy_order_id = ?, buy_client_order_id = ?, quantity = ? "
            "WHERE symbol = ? AND grid_price = ?",
            (order_id, client_oid, quantity, symbol, grid_price),
        )
        _log_grid_order(conn, symbol, "BUY", grid_price, quantity,
                        limit_price, order_id, client_oid, "PLACED", direction="LONG")
        conn.commit()
        if offset > 0:
            print(f"[Grid] {symbol}: LONG BUY offset +{offset*100:.3f}% "
                  f"(grid=${grid_price:.2f} → limit=${limit_price:.2f})")
    else:
        _log_grid_order(conn, symbol, "BUY", grid_price, quantity,
                        limit_price, None, client_oid, "FAILED", direction="LONG")
        conn.commit()


def _place_entry_sell_limit(conn, ex, symbol: str, grid_price: float,
                            quantity: float, grid_spacing: float = 0):
    """SHORT 진입: SELL LIMIT (벽 안쪽으로 오프셋 적용)"""
    offset = _get_entry_offset(symbol, grid_spacing)
    limit_price = grid_price * (1 - offset)  # 약간 낮게 → 체결 확률 ↑

    client_oid = f"gv2_se_{symbol}_{grid_price}_{int(time.time())}"
    result = ex.place_limit_order_with_id(symbol, "SELL", quantity, limit_price, client_oid)
    if result:
        order_id = str(result.get("orderId", ""))
        conn.execute(
            "UPDATE grid_positions SET status = 'SELL_OPEN', direction = 'SHORT', "
            "sell_order_id = ?, sell_client_order_id = ?, quantity = ? "
            "WHERE symbol = ? AND grid_price = ?",
            (order_id, client_oid, quantity, symbol, grid_price),
        )
        _log_grid_order(conn, symbol, "SELL", grid_price, quantity,
                        limit_price, order_id, client_oid, "PLACED", direction="SHORT")
        conn.commit()
        if offset > 0:
            print(f"[Grid] {symbol}: SHORT SELL offset -{offset*100:.3f}% "
                  f"(grid=${grid_price:.2f} → limit=${limit_price:.2f})")
    else:
        _log_grid_order(conn, symbol, "SELL", grid_price, quantity,
                        limit_price, None, client_oid, "FAILED", direction="SHORT")
        conn.commit()


def _place_exit_sell_limit(conn, ex, symbol: str, grid_price: float,
                            sell_price: float, quantity: float):
    """LONG 익절: SELL LIMIT (grid_price + spacing)"""
    client_oid = f"gv2_ls_{symbol}_{grid_price}_{int(time.time())}"
    result = ex.place_limit_order_with_id(symbol, "SELL", quantity, sell_price, client_oid)
    if result:
        order_id = str(result.get("orderId", ""))
        conn.execute(
            "UPDATE grid_positions SET status = 'SELL_OPEN', "
            "sell_order_id = ?, sell_client_order_id = ? "
            "WHERE symbol = ? AND grid_price = ?",
            (order_id, client_oid, symbol, grid_price),
        )
        _log_grid_order(conn, symbol, "SELL", grid_price, quantity,
                        sell_price, order_id, client_oid, "PLACED", direction="LONG")
        conn.commit()
        print(f"[Live V2] {symbol}: LONG exit SELL ${sell_price:,.2f} — grid ${grid_price:,.2f}")
    else:
        _log_grid_order(conn, symbol, "SELL", grid_price, quantity,
                        sell_price, None, client_oid, "FAILED", direction="LONG")
        conn.commit()


def _place_exit_buy_limit(conn, ex, symbol: str, grid_price: float,
                           buy_price: float, quantity: float):
    """SHORT 커버: BUY LIMIT (grid_price - spacing)"""
    client_oid = f"gv2_sb_{symbol}_{grid_price}_{int(time.time())}"
    result = ex.place_limit_order_with_id(symbol, "BUY", quantity, buy_price, client_oid)
    if result:
        order_id = str(result.get("orderId", ""))
        conn.execute(
            "UPDATE grid_positions SET status = 'BUY_OPEN', "
            "buy_order_id = ?, buy_client_order_id = ? "
            "WHERE symbol = ? AND grid_price = ?",
            (order_id, client_oid, symbol, grid_price),
        )
        _log_grid_order(conn, symbol, "BUY", grid_price, quantity,
                        buy_price, order_id, client_oid, "PLACED", direction="SHORT")
        conn.commit()
        print(f"[Live V2] {symbol}: SHORT exit BUY ${buy_price:,.2f} — grid ${grid_price:,.2f}")
    else:
        _log_grid_order(conn, symbol, "BUY", grid_price, quantity,
                        buy_price, None, client_oid, "FAILED", direction="SHORT")
        conn.commit()


# ============================
# 안전장치
# ============================

def _is_trend_guard_active(symbol: str, current_price: float) -> bool:
    """4시간 내 5% 이상 방향성 이동 감지"""
    now = time.time()

    if symbol not in _price_history:
        # 재시작 후 첫 호출: DB에서 최근 4시간 5분봉으로 초기화
        _price_history[symbol] = []
        try:
            conn = get_connection()
            rows = conn.execute(
                "SELECT open_time / 1000, close FROM klines "
                "WHERE symbol = ? AND interval = '5m' "
                "ORDER BY open_time DESC LIMIT 48",  # 4h = 48 * 5min
                (symbol,),
            ).fetchall()
            conn.close()
            for ts, price in reversed(rows):
                if ts >= now - GRID_V2_TREND_GUARD_WINDOW:
                    _price_history[symbol].append((float(ts), float(price)))
        except Exception:
            pass

    _price_history[symbol].append((now, current_price))

    # 4시간 이전 데이터 제거
    cutoff = now - GRID_V2_TREND_GUARD_WINDOW
    _price_history[symbol] = [
        (t, p) for t, p in _price_history[symbol] if t >= cutoff
    ]

    if len(_price_history[symbol]) < 2:
        return False

    oldest_price = _price_history[symbol][0][1]
    change_pct = abs(current_price - oldest_price) / oldest_price * 100

    if change_pct >= GRID_V2_TREND_GUARD_PCT:
        direction = "상승" if current_price > oldest_price else "하락"
        print(f"[Live V2] {symbol}: 트렌드 가드! {direction} {change_pct:.1f}% "
              f"(4h 내 {GRID_V2_TREND_GUARD_PCT}% 초과)")
        return True

    return False


def _handle_oob(symbol: str, mark_price: float, levels: list[float], conn) -> str:
    """Out of Bounds 처리: 거래량+청산 기반 즉시 판정, 시간은 폴백

    판정 로직:
    - 범위 밖 + 거래량 > 평균 2배 + 청산 급증 → 즉시 PAUSE (확정 이탈)
    - 범위 밖 + 거래량 > 평균 2배             → 즉시 PAUSE (높은 확률 이탈)
    - 범위 밖 + 거래량 보통                    → WAIT (폴백: 30분 후 PAUSE)
    """
    now = time.time()

    if symbol not in _oob_since:
        _oob_since[symbol] = now
        print(f"[Live V2] {symbol}: 범위 이탈 감지 (${mark_price:,.2f}) — "
              f"범위: ${levels[0]:,.2f}-${levels[-1]:,.2f}")

    # === 거래량 체크: 최근 1시간 vs 24시간 평균 ===
    volume_signal = _check_volume_breakout(conn, symbol)

    # === 청산 체크: 최근 1시간 청산 금액 ===
    liq_signal = _check_liquidation_surge(conn, symbol)

    # === 판정 ===
    reason = None
    if volume_signal and liq_signal:
        reason = f"거래량 급증 + 청산 급증"
    elif volume_signal:
        reason = f"거래량 급증"

    if reason:
        # 즉시 PAUSE — 시간 기다릴 필요 없음
        _execute_oob_pause(symbol, mark_price, levels, conn, reason)
        return "PAUSE"

    # === 폴백: 시간 기반 (거래량 신호 없으면 30분 대기) ===
    elapsed_min = (now - _oob_since[symbol]) / 60

    if elapsed_min >= GRID_V2_OOB_PAUSE_MINUTES:
        _execute_oob_pause(symbol, mark_price, levels, conn,
                           f"범위 밖 {elapsed_min:.0f}분 (시간 폴백)")
        return "PAUSE"

    return "WAIT"


def _check_volume_breakout(conn, symbol: str) -> bool:
    """최근 1시간 거래량이 24시간 평균의 N배 이상인지 확인"""
    # 최근 1시간 거래량 (5분봉 12개)
    recent = conn.execute(
        "SELECT SUM(volume) FROM "
        "(SELECT volume FROM klines "
        " WHERE symbol = ? AND interval = '5m' "
        " ORDER BY open_time DESC LIMIT 12)",
        (symbol,),
    ).fetchone()

    # 24시간 거래량 (5분봉 288개)
    daily = conn.execute(
        "SELECT SUM(volume), COUNT(*) FROM "
        "(SELECT volume FROM klines "
        " WHERE symbol = ? AND interval = '5m' "
        " ORDER BY open_time DESC LIMIT 288)",
        (symbol,),
    ).fetchone()

    if not recent or not recent[0] or not daily or not daily[0]:
        return False

    vol_1h = recent[0]
    candle_count = daily[1] or 288
    vol_avg_1h = daily[0] / (candle_count / 12)  # 24시간을 1시간 단위로 평균

    if vol_avg_1h <= 0:
        return False

    ratio = vol_1h / vol_avg_1h

    if ratio >= GRID_V2_OOB_VOLUME_MULTIPLIER:
        print(f"[Live V2] {symbol}: 거래량 급증 — "
              f"1h={vol_1h:,.0f} / avg={vol_avg_1h:,.0f} ({ratio:.1f}x)")
        return True

    return False


def _check_liquidation_surge(conn, symbol: str) -> bool:
    """최근 1시간 청산 금액이 임계치 이상인지 확인"""
    try:
        cutoff_ms = int((time.time() - 3600) * 1000)
        row = conn.execute(
            "SELECT SUM(qty * price) FROM liquidations "
            "WHERE symbol = ? AND trade_time > ?",
            (symbol, cutoff_ms),
        ).fetchone()
    except Exception as e:
        print(f"[Live V2] {symbol}: 청산 데이터 조회 실패 — {e}")
        return False

    liq_amount = row[0] if row and row[0] else 0

    if liq_amount >= GRID_V2_OOB_LIQ_THRESHOLD:
        print(f"[Live V2] {symbol}: 청산 급증 — ${liq_amount:,.0f} "
              f"(임계: ${GRID_V2_OOB_LIQ_THRESHOLD:,.0f})")
        return True

    return False


def _execute_oob_pause(symbol: str, mark_price: float,
                        levels: list[float], conn, reason: str):
    """OOB 확정 → 모든 오픈 주문 취소 (방향별 처리)"""
    ex = _get_executor()
    ex.cancel_all_orders(symbol)

    # BUY_OPEN + LONG → EMPTY (롱 진입 취소)
    conn.execute(
        "UPDATE grid_positions SET status = 'EMPTY', direction = NULL, "
        "buy_order_id = NULL, buy_client_order_id = NULL, quantity = 0 "
        "WHERE symbol = ? AND status = 'BUY_OPEN' AND direction = 'LONG'",
        (symbol,),
    )
    # BUY_OPEN + SHORT → HOLDING (숏 커버 취소, 포지션 유지)
    conn.execute(
        "UPDATE grid_positions SET status = 'HOLDING', "
        "buy_order_id = NULL, buy_client_order_id = NULL "
        "WHERE symbol = ? AND status = 'BUY_OPEN' AND direction = 'SHORT'",
        (symbol,),
    )
    # SELL_OPEN + SHORT → EMPTY (숏 진입 취소)
    conn.execute(
        "UPDATE grid_positions SET status = 'EMPTY', direction = NULL, "
        "sell_order_id = NULL, sell_client_order_id = NULL, quantity = 0 "
        "WHERE symbol = ? AND status = 'SELL_OPEN' AND direction = 'SHORT'",
        (symbol,),
    )
    # SELL_OPEN + LONG → HOLDING (롱 익절 취소, 포지션 유지)
    conn.execute(
        "UPDATE grid_positions SET status = 'HOLDING', "
        "sell_order_id = NULL, sell_client_order_id = NULL "
        "WHERE symbol = ? AND status = 'SELL_OPEN' AND direction = 'LONG'",
        (symbol,),
    )
    # direction NULL인 레거시 주문 (안전 폴백)
    conn.execute(
        "UPDATE grid_positions SET status = 'EMPTY', "
        "buy_order_id = NULL, buy_client_order_id = NULL, quantity = 0 "
        "WHERE symbol = ? AND status = 'BUY_OPEN' AND direction IS NULL",
        (symbol,),
    )
    conn.execute(
        "UPDATE grid_positions SET status = 'HOLDING', "
        "sell_order_id = NULL, sell_client_order_id = NULL "
        "WHERE symbol = ? AND status = 'SELL_OPEN' AND direction IS NULL",
        (symbol,),
    )
    conn.commit()

    # OOB 타이머 리셋
    _oob_since.pop(symbol, None)

    print(f"[Live V2] {symbol}: OOB 확정 — {reason}")
    _send_telegram(
        f"[OOB] {symbol}\n"
        f"사유: {reason}\n"
        f"가격: ${mark_price:,.2f}\n"
        f"범위: ${levels[0]:,.2f}-${levels[-1]:,.2f}\n"
        f"주문 전량 취소"
    )


def _cleanup_stale_orders(conn, symbol: str):
    """1시간 이상 미체결 주문 취소 (방향별 처리)"""
    ex = _get_executor()

    # grid_order_log에서 1시간 이상된 PLACED 주문 찾기
    stale = conn.execute(
        "SELECT order_id, side, grid_price, direction FROM grid_order_log "
        "WHERE symbol = ? AND status = 'PLACED' "
        "AND created_at < datetime('now', '-1 hour')",
        (symbol,),
    ).fetchall()

    for order_id, side, grid_price, direction in stale:
        if order_id:
            result = ex.cancel_order(symbol, order_id)
            conn.execute(
                "UPDATE grid_order_log SET status = 'CANCELLED' "
                "WHERE order_id = ? AND status = 'PLACED'",
                (order_id,),
            )
            if result is not None:
                # 방향별 상태 리셋
                if side == "BUY":
                    if direction == "SHORT":
                        # SHORT 커버 취소 → HOLDING 유지
                        conn.execute(
                            "UPDATE grid_positions SET status = 'HOLDING', "
                            "buy_order_id = NULL, buy_client_order_id = NULL "
                            "WHERE symbol = ? AND grid_price = ? AND status = 'BUY_OPEN'",
                            (symbol, grid_price),
                        )
                    else:
                        # LONG 진입 취소 → EMPTY
                        conn.execute(
                            "UPDATE grid_positions SET status = 'EMPTY', direction = NULL, "
                            "buy_order_id = NULL, buy_client_order_id = NULL, quantity = 0 "
                            "WHERE symbol = ? AND grid_price = ? AND status = 'BUY_OPEN'",
                            (symbol, grid_price),
                        )
                elif side == "SELL":
                    if direction == "SHORT":
                        # SHORT 진입 취소 → EMPTY
                        conn.execute(
                            "UPDATE grid_positions SET status = 'EMPTY', direction = NULL, "
                            "sell_order_id = NULL, sell_client_order_id = NULL, quantity = 0 "
                            "WHERE symbol = ? AND grid_price = ? AND status = 'SELL_OPEN'",
                            (symbol, grid_price),
                        )
                    else:
                        # LONG 익절 취소 → HOLDING 유지
                        conn.execute(
                            "UPDATE grid_positions SET status = 'HOLDING', "
                            "sell_order_id = NULL, sell_client_order_id = NULL "
                            "WHERE symbol = ? AND grid_price = ? AND status = 'SELL_OPEN'",
                            (symbol, grid_price),
                        )
                print(f"[Live V2] {symbol}: 타임아웃 취소 — {side}({direction}) @ ${grid_price:,.2f}")
            conn.commit()
        else:
            conn.execute(
                "UPDATE grid_order_log SET status = 'CANCELLED' "
                "WHERE symbol = ? AND grid_price = ? AND side = ? AND status = 'PLACED' "
                "AND order_id IS NULL AND created_at < datetime('now', '-1 hour')",
                (symbol, grid_price, side),
            )
            conn.commit()


def _reconcile_positions(conn, symbol: str):
    """DB vs Binance 포지션 동기화 (방향별 처리 + 넷포지션 검증)"""
    ex = _get_executor()

    # Binance 오픈 주문 조회
    binance_orders = ex.get_open_orders(symbol)
    binance_oids = {str(o["orderId"]) for o in binance_orders}

    # DB에서 오픈 상태 주문 조회
    db_opens = conn.execute(
        "SELECT grid_price, status, buy_order_id, sell_order_id, direction "
        "FROM grid_positions WHERE symbol = ? AND status IN ('BUY_OPEN', 'SELL_OPEN')",
        (symbol,),
    ).fetchall()

    for grid_price, status, buy_oid, sell_oid, direction in db_opens:
        oid = buy_oid if status == "BUY_OPEN" else sell_oid

        if oid and oid not in binance_oids:
            order_info = ex.get_order_status(symbol, int(oid))

            if order_info:
                actual_status = order_info.get("status", "")

                if actual_status == "FILLED":
                    # 체결 감지 → 다음 사이클의 _detect_fills에서 처리됨
                    pass
                elif actual_status in ("CANCELED", "EXPIRED"):
                    _reconcile_reset_position(conn, symbol, grid_price,
                                              status, direction)
                    conn.commit()
            else:
                # API 실패 → 다음 사이클에서 재시도 (리셋하면 Binance 주문 고아화)
                print(f"[Live V2] {symbol}: 주문 상태 조회 실패 — "
                      f"oid={oid} 스킵 (다음 사이클 재시도)")

    # Binance에 있는데 DB에 없는 고아 주문 취소
    db_all_oids = set()
    all_db = conn.execute(
        "SELECT buy_order_id, sell_order_id FROM grid_positions WHERE symbol = ?",
        (symbol,),
    ).fetchall()
    for buy_oid, sell_oid in all_db:
        if buy_oid:
            db_all_oids.add(buy_oid)
        if sell_oid:
            db_all_oids.add(sell_oid)

    for bo in binance_orders:
        oid_str = str(bo["orderId"])
        if oid_str not in db_all_oids:
            client_oid = bo.get("clientOrderId", "")
            if client_oid.startswith("gv2_"):
                ex.cancel_order(symbol, oid_str)
                print(f"[Live V2] {symbol}: 고아 주문 취소 — orderId={oid_str}")

    # 넷 포지션 검증: DB vs Binance positionAmt
    # HOLDING = 포지션 보유중, SELL_OPEN(LONG) = 롱 익절 대기중, BUY_OPEN(SHORT) = 숏 커버 대기중
    # 이 3가지 모두 실제 Binance 포지션이 있는 상태
    db_positions = conn.execute(
        "SELECT direction, quantity FROM grid_positions "
        "WHERE symbol = ? AND quantity > 0 "
        "AND (status = 'HOLDING' "
        "  OR (status = 'SELL_OPEN' AND direction = 'LONG') "
        "  OR (status = 'BUY_OPEN' AND direction = 'SHORT'))",
        (symbol,),
    ).fetchall()
    db_net = sum(q for d, q in db_positions if d == "LONG") \
             - sum(q for d, q in db_positions if d == "SHORT")

    try:
        positions = ex.get_positions()
        binance_net = 0.0
        for p in positions:
            if p["symbol"] == symbol:
                binance_net = float(p.get("positionAmt", 0))
                break
        # 넷포지션 차이 확인
        diff = abs(db_net - binance_net)
        min_qty = _get_min_qty(symbol)
        if diff < min_qty:
            _reconcile_skip_count.pop(symbol, None)  # 정상 → 카운터 리셋
        if diff >= min_qty:
            # 즉시 체결된 주문이 있을 수 있음 → 체결 확인 후 재계산
            pending_opens = conn.execute(
                "SELECT grid_price, status, buy_order_id, sell_order_id, direction "
                "FROM grid_positions WHERE symbol = ? "
                "AND status IN ('BUY_OPEN', 'SELL_OPEN')",
                (symbol,),
            ).fetchall()

            rechecked = False
            for gp, st, b_oid, s_oid, dirn in pending_opens:
                oid = b_oid if st == "BUY_OPEN" else s_oid
                if not oid:
                    continue
                order_info = ex.get_order_status(symbol, int(oid))
                if order_info and order_info.get("status") == "FILLED":
                    # 즉시 체결됨 → 다음 사이클의 _detect_fills에서 처리
                    rechecked = True

            if rechecked:
                # 타임아웃 체크: 5분 이상 경과 시 카운터 리셋
                now_ts = time.time()
                if symbol in _reconcile_skip_time:
                    if now_ts - _reconcile_skip_time[symbol] > _RECONCILE_SKIP_TIMEOUT:
                        _reconcile_skip_count.pop(symbol, None)
                        _reconcile_skip_time.pop(symbol, None)
                        print(f"[Live V2] {symbol}: 넷포지션 스킵 타임아웃 (5분) — 카운터 리셋")
                else:
                    _reconcile_skip_time[symbol] = now_ts

                skip_count = _reconcile_skip_count.get(symbol, 0) + 1
                _reconcile_skip_count[symbol] = skip_count
                if skip_count <= _RECONCILE_MAX_SKIPS:
                    print(f"[Live V2] {symbol}: 넷포지션 불일치 감지 "
                          f"DB={db_net:+.4f} Binance={binance_net:+.4f} "
                          f"— 미반영 체결 있음, 스킵 {skip_count}/{_RECONCILE_MAX_SKIPS}")
                    return
                else:
                    print(f"[Live V2] {symbol}: 넷포지션 불일치 스킵 한도 초과 "
                          f"({skip_count}회) — 강제 해소")

            # 진짜 불일치 → 시장가로 해소
            excess = binance_net - db_net  # 양수=롱 초과, 음수=숏 초과
            close_side = "SELL" if excess > 0 else "BUY"
            close_qty = abs(excess)
            close_qty = round(close_qty / min_qty) * min_qty
            if close_qty >= min_qty:
                msg = (f"[Live V2] {symbol}: 넷포지션 불일치 해소 — "
                       f"DB={db_net:+.4f} Binance={binance_net:+.4f} "
                       f"→ {close_side} {close_qty:.4f}")
                print(msg)
                ex.place_market_order(symbol, close_side, close_qty)
                _reconcile_skip_count.pop(symbol, None)
                _send_telegram(msg)
            else:
                print(f"[Live V2] {symbol}: 넷포지션 불일치 "
                      f"DB={db_net:+.4f} Binance={binance_net:+.4f} "
                      f"(차이={diff:.4f} < min_qty, 무시)")
    except Exception as e:
        print(f"[Live V2] {symbol}: 넷포지션 검증 실패 — {e}")


def _reconcile_reset_position(conn, symbol: str, grid_price: float,
                               status: str, direction: str):
    """reconcile 리셋 헬퍼: 방향별 상태 복원"""
    if status == "BUY_OPEN":
        if direction == "SHORT":
            # SHORT 커버 취소 → HOLDING 유지
            conn.execute(
                "UPDATE grid_positions SET status = 'HOLDING', "
                "buy_order_id = NULL, buy_client_order_id = NULL "
                "WHERE symbol = ? AND grid_price = ?",
                (symbol, grid_price),
            )
        else:
            # LONG 진입 취소 → EMPTY
            conn.execute(
                "UPDATE grid_positions SET status = 'EMPTY', direction = NULL, "
                "buy_order_id = NULL, buy_client_order_id = NULL, quantity = 0 "
                "WHERE symbol = ? AND grid_price = ?",
                (symbol, grid_price),
            )
    else:  # SELL_OPEN
        if direction == "SHORT":
            # SHORT 진입 취소 → EMPTY
            conn.execute(
                "UPDATE grid_positions SET status = 'EMPTY', direction = NULL, "
                "sell_order_id = NULL, sell_client_order_id = NULL, quantity = 0 "
                "WHERE symbol = ? AND grid_price = ?",
                (symbol, grid_price),
            )
        else:
            # LONG 익절 취소 → HOLDING 유지
            conn.execute(
                "UPDATE grid_positions SET status = 'HOLDING', "
                "sell_order_id = NULL, sell_client_order_id = NULL "
                "WHERE symbol = ? AND grid_price = ?",
                (symbol, grid_price),
            )


# ============================
# Circuit Breaker (fail-safe)
# ============================

def _is_circuit_breaker_hit() -> bool:
    """일일 손실 한도 체크 — API 실패시 True(안전) 반환"""
    conn = get_connection()
    today = date.today().isoformat()

    # 1. 이미 발동된 경우
    row = conn.execute(
        "SELECT realized_pnl, circuit_breaker_hit FROM live_daily_pnl "
        "WHERE trade_date = ?",
        (today,),
    ).fetchone()
    conn.close()

    if row and row[1]:
        print(f"[Live V2] Circuit Breaker 이미 발동됨 (오늘 {today}) — 매매 중단 유지")
        return True

    realized_pnl = row[0] if row else 0.0

    # 2. Binance 미실현 손익 조회 (실시간, totalWalletBalance 기준)
    unrealized_pnl_pct = 0.0
    positions = []
    try:
        ex = _get_executor()
        wallet_balance = ex.get_total_balance()
        if wallet_balance > 0:
            positions = ex.get_positions()
            total_unrealized = 0.0
            for p in positions:
                amt = float(p.get("positionAmt", 0))
                if amt != 0:
                    total_unrealized += float(p.get("unRealizedProfit", 0))
            if wallet_balance > 0:
                unrealized_pnl_pct = total_unrealized / wallet_balance * 100
    except Exception as e:
        # API 실패 → fail-safe: 거래 중단
        print(f"[Live V2] Circuit Breaker: API 실패 — 안전 모드 ({e})")
        return True

    total_pnl = realized_pnl + unrealized_pnl_pct

    if total_pnl <= LIVE_DAILY_LOSS_LIMIT:
        conn = get_connection()
        conn.execute(
            "INSERT OR REPLACE INTO live_daily_pnl "
            "(trade_date, realized_pnl, unrealized_pnl, total_orders, circuit_breaker_hit) "
            "VALUES (?, ?, ?, COALESCE((SELECT total_orders FROM live_daily_pnl WHERE trade_date=?), 0), 1)",
            (today, realized_pnl, unrealized_pnl_pct, today),
        )
        conn.commit()

        msg = (f"[Live V2] CIRCUIT BREAKER! "
               f"realized={realized_pnl:+.2f}% + unrealized={unrealized_pnl_pct:+.2f}% "
               f"= {total_pnl:+.2f}% <= 한도 {LIVE_DAILY_LOSS_LIMIT}%")
        print(msg)

        # === CB 발동시 전 포지션 청산 + 주문 취소 ===
        liquidation_msg = ""
        try:
            for symbol in LIVE_SYMBOLS:
                # 1) 미체결 주문 전체 취소
                ex.cancel_all_orders(symbol)
                print(f"[CB] {symbol}: 미체결 주문 전체 취소")

                # 2) 오픈 포지션 시장가 청산
                for p in positions:
                    if p["symbol"] == symbol:
                        amt = float(p.get("positionAmt", 0))
                        if amt != 0:
                            close_side = "BUY" if amt < 0 else "SELL"
                            close_qty = abs(amt)
                            ex.place_market_order(symbol, close_side, close_qty)
                            print(f"[CB] {symbol}: 포지션 청산 {amt:+.4f} → {close_side} {close_qty}")
                            liquidation_msg += f"\n{symbol}: {close_side} {close_qty:.4f}"

                # 3) DB grid_positions 초기화
                conn.execute(
                    "UPDATE grid_positions SET status = 'EMPTY', direction = NULL, quantity = 0, "
                    "buy_fill_price = NULL, entry_fill_price = NULL, "
                    "buy_order_id = NULL, sell_order_id = NULL, "
                    "buy_client_order_id = NULL, sell_client_order_id = NULL "
                    "WHERE symbol = ? AND status != 'EMPTY'",
                    (symbol,),
                )
            conn.commit()
            print(f"[CB] 전 포지션 청산 완료")
        except Exception as e:
            print(f"[CB] 포지션 청산 중 오류 (수동 확인 필요): {e}")
            liquidation_msg += f"\n청산 오류: {e}"

        conn.close()

        _send_telegram(
            f"CIRCUIT BREAKER!\n"
            f"Realized: {realized_pnl:+.2f}%\n"
            f"Unrealized: {unrealized_pnl_pct:+.2f}%\n"
            f"Total: {total_pnl:+.2f}% <= {LIVE_DAILY_LOSS_LIMIT}%\n"
            f"주문 취소 + 포지션 청산 완료{liquidation_msg}"
        )
        return True

    return False


# ============================
# 일일 PnL (USD 기반)
# ============================

def _update_daily_pnl_usd(conn, pnl_usd: float):
    """일일 PnL 업데이트 (USD → % 변환, 일일 시작 잔고 기준)"""
    today = date.today().isoformat()

    existing = conn.execute(
        "SELECT id, realized_pnl, total_orders, starting_balance "
        "FROM live_daily_pnl WHERE trade_date = ?",
        (today,),
    ).fetchone()

    # 일일 시작 잔고 결정 (totalWalletBalance 사용 — 마진 포함 전체)
    if existing and existing[3] > 0:
        start_balance = existing[3]
    else:
        try:
            ex = _get_executor()
            start_balance = ex.get_total_balance()
        except Exception:
            start_balance = 0.0

    # 시작 잔고 기준 PnL%
    if start_balance > 0:
        pnl_pct = pnl_usd / start_balance * 100
    else:
        pnl_pct = 0.0

    if existing:
        if existing[3] <= 0 and start_balance > 0:
            # starting_balance가 0이면 현재 잔고로 복구
            conn.execute(
                "UPDATE live_daily_pnl SET realized_pnl = ?, total_orders = ?, starting_balance = ? WHERE id = ?",
                (round(existing[1] + pnl_pct, 4), existing[2] + 1, start_balance, existing[0]),
            )
        else:
            conn.execute(
                "UPDATE live_daily_pnl SET realized_pnl = ?, total_orders = ? WHERE id = ?",
                (round(existing[1] + pnl_pct, 4), existing[2] + 1, existing[0]),
            )
    else:
        conn.execute(
            "INSERT INTO live_daily_pnl (trade_date, realized_pnl, total_orders, starting_balance) "
            "VALUES (?, ?, 1, ?)",
            (today, round(pnl_pct, 4), start_balance),
        )
    conn.commit()


# ============================
# 감사 로깅
# ============================

def _log_grid_order(conn, symbol: str, side: str, grid_price: float,
                     quantity: float, limit_price: float,
                     order_id: str | None, client_order_id: str | None,
                     status: str, fill_price: float = None,
                     fee: float = 0, pnl_usd: float = 0,
                     direction: str = None):
    """grid_order_log에 주문 기록"""
    conn.execute(
        "INSERT INTO grid_order_log "
        "(symbol, side, direction, grid_price, quantity, limit_price, "
        "order_id, client_order_id, status, fill_price, fee, pnl_usd, filled_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (symbol, side, direction, grid_price, round(quantity, 4), round(limit_price, 2),
         order_id, client_order_id, status, fill_price,
         round(fee, 6), round(pnl_usd, 4),
         datetime.utcnow().isoformat() if status == "FILLED" else None),
    )


# ============================
# 헬퍼
# ============================

def _get_min_qty(symbol: str) -> float:
    """심볼별 최소 주문 수량"""
    if "BTC" in symbol:
        return 0.001
    elif "ETH" in symbol:
        return 0.01
    elif "SOL" in symbol:
        return 0.1
    return 0.01


def _check_slippage(symbol: str, expected_price: float, fill_price: float, side: str):
    """슬리피지 경고: 0.5% 초과 시 로깅"""
    if not expected_price or not fill_price or expected_price <= 0:
        return
    slippage_pct = abs(fill_price - expected_price) / expected_price * 100
    if slippage_pct >= 0.5:
        direction = "불리" if (
            (side == "BUY" and fill_price > expected_price) or
            (side == "SELL" and fill_price < expected_price)
        ) else "유리"
        print(f"[Live V2][슬리피지] {symbol} {side}: "
              f"기대=${expected_price:,.2f} 체결=${fill_price:,.2f} "
              f"({direction} {slippage_pct:.2f}%)")


def _extract_fill_price(result: dict | None) -> float | None:
    """주문 결과에서 체결 가격 추출"""
    if not result:
        return None
    if "fills" in result and result["fills"]:
        total_qty = 0.0
        total_cost = 0.0
        for fill in result["fills"]:
            fq = float(fill.get("qty", 0))
            fp = float(fill.get("price", 0))
            total_qty += fq
            total_cost += fp * fq
        if total_qty > 0:
            return total_cost / total_qty
    avg = result.get("avgPrice")
    if avg and float(avg) > 0:
        return float(avg)
    return None


# ============================
# 상태 조회
# ============================

def get_live_status() -> dict:
    """라이브 트레이딩 상태 조회 (양방향 지원)"""
    conn = get_connection()
    today = date.today().isoformat()

    # 오늘 주문 수 (grid_order_log에서)
    orders_today = conn.execute(
        "SELECT COUNT(*), "
        "SUM(CASE WHEN status='FILLED' THEN 1 ELSE 0 END), "
        "SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) "
        "FROM grid_order_log WHERE date(created_at) = ?",
        (today,),
    ).fetchone()

    # 일일 PnL
    daily = conn.execute(
        "SELECT realized_pnl, total_orders, circuit_breaker_hit "
        "FROM live_daily_pnl WHERE trade_date = ?",
        (today,),
    ).fetchone()

    # 전체 PnL (pnl_usd != 0인 체결 — 양방향이므로 BUY/SELL 모두 익절 가능)
    total = conn.execute(
        "SELECT COUNT(*), SUM(pnl_usd) FROM grid_order_log "
        "WHERE status = 'FILLED' AND pnl_usd != 0"
    ).fetchone()

    # 그리드 레벨 상태 (방향별)
    grid_status = {}
    for sym in LIVE_SYMBOLS:
        levels = conn.execute(
            "SELECT status, direction, COUNT(*) FROM grid_positions "
            "WHERE symbol = ? GROUP BY status, direction",
            (sym,),
        ).fetchall()
        detail = {}
        for s, d, c in levels:
            key = f"{s}({d})" if d else s
            detail[key] = c
        grid_status[sym] = detail

        # 방향 편향 정보
        bias = _direction_bias.get(sym)
        if bias:
            grid_status[sym]["_bias"] = bias[0]

    # 현재 오픈 포지션
    try:
        ex = _get_executor()
        open_positions = ex.get_positions()
    except Exception:
        open_positions = []

    conn.close()

    # 하이브리드 모드 상태
    hybrid_status = {}
    for sym in LIVE_SYMBOLS:
        mode = _current_mode.get(sym, "GRID")
        info = {"mode": mode}
        if mode == "L2":
            info["direction"] = _l2_direction.get(sym, "?")
            info["entry_price"] = _l2_entry_price.get(sym, 0)
            entry_time = _l2_entry_time.get(sym, 0)
            info["elapsed_min"] = round((time.time() - entry_time) / 60) if entry_time else 0
            info["highest_pnl"] = round(_l2_highest_pnl.get(sym, 0), 2)
        hybrid_status[sym] = info

    return {
        "version": "V3-Bidirectional",
        "enabled": LIVE_TRADING_ENABLED,
        "testnet": LIVE_USE_TESTNET,
        "symbols": LIVE_SYMBOLS,
        "hybrid": hybrid_status,
        "today": {
            "orders_total": orders_today[0] if orders_today else 0,
            "orders_filled": orders_today[1] if orders_today else 0,
            "orders_failed": orders_today[2] if orders_today else 0,
            "realized_pnl": daily[0] if daily else 0,
            "circuit_breaker": bool(daily[2]) if daily else False,
        },
        "all_time": {
            "completed_trades": total[0] if total else 0,
            "total_pnl_usd": round(total[1], 4) if total and total[1] else 0,
        },
        "grid_levels": grid_status,
        "open_positions": [
            {
                "symbol": p["symbol"],
                "amount": float(p["positionAmt"]),
                "entry_price": float(p["entryPrice"]),
                "unrealized_pnl": float(p["unRealizedProfit"]),
            }
            for p in open_positions
        ],
    }


# ============================
# 하이브리드: Grid → L2 전환
# ============================

def _try_enter_l2(symbol: str, mark_price: float,
                   levels: list[float], conn) -> bool:
    """OOB + 거래량 확인 후 L2 진입 시도

    조건: SSM ≥ 2.0 + SSM 방향 = OOB 방향
    Returns: True if L2 entered, False otherwise
    """
    # SSM 점수 + 방향 확인
    ssm = conn.execute(
        "SELECT total_score, direction FROM ssm_scores "
        "WHERE symbol = ? ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()

    if not ssm or ssm[0] < HYBRID_L2_MIN_SSM:
        print(f"[Hybrid][{symbol}] L2 진입 보류: SSM 부족 ({ssm[0] if ssm else 'N/A'} < {HYBRID_L2_MIN_SSM})")
        return False

    ssm_direction = ssm[1]  # "BULLISH" or "BEARISH"
    oob_direction = "BULLISH" if mark_price > levels[-1] else "BEARISH"
    print(f"[Hybrid][{symbol}] SSM={ssm_direction}, OOB={oob_direction}")

    if ssm_direction != oob_direction:
        print(f"[Hybrid][{symbol}] L2 진입 보류: SSM 방향 불일치")
        return False

    # HOLDING 포지션 전부 시장가 청산 (그리드 정리)
    _close_all_grid_holdings(symbol, conn)

    # L2 진입
    ex = _get_executor()
    balance = ex.get_account_balance()
    if balance < 5:
        print(f"[Hybrid] {symbol}: 잔고 부족 ${balance:.2f} — L2 진입 불가")
        return False

    side = "BUY" if oob_direction == "BULLISH" else "SELL"
    direction = "LONG" if side == "BUY" else "SHORT"

    # 잔고의 90%를 L2에 투입 (약간의 여유)
    usdt_amount = balance * 0.9 * LIVE_LEVERAGE
    qty = usdt_amount / mark_price
    min_qty = _get_min_qty(symbol)
    qty = round(max(min_qty, qty), _get_qty_decimals(symbol))

    result = ex.place_market_order(symbol, side, qty)
    if not result:
        print(f"[Hybrid] {symbol}: L2 진입 실패 — MARKET {side} {qty}")
        return False

    fill_price = _extract_fill_price(result) or mark_price
    _check_slippage(symbol, mark_price, fill_price, side)

    # 상태 전환: GRID → L2
    _current_mode[symbol] = "L2"
    _l2_entry_price[symbol] = fill_price
    _l2_direction[symbol] = direction
    _l2_entry_time[symbol] = time.time()
    _l2_highest_pnl[symbol] = 0.0
    _l2_quantity[symbol] = qty

    # 로깅
    fee = fill_price * qty * TAKER_FEE_RATE
    _log_grid_order(conn, symbol, side, fill_price, qty, fill_price,
                    str(result.get("orderId", "")), None, "FILLED",
                    fill_price=fill_price, fee=fee)
    conn.commit()

    msg = (f"[Hybrid] {symbol}: L2 {direction} 진입!\n"
           f"  가격: ${fill_price:,.2f} | 수량: {qty}\n"
           f"  SSM: {ssm[0]:.1f} ({ssm_direction}) | 스톱: {HYBRID_L2_STOP_LOSS_PCT}%")
    print(msg)
    _send_telegram(msg)

    return True


def _check_holding_stop_loss(conn, symbol: str, mark_price: float):
    """HOLDING/SELL_OPEN(LONG)/BUY_OPEN(SHORT) 포지션 개별 -1.5% 스톱로스 체크 — 매 사이클(30s) 실행"""
    INDIVIDUAL_STOP_PCT = 0.015  # 개별 포지션 -1.5% 손절

    # HOLDING + 익절대기(SELL_OPEN+LONG, BUY_OPEN+SHORT) 모두 체크
    holdings = conn.execute(
        "SELECT grid_price, quantity, direction, entry_fill_price, status, sell_order_id, buy_order_id "
        "FROM grid_positions "
        "WHERE symbol = ? AND quantity > 0 AND entry_fill_price > 0 "
        "AND (status = 'HOLDING' "
        "  OR (status = 'SELL_OPEN' AND direction = 'LONG') "
        "  OR (status = 'BUY_OPEN' AND direction = 'SHORT'))",
        (symbol,),
    ).fetchall()

    if not holdings:
        return

    ex = _get_executor()

    for grid_price, qty, direction, entry_price, status, sell_oid, buy_oid in holdings:
        # -3% 스톱로스 거리
        stop_dist = entry_price * INDIVIDUAL_STOP_PCT

        triggered = False
        if direction == "LONG" and mark_price < entry_price - stop_dist:
            triggered = True
            close_side = "SELL"
            cancel_oid = sell_oid  # 익절 주문 취소 필요
        elif direction == "SHORT" and mark_price > entry_price + stop_dist:
            triggered = True
            close_side = "BUY"
            cancel_oid = buy_oid  # 커버 주문 취소 필요

        if not triggered:
            continue

        # 기존 익절/커버 주문 취소 (SELL_OPEN/BUY_OPEN인 경우)
        if status != "HOLDING" and cancel_oid:
            ex.cancel_order(symbol, cancel_oid)
            conn.execute(
                "UPDATE grid_order_log SET status = 'CANCELLED' "
                "WHERE order_id = ? AND status = 'PLACED'", (cancel_oid,))

        # 시장가 청산
        result = ex.place_market_order(symbol, close_side, qty)
        fill_price = _extract_fill_price(result) or mark_price

        # PnL 계산
        if direction == "LONG":
            gross = (fill_price - entry_price) * qty
        else:
            gross = (entry_price - fill_price) * qty
        fee = (entry_price * qty + fill_price * qty) * TAKER_FEE_RATE
        pnl_usd = gross - fee

        # DB 업데이트
        conn.execute(
            "UPDATE grid_positions SET status = 'EMPTY', direction = NULL, "
            "quantity = 0, buy_fill_price = NULL, entry_fill_price = NULL, "
            "buy_order_id = NULL, sell_order_id = NULL, "
            "buy_client_order_id = NULL, sell_client_order_id = NULL "
            "WHERE symbol = ? AND grid_price = ?",
            (symbol, grid_price),
        )
        _log_grid_order(conn, symbol, close_side, grid_price, qty, fill_price,
                        str(result.get("orderId", "")) if result else None,
                        None, "FILLED", fill_price=fill_price,
                        pnl_usd=pnl_usd, direction=direction)
        _update_daily_pnl_usd(conn, pnl_usd)
        conn.commit()

        stop_pct = stop_dist / entry_price * 100
        msg = (f"[StopLoss] {symbol} {direction} -{stop_pct:.0f}% 개별손절!\n"
               f"  진입 ${entry_price:.2f} → 청산 ${fill_price:.2f}\n"
               f"  PnL ${pnl_usd:+.4f}")
        print(msg)
        _send_telegram(msg)


def _close_all_grid_holdings(symbol: str, conn):
    """그리드 HOLDING 포지션 전부 시장가 청산 (LONG→SELL, SHORT→BUY)"""
    holdings = conn.execute(
        "SELECT grid_price, quantity, buy_fill_price, direction, entry_fill_price "
        "FROM grid_positions "
        "WHERE symbol = ? AND status IN ('HOLDING', 'SELL_OPEN', 'BUY_OPEN') AND quantity > 0",
        (symbol,),
    ).fetchall()

    if not holdings:
        return

    ex = _get_executor()

    # 방향별 분류
    long_qty = sum(row[1] for row in holdings if row[3] != "SHORT")
    short_qty = sum(row[1] for row in holdings if row[3] == "SHORT")
    total_pnl = 0.0

    # LONG 청산 → SELL
    if long_qty > 0:
        long_holdings = [(gp, q, bf, d, ef) for gp, q, bf, d, ef in holdings if d != "SHORT"]
        avg_entry = sum((ef or bf or 0) * q for gp, q, bf, d, ef in long_holdings) / long_qty if long_qty > 0 else 0
        result = ex.place_market_order(symbol, "SELL", long_qty)
        fill_price = _extract_fill_price(result) or ex.get_mark_price(symbol) or 0
        for grid_price, qty, buy_fill, direction, entry_fill in long_holdings:
            ref_price = entry_fill or buy_fill
            if ref_price and fill_price:
                fee = (ref_price + fill_price) * qty * TAKER_FEE_RATE
                pnl = (fill_price - ref_price) * qty - fee
                total_pnl += pnl
        _log_grid_order(conn, symbol, "SELL", avg_entry, long_qty, fill_price,
                        str(result.get("orderId", "")) if result else None,
                        None, "FILLED", fill_price=fill_price,
                        pnl_usd=total_pnl, direction="LONG")

    # SHORT 청산 → BUY
    if short_qty > 0:
        short_holdings = [(gp, q, bf, d, ef) for gp, q, bf, d, ef in holdings if d == "SHORT"]
        avg_entry = sum((ef or 0) * q for gp, q, bf, d, ef in short_holdings) / short_qty if short_qty > 0 else 0
        result = ex.place_market_order(symbol, "BUY", short_qty)
        fill_price = _extract_fill_price(result) or ex.get_mark_price(symbol) or 0
        short_pnl = 0.0
        for grid_price, qty, buy_fill, direction, entry_fill in short_holdings:
            ref_price = entry_fill
            if ref_price and fill_price:
                fee = (ref_price + fill_price) * qty * TAKER_FEE_RATE
                pnl = (ref_price - fill_price) * qty - fee
                short_pnl += pnl
        total_pnl += short_pnl
        _log_grid_order(conn, symbol, "BUY", avg_entry, short_qty, fill_price,
                        str(result.get("orderId", "")) if result else None,
                        None, "FILLED", fill_price=fill_price,
                        pnl_usd=short_pnl, direction="SHORT")

    # DB 업데이트 — 전부 EMPTY
    conn.execute(
        "UPDATE grid_positions SET status = 'EMPTY', direction = NULL, quantity = 0, "
        "buy_fill_price = NULL, entry_fill_price = NULL, "
        "buy_order_id = NULL, sell_order_id = NULL, "
        "buy_client_order_id = NULL, sell_client_order_id = NULL "
        "WHERE symbol = ? AND status IN ('HOLDING', 'SELL_OPEN', 'BUY_OPEN')",
        (symbol,),
    )

    if total_pnl != 0:
        _update_daily_pnl_usd(conn, total_pnl)
    conn.commit()

    print(f"[Hybrid] {symbol}: 그리드 청산 — LONG {long_qty:.1f} + SHORT {short_qty:.1f} | "
          f"PnL ${total_pnl:+.2f}")


# ============================
# L2 사이클 관리
# ============================

def _run_l2_cycle(symbol: str):
    """L2 모드 사이클 (30초마다 호출) — 포지션 관리 + 탈출 판정"""
    if not _trade_lock.acquire(blocking=False):
        return
    try:
        _run_l2_cycle_inner(symbol)
    except Exception as e:
        print(f"[Hybrid L2] {symbol}: 사이클 오류 — {e}")
        import traceback
        traceback.print_exc()
    finally:
        _trade_lock.release()


def _run_l2_cycle_inner(symbol: str):
    """L2 사이클 내부 로직"""
    ex = _get_executor()
    mark_price = ex.get_mark_price(symbol)
    if not mark_price:
        return

    entry_price = _l2_entry_price.get(symbol)
    direction = _l2_direction.get(symbol)
    if not entry_price or not direction:
        # 상태 비정상 → GRID로 복귀
        _exit_l2_mode(symbol, "상태 비정상")
        return

    # PnL 계산 (%)
    if direction == "LONG":
        pnl_pct = (mark_price - entry_price) / entry_price * 100
    else:
        pnl_pct = (entry_price - mark_price) / entry_price * 100

    # 최고 수익 갱신 (트레일링 스탑용)
    if pnl_pct > _l2_highest_pnl.get(symbol, 0):
        _l2_highest_pnl[symbol] = pnl_pct

    highest = _l2_highest_pnl.get(symbol, 0)
    elapsed = time.time() - _l2_entry_time.get(symbol, time.time())
    elapsed_min = elapsed / 60

    # === 탈출 조건 ===

    # 1. 스톱로스
    if pnl_pct <= HYBRID_L2_STOP_LOSS_PCT:
        _exit_l2_mode(symbol, f"스톱로스 {pnl_pct:+.1f}% <= {HYBRID_L2_STOP_LOSS_PCT}%")
        return

    # 2. 트레일링 스탑: 수익 1% 이상 도달 후 0.5% 후퇴
    if highest >= HYBRID_L2_TRAILING_ACTIVATE:
        if pnl_pct < highest - HYBRID_L2_TRAILING_DISTANCE:
            _exit_l2_mode(symbol,
                          f"트레일링 스탑 (최고 {highest:+.1f}% → 현재 {pnl_pct:+.1f}%)")
            return

    # 3. 시간 제한 (4시간)
    if elapsed >= HYBRID_L2_MAX_DURATION:
        _exit_l2_mode(symbol, f"시간 제한 {elapsed_min:.0f}분 (PnL {pnl_pct:+.1f}%)")
        return

    # 4. SSM 방향 전환
    conn = get_connection()
    ssm = conn.execute(
        "SELECT direction FROM ssm_scores "
        "WHERE symbol = ? ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    conn.close()

    if ssm:
        expected_ssm = "BULLISH" if direction == "LONG" else "BEARISH"
        if ssm[0] != expected_ssm:
            _exit_l2_mode(symbol,
                          f"SSM 방향 전환 ({expected_ssm}→{ssm[0]}, PnL {pnl_pct:+.1f}%)")
            return


def _exit_l2_mode(symbol: str, reason: str):
    """L2 포지션 청산 → GRID 모드로 복귀"""
    ex = _get_executor()
    direction = _l2_direction.get(symbol, "LONG")
    close_side = "SELL" if direction == "LONG" else "BUY"

    # 바이낸스에서 실제 포지션 수량 확인
    qty = 0.0
    try:
        positions = ex.get_positions()
        for p in positions:
            if p["symbol"] == symbol:
                amt = float(p.get("positionAmt", 0))
                if amt != 0:
                    qty = abs(amt)
                    break
    except Exception:
        qty = _l2_quantity.get(symbol, 0)

    conn = get_connection()
    mark_price = ex.get_mark_price(symbol) or 0
    entry_price = _l2_entry_price.get(symbol, 0)

    pnl_usd = 0.0
    if qty > 0:
        result = ex.place_market_order(symbol, close_side, qty)
        fill_price = _extract_fill_price(result) or mark_price

        if direction == "LONG":
            gross = (fill_price - entry_price) * qty
        else:
            gross = (entry_price - fill_price) * qty
        fee = (entry_price * qty + fill_price * qty) * TAKER_FEE_RATE
        pnl_usd = gross - fee

        _log_grid_order(conn, symbol, close_side, entry_price, qty, fill_price,
                        str(result.get("orderId", "")) if result else None,
                        None, "FILLED", fill_price=fill_price,
                        fee=fee, pnl_usd=pnl_usd)
        _update_daily_pnl_usd(conn, pnl_usd)
        conn.commit()

    # 상태 복원: L2 → GRID
    _current_mode[symbol] = "GRID"
    _l2_entry_price.pop(symbol, None)
    _l2_direction.pop(symbol, None)
    _l2_entry_time.pop(symbol, None)
    _l2_highest_pnl.pop(symbol, None)
    _l2_quantity.pop(symbol, None)

    conn.close()

    msg = (f"[Hybrid] {symbol}: L2 {direction} 청산 — {reason}\n"
           f"  PnL: ${pnl_usd:+.4f} | 수량: {qty}")
    print(msg)
    _send_telegram(msg)


def _get_qty_decimals(symbol: str) -> int:
    """심볼별 수량 소수점 자릿수"""
    if "BTC" in symbol:
        return 3
    elif "ETH" in symbol:
        return 2
    elif "SOL" in symbol:
        return 1
    return 2


if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8")
    from db import init_db
    init_db()

    status = get_live_status()
    print(f"\n=== Grid V2 라이브 트레이딩 상태 ===")
    print(f"  버전: {status['version']}")
    print(f"  활성화: {status['enabled']}")
    print(f"  테스트넷: {status['testnet']}")
    print(f"  심볼: {status['symbols']}")
    print(f"  오늘 주문: {status['today']['orders_total']}건 "
          f"(체결 {status['today']['orders_filled']}, 실패 {status['today']['orders_failed']})")
    print(f"  오늘 PnL: {status['today']['realized_pnl']:+.4f}%")
    print(f"  누적 SELL: {status['all_time']['sell_trades']}건 | "
          f"PnL: ${status['all_time']['total_pnl_usd']:+.4f}")
    print(f"\n=== 그리드 레벨 상태 ===")
    for sym, levels in status["grid_levels"].items():
        print(f"  {sym}: {levels}")
    print(f"\n=== 오픈 포지션 ===")
    if status["open_positions"]:
        for p in status["open_positions"]:
            print(f"  {p['symbol']}: {p['amount']:.4f} @ ${p['entry_price']:,.2f} "
                  f"(PnL ${p['unrealized_pnl']:,.2f})")
    else:
        print("  없음")
