"""① 바이낸스 WebSocket 실시간 청산 수집기"""
import asyncio
import json
import time
import websockets
from db import get_connection
from config import BINANCE_WS_BASE, SYMBOLS, WS_RECONNECT_ATTEMPTS, WS_RECONNECT_DELAY


# BTC만 필터링 (검증 기간)
_WATCH_SYMBOLS = set(SYMBOLS)

# 배치 쓰기 버퍼 (청산 폭주 시 DB contention 방지)
_buffer = []
_FLUSH_INTERVAL = 2.0   # 최대 2초마다 flush
_FLUSH_SIZE = 20         # 20건 이상이면 즉시 flush
_last_flush = 0.0


def _flush_buffer():
    """버퍼의 청산 데이터를 DB에 일괄 저장"""
    global _buffer, _last_flush
    if not _buffer:
        return
    try:
        conn = get_connection()
        conn.executemany(
            "INSERT INTO liquidations (symbol, side, price, qty, trade_time) VALUES (?, ?, ?, ?, ?)",
            _buffer,
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[WS] DB flush 실패: {e}")
    _buffer = []
    _last_flush = time.time()


async def _handle_message(msg: str):
    """forceOrder 이벤트 파싱 → 버퍼에 추가"""
    global _buffer
    data = json.loads(msg)
    order = data.get("o", {})
    symbol = order.get("s", "")

    if symbol not in _WATCH_SYMBOLS:
        return

    side = order.get("S", "")          # BUY=숏 청산, SELL=롱 청산
    price = float(order.get("p", 0))
    qty = float(order.get("q", 0))
    trade_time = int(order.get("T", 0))

    _buffer.append((symbol, side, price, qty, trade_time))

    # 즉시 flush 조건: 버퍼 크기 초과 또는 시간 초과
    if len(_buffer) >= _FLUSH_SIZE or (time.time() - _last_flush) >= _FLUSH_INTERVAL:
        _flush_buffer()

    direction = "숏 청산" if side == "BUY" else "롱 청산"
    print(f"[청산] {symbol} {direction} | 가격 ${price:,.2f} | 수량 {qty} | {time.strftime('%H:%M:%S')}")


async def _periodic_flush():
    """주기적 flush (메시지가 없어도 버퍼 비우기)"""
    while True:
        await asyncio.sleep(_FLUSH_INTERVAL)
        if _buffer and (time.time() - _last_flush) >= _FLUSH_INTERVAL:
            _flush_buffer()


async def run_liquidation_stream():
    """WebSocket 청산 스트림 실행 (자동 재연결 포함)"""
    url = f"{BINANCE_WS_BASE}/!forceOrder@arr"
    attempt = 0

    # 주기적 flush 태스크 시작
    asyncio.create_task(_periodic_flush())

    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                print(f"[WS] 청산 스트림 연결 성공: {url}")
                attempt = 0  # 연결 성공 시 카운터 리셋

                async for msg in ws:
                    await _handle_message(msg)

        except (websockets.ConnectionClosed, ConnectionError, OSError) as e:
            _flush_buffer()  # 연결 끊기기 전 버퍼 flush
            attempt += 1
            if attempt > WS_RECONNECT_ATTEMPTS:
                print(f"[WS] 재연결 {WS_RECONNECT_ATTEMPTS}회 실패 — 스트림 중단")
                print(f"[WS] ⚠️ 데이터 연결 끊김 — 텔레그램 알림 필요 (Phase 3)")
                # 무한 재시도 (간격 늘려서)
                await asyncio.sleep(60)
                attempt = 0
                continue

            print(f"[WS] 연결 끊김 ({e}) — {WS_RECONNECT_DELAY}초 후 재연결 ({attempt}/{WS_RECONNECT_ATTEMPTS})")
            await asyncio.sleep(WS_RECONNECT_DELAY)

        except Exception as e:
            _flush_buffer()
            print(f"[WS] 예상치 못한 오류: {e}")
            await asyncio.sleep(WS_RECONNECT_DELAY)


if __name__ == "__main__":
    from db import init_db
    init_db()
    asyncio.run(run_liquidation_stream())
