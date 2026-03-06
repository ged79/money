"""Binance Futures 주문 실행 클라이언트 — 테스트넷/메인넷 자동 전환"""
import time
import hashlib
import hmac
import requests

from config import (
    BINANCE_API_KEY, BINANCE_SECRET_KEY, BINANCE_FUTURES_BASE,
    BINANCE_TESTNET_BASE, BINANCE_TESTNET_API_KEY, BINANCE_TESTNET_SECRET_KEY,
    LIVE_USE_TESTNET,
)

MAX_RETRIES = 3
RETRY_DELAY = 2  # 초


class BinanceExecutor:
    """Binance Futures API 주문 실행기"""

    def __init__(self, use_testnet: bool = None):
        if use_testnet is None:
            use_testnet = LIVE_USE_TESTNET

        self.use_testnet = use_testnet
        if use_testnet:
            self.base_url = BINANCE_TESTNET_BASE
            self.api_key = BINANCE_TESTNET_API_KEY
            self.secret_key = BINANCE_TESTNET_SECRET_KEY
            self._net_label = "TESTNET"
        else:
            self.base_url = BINANCE_FUTURES_BASE
            self.api_key = BINANCE_API_KEY
            self.secret_key = BINANCE_SECRET_KEY
            self._net_label = "MAINNET"

        if not self.api_key or not self.secret_key:
            raise ValueError(
                f"[Executor] {self._net_label} API 키가 설정되지 않음. "
                f".env에 {'BINANCE_TESTNET_' if use_testnet else 'BINANCE_'}API_KEY 확인"
            )

        # 서버 시간 오프셋 계산 (PC 시계 오차 보정)
        self._time_offset = 0
        self._last_time_sync = 0
        self._sync_time_offset()

        print(f"[Executor] {self._net_label} 초기화 완료 ({self.base_url})")

    def _sync_time_offset(self):
        """서버 시간 오프셋 동기화 (1시간마다 자동 갱신)"""
        try:
            resp = requests.get(f"{self.base_url}/fapi/v1/time", timeout=5)
            server_time = resp.json()["serverTime"]
            local_time = int(time.time() * 1000)
            self._time_offset = server_time - local_time
            self._last_time_sync = time.time()
            if abs(self._time_offset) > 500:
                print(f"[Executor] 시간 보정: {self._time_offset:+d}ms")
        except Exception:
            pass

    # === 서명 / 헤더 (binance_rest.py 패턴 재사용) ===

    def _signed_params(self, params: dict) -> dict:
        # 1시간마다 시간 오프셋 갱신
        if time.time() - self._last_time_sync > 3600:
            self._sync_time_offset()
        params["timestamp"] = int(time.time() * 1000) + self._time_offset
        query = "&".join(f"{k}={v}" for k, v in params.items())
        signature = hmac.new(
            self.secret_key.encode(), query.encode(), hashlib.sha256
        ).hexdigest()
        params["signature"] = signature
        return params

    def _headers(self) -> dict:
        return {"X-MBX-APIKEY": self.api_key}

    # === HTTP 요청 ===

    def _get(self, endpoint: str, params: dict = None, signed: bool = True) -> dict | list:
        url = f"{self.base_url}{endpoint}"
        params = params or {}
        if signed:
            params = self._signed_params(params)
        resp = requests.get(url, params=params, headers=self._headers(), timeout=10)
        resp.raise_for_status()
        return resp.json()

    def _post(self, endpoint: str, params: dict, signed: bool = True) -> dict:
        url = f"{self.base_url}{endpoint}"
        if signed:
            params = self._signed_params(params)
        resp = requests.post(url, params=params, headers=self._headers(), timeout=10)
        resp.raise_for_status()
        return resp.json()

    def _post_with_retry(self, endpoint: str, params: dict,
                         is_market: bool = False) -> dict | None:
        """재시도 POST — Market 주문은 재시도 금지 (중복 체결 방지)"""
        max_attempts = 1 if is_market else MAX_RETRIES
        for attempt in range(1, max_attempts + 1):
            try:
                result = self._post(endpoint, dict(params))  # copy params (서명 추가됨)
                return result
            except requests.exceptions.HTTPError as e:
                error_body = ""
                status_code = 0
                if e.response is not None:
                    error_body = e.response.text
                    status_code = e.response.status_code
                print(f"[Executor] {self._net_label} 주문 실패 (시도 {attempt}/{max_attempts}): "
                      f"{e} | {error_body}")
                # 4xx 클라이언트 에러: 재시도 무의미 (파라미터 오류, 인증 실패 등)
                if 400 <= status_code < 500 and status_code != 429:
                    return None
                # 429 레이트 리밋: 60초 대기 후 재시도
                if status_code == 429 and attempt < max_attempts:
                    print(f"[Executor] 레이트 리밋 — 60초 대기")
                    time.sleep(60)
                elif attempt < max_attempts:
                    time.sleep(RETRY_DELAY)
            except Exception as e:
                print(f"[Executor] {self._net_label} 요청 오류 (시도 {attempt}/{max_attempts}): {e}")
                # 네트워크 에러 + Market 주문 = 재시도 금지 (이미 체결됐을 수 있음)
                if is_market:
                    print(f"[Executor] Market 주문 네트워크 오류 — 재시도 안함 (중복 방지)")
                    return None
                if attempt < max_attempts:
                    time.sleep(RETRY_DELAY)
        return None

    # === 주문 API ===

    def place_market_order(self, symbol: str, side: str, quantity: float) -> dict | None:
        """시장가 주문 (MARKET) — avgPrice=0이면 주문 조회로 실제 체결가 확인"""
        params = {
            "symbol": symbol,
            "side": side.upper(),  # BUY / SELL
            "type": "MARKET",
            "quantity": _format_qty(symbol, quantity),
        }
        print(f"[Executor] {self._net_label} {side} {params['quantity']} {symbol} MARKET")
        result = self._post_with_retry("/fapi/v1/order", params, is_market=True)
        if result:
            order_id = result.get("orderId", "")
            status = result.get("status", "")
            avg_price = float(result.get("avgPrice", 0))
            # avgPrice=0이면 주문 조회로 실제 체결가 확인 (최대 3회)
            if avg_price == 0 and order_id:
                import time as _t
                for _ in range(3):
                    _t.sleep(0.3)
                    try:
                        info = self._get("/fapi/v1/order", {
                            "symbol": symbol, "orderId": order_id})
                        avg_price = float(info.get("avgPrice", 0))
                        status = info.get("status", status)
                        if avg_price > 0:
                            result["avgPrice"] = str(avg_price)
                            result["status"] = status
                            break
                    except Exception:
                        pass
            print(f"[Executor] {self._net_label} 체결: orderId={order_id} "
                  f"status={status} avgPrice=${avg_price:,.2f}")
        return result

    def place_limit_order(self, symbol: str, side: str, quantity: float,
                          price: float, time_in_force: str = "GTC") -> dict | None:
        """지정가 주문 (LIMIT)"""
        params = {
            "symbol": symbol,
            "side": side.upper(),
            "type": "LIMIT",
            "quantity": _format_qty(symbol, quantity),
            "price": _format_price(symbol, price),
            "timeInForce": time_in_force,
        }
        print(f"[Executor] {self._net_label} {side} {params['quantity']} {symbol} "
              f"LIMIT @ ${price:,.2f}")
        return self._post_with_retry("/fapi/v1/order", params)

    # === HTTP DELETE ===

    def _delete(self, endpoint: str, params: dict, signed: bool = True) -> dict:
        url = f"{self.base_url}{endpoint}"
        if signed:
            params = self._signed_params(params)
        resp = requests.delete(url, params=params, headers=self._headers(), timeout=10)
        resp.raise_for_status()
        return resp.json()

    def _delete_with_retry(self, endpoint: str, params: dict) -> dict | None:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                result = self._delete(endpoint, dict(params))
                return result
            except requests.exceptions.HTTPError as e:
                error_body = ""
                status_code = 0
                if e.response is not None:
                    error_body = e.response.text or ""
                    status_code = e.response.status_code
                error_str = str(e)
                # 4xx 클라이언트 에러: 재시도 무의미 (이미 체결/취소/만료)
                is_client_error = (
                    (400 <= status_code < 500 and status_code != 429)
                    or "400 Client Error" in error_str
                    or "404 Not Found" in error_str
                )
                if is_client_error:
                    # -2011: Unknown order (이미 취소/체결됨) → 조용히 스킵
                    if "-2011" in error_body or "-2011" in error_str:
                        return None
                    print(f"[Executor] DELETE {status_code or '4xx'}: "
                          f"{error_body[:100] or error_str[:100]} — 스킵")
                    return None
                print(f"[Executor] DELETE 실패 (시도 {attempt}/{MAX_RETRIES}): {e} | {error_body[:100]}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
            except Exception as e:
                print(f"[Executor] DELETE 오류 (시도 {attempt}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
        return None

    # === 주문 관리 ===

    def cancel_order(self, symbol: str, order_id: str) -> dict | None:
        """주문 취소 (DELETE)"""
        params = {"symbol": symbol, "orderId": order_id}
        return self._delete_with_retry("/fapi/v1/order", params)

    def cancel_all_orders(self, symbol: str) -> bool:
        """심볼의 모든 오픈 주문 취소"""
        try:
            self._delete("/fapi/v1/allOpenOrders", {"symbol": symbol})
            print(f"[Executor] {self._net_label} {symbol}: 모든 주문 취소 완료")
            return True
        except Exception as e:
            print(f"[Executor] 전체 주문 취소 실패: {e}")
            return False

    def get_open_orders(self, symbol: str) -> list:
        """심볼의 오픈 주문 조회"""
        try:
            return self._get("/fapi/v1/openOrders", {"symbol": symbol})
        except Exception as e:
            print(f"[Executor] 오픈 주문 조회 실패: {e}")
            return []

    def get_order_status(self, symbol: str, order_id: int) -> dict | None:
        """특정 주문 상태 조회"""
        try:
            return self._get("/fapi/v1/order", {"symbol": symbol, "orderId": order_id})
        except Exception as e:
            print(f"[Executor] 주문 상태 조회 실패: {e}")
            return None

    def get_klines(self, symbol: str, interval: str = "1m",
                   limit: int = 10) -> list:
        """Kline 데이터 실시간 조회 (unsigned, DB 미저장)"""
        try:
            return self._get("/fapi/v1/klines", {
                "symbol": symbol, "interval": interval, "limit": limit,
            }, signed=False)
        except Exception as e:
            print(f"[Executor] klines 조회 실패: {e}")
            return []

    def get_mark_price(self, symbol: str) -> float | None:
        """실시간 마크 프라이스 조회"""
        try:
            data = self._get("/fapi/v1/premiumIndex", {"symbol": symbol}, signed=False)
            price = float(data.get("markPrice", 0))
            return price if price > 0 else None
        except Exception as e:
            print(f"[Executor] 마크 프라이스 조회 실패: {e}")
            return None

    def place_limit_order_with_id(self, symbol: str, side: str, quantity: float,
                                   price: float, client_order_id: str,
                                   time_in_force: str = "GTC") -> dict | None:
        """clientOrderId 포함 지정가 주문 (멱등성)"""
        params = {
            "symbol": symbol,
            "side": side.upper(),
            "type": "LIMIT",
            "quantity": _format_qty(symbol, quantity),
            "price": _format_price(symbol, price),
            "timeInForce": time_in_force,
            "newClientOrderId": client_order_id,
        }
        print(f"[Executor] {self._net_label} {side} {params['quantity']} {symbol} "
              f"LIMIT @ ${price:,.2f} (cid={client_order_id[:20]})")
        return self._post_with_retry("/fapi/v1/order", params)

    # === 계좌 조회 ===

    def get_account_balance(self) -> float:
        """USDT 총 자산 반환 (walletBalance)"""
        try:
            data = self._get("/fapi/v2/balance")
            for asset in data:
                if asset["asset"] == "USDT":
                    return float(asset["balance"])
        except Exception as e:
            print(f"[Executor] 잔고 조회 실패: {e}")
        return 0.0

    def get_total_balance(self) -> float:
        """USDT 총 자산 반환 (walletBalance = 마진 포함 전체)"""
        try:
            data = self._get("/fapi/v2/balance")
            for asset in data:
                if asset["asset"] == "USDT":
                    return float(asset["balance"])
        except Exception as e:
            print(f"[Executor] 총 잔고 조회 실패: {e}")
        return 0.0

    def get_positions(self, symbol: str = None) -> list:
        """오픈 포지션 조회"""
        try:
            params = {}
            if symbol:
                params["symbol"] = symbol
            data = self._get("/fapi/v2/positionRisk", params)
            # 포지션 있는 것만 필터
            positions = [
                p for p in data
                if float(p.get("positionAmt", 0)) != 0
            ]
            return positions
        except Exception as e:
            print(f"[Executor] 포지션 조회 실패: {e}")
            return []

    def set_leverage(self, symbol: str, leverage: int) -> bool:
        """레버리지 설정"""
        try:
            result = self._post("/fapi/v1/leverage", {
                "symbol": symbol, "leverage": leverage,
            })
            actual = result.get("leverage", leverage)
            print(f"[Executor] {self._net_label} {symbol} 레버리지 → {actual}x")
            return True
        except Exception as e:
            print(f"[Executor] 레버리지 설정 실패: {e}")
            return False

    def set_margin_type(self, symbol: str, margin_type: str = "CROSSED") -> bool:
        """마진 타입 설정 (CROSSED / ISOLATED)"""
        try:
            self._post("/fapi/v1/marginType", {
                "symbol": symbol, "marginType": margin_type,
            })
            print(f"[Executor] {self._net_label} {symbol} 마진 → {margin_type}")
            return True
        except requests.exceptions.HTTPError as e:
            # -4046: No need to change margin type (이미 설정됨)
            if e.response is not None and "-4046" in e.response.text:
                return True
            print(f"[Executor] 마진타입 설정 실패: {e}")
            return False


# === 수량/가격 포맷 헬퍼 ===

def _format_qty(symbol: str, qty: float) -> str:
    """심볼별 수량 소수점 포맷"""
    if symbol.startswith("BTC"):
        return f"{qty:.3f}"     # 0.001 단위
    elif symbol.startswith("ETH"):
        return f"{qty:.3f}"     # 0.001 단위
    elif symbol.startswith("SOL"):
        return f"{qty:.1f}"     # 0.1 단위
    print(f"[Executor] 경고: {symbol} 수량 포맷 미등록 — 기본 4자리 사용")
    return f"{qty:.4f}"


def _format_price(symbol: str, price: float) -> str:
    """심볼별 가격 소수점 포맷"""
    if symbol.startswith("BTC"):
        return f"{price:.1f}"   # $0.1 단위
    elif symbol.startswith("ETH"):
        return f"{price:.2f}"   # $0.01 단위
    elif symbol.startswith("SOL"):
        return f"{price:.2f}"   # $0.01 단위
    print(f"[Executor] 경고: {symbol} 가격 포맷 미등록 — 기본 2자리 사용")
    return f"{price:.2f}"


if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8")

    # 테스트넷 연결 테스트 및 포지션 조회
    try:
        ex = BinanceExecutor(use_testnet=True)
        balance = ex.get_account_balance()
        print(f"테스트넷 잔고: ${balance:,.2f}")
        positions = ex.get_positions()
        print(f"오픈 포지션: {len(positions)}개")
        if positions:
            print("--- 오픈 포지션 상세 --- ")
            for p in positions:
                print(f"  종목: {p['symbol']}, 수량: {float(p['positionAmt']):.4f}, "
                      f"평균 진입가: ${float(p['entryPrice']):,.2f}, "
                      f"미실현 PnL: ${float(p['unRealizedProfit']):,.2f}")
        else:
            print("오픈 포지션이 없습니다.")
    except Exception as e:
        print(f"초기화 실패: {e}")
