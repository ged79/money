"""Trend Engine v8.2 — SSM+T 모델 (진짜 돈 데이터만)

S (Story)     = SSM Gemini grounding → 큰돈의 방향
S (Sentiment) = 펀딩비 극단 → 쏠림
M (Momentum)  = OI 변화 + 청산 비대칭 → 돈이 움직이는 증거
T (Trigger)   = 위 정렬 + VP 매물대 지지/저항 도달 → 진입 시점

진입: 방향 확인 + 되돌림이 매물대 지지에 닿을 때
손절: 매물대 이탈 (자동 주문)
"""
import time
import json
from datetime import datetime, timezone
from db import get_connection

# 심볼별 수량 소수점 자릿수
QTY_DECIMALS = {
    "BTCUSDT": 3,       # 0.001
    "ETHUSDT": 3,        # 0.001
    "SOLUSDT": 1,        # 0.1
    "AVAXUSDT": 1,       # 0.1
    "SUIUSDT": 1,        # 0.1
    "DOGEUSDT": 0,       # 1
    "1000PEPEUSDT": 0,   # 1
    "AAVEUSDT": 1,       # 0.1
    "LINKUSDT": 1,       # 0.1
    "ARBUSDT": 0,        # 1
}


def _round_qty(symbol: str, qty: float) -> float:
    """심볼별 수량 반올림"""
    decimals = QTY_DECIMALS.get(symbol, 4)
    return round(qty, decimals)
from config import (
    LIVE_LEVERAGE, TAKER_FEE_RATE, LIVE_TRADING_ENABLED,
    MAX_CONCURRENT_POSITIONS, POSITION_PCT,
    SSM_VETO_CONFIDENCE, SSM_MIN_CONFIDENCE,
    FUNDING_EXTREME_HIGH, FUNDING_EXTREME_LOW,
    VP_SUPPORT_ENTRY_PCT, VP_STOP_BUFFER_PCT,
    TRAIL_PCT,
    MAX_HOLD_DURATION, COOLDOWN_AFTER_STOP,
    UNREALIZED_RATCHET, UNREALIZED_RATCHET_CLOSE_PCT,
)


class TrendTrader:
    """SSM+T 추세 트레이딩 — 5개 진짜 돈 지표만 사용"""

    def __init__(self, executor, ssm=None, risk=None):
        self.executor = executor
        self.ssm = ssm
        self.risk = risk
        self._cooldown_until = {}   # symbol → timestamp
        self._highest_roi = {}      # symbol → float (래칫용)
        self._peak_price = {}       # symbol → float (트레일링용)
        self._t1_hit = {}           # symbol → bool (T1 부분청산 여부)
        self._candidates = {}       # symbol → verdict (스캔 후보)

    # ══════════════════════════════════════════════════════════
    #  메인 사이클 (30초)
    # ══════════════════════════════════════════════════════════

    def run_cycle(self, symbol: str, cached_balance: float = 0):
        """매 30초 트레이딩 사이클 — 포지션 관리 + 신호 수집"""
        # 1. 서킷브레이커
        if self.risk:
            bal = cached_balance if cached_balance > 0 else (self.executor.get_account_balance() if self.executor else 0)
            if self.risk.check_circuit_breaker(symbol, balance=bal):
                return

        # 2. 포지션 있으면 관리
        if self.has_position(symbol):
            self._manage_position(symbol)
            return

        # 3. 동시 포지션 제한 — 자리 없으면 스캔도 안 함
        if self._count_open_positions() >= MAX_CONCURRENT_POSITIONS:
            return

        # 4. 쿨다운
        cd = self._cooldown_until.get(symbol, 0)
        if time.time() < cd:
            return

        # 5. 신호 수집 + 판정 (진입은 scan_and_enter에서)
        signals = self._collect_signals(symbol)
        if not signals:
            return

        verdict = self._judge(symbol, signals)
        self._log_judgment(symbol, signals, verdict)

        # 후보 저장 (scan_and_enter에서 비교 후 진입)
        if verdict["action"] == "ENTER":
            self._candidates[symbol] = verdict

    def scan_and_enter(self, symbols: list, cached_balance: float = 0):
        """전체 심볼 스캔 후 최고 스코어 순으로 진입"""
        self._candidates = {}

        # 1. 모든 심볼 스캔 (포지션 관리 + 신호 수집)
        for sym in symbols:
            try:
                self.run_cycle(sym, cached_balance=cached_balance)
            except Exception as e:
                print(f"[Trend] {sym} 스캔 오류: {e}")

        # 2. 후보가 없으면 종료
        if not self._candidates:
            return

        # 3. 스코어 순 정렬
        ranked = sorted(self._candidates.items(),
                        key=lambda x: x[1]["score"], reverse=True)

        # 4. 빈 자리 + 반대 방향만 허용
        slots = MAX_CONCURRENT_POSITIONS - self._count_open_positions()
        if slots <= 0:
            return

        existing_dirs = self._get_open_directions()
        entered = []
        for sym, verdict in ranked:
            if len(entered) >= slots:
                break
            # 같은 방향 포지션이 이미 있으면 스킵
            if verdict["direction"] in existing_dirs:
                continue
            self._execute_entry(sym, verdict)
            existing_dirs.add(verdict["direction"])
            entered.append(f"{sym}({verdict['direction']},score={verdict['score']:.1f})")

        if entered:
            skipped = [f"{s}({v['direction']},{v['score']:.1f})"
                       for s, v in ranked if s not in [e.split('(')[0] for e in entered]]
            if skipped:
                print(f"[Trend] 후보 탈락: {', '.join(skipped)}")

        self._candidates = {}

        # 5. 빈 자리 + SHORT 방향 비어있으면 브레이크다운 숏 스캔
        slots = MAX_CONCURRENT_POSITIONS - self._count_open_positions()
        if slots > 0 and "SHORT" not in self._get_open_directions():
            self._scan_breakdown_shorts(symbols, cached_balance)

    # ══════════════════════════════════════════════════════════
    #  브레이크다운 숏 — 지지 이탈 시 다음 지지까지 (켈리 사이징)
    # ══════════════════════════════════════════════════════════

    def _scan_breakdown_shorts(self, symbols: list, cached_balance: float = 0):
        """지지선 이탈 감지 → 켈리 사이징 → 다음 지지까지 숏"""
        candidates = []

        for sym in symbols:
            if self.has_position(sym):
                continue
            cd = self._cooldown_until.get(sym, 0)
            if time.time() < cd:
                continue

            bd = self._check_breakdown(sym)
            if bd:
                candidates.append((sym, bd))

        if not candidates:
            return

        # 켈리 스코어 순 정렬
        candidates.sort(key=lambda x: x[1]["kelly_pct"], reverse=True)

        # 켈리 최고 1개만 진입 (같은 방향 중복 방지)
        sym, bd = candidates[0]
        self._execute_breakdown_entry(sym, bd, cached_balance)

    def _check_breakdown(self, symbol: str) -> dict | None:
        """지지선 이탈 확인 + 켈리 계산"""
        price = self.executor.get_mark_price(symbol)
        if not price:
            return None

        vp = self._get_vp(symbol, price)
        oi = self._get_oi(symbol)
        liq = self._get_liquidations(symbol)

        # 최근 지지 이탈 확인: 가격이 VA Low 아래로 내려왔는지
        # 또는 가격 바로 위에 있던 지지가 깨졌는지
        conn = get_connection()
        try:
            # 이전 사이클 VP에서 지지였던 레벨 확인 (5분봉 기준)
            prev_cache = conn.execute(
                "SELECT va_high, va_low, poc FROM vp_cache "
                "WHERE symbol=? AND timeframe='5m'",
                (symbol,)).fetchone()
        finally:
            conn.close()

        if not prev_cache:
            return None

        broken_level = 0
        va_low_5m = prev_cache[1]
        poc_5m = prev_cache[2]

        # 5m VA Low 이탈
        if price < va_low_5m and va_low_5m > 0:
            broken_level = va_low_5m
        # composite VA Low 이탈
        elif price < vp["va_low"] and vp["va_low"] > 0:
            broken_level = vp["va_low"]

        if broken_level == 0:
            return None

        # 이탈 확인 지표 (최소 2개 필요)
        confirm_count = 0
        reasons = []

        # OI 감소 + 가격 하락 = 롱 이탈
        if oi["oi_down"] and oi["price_down"]:
            confirm_count += 1
            reasons.append(f"OI↓P↓({oi['change_pct']:+.2%})")

        # 매도 우세 (taker)
        conn = get_connection()
        try:
            taker = conn.execute(
                "SELECT buy_vol, sell_vol FROM taker_ratio "
                "WHERE symbol=? ORDER BY id DESC LIMIT 1",
                (symbol,)).fetchone()
        finally:
            conn.close()
        if taker and taker[0] > 0 and taker[1] / taker[0] > 1.3:
            confirm_count += 1
            reasons.append(f"SELL_DOM({taker[1]/taker[0]:.2f}x)")

        # 롱 청산 우세
        if liq["bias"] == "LONG_LIQUIDATED":
            confirm_count += 1
            reasons.append(f"LONG_LIQ(${liq['sell_usd']:,.0f})")

        # 가격이 이탈 레벨에서 충분히 떨어짐 (0.2% 이상)
        drop_pct = (broken_level - price) / broken_level
        if drop_pct > 0.002:
            confirm_count += 1
            reasons.append(f"DROP({drop_pct:.2%})")

        if confirm_count < 3:
            return None

        # 다음 지지 (타겟) 찾기
        supports = vp.get("supports", [])
        # composite 레벨에서 현재가 아래 지지 찾기
        next_supports = [s for s in supports if s < price * 0.99]
        if not next_supports:
            # POC가 아래에 있으면 사용
            if vp["poc"] < price * 0.99:
                next_supports = [vp["poc"]]
        if not next_supports:
            return None

        target = next_supports[0]  # 가장 가까운 다음 지지

        # 스톱: 깨진 지지 + 1.5% 위
        stop = round(broken_level * (1 + VP_STOP_BUFFER_PCT), 2)

        # 켈리 공식
        risk = abs(stop - price) / price       # 손실 비율
        reward = abs(price - target) / price   # 수익 비율
        if risk <= 0 or reward <= 0:
            return None

        payoff_ratio = reward / risk   # b = 보상/위험

        # 승률 추정: 기본 0.45 + 확인 지표당 +0.08
        win_prob = min(0.45 + confirm_count * 0.08, 0.75)
        loss_prob = 1 - win_prob

        # Kelly % = (p * b - q) / b
        kelly_raw = (win_prob * payoff_ratio - loss_prob) / payoff_ratio
        if kelly_raw <= 0:
            return None  # 기대값 마이너스 → 진입 안 함

        # Half Kelly (안전)
        kelly_pct = round(kelly_raw * 0.5, 4)
        kelly_pct = min(kelly_pct, POSITION_PCT)  # 최대 POSITION_PCT 제한

        print(f"[BD] {symbol} 지지 ${broken_level:,.2f} 이탈! "
              f"타겟 ${target:,.2f} 스톱 ${stop:,.2f} "
              f"R:R=1:{payoff_ratio:.1f} 승률={win_prob:.0%} "
              f"Kelly={kelly_pct:.1%} [{', '.join(reasons)}]")

        return {
            "direction": "SHORT",
            "broken_level": broken_level,
            "target": target,
            "stop_level": stop,
            "kelly_pct": kelly_pct,
            "payoff_ratio": payoff_ratio,
            "win_prob": win_prob,
            "risk": risk,
            "reward": reward,
            "reasons": reasons,
            "confirm_count": confirm_count,
        }

    def _execute_breakdown_entry(self, symbol: str, bd: dict,
                                  cached_balance: float = 0):
        """브레이크다운 숏 진입 (켈리 사이징)"""
        price = self.executor.get_mark_price(symbol)
        if not price:
            return

        if not LIVE_TRADING_ENABLED:
            print(f"[DRY_RUN] {symbol} BD SHORT "
                  f"${price:,.2f} → ${bd['target']:,.2f} "
                  f"stop=${bd['stop_level']:,.2f} Kelly={bd['kelly_pct']:.1%}")
            return

        self.executor.cancel_all_orders(symbol)

        balance = cached_balance if cached_balance > 0 else (
            self.executor.get_account_balance())
        if balance <= 0:
            return

        # 켈리 사이징
        qty = _round_qty(symbol, balance * LIVE_LEVERAGE * bd["kelly_pct"] / price)
        if qty <= 0:
            print(f"[BD] {symbol} 켈리 수량=0 (kelly={bd['kelly_pct']:.1%})")
            return

        result = self.executor.place_market_order(symbol, "SELL", qty)
        if not result:
            print(f"[BD] {symbol} SHORT 주문 실패")
            return

        ep = float(result.get("avgPrice", 0)) or price

        conn = get_connection()
        conn.execute(
            "INSERT INTO trend_positions "
            "(symbol, direction, entry_price, quantity, trailing_stop, highest_pnl, "
            "status, size_pct, signal_strength, trigger_reason) "
            "VALUES (?,?,?,?,?,0,'OPEN',?,?,?)",
            (symbol, "SHORT", ep, qty, bd["stop_level"],
             bd["kelly_pct"], bd["confirm_count"],
             "BD_SHORT|" + "|".join(bd["reasons"])))
        conn.commit()
        conn.close()

        self._highest_roi[symbol] = 0.0
        self._peak_price[symbol] = ep
        self._t1_hit[symbol] = False

        print(f"[BD] {symbol} SHORT 진입 — "
              f"${ep:,.2f} qty={qty} stop=${bd['stop_level']:,.2f} "
              f"타겟=${bd['target']:,.2f} Kelly={bd['kelly_pct']:.1%} "
              f"R:R=1:{bd['payoff_ratio']:.1f} [{', '.join(bd['reasons'])}]")

    # ══════════════════════════════════════════════════════════
    #  신호 수집 — 5개 진짜 돈 지표
    # ══════════════════════════════════════════════════════════

    def _collect_signals(self, symbol: str) -> dict | None:
        """5개 지표 수집 (VP는 진입 판정 직전 실시간 재계산)"""
        try:
            price = self.executor.get_mark_price(symbol)
            if not price:
                return None

            # VP 실시간 재계산 — 최신 5m 데이터 반영 (진입 후보만)
            try:
                self.update_vp(symbol)
            except Exception:
                pass

            return {
                "price": price,
                "ssm": self._get_ssm(),
                "funding": self._get_funding(symbol),
                "oi": self._get_oi(symbol),
                "liquidations": self._get_liquidations(symbol),
                "vp": self._get_vp(symbol, price),
            }
        except Exception as e:
            print(f"[Trend] {symbol} 신호 수집 오류: {e}")
            return None

    def _get_ssm(self) -> dict:
        """S: SSM — 큰돈의 방향 (Gemini grounding)"""
        if not self.ssm:
            return {"direction": "NEUTRAL", "confidence": 0, "reason": "no_ssm"}
        return self.ssm.get_direction()

    def _get_funding(self, symbol: str) -> dict:
        """S: 펀딩비 — 쏠림의 극단"""
        conn = get_connection()
        try:
            row = conn.execute(
                "SELECT funding_rate FROM funding_rates WHERE symbol=? "
                "ORDER BY id DESC LIMIT 1", (symbol,)).fetchone()
        finally:
            conn.close()

        if not row:
            return {"rate": 0, "signal": "NEUTRAL"}

        rate = row[0]
        if rate >= FUNDING_EXTREME_HIGH:
            signal = "EXTREME_LONG"   # 롱 과열 → 숏 유리
        elif rate <= FUNDING_EXTREME_LOW:
            signal = "EXTREME_SHORT"  # 숏 과열 → 롱 유리
        else:
            signal = "NEUTRAL"

        return {"rate": rate, "signal": signal}

    def _get_oi(self, symbol: str) -> dict:
        """M: OI 변화율 + 가격방향 결합 (선물 핵심 지표)"""
        conn = get_connection()
        try:
            rows = conn.execute(
                "SELECT open_interest FROM oi_snapshots WHERE symbol=? "
                "ORDER BY id DESC LIMIT 6", (symbol,)).fetchall()
            prices = conn.execute(
                "SELECT close FROM klines WHERE symbol=? AND interval='5m' "
                "ORDER BY open_time DESC LIMIT 7", (symbol,)).fetchall()
        finally:
            conn.close()

        if len(rows) < 2:
            return {"change_pct": 0, "oi_up": False, "oi_down": False,
                    "price_up": False, "price_down": False,
                    "price_change_pct": 0, "current": 0}

        current = rows[0][0]
        prev = rows[-1][0]
        change_pct = (current - prev) / prev if prev > 0 else 0

        # OI 방향 (0.3% 임계값 — 30분 내 의미 있는 변화)
        oi_up = change_pct > 0.003
        oi_down = change_pct < -0.003

        # 가격 방향 (같은 시간대 5m 캔들)
        price_change_pct = 0
        price_up = False
        price_down = False
        if len(prices) >= 2:
            p_now = prices[0][0]
            p_prev = prices[-1][0]
            price_change_pct = (p_now - p_prev) / p_prev if p_prev > 0 else 0
            price_up = price_change_pct > 0.001
            price_down = price_change_pct < -0.001

        return {
            "change_pct": round(change_pct, 4),
            "oi_up": oi_up, "oi_down": oi_down,
            "price_up": price_up, "price_down": price_down,
            "price_change_pct": round(price_change_pct, 4),
            "current": current,
        }

    def _get_liquidations(self, symbol: str) -> dict:
        """M: 청산 비대칭 — 어느 쪽이 쓸렸나"""
        conn = get_connection()
        try:
            rows = conn.execute(
                "SELECT side, SUM(qty * price), COUNT(*) FROM liquidations "
                "WHERE symbol=? AND trade_time > ? GROUP BY side",
                (symbol, int(time.time() * 1000) - 3600000)).fetchall()
        finally:
            conn.close()

        liq = {r[0]: {"usd": r[1], "count": r[2]} for r in rows}
        buy_usd = liq.get("BUY", {}).get("usd", 0)    # 숏 청산
        sell_usd = liq.get("SELL", {}).get("usd", 0)   # 롱 청산
        total = buy_usd + sell_usd

        if total < 1000:  # 청산 거의 없음
            return {"bias": "NONE", "buy_usd": buy_usd, "sell_usd": sell_usd}

        ratio = buy_usd / total if total > 0 else 0.5

        if ratio > 0.7:
            bias = "SHORT_SQUEEZED"    # 숏 청산 우세 → 상승 압력
        elif ratio < 0.3:
            bias = "LONG_LIQUIDATED"   # 롱 청산 우세 → 하방 후 반등 여지
        else:
            bias = "BALANCED"

        return {"bias": bias, "buy_usd": buy_usd, "sell_usd": sell_usd,
                "ratio": round(ratio, 2)}

    def _get_vp(self, symbol: str, price: float) -> dict:
        """T: VP 매물대 — 지지/저항 레벨 + 현재가 위치"""
        conn = get_connection()
        try:
            cache = conn.execute(
                "SELECT va_high, va_low, poc, data_json FROM vp_cache "
                "WHERE symbol=? AND timeframe='composite'",
                (symbol,)).fetchone()
        finally:
            conn.close()

        if not cache:
            return {"va_high": 0, "va_low": 0, "poc": 0,
                    "at_support": False, "at_resistance": False,
                    "support_level": 0, "resistance_level": 0}

        va_high, va_low, poc = cache[0], cache[1], cache[2]

        # HVN 레벨 추출
        supports = []
        resistances = []
        try:
            data = json.loads(cache[3]) if cache[3] else {}
            buckets = data.get("buckets", [])
            if buckets:
                volumes = sorted(b["volume"] for b in buckets)
                hvn_th = volumes[int(len(volumes) * 0.75)]  # 상위 25%
                for b in buckets:
                    if b["volume"] >= hvn_th and hvn_th > 0:
                        mid = round((b["price_low"] + b["price_high"]) / 2, 2)
                        if mid < price:
                            supports.append(mid)
                        else:
                            resistances.append(mid)
        except (json.JSONDecodeError, TypeError):
            pass

        # VA low/high도 지지/저항에 추가
        if va_low < price:
            supports.append(va_low)
        if va_high > price:
            resistances.append(va_high)
        supports.append(poc) if poc < price else resistances.append(poc)

        supports = sorted(set(supports), reverse=True)    # 가까운 순
        resistances = sorted(set(resistances))             # 가까운 순

        # 현재가가 지지/저항 근처인지
        nearest_sup = supports[0] if supports else 0
        nearest_res = resistances[0] if resistances else 0

        at_support = False
        at_resistance = False

        if nearest_sup > 0 and abs(price - nearest_sup) / nearest_sup <= VP_SUPPORT_ENTRY_PCT:
            if self._is_breakout_confirmed(symbol, nearest_sup):
                at_support = True

        if nearest_res > 0 and abs(price - nearest_res) / nearest_res <= VP_SUPPORT_ENTRY_PCT:
            if self._is_breakout_confirmed(symbol, nearest_res):
                at_resistance = True

        return {
            "va_high": va_high, "va_low": va_low, "poc": poc,
            "at_support": at_support,
            "at_resistance": at_resistance,
            "support_level": nearest_sup,
            "resistance_level": nearest_res,
            "supports": supports[:3],
            "resistances": resistances[:3],
        }

    def _is_breakout_confirmed(self, symbol: str, level: float) -> bool:
        """돌파 확인: 거래량 돌파 OR 시간 확인(3봉 종가 유지)"""
        conn = get_connection()
        try:
            candles = conn.execute(
                "SELECT close, volume FROM klines WHERE symbol=? AND interval='5m' "
                "ORDER BY open_time DESC LIMIT 20", (symbol,)).fetchall()
        finally:
            conn.close()

        if len(candles) < 4:
            return False

        # 1) 거래량 확인: 최신봉 볼륨 > 평균 × 1.5
        latest_vol = candles[0][1]
        avg_vol = sum(c[1] for c in candles[1:]) / len(candles[1:])
        if avg_vol > 0 and latest_vol > avg_vol * 1.5:
            return True

        # 2) 시간 확인: 최근 3봉 종가 모두 레벨 위(지지) 또는 아래(저항)
        recent_closes = [c[0] for c in candles[:3]]
        if all(c > level for c in recent_closes):
            return True
        if all(c < level for c in recent_closes):
            return True

        return False

    # ══════════════════════════════════════════════════════════
    #  판정 — 5개 지표 정렬 확인
    # ══════════════════════════════════════════════════════════

    def _judge(self, symbol: str, signals: dict) -> dict:
        """5개 지표 정렬 → ENTER / WAIT / BLOCKED"""
        ssm = signals["ssm"]
        funding = signals["funding"]
        oi = signals["oi"]
        liq = signals["liquidations"]
        vp = signals["vp"]
        price = signals["price"]

        ssm_dir = ssm.get("direction", "NEUTRAL")
        ssm_conf = ssm.get("confidence", 0)

        # ── LONG 조건 체크 ──
        long_score = 0
        long_reasons = []

        # S: SSM — 확신도 가중 (conf3=1.0, conf4=1.5, conf5=2.0)
        if ssm_dir == "BULLISH" and ssm_conf >= SSM_MIN_CONFIDENCE:
            ssm_pts = 1.0 + (ssm_conf - 3) * 0.5
            long_score += ssm_pts
            long_reasons.append(f"SSM_BULL(conf={ssm_conf},+{ssm_pts:.1f})")

        # S: 펀딩비 숏과열 (롱 유리)
        if funding["signal"] == "EXTREME_SHORT":
            long_score += 1
            long_reasons.append(f"FUND_SHORT_EXTREME({funding['rate']:.5f})")
        elif funding["signal"] == "NEUTRAL":
            long_score += 0.5

        # M: OI + 가격방향 결합
        if oi["oi_up"] and oi["price_up"]:
            # OI↑ + Price↑ = 신규 롱 유입 → 강한 상승 모멘텀
            long_score += 1
            long_reasons.append(f"OI↑P↑(OI{oi['change_pct']:+.2%},P{oi['price_change_pct']:+.2%})")
        elif oi.get("oi_down") and oi["price_up"]:
            # OI↓ + Price↑ = 숏커버링 → 약한 상승
            long_score += 0.25

        # M: 숏 스퀴즈 = 숏이 청산당함 = 상승 압력 = 롱 근거
        if liq["bias"] == "SHORT_SQUEEZED":
            long_score += 1
            long_reasons.append(f"SHORT_SQZ(${liq['buy_usd']:,.0f})")
        elif liq["bias"] == "BALANCED":
            long_score += 0.5

        # T: 매물대 지지 도달
        if vp["at_support"]:
            long_score += 1
            long_reasons.append(f"VP_SUPPORT(${vp['support_level']:,.2f})")

        # ── SHORT 조건 체크 ──
        short_score = 0
        short_reasons = []

        # S: SSM — 확신도 가중
        if ssm_dir == "BEARISH" and ssm_conf >= SSM_MIN_CONFIDENCE:
            ssm_pts = 1.0 + (ssm_conf - 3) * 0.5
            short_score += ssm_pts
            short_reasons.append(f"SSM_BEAR(conf={ssm_conf},+{ssm_pts:.1f})")

        if funding["signal"] == "EXTREME_LONG":
            short_score += 1
            short_reasons.append(f"FUND_LONG_EXTREME({funding['rate']:.5f})")
        elif funding["signal"] == "NEUTRAL":
            short_score += 0.5

        # M: OI + 가격방향 결합
        if oi["oi_up"] and oi["price_down"]:
            # OI↑ + Price↓ = 신규 숏 유입 → 강한 하락 모멘텀
            short_score += 1
            short_reasons.append(f"OI↑P↓(OI{oi['change_pct']:+.2%},P{oi['price_change_pct']:+.2%})")
        elif oi.get("oi_down") and oi["price_down"]:
            # OI↓ + Price↓ = 롱 투항 → 약한 하락
            short_score += 0.25

        # M: 롱 청산 = 롱이 청산당함 = 하락 압력 = 숏 근거
        if liq["bias"] == "LONG_LIQUIDATED":
            short_score += 1
            short_reasons.append(f"LONG_LIQ(${liq['sell_usd']:,.0f})")
        elif liq["bias"] == "BALANCED":
            short_score += 0.5

        if vp["at_resistance"]:
            short_score += 1
            short_reasons.append(f"VP_RESIST(${vp['resistance_level']:,.2f})")

        # ── 판정 ──

        # SSM 거부: 반대 방향 conf≥4이면 불가
        if ssm_dir == "BEARISH" and ssm_conf >= SSM_VETO_CONFIDENCE:
            long_score = 0  # 롱 불가
        if ssm_dir == "BULLISH" and ssm_conf >= SSM_VETO_CONFIDENCE:
            short_score = 0  # 숏 불가

        # 최소 4/5 정렬 필요 (SSM + 나머지 3개 이상)
        min_score = 3.5

        # R/R 필터: 저항/지지까지 <2%면 돌파 확인 필요
        RR_MIN_DIST = 0.02

        if long_score >= min_score and long_score > short_score:
            direction = "LONG"
            sup = vp["support_level"]
            stop_level = sup * (1 - VP_STOP_BUFFER_PCT) if sup else 0
            t1 = vp["resistances"][0] if vp.get("resistances") else 0
            # 저항까지 거리 < 2% → 돌파 확인 없으면 진입 금지
            if t1 and (t1 - price) / price < RR_MIN_DIST:
                if not self._is_breakout_confirmed(symbol, t1):
                    print(f"  [{symbol}] LONG 차단: 저항 ${t1:,.2f}까지 "
                          f"{(t1-price)/price*100:.1f}% — 돌파 미확인")
                    return {
                        "action": "WAIT", "reason": "RR_FILTER",
                        "long_score": long_score, "short_score": short_score,
                        "long_reasons": long_reasons, "short_reasons": short_reasons,
                    }
            return {
                "action": "ENTER", "direction": direction,
                "score": long_score, "reasons": long_reasons,
                "stop_level": round(stop_level, 2),
                "t1": round(t1, 2), "entry_price": price,
            }
        elif short_score >= min_score and short_score > long_score:
            direction = "SHORT"
            res = vp["resistance_level"]
            stop_level = res * (1 + VP_STOP_BUFFER_PCT) if res else 0
            t1 = vp["supports"][0] if vp.get("supports") else 0
            # 지지까지 거리 < 2% → 돌파 확인 없으면 진입 금지
            if t1 and (price - t1) / price < RR_MIN_DIST:
                if not self._is_breakout_confirmed(symbol, t1):
                    print(f"  [{symbol}] SHORT 차단: 지지 ${t1:,.2f}까지 "
                          f"{(price-t1)/price*100:.1f}% — 돌파 미확인")
                    return {
                        "action": "WAIT", "reason": "RR_FILTER",
                        "long_score": long_score, "short_score": short_score,
                        "long_reasons": long_reasons, "short_reasons": short_reasons,
                    }
            return {
                "action": "ENTER", "direction": direction,
                "score": short_score, "reasons": short_reasons,
                "stop_level": round(stop_level, 2),
                "t1": round(t1, 2), "entry_price": price,
            }
        else:
            return {
                "action": "WAIT",
                "long_score": long_score, "short_score": short_score,
                "long_reasons": long_reasons, "short_reasons": short_reasons,
            }

    # ══════════════════════════════════════════════════════════
    #  진입 실행
    # ══════════════════════════════════════════════════════════

    def _execute_entry(self, symbol: str, verdict: dict):
        """시장가 진입 + 스톱 주문"""
        direction = verdict["direction"]
        stop_level = verdict["stop_level"]
        t1 = verdict.get("t1", 0)

        price = self.executor.get_mark_price(symbol)
        if not price:
            print(f"[Trend] {symbol} 가격 조회 실패")
            return

        # 스톱 거리 확인 (1% 미만이면 스킵)
        if stop_level > 0:
            stop_dist = abs(price - stop_level) / price
            if stop_dist < 0.01:
                print(f"[Trend] {symbol} 스톱 너무 가까움 ({stop_dist:.1%}) — 스킵")
                return

        reasons_str = " + ".join(verdict["reasons"])

        # DRY_RUN 모드: 판정만 기록, 주문 안 함
        if not LIVE_TRADING_ENABLED:
            print(f"[DRY_RUN] {symbol} {direction} 신호! "
                  f"${price:,.2f} stop=${stop_level:,.2f} T1=${t1:,.2f} "
                  f"score={verdict['score']:.1f} [{reasons_str}]")
            return

        self.executor.cancel_all_orders(symbol)

        balance = self.executor.get_account_balance()
        if balance <= 0:
            print(f"[Trend] {symbol} 잔고 조회 실패")
            return

        qty = _round_qty(symbol, balance * LIVE_LEVERAGE * POSITION_PCT / price)
        if qty <= 0:
            print(f"[Trend] {symbol} 수량=0")
            return

        side = "BUY" if direction == "LONG" else "SELL"
        result = self.executor.place_market_order(symbol, side, qty)
        if not result:
            print(f"[Trend] {symbol} {direction} 주문 실패")
            return

        ep = float(result.get("avgPrice", 0)) or price

        # DB 저장
        conn = get_connection()
        conn.execute(
            "INSERT INTO trend_positions "
            "(symbol, direction, entry_price, quantity, trailing_stop, highest_pnl, "
            "status, size_pct, signal_strength, trigger_reason) "
            "VALUES (?,?,?,?,?,0,'OPEN',?,?,?)",
            (symbol, direction, ep, qty, stop_level,
             POSITION_PCT, verdict["score"],
             "|".join(verdict["reasons"])))
        conn.commit()
        conn.close()

        self._highest_roi[symbol] = 0.0
        self._peak_price[symbol] = ep
        self._t1_hit[symbol] = False

        reasons_str = " + ".join(verdict["reasons"])
        print(f"[Trend] {symbol} {direction} 진입 — "
              f"${ep:,.2f} qty={qty} stop=${stop_level:,.2f} "
              f"score={verdict['score']:.1f} [{reasons_str}]")

    # ══════════════════════════════════════════════════════════
    #  포지션 관리
    # ══════════════════════════════════════════════════════════

    def _manage_position(self, symbol: str):
        """포지션 관리: VP 스톱 + SSM 역전 + 시간 + 래칫"""
        conn = get_connection()
        row = conn.execute(
            "SELECT id, direction, entry_price, quantity, trailing_stop, "
            "highest_pnl, opened_at, trigger_reason FROM trend_positions "
            "WHERE symbol=? AND status='OPEN' LIMIT 1", (symbol,)).fetchone()
        if not row:
            conn.close()
            return

        pos_id, direction, ep, qty, stop_level, hp, oa, trigger_reason = row
        is_breakdown = (trigger_reason or "").startswith("BD_SHORT")
        price = self.executor.get_mark_price(symbol)
        if not price:
            conn.close()
            return

        # ROI 계산
        if direction == "LONG":
            roi_pct = ((price - ep) / ep) * 100
            upnl = (price - ep) * qty
        else:
            roi_pct = ((ep - price) / ep) * 100
            upnl = (ep - price) * qty

        cur_highest = max(self._highest_roi.get(symbol, 0), roi_pct)
        self._highest_roi[symbol] = cur_highest

        close_reason = None
        close_qty = qty

        # 1) VP 매물대 스톱 (자동 주문 역할)
        if stop_level > 0:
            if direction == "LONG" and price <= stop_level:
                close_reason = "VP_STOP"
            elif direction == "SHORT" and price >= stop_level:
                close_reason = "VP_STOP"

        # 2) SSM 역전 (수익 중에만, BD_SHORT은 제외 — 자체 스톱/타겟으로 관리)
        if not close_reason and self.ssm and roi_pct > 0 and not is_breakdown:
            ssm_data = self.ssm.get_direction()
            ssm_dir = ssm_data.get("direction", "NEUTRAL")
            ssm_conf = ssm_data.get("confidence", 0)
            if direction == "LONG" and ssm_dir == "BEARISH" and ssm_conf >= SSM_VETO_CONFIDENCE:
                close_reason = "SSM_REVERSAL"
            elif direction == "SHORT" and ssm_dir == "BULLISH" and ssm_conf >= SSM_VETO_CONFIDENCE:
                close_reason = "SSM_REVERSAL"

        # 2.5) 저항 익절 (수익 중 + 저항 근처)
        if not close_reason and roi_pct > 0:
            vp = self._get_vp(symbol, price)
            at_res = False
            res_level = 0

            if direction == "LONG" and vp["resistances"]:
                res_level = vp["resistances"][0]
                at_res = (res_level > 0 and
                          abs(price - res_level) / res_level <= VP_SUPPORT_ENTRY_PCT)
            elif direction == "SHORT" and vp["supports"]:
                res_level = vp["supports"][0]
                at_res = (res_level > 0 and
                          abs(price - res_level) / res_level <= VP_SUPPORT_ENTRY_PCT)

            if at_res:
                # 돌파 가능성 판단
                breakout_confirmed = self._is_breakout_confirmed(symbol, res_level)

                if not breakout_confirmed:
                    # b) 저항에서 하락 신호 2개 이상 → 전량 익절 (우선)
                    oi = self._get_oi(symbol)
                    liq = self._get_liquidations(symbol)
                    bearish_count = 0
                    if oi["oi_down"] and oi["price_down"]:
                        bearish_count += 1
                    if liq["bias"] == "LONG_LIQUIDATED":
                        bearish_count += 1
                    if oi["price_down"] and oi["price_change_pct"] < -0.003:
                        bearish_count += 1

                    if bearish_count >= 2:
                        close_reason = "RESIST_BEARISH"
                        close_qty = qty
                        print(f"[Trend] {symbol} 저항 ${res_level:,.2f}에서 "
                              f"하락 신호 {bearish_count}개 — 전량 익절")
                    # a) 하락 신호 부족 + 아직 부분청산 안 했으면 → 50% 익절
                    elif not self._t1_hit.get(symbol, False):
                        close_reason = "RESIST_PARTIAL"
                        close_qty = _round_qty(symbol, qty * 0.5)
                        self._t1_hit[symbol] = True
                        print(f"[Trend] {symbol} 저항 ${res_level:,.2f} 근처 — "
                              f"돌파 미확인, 50% 익절")

        # 3) 시간 기반
        if not close_reason:
            try:
                oa_dt = datetime.fromisoformat(oa.replace("Z", "+00:00"))
                if oa_dt.tzinfo is None:
                    oa_dt = oa_dt.replace(tzinfo=timezone.utc)
                elapsed = (datetime.now(timezone.utc) - oa_dt).total_seconds()
                if elapsed >= MAX_HOLD_DURATION:
                    if roi_pct > 0 or roi_pct < -2.0:
                        close_reason = "MAX_DURATION"
            except Exception:
                pass

        # 4) 미실현 래칫
        if not close_reason:
            for threshold_roi, floor_roi in UNREALIZED_RATCHET:
                if cur_highest >= threshold_roi and roi_pct < floor_roi:
                    close_reason = f"RATCHET_{threshold_roi:.0f}"
                    close_qty = _round_qty(symbol, qty * UNREALIZED_RATCHET_CLOSE_PCT)
                    break

        # 5) 트레일링 스톱 (피크에서 TRAIL_PCT 후퇴)
        if not close_reason and roi_pct > 0:
            peak = self._peak_price.get(symbol, ep)
            if direction == "LONG":
                if price > peak:
                    self._peak_price[symbol] = price
                    peak = price
                trail_stop = peak * (1 - TRAIL_PCT)
                if trail_stop > stop_level:
                    stop_level = round(trail_stop, 2)
            else:
                if price < peak:
                    self._peak_price[symbol] = price
                    peak = price
                trail_stop = peak * (1 + TRAIL_PCT)
                if 0 < trail_stop < stop_level:
                    stop_level = round(trail_stop, 2)

        # DB 업데이트
        conn.execute(
            "UPDATE trend_positions SET trailing_stop=?, highest_pnl=? WHERE id=?",
            (stop_level, max(hp, upnl), pos_id))
        conn.commit()
        conn.close()

        # 청산 실행
        if close_reason:
            if close_qty < qty:
                pnl = self._partial_close(symbol, pos_id, direction, ep,
                                          close_qty, qty, close_reason)
            else:
                pnl = self._full_close(symbol, close_reason)
                if close_reason == "VP_STOP" and upnl < 0:
                    self._cooldown_until[symbol] = time.time() + COOLDOWN_AFTER_STOP

    # ══════════════════════════════════════════════════════════
    #  청산
    # ══════════════════════════════════════════════════════════

    def _full_close(self, symbol: str, reason: str) -> float:
        conn = get_connection()
        row = conn.execute(
            "SELECT id, direction, entry_price, quantity FROM trend_positions "
            "WHERE symbol=? AND status='OPEN' LIMIT 1", (symbol,)).fetchone()
        if not row:
            conn.close()
            return 0.0
        pos_id, d, ep, qty = row
        close_side = "SELL" if d == "LONG" else "BUY"
        result = self.executor.place_market_order(symbol, close_side, qty)
        xp = float(result.get("avgPrice", 0)) if result else (
            self.executor.get_mark_price(symbol) or ep)
        gross = ((xp - ep) if d == "LONG" else (ep - xp)) * qty
        fee = (ep * qty + xp * qty) * TAKER_FEE_RATE
        pnl = round(gross - fee, 2)
        conn.execute(
            "UPDATE trend_positions SET status='CLOSED', pnl_usd=?, close_reason=?, "
            "closed_at=CURRENT_TIMESTAMP WHERE id=?", (pnl, reason, pos_id))
        conn.commit()
        conn.close()
        self._highest_roi.pop(symbol, None)
        if self.risk:
            self.risk.update_daily_pnl(symbol, trend_pnl=pnl)
        print(f"[Trend] {symbol} {d} 청산 — ${ep:,.2f}→${xp:,.2f} "
              f"PnL=${pnl:+,.2f} [{reason}]")
        return pnl

    def _partial_close(self, symbol: str, pos_id: int, direction: str,
                       entry_price: float, close_qty: float,
                       total_qty: float, reason: str) -> float:
        close_side = "SELL" if direction == "LONG" else "BUY"
        result = self.executor.place_market_order(symbol, close_side, close_qty)
        xp = float(result.get("avgPrice", 0)) if result else (
            self.executor.get_mark_price(symbol) or entry_price)
        gross = ((xp - entry_price) if direction == "LONG" else (entry_price - xp)) * close_qty
        fee = (entry_price * close_qty + xp * close_qty) * TAKER_FEE_RATE
        pnl = round(gross - fee, 2)
        remaining = _round_qty(symbol, total_qty - close_qty)
        conn = get_connection()
        if remaining > 0:
            conn.execute("UPDATE trend_positions SET quantity=? WHERE id=?",
                         (remaining, pos_id))
        else:
            conn.execute(
                "UPDATE trend_positions SET status='CLOSED', pnl_usd=?, close_reason=?, "
                "closed_at=CURRENT_TIMESTAMP WHERE id=?", (pnl, reason, pos_id))
        conn.commit()
        conn.close()
        if self.risk:
            self.risk.update_daily_pnl(symbol, trend_pnl=pnl)
        print(f"[Trend] {symbol} 부분청산 {close_qty}/{total_qty} — "
              f"PnL=${pnl:+,.2f} [{reason}]")
        return pnl

    # ══════════════════════════════════════════════════════════
    #  유틸리티
    # ══════════════════════════════════════════════════════════

    def has_position(self, symbol: str) -> bool:
        conn = get_connection()
        row = conn.execute(
            "SELECT 1 FROM trend_positions WHERE symbol=? AND status='OPEN' LIMIT 1",
            (symbol,)).fetchone()
        conn.close()
        return row is not None

    def _count_open_positions(self) -> int:
        conn = get_connection()
        row = conn.execute(
            "SELECT COUNT(*) FROM trend_positions WHERE status='OPEN'").fetchone()
        conn.close()
        return row[0] if row else 0

    def _get_open_directions(self) -> set:
        """현재 오픈 포지션들의 방향 집합 반환"""
        conn = get_connection()
        rows = conn.execute(
            "SELECT direction FROM trend_positions WHERE status='OPEN'"
        ).fetchall()
        conn.close()
        return {r[0] for r in rows}

    def get_status(self, symbol: str) -> dict | None:
        conn = get_connection()
        row = conn.execute(
            "SELECT id, direction, entry_price, quantity, trailing_stop, "
            "highest_pnl, opened_at, size_pct, signal_strength, trigger_reason "
            "FROM trend_positions WHERE symbol=? AND status='OPEN' LIMIT 1",
            (symbol,)).fetchone()
        conn.close()
        if not row:
            return None
        _id, d, ep, qty, ts, hp, oa, sp, ss, tr = row
        price = self.executor.get_mark_price(symbol)
        upnl = ((price - ep) if d == "LONG" else (ep - price)) * qty if price else 0
        return {"id": _id, "direction": d, "entry_price": ep, "quantity": qty,
                "stop_level": ts, "highest_pnl": hp, "opened_at": oa,
                "trigger_reason": tr, "current_price": price,
                "unrealized_pnl": round(upnl, 2)}

    def _log_judgment(self, symbol: str, signals: dict, verdict: dict):
        """판정 기록 → smart_judgments"""
        try:
            price = signals["price"]
            ssm = signals["ssm"]
            vp = signals["vp"]

            detail = json.dumps({
                "ssm": f"{ssm.get('direction')}(conf={ssm.get('confidence')})",
                "funding": signals["funding"],
                "oi": signals["oi"],
                "liq": signals["liquidations"],
                "verdict": verdict,
            }, ensure_ascii=False, default=str)

            direction = verdict.get("direction", "NONE")
            action = verdict.get("action", "WAIT")
            reasons = verdict.get("reasons", verdict.get("long_reasons", []))

            conn = get_connection()
            conn.execute(
                "INSERT OR REPLACE INTO smart_judgments "
                "(symbol, candle_time, mark_price, direction, reason, "
                "blocked, entered, bias, trend_score, "
                "nearest_support, nearest_resistance, "
                "oi_change, liq_buy_vol, liq_sell_vol, detail) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (symbol, int(time.time()), price, direction,
                 "|".join(str(r) for r in reasons),
                 int(action == "WAIT"), int(action == "ENTER"),
                 ssm.get("direction", ""),
                 verdict.get("score", verdict.get("long_score", 0)),
                 json.dumps(vp.get("supports", [])),
                 json.dumps(vp.get("resistances", [])),
                 signals["oi"]["change_pct"],
                 signals["liquidations"].get("buy_usd", 0),
                 signals["liquidations"].get("sell_usd", 0),
                 detail))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[Trend] 판정 기록 오류: {e}")

    def update_vp(self, symbol: str):
        """VP 재계산"""
        from engines.volume_profile import get_multi_tf_vp
        vp = get_multi_tf_vp(symbol)
        d = vp.get("daily", {})
        if d.get("poc"):
            print(f"[VP] {symbol} POC=${d['poc']:,.2f} "
                  f"VA=${d['va_low']:,.2f}~${d['va_high']:,.2f}")

    def reconcile(self, symbol: str):
        """Binance vs DB 동기화"""
        positions = self.executor.get_positions(symbol)
        binance_pos = None
        for p in positions:
            if p.get("symbol") == symbol:
                amt = float(p.get("positionAmt", 0))
                if abs(amt) > 0:
                    binance_pos = p
                    break
        db_has = self.has_position(symbol)
        if db_has and not binance_pos:
            conn = get_connection()
            conn.execute(
                "UPDATE trend_positions SET status='CLOSED', close_reason='SYNC_MISSING', "
                "closed_at=CURRENT_TIMESTAMP WHERE symbol=? AND status='OPEN'", (symbol,))
            conn.commit()
            conn.close()
            print(f"[Sync] {symbol} DB 정리 (Binance에 없음)")
        elif not db_has and binance_pos:
            amt = float(binance_pos.get("positionAmt", 0))
            ep = float(binance_pos.get("entryPrice", 0))
            direction = "LONG" if amt > 0 else "SHORT"
            qty = abs(amt)
            # DB에 등록하여 관리 대상에 포함
            conn = get_connection()
            conn.execute(
                "INSERT INTO trend_positions "
                "(symbol, direction, entry_price, quantity, trailing_stop, highest_pnl, "
                "status, size_pct, signal_strength, trigger_reason) "
                "VALUES (?,?,?,?,?,0,'OPEN',0,0,?)",
                (symbol, direction, ep, qty, 0,
                 "SYNC_RECOVERED"))
            conn.commit()
            conn.close()
            print(f"[Sync] {symbol} 미추적 {direction} {qty} @ ${ep:,.2f} → DB 등록")
