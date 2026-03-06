"""Risk Manager — 수익 래칫 + 서킷브레이커 + 일일 PnL 추적
Grid/Trend 공용: 래칫 바닥 보호, 일일 손실 한도, PnL 집계.
"""
from datetime import date
from db import get_connection
from config import RATCHET_THRESHOLDS, DAILY_LOSS_LIMIT, UNREALIZED_LOSS_WARN


class RiskManager:
    """수익 래칫 + 서킷브레이커 관리"""

    def __init__(self):
        pass

    # --- 수익 래칫 -------------------------------------------------------

    def _calc_floor(self, pnl: float) -> float:
        """RATCHET_THRESHOLDS 기반 보호 바닥 계산"""
        floor = 0.0
        for amt, pct in sorted(RATCHET_THRESHOLDS):
            if pnl >= amt:
                floor = pnl * pct
        return round(floor, 2)

    def check_profit_ratchet(self, symbol: str) -> bool:
        """누적 수익이 래칫 바닥 아래로 떨어졌는지 확인. True면 포지션 축소 필요."""
        conn = get_connection()
        row = conn.execute(
            "SELECT cumulative_pnl, ratchet_floor FROM grid_state WHERE symbol=? LIMIT 1",
            (symbol,)).fetchone()
        if not row:
            conn.close()
            return False
        cum_pnl, cur_floor = row[0] or 0.0, row[1] or 0.0
        new_floor = max(cur_floor, self._calc_floor(cum_pnl))
        if new_floor != cur_floor:
            conn.execute("UPDATE grid_state SET ratchet_floor=? WHERE symbol=?", (new_floor, symbol))
            conn.commit()
            print(f"[Risk] {symbol} 래칫 바닥 갱신: ${new_floor:,.2f}")
        conn.close()
        if new_floor > 0 and cum_pnl < new_floor:
            print(f"[Risk] {symbol} 래칫 이탈! 누적=${cum_pnl:,.2f} < 바닥=${new_floor:,.2f}")
            return True
        return False

    def update_ratchet_floor(self, symbol: str, cumulative_pnl: float):
        """그리드 거래 후 래칫 바닥 갱신 (외부 호출용)"""
        conn = get_connection()
        row = conn.execute("SELECT ratchet_floor FROM grid_state WHERE symbol=? LIMIT 1", (symbol,)).fetchone()
        cur = (row[0] or 0.0) if row else 0.0
        new = max(cur, self._calc_floor(cumulative_pnl))
        if new != cur:
            conn.execute("UPDATE grid_state SET ratchet_floor=? WHERE symbol=?", (new, symbol))
            conn.commit()
            print(f"[Risk] {symbol} 래칫 바닥 → ${new:,.2f} (PnL=${cumulative_pnl:,.2f})")
        conn.close()

    # --- 서킷브레이커 ----------------------------------------------------

    def check_circuit_breaker(self, symbol: str, balance: float = 0) -> bool:
        """일일 손실 한도 체크 (% 기반). True면 매매 전면 중단."""
        today = date.today().isoformat()
        conn = get_connection()
        # 이미 발동 상태면 유지
        cb = conn.execute(
            "SELECT circuit_breaker_hit FROM daily_pnl WHERE date=? AND symbol=?",
            (today, symbol)).fetchone()
        if cb and cb[0]:
            conn.close()
            print(f"[Risk] {symbol} 서킷브레이커 발동 상태 유지")
            return True
        # 오늘 그리드 + 추세 실현 손익 합산
        g = conn.execute(
            "SELECT COALESCE(SUM(pnl_usd),0) FROM grid_trades WHERE symbol=? AND DATE(traded_at)=?",
            (symbol, today)).fetchone()[0]
        t = conn.execute(
            "SELECT COALESCE(SUM(pnl_usd),0) FROM trend_positions "
            "WHERE symbol=? AND status='CLOSED' AND DATE(closed_at)=?",
            (symbol, today)).fetchone()[0]
        conn.close()
        total = g + t
        # % 변환: balance가 있으면 % 기반, 없으면 USD 비교
        if balance > 0:
            loss_pct = (total / balance) * 100  # e.g. -2.5 means -2.5%
        else:
            loss_pct = total  # 폴백: USD 그대로
        if total < 0:
            print(f"[Risk] {symbol} 오늘 손익: ${total:+,.2f} ({loss_pct:+.2f}%)")
        if loss_pct <= DAILY_LOSS_LIMIT:
            print(f"[Risk] {symbol} 서킷브레이커 발동! 손실=${total:+,.2f} ({loss_pct:+.2f}%)")
            self.update_daily_pnl(symbol, grid_pnl=g, trend_pnl=t)
            conn2 = get_connection()
            conn2.execute("UPDATE daily_pnl SET circuit_breaker_hit=1 WHERE date=? AND symbol=?",
                          (today, symbol))
            conn2.commit()
            conn2.close()
            return True
        elif loss_pct <= UNREALIZED_LOSS_WARN:
            print(f"[Risk] {symbol} 손실 경고: ${total:+,.2f} ({loss_pct:+.2f}%)")
        return False

    # --- 일일 PnL --------------------------------------------------------

    def update_daily_pnl(self, symbol: str, grid_pnl: float = 0, trend_pnl: float = 0):
        """일일 PnL 누적 기록"""
        today = date.today().isoformat()
        conn = get_connection()
        row = conn.execute(
            "SELECT id, grid_pnl, trend_pnl, total_trades FROM daily_pnl WHERE date=? AND symbol=?",
            (today, symbol)).fetchone()
        if row:
            ng = round((row[1] or 0) + grid_pnl, 2)
            nt = round((row[2] or 0) + trend_pnl, 2)
            trades = (row[3] or 0) + (1 if grid_pnl or trend_pnl else 0)
            conn.execute("UPDATE daily_pnl SET grid_pnl=?, trend_pnl=?, total_pnl=?, total_trades=? WHERE id=?",
                          (ng, nt, round(ng + nt, 2), trades, row[0]))
        else:
            total = round(grid_pnl + trend_pnl, 2)
            conn.execute(
                "INSERT INTO daily_pnl (date,symbol,grid_pnl,trend_pnl,total_pnl,total_trades) "
                "VALUES (?,?,?,?,?,?)", (today, symbol, grid_pnl, trend_pnl, total, 1 if total else 0))
        conn.commit()
        conn.close()

    def get_daily_summary(self, symbol: str) -> dict:
        """오늘 매매 요약"""
        today = date.today().isoformat()
        conn = get_connection()
        row = conn.execute(
            "SELECT grid_pnl,trend_pnl,total_pnl,total_trades,circuit_breaker_hit "
            "FROM daily_pnl WHERE date=? AND symbol=?", (today, symbol)).fetchone()
        conn.close()
        if row:
            return {"date": today, "symbol": symbol, "grid_pnl": row[0] or 0, "trend_pnl": row[1] or 0,
                    "total_pnl": row[2] or 0, "total_trades": row[3] or 0, "circuit_breaker": bool(row[4])}
        return {"date": today, "symbol": symbol, "grid_pnl": 0, "trend_pnl": 0,
                "total_pnl": 0, "total_trades": 0, "circuit_breaker": False}


if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8")
    print("=== RiskManager 테스트 ===")
    rm = RiskManager()
    for s in ["SOLUSDT"]:
        print(f"  {s} 요약: {rm.get_daily_summary(s)}")
        print(f"  {s} 래칫: {'이탈' if rm.check_profit_ratchet(s) else 'OK'}")
        print(f"  {s} CB: {'발동' if rm.check_circuit_breaker(s) else '정상'}")
