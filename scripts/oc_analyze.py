"""OpenClaw용 시장 분석 요약 생성 - 텍스트 출력 (멀티 심볼)"""
import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from db import get_connection
from config import SYMBOLS


def analyze_symbol(conn, symbol):
    lines = []
    base = symbol.replace("USDT", "")

    # 가격 정보
    kline = conn.execute(
        "SELECT close, open, high, low, volume FROM klines "
        "WHERE symbol=? AND interval='1d' ORDER BY open_time DESC LIMIT 3",
        (symbol,),
    ).fetchall()
    if kline:
        price = kline[0][0]
        lines.append(f"{base} 현재가: ${price:,.0f}")
        if len(kline) >= 2:
            change = ((kline[0][0] - kline[1][0]) / kline[1][0]) * 100
            lines.append(f"  24h 변동: {change:+.2f}%")
        if len(kline) >= 3:
            change3d = ((kline[0][0] - kline[2][0]) / kline[2][0]) * 100
            lines.append(f"  3일 변동: {change3d:+.2f}%")
    else:
        lines.append(f"{base}: 데이터 없음")
        return "\n".join(lines)

    # 펀딩비
    fr = conn.execute(
        "SELECT funding_rate FROM funding_rates WHERE symbol=? ORDER BY collected_at DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    if fr:
        lines.append(f"  펀딩비: {fr[0]*100:.4f}%")

    # 롱숏
    ls = conn.execute(
        "SELECT long_account FROM long_short_ratios WHERE symbol=? ORDER BY collected_at DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    if ls:
        lines.append(f"  롱/숏: {ls[0]*100:.1f}% / {(1-ls[0])*100:.1f}%")

    # OI
    oi = conn.execute(
        "SELECT open_interest FROM oi_snapshots WHERE symbol=? ORDER BY collected_at DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    if oi:
        lines.append(f"  OI: {oi[0]:,.0f} {base}")

    # ATR
    atr = conn.execute(
        "SELECT atr, atr_pct, stop_loss_pct FROM atr_values WHERE symbol=? ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    if atr:
        lines.append(f"  ATR(14d): ${atr[0]:,.0f} ({atr[1]:.2f}%) -> 스톱로스 {atr[2]:.2f}%")

    # 임계점
    thr = conn.execute(
        "SELECT trigger_active, liq_amount_1h FROM threshold_signals WHERE symbol=? ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    if thr:
        lines.append(f"  Trigger: {'ON' if thr[0] else 'OFF'} (1h 청산: ${thr[1]:,.0f})")

    # SSM 점수
    score = conn.execute(
        "SELECT trigger_active, momentum_score, sentiment_score, story_score, "
        "value_score, total_score, direction FROM ssm_scores WHERE symbol=? ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    if score:
        lines.append(f"  SSM+V+T: {score[5]:.2f}/5.0 ({score[6]})")
        lines.append(f"    T={'ON' if score[0] else 'OFF'} | M={score[1]:.1f} | Ss={score[2]:.1f} | Story={score[3]:.1f} | V={score[4]:.1f}")

    # 전략 상태
    state = conn.execute(
        "SELECT state, l1_active, l2_active, l2_direction, l2_step, l4_active, macro_blocked "
        "FROM strategy_state WHERE symbol=? ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    if state:
        lines.append(f"  전략: State {state[0]} | L1={'ON' if state[1] else 'OFF'} | "
                     f"L2={'ON('+state[3]+' step'+str(state[4])+')' if state[2] else 'OFF'} | "
                     f"L4={'ON' if state[5] else 'OFF'} | "
                     f"매크로={'BLOCKED' if state[6] else 'OK'}")

    # 최근 시그널
    signals = conn.execute(
        "SELECT signal_type, direction, created_at FROM signal_log WHERE symbol=? ORDER BY id DESC LIMIT 3",
        (symbol,),
    ).fetchall()
    if signals:
        lines.append(f"  최근 시그널:")
        for s in signals:
            lines.append(f"    [{s[2]}] {s[0]} {s[1]}")

    # 청산 통계 (최근 1시간)
    now_ms = int(time.time() * 1000)
    liq = conn.execute(
        "SELECT side, COUNT(*), SUM(price*qty) FROM liquidations "
        "WHERE symbol=? AND trade_time > ? GROUP BY side",
        (symbol, now_ms - 3600_000),
    ).fetchall()
    if liq:
        parts = []
        for l in liq:
            name = "숏청산" if l[0] == "BUY" else "롱청산"
            parts.append(f"{name} {l[1]}건 ${l[2]:,.0f}")
        lines.append(f"  1h 청산: {' | '.join(parts)}")

    return "\n".join(lines)


def analyze():
    conn = get_connection()
    sections = []

    # F&G (글로벌 지표)
    fg = conn.execute("SELECT value, classification FROM fear_greed ORDER BY collected_at DESC LIMIT 1").fetchone()
    if fg:
        sections.append(f"Fear & Greed: {fg[0]} ({fg[1]})")

    sections.append("")

    # 각 심볼 분석
    for symbol in SYMBOLS:
        sections.append(analyze_symbol(conn, symbol))
        sections.append("")

    # 페이퍼 트레이딩 요약
    paper_lines = []
    for symbol in SYMBOLS:
        base = symbol.replace("USDT", "")
        # 전체 성과
        stats = conn.execute(
            "SELECT COUNT(*), "
            "SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END), "
            "SUM(pnl_weighted) "
            "FROM paper_trades WHERE symbol = ? AND status = 'CLOSED'",
            (symbol,),
        ).fetchone()
        total = stats[0] or 0
        wins = stats[1] or 0
        pnl = stats[2] or 0

        # OPEN 포지션
        open_pos = conn.execute(
            "SELECT direction, entry_price, entry_pct, l2_step, stop_loss "
            "FROM paper_trades WHERE symbol = ? AND status = 'OPEN' "
            "ORDER BY id DESC LIMIT 1",
            (symbol,),
        ).fetchone()

        if open_pos:
            current = conn.execute(
                "SELECT close FROM klines WHERE symbol = ? AND interval = '1d' "
                "ORDER BY open_time DESC LIMIT 1",
                (symbol,),
            ).fetchone()
            cp = current[0] if current else 0
            if cp and open_pos[1]:
                if open_pos[0] == "LONG":
                    fl = (cp - open_pos[1]) / open_pos[1] * 100
                else:
                    fl = (open_pos[1] - cp) / open_pos[1] * 100
                paper_lines.append(
                    f"  {base}: {open_pos[0]} step{open_pos[3]} "
                    f"${open_pos[1]:,.0f}->${cp:,.0f} "
                    f"PnL {fl:+.2f}% | 누적 {pnl:+.2f}% ({total}건 승률 {wins/total*100:.0f}%)" if total > 0 else
                    f"  {base}: {open_pos[0]} step{open_pos[3]} "
                    f"${open_pos[1]:,.0f}->${cp:,.0f} PnL {fl:+.2f}%"
                )
            else:
                paper_lines.append(f"  {base}: {open_pos[0]} step{open_pos[3]} 진입 ${open_pos[1]:,.0f}")
        elif total > 0:
            wr = round(wins / total * 100, 1)
            paper_lines.append(f"  {base}: 대기중 | 누적 {pnl:+.2f}% ({total}건 승률 {wr}%)")

    if paper_lines:
        sections.append("페이퍼 트레이딩:")
        sections.extend(paper_lines)
        sections.append("")

    conn.close()
    return "\n".join(sections).strip()


if __name__ == "__main__":
    print(analyze())
