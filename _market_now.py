"""현재 시장 상태 종합 — 가격, 추세, 지표"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from db import get_connection
from engines.binance_executor import BinanceExecutor

conn = get_connection()
ex = BinanceExecutor(use_testnet=False)

print("=" * 65)
print("  시장 현황 리포트")
print("=" * 65)

for symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
    mark = ex.get_mark_price(symbol)
    print(f"\n{'─'*65}")
    print(f"  {symbol} — 현재가: ${mark:,.2f}" if mark else f"  {symbol} — 현재가 조회 실패")

    # 5분봉 가격 추이
    rows = conn.execute(
        "SELECT close, volume, open_time FROM klines "
        "WHERE symbol=? AND interval='5m' ORDER BY open_time DESC LIMIT 288",
        (symbol,),
    ).fetchall()
    if rows:
        prices = [r[0] for r in rows]
        now_p = prices[0]
        h1 = prices[11] if len(prices) > 11 else prices[-1]
        h4 = prices[47] if len(prices) > 47 else prices[-1]
        h24 = prices[-1]
        print(f"  1h전: ${h1:,.2f} ({(now_p-h1)/h1*100:+.2f}%) | "
              f"4h전: ${h4:,.2f} ({(now_p-h4)/h4*100:+.2f}%) | "
              f"24h전: ${h24:,.2f} ({(now_p-h24)/h24*100:+.2f}%)")

        # EMA 계산
        closes = list(reversed(prices))  # 오래된→최신
        if len(closes) >= 48:
            k = 2.0 / 49
            ema = [sum(closes[:48]) / 48]
            for p in closes[48:]:
                ema.append(p * k + ema[-1] * (1 - k))
            ema_now = ema[-1]
            ema_1h = ema[-13] if len(ema) > 13 else ema[0]
            slope = (ema_now - ema_1h) / ema_1h * 100
            pos = "위" if now_p > ema_now else "아래"
            print(f"  EMA48: ${ema_now:,.2f} (가격 {pos}) | 1h 기울기: {slope:+.3f}%")

        # 거래량 추이
        vol_1h = sum(r[1] for r in rows[:12])
        vol_4h = sum(r[1] for r in rows[:48]) / 4 if len(rows) >= 48 else 0
        vol_24h_avg = sum(r[1] for r in rows) / (len(rows) / 12) if rows else 0
        vol_ratio = vol_1h / vol_24h_avg if vol_24h_avg > 0 else 0
        print(f"  거래량 1h: {vol_1h:,.0f} | 24h평균/h: {vol_24h_avg:,.0f} | 비율: {vol_ratio:.2f}x")

    # 일봉
    daily = conn.execute(
        "SELECT open, high, low, close FROM klines "
        "WHERE symbol=? AND interval='1d' ORDER BY open_time DESC LIMIT 3",
        (symbol,),
    ).fetchall()
    if daily:
        d = daily[0]
        chg = (d[3] - d[0]) / d[0] * 100
        print(f"  일봉: O=${d[0]:,.2f} H=${d[1]:,.2f} L=${d[2]:,.2f} C=${d[3]:,.2f} ({chg:+.2f}%)")

    # 펀딩비
    fund = conn.execute(
        "SELECT funding_rate, collected_at FROM funding_rates "
        "WHERE symbol=? ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    if fund:
        fr = fund[0] * 100
        print(f"  펀딩비: {fr:+.4f}% ({fund[1]})")

    # 롱숏비율
    ls = conn.execute(
        "SELECT long_account, short_account, long_short_ratio, collected_at FROM long_short_ratios "
        "WHERE symbol=? ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    if ls:
        print(f"  롱/숏: {ls[0]*100:.1f}% / {ls[1]*100:.1f}% (비율 {ls[2]:.2f}) ({ls[3]})")

    # OI
    oi = conn.execute(
        "SELECT open_interest, collected_at FROM oi_snapshots "
        "WHERE symbol=? ORDER BY id DESC LIMIT 2",
        (symbol,),
    ).fetchall()
    if len(oi) >= 2:
        oi_chg = (oi[0][0] - oi[1][0]) / oi[1][0] * 100 if oi[1][0] > 0 else 0
        print(f"  OI: {oi[0][0]:,.0f} ({oi_chg:+.2f}%) ({oi[0][1]})")

# 공포/탐욕
fg = conn.execute("SELECT value, classification, collected_at FROM fear_greed ORDER BY id DESC LIMIT 1").fetchone()
if fg:
    print(f"\n{'─'*65}")
    print(f"  공포/탐욕 지수: {fg[0]} ({fg[1]}) — {fg[2]}")

# SSM 점수
print(f"\n{'─'*65}")
print(f"  SSM 점수:")
for symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
    score = conn.execute(
        "SELECT trigger_active, momentum_score, sentiment_score, story_score, "
        "value_score, total_score, direction, calculated_at "
        "FROM ssm_scores WHERE symbol=? ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    if score:
        print(f"  {symbol}: T={'ON' if score[0] else 'OFF'} M={score[1]:.1f} Ss={score[2]:.1f} "
              f"Story={score[3]:.1f} V={score[4]:.1f} | Total={score[5]:.2f} → {score[6]}")

# 온체인
print(f"\n{'─'*65}")
print(f"  온체인 지표:")
onchain = conn.execute(
    "SELECT metric, value, collected_at FROM onchain_metrics ORDER BY id DESC LIMIT 5"
).fetchall()
for r in onchain:
    print(f"  {r[0]}: {r[1]:.4f} — {r[2]}")

# 그리드 범위
print(f"\n{'─'*65}")
print(f"  현재 그리드 범위:")
for symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
    grid = conn.execute(
        "SELECT lower_bound, upper_bound, grid_count, grid_spacing_pct, calculated_at "
        "FROM grid_configs WHERE symbol=? ORDER BY id DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    if grid:
        m = ex.get_mark_price(symbol) or 0
        in_range = "범위내" if grid[0] <= m <= grid[1] else "OOB"
        print(f"  {symbol}: ${grid[0]:,.2f}~${grid[1]:,.2f} ({grid[2]}grids, {grid[3]:.2f}%) [{in_range}]")

conn.close()
