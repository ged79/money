"""시스템 종합 점검 — 2026-03-01"""
import sys, os, time
from datetime import datetime, timezone, timedelta, date
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
from db import get_connection
from engines.binance_executor import BinanceExecutor

conn = get_connection()
ex = BinanceExecutor(use_testnet=False)
now = time.time()
now_ms = int(now * 1000)

print("=" * 70)
print("  시스템 종합 점검 — 2026-03-01")
print("=" * 70)

# ============================================
# 1. Binance 연결 + 잔고
# ============================================
print(f"\n{'─'*70}")
print("  [1] Binance 연결 & 잔고")
print("─"*70)
try:
    total = ex.get_total_balance()
    avail = ex.get_account_balance()
    positions = ex.get_positions()
    open_positions = [p for p in positions if float(p.get("positionAmt", 0)) != 0]
    print(f"  연결 정상")
    print(f"  총 잔고: ${total:.2f} | 가용: ${avail:.2f}")
    if open_positions:
        for p in open_positions:
            amt = float(p["positionAmt"])
            upnl = float(p.get("unRealizedProfit", 0))
            entry = float(p.get("entryPrice", 0))
            print(f"  포지션: {p['symbol']} {amt:+.4f} @ ${entry:.2f} | 미실현 ${upnl:+.2f}")
    else:
        print(f"  포지션: 없음 (정상)")
    
    for symbol in ["SOLUSDT"]:
        try:
            orders = ex.get_open_orders(symbol)
            if orders:
                print(f"  미체결 주문 ({symbol}): {len(orders)}건")
                for o in orders[:5]:
                    print(f"    {o.get('side')} {o.get('type')} ${float(o.get('price',0)):.2f} qty={o.get('origQty')}")
            else:
                print(f"  미체결 주문 ({symbol}): 0건")
        except:
            pass
except Exception as e:
    print(f"  X Binance 연결 실패: {e}")

# ============================================
# 2. 데이터 수집 상태
# ============================================
print(f"\n{'─'*70}")
print("  [2] 데이터 수집 상태")
print("─"*70)

# klines 5분봉
for symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
    row = conn.execute(
        "SELECT open_time, close FROM klines WHERE symbol=? AND interval='5m' ORDER BY open_time DESC LIMIT 1",
        (symbol,)
    ).fetchone()
    if row:
        age_min = (now_ms - row[0]) / 60000
        status = "OK" if age_min < 10 else "WARN" if age_min < 30 else "FAIL"
        print(f"  [{status}] klines 5m {symbol}: ${row[1]:.2f} | {age_min:.0f}분 전")
    else:
        print(f"  [FAIL] klines 5m {symbol}: 데이터 없음")

# klines 1d
for symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
    row = conn.execute(
        "SELECT open_time, close FROM klines WHERE symbol=? AND interval='1d' ORDER BY open_time DESC LIMIT 1",
        (symbol,)
    ).fetchone()
    if row:
        age_hr = (now_ms - row[0]) / 3600000
        status = "OK" if age_hr < 48 else "FAIL"
        print(f"  [{status}] klines 1d {symbol}: ${row[1]:.2f} | {age_hr:.0f}시간 전")

# OI
for symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
    row = conn.execute(
        "SELECT open_interest, collected_at FROM oi_snapshots WHERE symbol=? ORDER BY id DESC LIMIT 1",
        (symbol,)
    ).fetchone()
    if row:
        try:
            dt = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            age_min = (now - dt.timestamp()) / 60
            status = "OK" if age_min < 30 else "WARN" if age_min < 120 else "FAIL"
        except:
            age_min = -1; status = "??"
        print(f"  [{status}] OI {symbol}: {row[0]:,.0f} | {row[1]} ({age_min:.0f}분 전)")

# 펀딩비
for symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
    row = conn.execute(
        "SELECT funding_rate, collected_at FROM funding_rates WHERE symbol=? ORDER BY id DESC LIMIT 1",
        (symbol,)
    ).fetchone()
    if row:
        try:
            dt = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            age_hr = (now - dt.timestamp()) / 3600
            status = "OK" if age_hr < 12 else "FAIL"
        except:
            age_hr = -1; status = "??"
        print(f"  [{status}] 펀딩비 {symbol}: {row[0]*100:+.4f}% | {row[1]}")

# 롱숏비율
for symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
    row = conn.execute(
        "SELECT long_account, short_account, long_short_ratio, collected_at FROM long_short_ratios WHERE symbol=? ORDER BY id DESC LIMIT 1",
        (symbol,)
    ).fetchone()
    if row:
        try:
            dt = datetime.strptime(row[3], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            age_hr = (now - dt.timestamp()) / 3600
            status = "OK" if age_hr < 12 else "FAIL"
        except:
            age_hr = -1; status = "??"
        print(f"  [{status}] 롱숏 {symbol}: L={row[0]*100:.1f}% S={row[1]*100:.1f}% | {row[3]}")

# 공포탐욕
row = conn.execute("SELECT value, classification, collected_at FROM fear_greed ORDER BY id DESC LIMIT 1").fetchone()
if row:
    try:
        dt = datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        age_hr = (now - dt.timestamp()) / 3600
        status = "OK" if age_hr < 24 else "FAIL"
    except:
        age_hr = -1; status = "??"
    print(f"  [{status}] 공포/탐욕: {row[0]} ({row[1]}) | {row[2]}")

# 온체인
row = conn.execute("SELECT metric, value, collected_at FROM onchain_metrics ORDER BY id DESC LIMIT 1").fetchone()
if row:
    try:
        dt = datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        age_hr = (now - dt.timestamp()) / 3600
        status = "OK" if age_hr < 48 else "FAIL"
    except:
        age_hr = -1; status = "??"
    print(f"  [{status}] 온체인: {row[0]}={row[1]:.4f} | {row[2]}")

# 청산
row = conn.execute("SELECT symbol, COUNT(*), MAX(collected_at) FROM liquidations GROUP BY symbol ORDER BY MAX(collected_at) DESC LIMIT 1").fetchone()
if row:
    try:
        dt = datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        age_hr = (now - dt.timestamp()) / 3600
        status = "OK" if age_hr < 6 else "WARN" if age_hr < 24 else "FAIL"
    except:
        age_hr = -1; status = "??"
    total_liq = conn.execute("SELECT COUNT(*) FROM liquidations").fetchone()[0]
    print(f"  [{status}] 청산: {total_liq}건 | 최신: {row[0]} {row[2]}")

# 오더북 벽
row = conn.execute("SELECT symbol, MAX(collected_at), COUNT(*) FROM orderbook_walls GROUP BY symbol ORDER BY MAX(collected_at) DESC LIMIT 1").fetchone()
if row:
    try:
        dt = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        age_hr = (now - dt.timestamp()) / 3600
        status = "OK" if age_hr < 6 else "FAIL"
    except:
        age_hr = -1; status = "??"
    total_walls = conn.execute("SELECT COUNT(*) FROM orderbook_walls").fetchone()[0]
    print(f"  [{status}] 오더북벽: {total_walls}건 | 최신: {row[0]} {row[1]}")

# 테이커 비율
row = conn.execute("SELECT symbol, buy_sell_ratio, collected_at FROM taker_ratio ORDER BY id DESC LIMIT 1").fetchone()
if row:
    try:
        dt = datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        age_hr = (now - dt.timestamp()) / 3600
        status = "OK" if age_hr < 12 else "FAIL"
    except:
        age_hr = -1; status = "??"
    print(f"  [{status}] 테이커비율: {row[0]} ratio={row[1]:.4f} | {row[2]}")

# whale_transactions
wt = conn.execute("SELECT COUNT(*) FROM whale_transactions").fetchone()[0]
print(f"  [{'WARN' if wt == 0 else 'OK'}] 고래거래: {wt}건")

# ============================================
# 3. 엔진 상태 (Phase 2)
# ============================================
print(f"\n{'─'*70}")
print("  [3] 분석 엔진 상태")
print("─"*70)

# ATR
for symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
    row = conn.execute(
        "SELECT atr, atr_pct, calculated_at FROM atr_values WHERE symbol=? ORDER BY id DESC LIMIT 1",
        (symbol,)
    ).fetchone()
    if row:
        try:
            dt = datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            age_hr = (now - dt.timestamp()) / 3600
            status = "OK" if age_hr < 6 else "WARN" if age_hr < 12 else "FAIL"
        except:
            age_hr = -1; status = "??"
        print(f"  [{status}] ATR {symbol}: ${row[0]:.2f} ({row[1]:.2f}%) | {row[2]}")

# SSM 점수
for symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
    row = conn.execute(
        "SELECT trigger_active, momentum_score, sentiment_score, story_score, value_score, "
        "total_score, direction, calculated_at FROM ssm_scores WHERE symbol=? ORDER BY id DESC LIMIT 1",
        (symbol,)
    ).fetchone()
    if row:
        try:
            dt = datetime.strptime(row[7], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            age_min = (now - dt.timestamp()) / 60
            status = "OK" if age_min < 30 else "WARN" if age_min < 120 else "FAIL"
        except:
            age_min = -1; status = "??"
        print(f"  [{status}] SSM {symbol}: T={'ON' if row[0] else 'OFF'} M={row[1]:.1f} Ss={row[2]:.1f} "
              f"St={row[3]:.1f} V={row[4]:.1f} | Total={row[5]:.2f} -> {row[6]} | {row[7]} ({age_min:.0f}분 전)")

# 그리드 범위
for symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
    row = conn.execute(
        "SELECT lower_bound, upper_bound, grid_count, grid_spacing_pct, calculated_at FROM grid_configs WHERE symbol=? ORDER BY id DESC LIMIT 1",
        (symbol,)
    ).fetchone()
    if row:
        try:
            dt = datetime.strptime(row[4], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            age_hr = (now - dt.timestamp()) / 3600
            status = "OK" if age_hr < 6 else "WARN" if age_hr < 12 else "FAIL"
        except:
            age_hr = -1; status = "??"
        mark = ex.get_mark_price(symbol) or 0
        in_range = "IN" if row[0] <= mark <= row[1] else "OOB"
        print(f"  [{status}] Grid {symbol}: ${row[0]:.2f}~${row[1]:.2f} ({row[2]}lvl, {row[3]:.2f}%) [{in_range}] mark=${mark:.2f} | {row[4]}")

# 전략 상태
for symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
    row = conn.execute(
        "SELECT state, l1_active, l2_active, l4_active, updated_at FROM strategy_state WHERE symbol=? ORDER BY id DESC LIMIT 1",
        (symbol,)
    ).fetchone()
    if row:
        print(f"  전략 {symbol}: State={row[0]} L1={'ON' if row[1] else 'OFF'} L2={'ON' if row[2] else 'OFF'} L4={'ON' if row[3] else 'OFF'} | {row[4]}")

# threshold_signals 최신
row = conn.execute("SELECT symbol, threshold_value, trigger_active, direction, calculated_at FROM threshold_signals ORDER BY id DESC LIMIT 1").fetchone()
if row:
    try:
        dt = datetime.strptime(row[4], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        age_min = (now - dt.timestamp()) / 60
        status = "OK" if age_min < 30 else "FAIL"
    except:
        age_min = -1; status = "??"
    print(f"  [{status}] threshold: {row[0]} thr={row[1]:.6f} active={'ON' if row[2] else 'OFF'} dir={row[3]} | {row[4]} ({age_min:.0f}분 전)")

# ============================================
# 4. 라이브 트레이딩 상태 (Phase 3)
# ============================================
print(f"\n{'─'*70}")
print("  [4] 라이브 트레이딩 상태")
print("─"*70)

# CB
today = date.today().isoformat()
cb = conn.execute(
    "SELECT realized_pnl, unrealized_pnl, circuit_breaker_hit, total_orders FROM live_daily_pnl WHERE trade_date=?",
    (today,)
).fetchone()
if cb:
    cb_status = "TRIGGERED!" if cb[2] else "OK"
    print(f"  [{cb_status}] CB [{today}]: realized={cb[0]:+.2f}% unrealized={cb[1]:+.2f}% | orders={cb[3]}")
else:
    print(f"  [OK] CB [{today}]: 기록 없음 (정상 - 새로운 날)")

# grid_positions
for symbol in ["SOLUSDT"]:
    stats = conn.execute(
        "SELECT status, COUNT(*), COALESCE(SUM(quantity),0) FROM grid_positions WHERE symbol=? GROUP BY status",
        (symbol,)
    ).fetchall()
    total_levels = conn.execute("SELECT COUNT(*) FROM grid_positions WHERE symbol=?", (symbol,)).fetchone()[0]
    print(f"  Grid positions [{symbol}]: 총 {total_levels}레벨")
    for s in stats:
        print(f"    {s[0]}: {s[1]}건 (qty={s[2]:.1f})")

# 오늘 주문
today_orders = conn.execute(
    "SELECT COUNT(*), "
    "SUM(CASE WHEN status='FILLED' THEN 1 ELSE 0 END), "
    "SUM(CASE WHEN status='PLACED' THEN 1 ELSE 0 END), "
    "SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END), "
    "COALESCE(SUM(pnl_usd), 0) "
    "FROM grid_order_log WHERE created_at >= ?",
    (today,)
).fetchone()
if today_orders and today_orders[0] > 0:
    print(f"  오늘 주문: 총 {today_orders[0]} | FILLED={today_orders[1]} PLACED={today_orders[2]} FAILED={today_orders[3]} | PnL=${today_orders[4]:+.4f}")
else:
    print(f"  오늘 주문: 0건")

# 최근 FAILED 주문
failed = conn.execute(
    "SELECT symbol, side, direction, grid_price, status, created_at FROM grid_order_log "
    "WHERE status='FAILED' ORDER BY id DESC LIMIT 3"
).fetchall()
if failed:
    print(f"  최근 FAILED 주문:")
    for f in failed:
        print(f"    {f[5]} | {f[0]} {f[1]}({f[2]}) @ ${f[3]:.2f}")

# ============================================
# 5. 데이터 연속성 (5분봉 빈 구간)
# ============================================
print(f"\n{'─'*70}")
print("  [5] 데이터 연속성 (24h 5분봉)")
print("─"*70)

for symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
    cutoff = now_ms - 86400000
    candles = conn.execute(
        "SELECT open_time FROM klines WHERE symbol=? AND interval='5m' AND open_time >= ? ORDER BY open_time",
        (symbol, cutoff)
    ).fetchall()
    expected = 288
    actual = len(candles)
    
    gaps = []
    for i in range(1, len(candles)):
        diff = candles[i][0] - candles[i-1][0]
        if diff > 450000:  # 7.5분 이상 갭
            gap_start = datetime.fromtimestamp(candles[i-1][0]/1000, tz=timezone.utc).strftime("%m/%d %H:%M")
            gap_end = datetime.fromtimestamp(candles[i][0]/1000, tz=timezone.utc).strftime("%H:%M")
            gap_min = diff / 60000
            gaps.append(f"{gap_start}~{gap_end} ({gap_min:.0f}분)")
    
    pct = actual / expected * 100
    status = "OK" if pct >= 95 and not gaps else "WARN" if pct >= 80 else "FAIL"
    print(f"  [{status}] {symbol}: {actual}/{expected}봉 ({pct:.0f}%)")
    for g in gaps[:3]:
        print(f"    GAP: {g}")

# ============================================
# 6. 수집 주기 검증
# ============================================
print(f"\n{'─'*70}")
print("  [6] 수집 주기")
print("─"*70)

def calc_interval(table, col, symbol=None, unit="min"):
    q = f"SELECT {col} FROM {table}"
    params = ()
    if symbol:
        q += f" WHERE symbol=?"
        params = (symbol,)
    q += f" ORDER BY id DESC LIMIT 10"
    times = conn.execute(q, params).fetchall()
    if len(times) < 2:
        return None
    intervals = []
    for i in range(len(times)-1):
        try:
            t1 = datetime.strptime(times[i][0], "%Y-%m-%d %H:%M:%S")
            t2 = datetime.strptime(times[i+1][0], "%Y-%m-%d %H:%M:%S")
            diff = (t1-t2).total_seconds()
            if unit == "min":
                intervals.append(diff/60)
            else:
                intervals.append(diff/3600)
        except:
            pass
    return sum(intervals)/len(intervals) if intervals else None

oi_int = calc_interval("oi_snapshots", "collected_at", "SOLUSDT")
if oi_int: print(f"  OI: 평균 {oi_int:.0f}분 간격")

fr_int = calc_interval("funding_rates", "collected_at", "SOLUSDT", "hr")
if fr_int: print(f"  펀딩비: 평균 {fr_int:.1f}시간 간격")

ssm_int = calc_interval("ssm_scores", "calculated_at", "SOLUSDT")
if ssm_int: print(f"  SSM: 평균 {ssm_int:.0f}분 간격")

grid_int = calc_interval("grid_configs", "calculated_at", "SOLUSDT")
if grid_int: print(f"  그리드범위: 평균 {grid_int:.0f}분 간격")

ls_int = calc_interval("long_short_ratios", "collected_at", "SOLUSDT")
if ls_int: print(f"  롱숏비율: 평균 {ls_int:.0f}분 간격")

th_int = calc_interval("threshold_signals", "calculated_at", "SOLUSDT")
if th_int: print(f"  threshold: 평균 {th_int:.0f}분 간격")

# ============================================
# 7. 코드 수정 적용 확인
# ============================================
print(f"\n{'─'*70}")
print("  [7] 코드 수정 적용 확인")
print("─"*70)

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "engines", "live_trader.py"), "r", encoding="utf-8") as f:
    code = f.read()

checks = [
    ("GRID_V2_MAX_NET_LEVELS", "넷포지션 한도 import"),
    ("block_long", "넷포지션 LONG 차단"),
    ("block_short", "넷포지션 SHORT 차단"),
    ("[CB]", "CB 포지션 청산"),
    ("# Step 3: OOB", "OOB > 트렌드가드 순서"),
    ("# Step 4: 트렌드 가드", "트렌드가드 뒤로 이동"),
    ("캐시를 클리어하지 않음", "OOB시 캐시 유지"),
    ("L2 전환 성공", "L2 성공시만 캐시 클리어"),
]

for keyword, desc in checks:
    found = keyword in code
    print(f"  [{'OK' if found else 'FAIL'}] {desc}")

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.py"), "r", encoding="utf-8") as f:
    cc = f.read()
print(f"  [{'OK' if 'GRID_V2_MAX_NET_LEVELS' in cc else 'FAIL'}] config GRID_V2_MAX_NET_LEVELS")

conn.close()
print(f"\n{'=' * 70}")
print("  점검 완료")
print("=" * 70)
