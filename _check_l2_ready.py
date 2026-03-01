# -*- coding: utf-8 -*-
"""OOB -> L2 entry condition check"""
import sys, sqlite3, time, datetime
sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('data/trades.db')

print("=" * 60)
print("  OOB -> L2 Entry Condition Check")
print("=" * 60)

# 1. Grid range
print("\n[1] Grid Range")
grid = conn.execute(
    "SELECT id, lower_bound, upper_bound, grid_count, grid_spacing "
    "FROM grid_configs WHERE symbol='SOLUSDT' ORDER BY id DESC LIMIT 1"
).fetchone()
if grid:
    print(f"    Grid ID: {grid[0]}")
    print(f"    Range: ${grid[1]:,.2f} ~ ${grid[2]:,.2f}")
    print(f"    Count: {grid[3]}, Spacing: {grid[4]}")
    lower, upper = grid[1], grid[2]
else:
    print("    No grid config!")
    lower, upper = 0, 0

# 2. Recent prices
print("\n[2] Recent Prices (5m candle)")
prices = conn.execute(
    "SELECT open_time, close FROM klines "
    "WHERE symbol='SOLUSDT' AND interval='5m' "
    "ORDER BY open_time DESC LIMIT 10"
).fetchall()
if prices:
    latest_price = prices[0][1]
    for p in prices:
        ts = datetime.datetime.fromtimestamp(p[0]/1000).strftime('%m-%d %H:%M')
        marker = ""
        if lower and upper:
            if p[1] < lower:
                marker = " << OOB (below)"
            elif p[1] > upper:
                marker = " << OOB (above)"
        print(f"    {ts} -> ${p[1]:,.2f}{marker}")

    print(f"\n    * Current: ${latest_price:,.2f}", end="")
    if lower and upper:
        if latest_price < lower:
            pct = (lower - latest_price) / lower * 100
            print(f" -> {pct:.2f}% below lower(${lower:,.2f}) [OOB]")
        elif latest_price > upper:
            pct = (latest_price - upper) / upper * 100
            print(f" -> {pct:.2f}% above upper(${upper:,.2f}) [OOB]")
        else:
            pct_to_lower = (latest_price - lower) / (upper - lower) * 100
            print(f" -> In range ({pct_to_lower:.1f}% from lower)")
    else:
        print()
else:
    latest_price = 0
    print("    No price data!")

# 3. SSM Score
print("\n[3] SSM Score (latest 5)")
ssm_rows = conn.execute(
    "SELECT total_score, direction, calculated_at FROM ssm_scores "
    "WHERE symbol='SOLUSDT' ORDER BY id DESC LIMIT 5"
).fetchall()
if ssm_rows:
    latest_ssm = ssm_rows[0]
    for s in ssm_rows:
        gate = "OK" if s[0] >= 2.0 else "LOW"
        print(f"    [{gate}] Score: {s[0]:.2f}, Dir: {s[1]}, Time: {s[2]}")

    ssm_ok = latest_ssm[0] >= 2.0
    ssm_dir = latest_ssm[1]
    print(f"\n    * Latest SSM: {ssm_ok and 'PASS' or 'FAIL'} -> {latest_ssm[0]:.2f} {latest_ssm[1]} (need >= 2.0)")
else:
    ssm_ok = False
    ssm_dir = None
    print("    No SSM data!")

# 4. Volume ratio
print("\n[4] Volume Ratio (1h vs 24h avg)")
recent = conn.execute(
    "SELECT SUM(volume) FROM "
    "(SELECT volume FROM klines WHERE symbol='SOLUSDT' AND interval='5m' "
    "ORDER BY open_time DESC LIMIT 12)"
).fetchone()
daily = conn.execute(
    "SELECT SUM(volume), COUNT(*) FROM "
    "(SELECT volume FROM klines WHERE symbol='SOLUSDT' AND interval='5m' "
    "ORDER BY open_time DESC LIMIT 288)"
).fetchone()

if recent[0] and daily[0] and daily[1] and daily[1] >= 12:
    vol_1h = recent[0]
    vol_avg_1h = daily[0] / (daily[1] / 12)
    ratio = vol_1h / vol_avg_1h if vol_avg_1h > 0 else 0
    vol_ok = ratio >= 2.0
    print(f"    1h Volume:   {vol_1h:,.0f}")
    print(f"    24h avg 1h:  {vol_avg_1h:,.0f}")
    print(f"    Ratio:       {ratio:.2f}x -> {'SURGE' if vol_ok else 'NORMAL'} (need >= 2.0x)")
else:
    vol_ok = False
    ratio = 0
    print("    Insufficient data!")

# 5. Liquidation
print("\n[5] Liquidation (last 1h)")
cutoff_ms = int((time.time() - 3600) * 1000)
liq = conn.execute(
    "SELECT SUM(qty * price) FROM liquidations "
    "WHERE symbol='SOLUSDT' AND trade_time > ?", (cutoff_ms,)
).fetchone()
liq_amt = liq[0] if liq and liq[0] else 0
liq_ok = liq_amt >= 50000
print(f"    Liq Amount: ${liq_amt:,.0f} -> {'SURGE' if liq_ok else 'NORMAL'} (need >= $50,000)")

# 6. System state
print("\n[6] System State")
state = conn.execute(
    "SELECT l4_active, l2_active, l2_direction FROM strategy_state WHERE symbol='SOLUSDT'"
).fetchone()
l4_ok = state and state[0]
l2_active = state[1] if state else 0
l2_dir = state[2] if state else None
print(f"    L4 Active: {'YES' if l4_ok else 'NO'}")
print(f"    L2 Active: {'YES -> ' + str(l2_dir) if l2_active else 'NO'}")

# 7. Grid positions
print("\n[7] Grid Positions")
positions = conn.execute(
    "SELECT level_index, side, direction, status, entry_price "
    "FROM grid_positions WHERE symbol='SOLUSDT' AND status != 'EMPTY' "
    "ORDER BY level_index"
).fetchall()
if positions:
    for pos in positions:
        print(f"    Lv {pos[0]}: {pos[1]} {pos[2]} [{pos[3]}] @ ${pos[4]:,.2f}")
else:
    print("    No active positions")

# 8. HYBRID_L2_ENABLED check
print("\n[8] Config Check")
try:
    from config import HYBRID_L2_ENABLED, HYBRID_L2_MIN_SSM, GRID_V2_OOB_VOLUME_MULTIPLIER, GRID_V2_OOB_PAUSE_MINUTES
    print(f"    HYBRID_L2_ENABLED:     {HYBRID_L2_ENABLED}")
    print(f"    HYBRID_L2_MIN_SSM:     {HYBRID_L2_MIN_SSM}")
    print(f"    OOB_VOLUME_MULTIPLIER: {GRID_V2_OOB_VOLUME_MULTIPLIER}")
    print(f"    OOB_PAUSE_MINUTES:     {GRID_V2_OOB_PAUSE_MINUTES}")
    hybrid_enabled = HYBRID_L2_ENABLED
except Exception as e:
    print(f"    Config load error: {e}")
    hybrid_enabled = None

# === Summary ===
print("\n" + "=" * 60)
print("  SUMMARY")
print("=" * 60)

is_oob = lower > 0 and upper > 0 and (latest_price < lower or latest_price > upper)
oob_dir = None
if latest_price < lower:
    oob_dir = "BEARISH"
elif latest_price > upper:
    oob_dir = "BULLISH"

print(f"\n  1. OOB Status:     {'YES - ' + str(oob_dir) if is_oob else 'NO (in range)'}")
print(f"  2. OOB Confirm:")
if vol_ok and liq_ok:
    print(f"     -> Volume SURGE + Liq SURGE -> Immediate PAUSE")
    oob_confirmed = True
elif vol_ok:
    print(f"     -> Volume SURGE -> Immediate PAUSE")
    oob_confirmed = True
else:
    print(f"     -> Volume {ratio:.2f}x (low) -> Need 30min fallback")
    oob_confirmed = vol_ok

print(f"  3. L2 Entry Conditions:")
print(f"     a) HYBRID_L2_ENABLED: {hybrid_enabled}")
print(f"     b) Volume Surge:      {'PASS' if vol_ok else 'FAIL'} ({ratio:.2f}x)")
print(f"     c) SSM >= 2.0:        {'PASS' if ssm_ok else 'FAIL'} ({ssm_rows[0][0]:.2f if ssm_rows else 'N/A'})")

dir_match = False
if is_oob and ssm_dir:
    dir_match = ssm_dir == oob_dir
    print(f"     d) Direction Match:   {'PASS' if dir_match else 'FAIL'} (OOB={oob_dir}, SSM={ssm_dir})")
else:
    print(f"     d) Direction Match:   N/A")

print(f"     e) L4 Active:         {'PASS' if l4_ok else 'FAIL'}")

all_ok = is_oob and vol_ok and ssm_ok and dir_match and l4_ok and hybrid_enabled
print(f"\n  >>> L2 ENTRY POSSIBLE: {'YES <<<' if all_ok else 'NO <<<'}")

if not all_ok:
    print("\n  [BLOCKERS]")
    if not is_oob:
        print(f"    - Price is within grid range (no OOB)")
    if hybrid_enabled is False:
        print(f"    - HYBRID_L2_ENABLED = False")
    if not vol_ok:
        print(f"    - Volume {ratio:.2f}x < 2.0x threshold")
    if not ssm_ok and ssm_rows:
        print(f"    - SSM score {ssm_rows[0][0]:.2f} < 2.0 threshold")
    if is_oob and ssm_dir and not dir_match:
        print(f"    - SSM direction({ssm_dir}) != OOB direction({oob_dir})")
    if not l4_ok:
        print(f"    - L4 not active")

conn.close()
