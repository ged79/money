"""L2 전환 조건 분석 — 2026-02-28 SOLUSDT 가격 데이터 기반"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from db import get_connection
from datetime import datetime

SYMBOL = "SOLUSDT"
TARGET_DATE = "2026-02-28"

conn = get_connection()

# ============================================================
# 1. 5분봉 가격 + 거래량 (2026-02-28)
# ============================================================
print("=" * 70)
print(f"  1. {SYMBOL} 5분봉 가격/거래량 — {TARGET_DATE}")
print("=" * 70)

# open_time is millisecond epoch
# 2026-02-28 00:00:00 UTC ~ 2026-02-28 23:59:59 UTC
day_start_ms = int(datetime(2026, 2, 28, 0, 0, 0).timestamp() * 1000)
day_end_ms   = int(datetime(2026, 2, 28, 23, 59, 59).timestamp() * 1000)

klines = conn.execute(
    "SELECT open_time, open, high, low, close, volume "
    "FROM klines WHERE symbol = ? AND interval = '5m' "
    "AND open_time >= ? AND open_time <= ? "
    "ORDER BY open_time ASC",
    (SYMBOL, day_start_ms, day_end_ms),
).fetchall()

if not klines:
    # collected_at 기반 폴백
    klines = conn.execute(
        "SELECT open_time, open, high, low, close, volume "
        "FROM klines WHERE symbol = ? AND interval = '5m' "
        "AND collected_at >= ? AND collected_at < ? "
        "ORDER BY open_time ASC",
        (SYMBOL, TARGET_DATE + " 00:00:00", "2026-03-01 00:00:00"),
    ).fetchall()

if klines:
    closes = [k[4] for k in klines]
    volumes = [k[5] for k in klines]
    print(f"  캔들 수: {len(klines)}")
    print(f"  시가(첫 봉): ${klines[0][1]:,.2f}")
    print(f"  종가(마지막): ${klines[-1][4]:,.2f}")
    print(f"  일중 최고: ${max(k[2] for k in klines):,.2f}")
    print(f"  일중 최저: ${min(k[3] for k in klines):,.2f}")
    print(f"  평균 거래량/봉: {sum(volumes)/len(volumes):,.0f}")
    print(f"  총 거래량: {sum(volumes):,.0f}")
    day_change = (closes[-1] - klines[0][1]) / klines[0][1] * 100
    print(f"  일간 변동: {day_change:+.2f}%")

    # 시간대별 가격 샘플링 (1시간 = 12봉 간격)
    print(f"\n  시간대별 가격:")
    for i in range(0, len(klines), 12):
        k = klines[i]
        ts = datetime.fromtimestamp(k[0] / 1000).strftime("%H:%M")
        print(f"    {ts} | O=${k[1]:,.2f} H=${k[2]:,.2f} L=${k[3]:,.2f} C=${k[4]:,.2f} | Vol={k[5]:,.0f}")
    # 마지막 봉도 표시
    if len(klines) % 12 != 1:
        k = klines[-1]
        ts = datetime.fromtimestamp(k[0] / 1000).strftime("%H:%M")
        print(f"    {ts} | O=${k[1]:,.2f} H=${k[2]:,.2f} L=${k[3]:,.2f} C=${k[4]:,.2f} | Vol={k[5]:,.0f}  (마지막)")
else:
    closes = []
    volumes = []
    print("  해당 날짜의 5분봉 데이터 없음")

    # 범위 확인: 가장 가까운 데이터 표시
    nearest = conn.execute(
        "SELECT open_time, collected_at FROM klines "
        "WHERE symbol = ? AND interval = '5m' "
        "ORDER BY ABS(open_time - ?) LIMIT 3",
        (SYMBOL, day_start_ms),
    ).fetchall()
    if nearest:
        print(f"  가장 가까운 5분봉 데이터:")
        for r in nearest:
            ts = datetime.fromtimestamp(r[0] / 1000).strftime("%Y-%m-%d %H:%M")
            print(f"    open_time={ts} | collected_at={r[1]}")

    # 전체 범위 확인
    range_info = conn.execute(
        "SELECT MIN(open_time), MAX(open_time), COUNT(*) FROM klines "
        "WHERE symbol = ? AND interval = '5m'",
        (SYMBOL,),
    ).fetchone()
    if range_info and range_info[0]:
        min_ts = datetime.fromtimestamp(range_info[0] / 1000).strftime("%Y-%m-%d %H:%M")
        max_ts = datetime.fromtimestamp(range_info[1] / 1000).strftime("%Y-%m-%d %H:%M")
        print(f"  DB 내 5분봉 범위: {min_ts} ~ {max_ts} ({range_info[2]}건)")

# ============================================================
# 2. 그리드 설정 (활성 그리드 범위)
# ============================================================
print(f"\n{'=' * 70}")
print(f"  2. {SYMBOL} 그리드 설정 — {TARGET_DATE} 전후")
print("=" * 70)

# 해당 날짜 이전 최신 그리드 + 해당 날짜 중 생성된 그리드
grids = conn.execute(
    "SELECT id, lower_bound, upper_bound, grid_count, grid_spacing, "
    "grid_spacing_pct, spoofing_filtered, calculated_at "
    "FROM grid_configs WHERE symbol = ? "
    "AND calculated_at <= '2026-03-01 00:00:00' "
    "ORDER BY id DESC LIMIT 10",
    (SYMBOL,),
).fetchall()

active_grid = None
if grids:
    for g in grids:
        tag = ""
        if g[7] and g[7].startswith(TARGET_DATE):
            tag = " <-- 해당일"
        elif not active_grid:
            # 해당일 이전에 계산된 것 중 가장 최근 = 활성 그리드
            if g[7] and g[7] < TARGET_DATE + " 00:00:00":
                tag = " <-- 직전 활성"
        if not active_grid:
            active_grid = {
                "id": g[0], "lower_bound": g[1], "upper_bound": g[2],
                "grid_count": g[3], "grid_spacing": g[4],
            }
        print(f"  [{g[7]}] id={g[0]} | ${g[1]:,.2f} ~ ${g[2]:,.2f} | "
              f"{g[3]} grids @ ${g[4]:,.2f} ({g[5]:.2f}%) | spoof={g[6]}{tag}")
else:
    print("  그리드 설정 없음")

# ============================================================
# 3. SSM 점수 (해당일)
# ============================================================
print(f"\n{'=' * 70}")
print(f"  3. {SYMBOL} SSM 점수 — {TARGET_DATE}")
print("=" * 70)

ssm_rows = conn.execute(
    "SELECT trigger_active, momentum_score, sentiment_score, story_score, "
    "value_score, total_score, direction, score_detail, calculated_at "
    "FROM ssm_scores WHERE symbol = ? "
    "AND calculated_at >= ? AND calculated_at < ? "
    "ORDER BY calculated_at ASC",
    (SYMBOL, TARGET_DATE + " 00:00:00", "2026-03-01 00:00:00"),
).fetchall()

if ssm_rows:
    print(f"  총 {len(ssm_rows)}건\n")
    for s in ssm_rows:
        t_str = "ON" if s[0] else "OFF"
        print(f"  [{s[8]}] T={t_str} | M={s[1]:.1f} S={s[2]:.1f} St={s[3]:.1f} V={s[4]:.1f} "
              f"| Total={s[5]:.2f} -> {s[6] or '-'}")
    # 요약
    scores = [s[5] for s in ssm_rows]
    directions = [s[6] for s in ssm_rows]
    print(f"\n  SSM 요약:")
    print(f"    평균 점수: {sum(scores)/len(scores):.2f}")
    print(f"    최고 점수: {max(scores):.2f}")
    print(f"    최저 점수: {min(scores):.2f}")
    dir_counts = {}
    for d in directions:
        dir_counts[d] = dir_counts.get(d, 0) + 1
    print(f"    방향 분포: {dir_counts}")
    # L2 진입 가능 여부
    l2_eligible = [s for s in ssm_rows if s[5] >= 2.0]
    print(f"    L2 진입 가능 (score>=2.0): {len(l2_eligible)}/{len(ssm_rows)}건")
else:
    print("  해당 날짜의 SSM 데이터 없음")

    # 가장 가까운 SSM 데이터
    nearest_ssm = conn.execute(
        "SELECT total_score, direction, calculated_at FROM ssm_scores "
        "WHERE symbol = ? ORDER BY ABS(julianday(calculated_at) - julianday(?)) LIMIT 3",
        (SYMBOL, TARGET_DATE + " 12:00:00"),
    ).fetchall()
    if nearest_ssm:
        print(f"  가장 가까운 SSM 데이터:")
        for r in nearest_ssm:
            print(f"    score={r[0]:.2f} dir={r[1]} at={r[2]}")

# ============================================================
# 4. OOB (Out of Bounds) 타임라인
# ============================================================
print(f"\n{'=' * 70}")
print(f"  4. {SYMBOL} OOB 타임라인 — {TARGET_DATE}")
print("=" * 70)

if klines and active_grid:
    lower = active_grid["lower_bound"]
    upper = active_grid["upper_bound"]
    print(f"  활성 그리드 범위: ${lower:,.2f} ~ ${upper:,.2f}")
    print(f"  활성 그리드 ID: {active_grid['id']}\n")

    oob_events = []  # [(timestamp, close, direction, duration_so_far)]
    in_oob = False
    oob_start = None
    oob_direction = None
    consecutive_oob = 0

    for k in klines:
        ts = datetime.fromtimestamp(k[0] / 1000).strftime("%H:%M")
        close = k[4]
        vol = k[5]

        if close > upper:
            direction = "ABOVE"
            if not in_oob or oob_direction != "ABOVE":
                oob_start = k[0]
                oob_direction = "ABOVE"
                consecutive_oob = 0
            in_oob = True
            consecutive_oob += 1
            oob_events.append((ts, close, "ABOVE", consecutive_oob, vol))
        elif close < lower:
            direction = "BELOW"
            if not in_oob or oob_direction != "BELOW":
                oob_start = k[0]
                oob_direction = "BELOW"
                consecutive_oob = 0
            in_oob = True
            consecutive_oob += 1
            oob_events.append((ts, close, "BELOW", consecutive_oob, vol))
        else:
            if in_oob:
                oob_events.append((ts, close, "RETURN", 0, vol))
            in_oob = False
            oob_direction = None
            consecutive_oob = 0

    if oob_events:
        print(f"  OOB 이벤트 ({len(oob_events)}건):")
        for ts, price, direction, consec, vol in oob_events:
            if direction == "RETURN":
                print(f"    {ts} | ${price:,.2f} | RETURN to range | Vol={vol:,.0f}")
            else:
                dist = price - upper if direction == "ABOVE" else lower - price
                dist_pct = dist / price * 100
                confirm = "YES" if consec >= 3 else f"({consec}/3)"
                print(f"    {ts} | ${price:,.2f} | {direction} ${dist:,.2f} ({dist_pct:.2f}%) | "
                      f"연속 {consec}봉 | confirm={confirm} | Vol={vol:,.0f}")

        # L2 전환 조건 분석
        print(f"\n  L2 전환 조건 분석:")
        # 3봉 연속 이탈 구간 찾기
        streaks = []
        current_streak_start = None
        current_streak_dir = None
        current_streak_count = 0
        for ts, price, direction, consec, vol in oob_events:
            if direction in ("ABOVE", "BELOW") and consec >= 3:
                if current_streak_dir != direction:
                    if current_streak_count >= 3:
                        streaks.append((current_streak_start, current_streak_dir, current_streak_count))
                    current_streak_start = ts
                    current_streak_dir = direction
                    current_streak_count = consec
                else:
                    current_streak_count = consec
        if current_streak_count >= 3:
            streaks.append((current_streak_start, current_streak_dir, current_streak_count))

        if streaks:
            for start_ts, s_dir, s_count in streaks:
                l2_dir = "LONG" if s_dir == "ABOVE" else "SHORT"
                print(f"    breakout 확인 @ {start_ts} | {s_dir} | {s_count}봉 연속 | L2 방향: {l2_dir}")
        else:
            print(f"    3봉 연속 이탈 구간 없음 (breakout 미확인)")
    else:
        print("  OOB 이벤트 없음 — 종일 그리드 범위 내 유지")
else:
    if not klines:
        print("  5분봉 데이터 없어 OOB 분석 불가")
    if not active_grid:
        print("  활성 그리드 없어 OOB 분석 불가")

# ============================================================
# 5. 거래량 vs 평균 거래량 비율 (OOB 시점)
# ============================================================
print(f"\n{'=' * 70}")
print(f"  5. 거래량 분석 — OOB 시점 vs 평균")
print("=" * 70)

if klines:
    all_volumes = [k[5] for k in klines]
    avg_vol = sum(all_volumes) / len(all_volumes) if all_volumes else 0

    # 24시간 이전 데이터로 평균 계산 (당일 이전)
    prev_day_vols = conn.execute(
        "SELECT AVG(volume) FROM klines "
        "WHERE symbol = ? AND interval = '5m' "
        "AND open_time < ? AND open_time >= ?",
        (SYMBOL, day_start_ms, day_start_ms - 86400000),
    ).fetchone()
    prev_avg = prev_day_vols[0] if prev_day_vols and prev_day_vols[0] else avg_vol

    print(f"  당일 평균 거래량/봉: {avg_vol:,.0f}")
    print(f"  전일 평균 거래량/봉: {prev_avg:,.0f}")

    # 고거래량 봉 (평균의 2배 이상)
    high_vol_candles = [(k, k[5] / prev_avg if prev_avg > 0 else 0)
                        for k in klines if prev_avg > 0 and k[5] >= prev_avg * 2]
    if high_vol_candles:
        print(f"\n  고거래량 봉 (전일 평균의 2x 이상): {len(high_vol_candles)}건")
        for k, ratio in high_vol_candles[:20]:
            ts = datetime.fromtimestamp(k[0] / 1000).strftime("%H:%M")
            oob_mark = ""
            if active_grid:
                if k[4] > active_grid["upper_bound"]:
                    oob_mark = " [OOB ABOVE]"
                elif k[4] < active_grid["lower_bound"]:
                    oob_mark = " [OOB BELOW]"
            print(f"    {ts} | C=${k[4]:,.2f} | Vol={k[5]:,.0f} ({ratio:.1f}x){oob_mark}")
    else:
        print(f"  고거래량 봉 (2x 이상) 없음")

    # OOB 이벤트 시점의 거래량 분석
    if active_grid and oob_events:
        oob_vols = [v for _, _, d, _, v in oob_events if d in ("ABOVE", "BELOW")]
        if oob_vols:
            avg_oob_vol = sum(oob_vols) / len(oob_vols)
            print(f"\n  OOB 시점 평균 거래량: {avg_oob_vol:,.0f} ({avg_oob_vol/prev_avg:.1f}x vs 전일)")
            vol_breakout = avg_oob_vol >= prev_avg * 2.0
            print(f"  거래량 breakout (>= 2.0x): {'YES' if vol_breakout else 'NO'}")
else:
    print("  데이터 없음")

# ============================================================
# 6. signal_log 이력 (해당일)
# ============================================================
print(f"\n{'=' * 70}")
print(f"  6. 시그널 로그 — {TARGET_DATE}")
print("=" * 70)

signals = conn.execute(
    "SELECT created_at, signal_type, direction, ssm_score, details "
    "FROM signal_log WHERE symbol = ? "
    "AND created_at >= ? AND created_at < ? "
    "ORDER BY created_at ASC",
    (SYMBOL, TARGET_DATE + " 00:00:00", "2026-03-01 00:00:00"),
).fetchall()

if signals:
    print(f"  총 {len(signals)}건\n")
    for s in signals:
        score_str = f" score={s[3]:.2f}" if s[3] is not None else ""
        detail_str = ""
        if s[4]:
            detail_str = f" | {s[4][:80]}"
        print(f"  [{s[0]}] {s[1]} | {s[2] or '-'}{score_str}{detail_str}")
else:
    print("  해당 날짜의 시그널 없음")
    # 가장 가까운 시그널
    nearest_sig = conn.execute(
        "SELECT created_at, signal_type, direction, ssm_score "
        "FROM signal_log WHERE symbol = ? "
        "ORDER BY ABS(julianday(created_at) - julianday(?)) LIMIT 5",
        (SYMBOL, TARGET_DATE + " 12:00:00"),
    ).fetchall()
    if nearest_sig:
        print(f"  가장 가까운 시그널:")
        for r in nearest_sig:
            score_str = f" score={r[3]:.2f}" if r[3] is not None else ""
            print(f"    [{r[0]}] {r[1]} | {r[2] or '-'}{score_str}")

# ============================================================
# 7. strategy_state (현재 상태)
# ============================================================
print(f"\n{'=' * 70}")
print(f"  7. {SYMBOL} 전략 상태 (strategy_state)")
print("=" * 70)

state = conn.execute(
    "SELECT state, l1_active, l2_active, l2_direction, l2_step, l2_entry_pct, "
    "l2_avg_entry_price, l2_step1_time, l2_score_at_entry, "
    "l2_direction_changes_today, l4_active, l4_grid_config_id, "
    "macro_blocked, macro_block_reason, l2_trailing_stop_price, updated_at "
    "FROM strategy_state WHERE symbol = ?",
    (SYMBOL,),
).fetchone()

if state:
    print(f"  State: {state[0]}")
    print(f"  L1: {'ON' if state[1] else 'OFF'}")
    print(f"  L2: {'ON' if state[2] else 'OFF'} | dir={state[3]} | step={state[4]} | "
          f"entry_pct={state[5]}% | avg_price={state[6]}")
    print(f"  L2 step1_time: {state[7]}")
    print(f"  L2 score_at_entry: {state[8]}")
    print(f"  L2 direction_changes_today: {state[9]}")
    print(f"  L2 trailing_stop: {state[14]}")
    print(f"  L4: {'ON' if state[10] else 'OFF'} | grid_id={state[11]}")
    print(f"  Macro: {'BLOCKED' if state[12] else 'OK'} | reason={state[13]}")
    print(f"  updated_at: {state[15]}")
else:
    print("  strategy_state 없음")

# ============================================================
# 8. grid_order_log (해당일 주문 이력)
# ============================================================
print(f"\n{'=' * 70}")
print(f"  8. {SYMBOL} 그리드 주문 로그 — {TARGET_DATE}")
print("=" * 70)

grid_orders = conn.execute(
    "SELECT id, side, direction, grid_price, quantity, limit_price, "
    "fill_price, status, pnl_usd, created_at "
    "FROM grid_order_log WHERE symbol = ? "
    "AND created_at >= ? AND created_at < ? "
    "ORDER BY created_at ASC",
    (SYMBOL, TARGET_DATE + " 00:00:00", "2026-03-01 00:00:00"),
).fetchall()

if grid_orders:
    print(f"  총 {len(grid_orders)}건\n")
    total_pnl = 0
    for g in grid_orders:
        pnl = g[8] if g[8] else 0
        total_pnl += pnl
        pnl_str = f" PnL=${pnl:+.4f}" if pnl else ""
        print(f"  [{g[9]}] {g[1]} {g[2] or '-'} | grid=${g[3]:,.2f} limit=${g[5]:,.2f} "
              f"fill={g[6] or '-'} | qty={g[4]} | {g[7]}{pnl_str}")
    print(f"\n  총 PnL: ${total_pnl:+.4f}")
else:
    print("  해당 날짜의 주문 없음")

# ============================================================
# 9. L2 전환 조건 종합 판정
# ============================================================
print(f"\n{'=' * 70}")
print(f"  9. L2 전환 조건 종합 판정 — {TARGET_DATE}")
print("=" * 70)

print(f"\n  [조건 체크리스트]")

# 조건 1: 그리드 이탈 (breakout)
if klines and active_grid:
    lower = active_grid["lower_bound"]
    upper = active_grid["upper_bound"]
    oob_candles = [k for k in klines if k[4] > upper or k[4] < lower]
    had_breakout = len(oob_candles) > 0
    print(f"  1) 그리드 이탈: {'YES' if had_breakout else 'NO'} "
          f"({len(oob_candles)}/{len(klines)} 봉 OOB)")
else:
    had_breakout = False
    print(f"  1) 그리드 이탈: 판정 불가 (데이터 부족)")

# 조건 2: 3봉 연속 이탈 (breakout 확인)
if klines and active_grid:
    confirmed = False
    consec = 0
    consec_dir = None
    for k in klines:
        close = k[4]
        if close > upper:
            if consec_dir == "LONG":
                consec += 1
            else:
                consec = 1
                consec_dir = "LONG"
        elif close < lower:
            if consec_dir == "SHORT":
                consec += 1
            else:
                consec = 1
                consec_dir = "SHORT"
        else:
            consec = 0
            consec_dir = None
        if consec >= 3:
            confirmed = True
            break
    print(f"  2) 3봉 연속 확인: {'YES' if confirmed else 'NO'} "
          f"(L2_BREAKOUT_CONFIRM_CANDLES=3)")
else:
    confirmed = False
    print(f"  2) 3봉 연속 확인: 판정 불가")

# 조건 3: SSM >= 2.0
if ssm_rows:
    latest_ssm = ssm_rows[-1]
    ssm_ok = latest_ssm[5] >= 2.0
    print(f"  3) SSM >= 2.0: {'YES' if ssm_ok else 'NO'} "
          f"(최종 score={latest_ssm[5]:.2f}, dir={latest_ssm[6]})")
else:
    ssm_ok = False
    print(f"  3) SSM >= 2.0: 판정 불가 (데이터 없음)")

# 조건 4: SSM 방향 일치
if ssm_rows and confirmed and consec_dir:
    ssm_dir = latest_ssm[6]
    if consec_dir == "LONG":
        dir_match = ssm_dir in ("BULLISH", None)
    else:
        dir_match = ssm_dir in ("BEARISH", None)
    print(f"  4) SSM 방향 일치: {'YES' if dir_match else 'NO'} "
          f"(breakout={consec_dir}, SSM={ssm_dir})")
else:
    dir_match = False
    print(f"  4) SSM 방향 일치: 판정 불가")

# 조건 5: 방향 전환 한도 (1회/일)
print(f"  5) 방향 전환 한도: L2_MAX_DIRECTION_CHANGES=1/일")

# 조건 6: 매크로 차단
if state:
    macro_ok = not state[12]
    print(f"  6) 매크로 차단: {'BLOCKED' if not macro_ok else 'OK'}")
else:
    macro_ok = True
    print(f"  6) 매크로 차단: 판정 불가 (상태 없음)")

# 조건 7: 거래량 동반 (하이브리드 모드)
if klines and active_grid and oob_events:
    oob_vol_list = [v for _, _, d, _, v in oob_events if d in ("ABOVE", "BELOW")]
    avg_oob = sum(oob_vol_list) / len(oob_vol_list) if oob_vol_list else 0
    vol_confirm = avg_oob >= prev_avg * 2.0 if prev_avg > 0 else False
    print(f"  7) 거래량 동반 (hybrid OOB): {'YES' if vol_confirm else 'NO'} "
          f"(OOB avg={avg_oob:,.0f}, threshold={prev_avg*2:,.0f})")
else:
    vol_confirm = False
    print(f"  7) 거래량 동반: 판정 불가")

# 종합 판정
all_conditions = had_breakout and confirmed and ssm_ok and dir_match and macro_ok
print(f"\n  === 종합 판정 ===")
print(f"  전략 매니저 L2 전환: {'조건 충족' if all_conditions else '조건 미충족'}")
if had_breakout and confirmed and vol_confirm:
    print(f"  하이브리드 L2 전환 (거래량 OOB): 가능성 있음 (SSM 방향 추가 확인 필요)")

conn.close()
print(f"\n{'=' * 70}")
print(f"  분석 완료")
print(f"{'=' * 70}")
