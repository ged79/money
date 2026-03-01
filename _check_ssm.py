# -*- coding: utf-8 -*-
"""SSM Scoring status check"""
import sys, sqlite3, time, datetime
sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('data/trades.db')

print("=" * 60)
print("  SSM Scoring Status Check")
print("=" * 60)

# 1. Latest SSM scores
print("\n[1] Latest SSM Scores (20)")
rows = conn.execute(
    "SELECT id, total_score, direction, momentum_score, sentiment_score, "
    "story_score, value_score, trigger_active, score_detail, gemini_calls_used, calculated_at "
    "FROM ssm_scores WHERE symbol='SOLUSDT' ORDER BY id DESC LIMIT 20"
).fetchall()
if rows:
    print(f"    Latest ID: {rows[0][0]}, Time: {rows[0][10]}")
    print(f"    Oldest ID: {rows[-1][0]}, Time: {rows[-1][10]}")
    now = datetime.datetime.now()
    latest_time_str = rows[0][10]
    try:
        latest_time = datetime.datetime.strptime(latest_time_str, '%Y-%m-%d %H:%M:%S')
        gap = now - latest_time
        print(f"    Time since last: {gap}")
    except:
        print(f"    Latest time raw: {latest_time_str}")

    print(f"\n    {'ID':>6} | {'Total':>5} | {'Dir':>8} | {'Mom':>4} | {'Sent':>4} | {'Story':>5} | {'Val':>4} | {'Trig':>4} | {'Gemini':>6} | Time")
    print(f"    {'-'*6}-+-{'-'*5}-+-{'-'*8}-+-{'-'*4}-+-{'-'*4}-+-{'-'*5}-+-{'-'*4}-+-{'-'*4}-+-{'-'*6}-+-{'-'*19}")
    for r in rows:
        print(f"    {r[0]:>6} | {r[1]:>5.2f} | {r[2]:>8} | {r[3]:>4.1f} | {r[4]:>4.1f} | {r[5]:>5.1f} | {r[6]:>4.1f} | {r[7]:>4} | {r[9]:>6} | {r[10]}")
else:
    print("    No SSM data!")

# 2. Score distribution
print("\n[2] Score Distribution (last 100)")
dist = conn.execute(
    "SELECT total_score, COUNT(*) FROM "
    "(SELECT total_score FROM ssm_scores WHERE symbol='SOLUSDT' ORDER BY id DESC LIMIT 100) "
    "GROUP BY total_score ORDER BY total_score DESC"
).fetchall()
for d in dist:
    print(f"    Score {d[0]:.2f}: {d[1]} times")

# 3. Direction distribution
print("\n[3] Direction Distribution (last 100)")
dirs = conn.execute(
    "SELECT direction, COUNT(*) FROM "
    "(SELECT direction FROM ssm_scores WHERE symbol='SOLUSDT' ORDER BY id DESC LIMIT 100) "
    "GROUP BY direction ORDER BY COUNT(*) DESC"
).fetchall()
for d in dirs:
    print(f"    {d[0]}: {d[1]} times")

# 4. Check trigger_active
print("\n[4] Trigger Active Status (last 20)")
trigs = conn.execute(
    "SELECT trigger_active, COUNT(*) FROM "
    "(SELECT trigger_active FROM ssm_scores WHERE symbol='SOLUSDT' ORDER BY id DESC LIMIT 20) "
    "GROUP BY trigger_active"
).fetchall()
for t in trigs:
    print(f"    trigger_active={t[0]}: {t[1]} times")

# 5. When was last score >= 2.0?
print("\n[5] Last Score >= 2.0")
high = conn.execute(
    "SELECT id, total_score, direction, calculated_at FROM ssm_scores "
    "WHERE symbol='SOLUSDT' AND total_score >= 2.0 ORDER BY id DESC LIMIT 5"
).fetchall()
if high:
    for h in high:
        print(f"    ID {h[0]}: {h[1]:.2f} {h[2]} @ {h[3]}")
else:
    print("    NEVER reached 2.0!")

# 6. Gemini API usage
print("\n[6] Gemini API Calls")
gem = conn.execute(
    "SELECT gemini_calls_used, COUNT(*) FROM "
    "(SELECT gemini_calls_used FROM ssm_scores WHERE symbol='SOLUSDT' ORDER BY id DESC LIMIT 50) "
    "GROUP BY gemini_calls_used ORDER BY gemini_calls_used"
).fetchall()
for g in gem:
    print(f"    Calls={g[0]}: {g[1]} times")

# 7. Score detail of latest
print("\n[7] Latest Score Detail")
if rows and rows[0][8]:
    detail = rows[0][8]
    if len(detail) > 500:
        print(f"    {detail[:500]}...")
    else:
        print(f"    {detail}")
else:
    print("    No detail")

# 8. SSM scorer source check - scheduling
print("\n[8] Main scheduler check")
print("    (Check main.py and engines/ssm_scorer.py for schedule)")

# 9. Threshold signals (SSM input)
print("\n[9] Threshold Signals (latest 5)")
th = conn.execute(
    "SELECT threshold_value, trigger_active, direction, calculated_at "
    "FROM threshold_signals WHERE symbol='SOLUSDT' ORDER BY id DESC LIMIT 5"
).fetchall()
if th:
    for t in th:
        print(f"    Threshold: {t[0]:.4f}, Active: {t[1]}, Dir: {t[2]}, Time: {t[3]}")
else:
    print("    No threshold data!")

conn.close()
