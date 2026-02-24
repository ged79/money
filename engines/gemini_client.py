"""Gemini Flash API 래퍼 - S(Story) 점수용 감성 분석"""
import json
import time
from datetime import date

from db import get_connection
from config import GEMINI_API_KEY, GEMINI_MODEL, GEMINI_DAILY_LIMIT

# Gemini SDK 로드 (없으면 스텁)
try:
    import google.generativeai as genai
    genai.configure(api_key=GEMINI_API_KEY)
    GEMINI_AVAILABLE = bool(GEMINI_API_KEY)
except ImportError:
    GEMINI_AVAILABLE = False


SYSTEM_PROMPT = """You are a crypto market sentiment analyst. Analyze the given market data and determine the overall sentiment direction.

Respond ONLY with a JSON object in this exact format:
{"sentiment": "bullish" or "bearish" or "neutral", "confidence": 0.0 to 1.0}

Rules:
- "bullish" means you expect prices to rise
- "bearish" means you expect prices to fall
- "neutral" means no clear direction
- confidence is how certain you are (0.0 = no confidence, 1.0 = very confident)
- Consider ALL data points, not just one indicator
- Be conservative: if signals are mixed, lean toward "neutral"
"""


def check_daily_budget() -> tuple[int, int]:
    """오늘 사용량 확인. (calls_used, daily_limit) 반환"""
    today = date.today().isoformat()
    conn = get_connection()
    row = conn.execute(
        "SELECT calls_used, daily_limit FROM gemini_usage WHERE call_date = ?",
        (today,),
    ).fetchone()
    conn.close()

    if row:
        return row[0], row[1]
    return 0, GEMINI_DAILY_LIMIT


def _increment_usage(count: int = 1):
    """일일 사용량 증가"""
    today = date.today().isoformat()
    conn = get_connection()
    conn.execute(
        "INSERT INTO gemini_usage (call_date, calls_used, daily_limit) "
        "VALUES (?, ?, ?) "
        "ON CONFLICT(call_date) DO UPDATE SET calls_used = calls_used + ?",
        (today, count, GEMINI_DAILY_LIMIT, count),
    )
    conn.commit()
    conn.close()


def build_market_prompt(symbol: str) -> str:
    """DB에서 최신 시장 데이터를 읽어 프롬프트 구성"""
    conn = get_connection()

    parts = [f"Market Data for {symbol}:\n"]

    # Fear & Greed
    fg = conn.execute(
        "SELECT value, classification FROM fear_greed ORDER BY collected_at DESC LIMIT 1"
    ).fetchone()
    if fg:
        parts.append(f"- Fear & Greed Index: {fg[0]} ({fg[1]})")

    # Funding rate
    fr = conn.execute(
        "SELECT funding_rate FROM funding_rates WHERE symbol = ? ORDER BY collected_at DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    if fr:
        parts.append(f"- Funding Rate: {fr[0]*100:.4f}%")

    # Long/Short ratio
    ls = conn.execute(
        "SELECT long_account, short_account FROM long_short_ratios "
        "WHERE symbol = ? ORDER BY collected_at DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    if ls:
        parts.append(f"- Long/Short Ratio: Long {ls[0]*100:.1f}% / Short {ls[1]*100:.1f}%")

    # OI
    oi = conn.execute(
        "SELECT open_interest FROM oi_snapshots WHERE symbol = ? ORDER BY collected_at DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    if oi:
        parts.append(f"- Open Interest: {oi[0]:,.0f} BTC")

    # Recent price action
    klines = conn.execute(
        "SELECT close, volume FROM klines WHERE symbol = ? AND interval = '1d' "
        "ORDER BY open_time DESC LIMIT 3",
        (symbol,),
    ).fetchall()
    if klines:
        prices = [k[0] for k in klines]
        parts.append(f"- Recent closes: {', '.join(f'${p:,.0f}' for p in prices)}")
        if len(prices) >= 2:
            change = ((prices[0] - prices[1]) / prices[1]) * 100
            parts.append(f"- 24h price change: {change:+.2f}%")

    # 1h liquidation summary
    now_ms = int(time.time() * 1000)
    liq = conn.execute(
        "SELECT side, COUNT(*), SUM(price * qty) FROM liquidations "
        "WHERE symbol = ? AND trade_time > ? GROUP BY side",
        (symbol, now_ms - 3600_000),
    ).fetchall()
    if liq:
        for row in liq:
            side_name = "Short liquidations" if row[0] == "BUY" else "Long liquidations"
            parts.append(f"- {side_name} (1h): {row[1]} events, ${row[2]:,.0f}")

    conn.close()

    parts.append("\nBased on this data, what is the overall market sentiment?")
    return "\n".join(parts)


def analyze_sentiment(market_summary: str) -> dict:
    """Gemini Flash 1회 호출로 감성 분석. 실패시 neutral 반환"""
    if not GEMINI_AVAILABLE:
        return {"sentiment": "neutral", "confidence": 0.0, "error": "gemini_unavailable"}

    try:
        model = genai.GenerativeModel(
            GEMINI_MODEL,
            system_instruction=SYSTEM_PROMPT,
        )
        response = model.generate_content(
            market_summary,
            generation_config=genai.types.GenerationConfig(
                temperature=0.3,
                max_output_tokens=100,
            ),
            request_options={"timeout": 15},
        )

        text = response.text.strip()
        # JSON 파싱 (```json 블록 처리)
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()

        data = json.loads(text)
        sentiment = data.get("sentiment", "neutral")
        confidence = float(data.get("confidence", 0.0))

        if sentiment not in ("bullish", "bearish", "neutral"):
            sentiment = "neutral"
        confidence = max(0.0, min(1.0, confidence))

        return {"sentiment": sentiment, "confidence": confidence}

    except Exception as e:
        print(f"[Gemini] API 호출 실패: {e}")
        return {"sentiment": "neutral", "confidence": 0.0, "error": str(e)}


def analyze_sentiment_majority(symbol: str, calls: int = 3) -> dict:
    """3회 호출 다수결로 감성 판정"""
    used, limit = check_daily_budget()
    if used + calls > limit:
        print(f"[Gemini] 일일 한도 초과 ({used}/{limit}) - 스킵")
        return {"sentiment": "neutral", "confidence": 0.0, "calls_used": 0, "budget_exceeded": True}

    prompt = build_market_prompt(symbol)
    results = []
    actual_calls = 0

    for i in range(calls):
        result = analyze_sentiment(prompt)
        actual_calls += 1
        results.append(result)
        if "error" in result and "gemini_unavailable" in str(result.get("error", "")):
            break  # Gemini 사용 불가 시 조기 중단

    _increment_usage(actual_calls)

    if not results:
        return {"sentiment": "neutral", "confidence": 0.0, "calls_used": 0}

    # 다수결
    sentiments = [r["sentiment"] for r in results]
    from collections import Counter
    counts = Counter(sentiments)
    majority_sentiment, majority_count = counts.most_common(1)[0]

    # 일치도 기반 점수
    agreement = majority_count / len(results)
    avg_confidence = sum(r.get("confidence", 0) for r in results) / len(results)

    return {
        "sentiment": majority_sentiment,
        "confidence": round(avg_confidence, 2),
        "agreement": round(agreement, 2),
        "calls_used": actual_calls,
        "votes": dict(counts),
    }


if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8")
    from db import init_db
    init_db()

    print("=== Gemini Budget ===")
    used, limit = check_daily_budget()
    print(f"Today: {used}/{limit} calls")

    print("\n=== Market Prompt ===")
    prompt = build_market_prompt("BTCUSDT")
    print(prompt)

    print("\n=== Sentiment Analysis (1 call) ===")
    result = analyze_sentiment(prompt)
    print(f"Result: {result}")
