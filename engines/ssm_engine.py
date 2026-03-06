"""SSM Engine — 고래/매크로 방향 판단 (Gemini + Google Search Grounding)

4시간마다 Gemini에 grounding 검색으로 고래 움직임/매크로 확인.
2번 질문(팩트체크)으로 환각 제거.
결과: BULLISH / BEARISH / NEUTRAL + 확신도 + 근거
"""
import json
import time
import re
from datetime import datetime, timezone
from db import get_connection
from config import GEMINI_API_KEY, GEMINI_MODEL, GEMINI_DAILY_LIMIT

# 새 Gemini SDK (google.genai)
try:
    from google import genai
    from google.genai import types
    _client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None
    _AVAILABLE = _client is not None
except ImportError:
    _AVAILABLE = False
    _client = None
    types = None


class SSMEngine:
    """고래/매크로 방향 판단 — Gemini + Google Search Grounding"""

    def __init__(self):
        self._last_result = {
            "direction": "NEUTRAL", "confidence": 0,
            "reason": "", "ts": 0,
        }

    def update(self, symbol: str = "SOLUSDT") -> dict:
        """4시간마다 호출. Gemini grounding으로 고래 움직임 확인.
        2번 질문으로 팩트체크."""
        if not _AVAILABLE:
            print("[SSM] Gemini API 사용 불가")
            return self._last_result

        try:
            # Google Search grounding 도구
            google_search_tool = types.Tool(
                google_search=types.GoogleSearch()
            )

            # 1차: 데이터 수집 (grounding 검색)
            prompt1 = (
                "You are a crypto market analyst. "
                "Search for the following data from the LAST 24 HOURS and summarize with source URLs:\n"
                "1. Large BTC/SOL whale transactions (exchange deposits/withdrawals, on-chain)\n"
                "2. Stablecoin minting/burning (USDT, USDC)\n"
                "3. Exchange BTC balance changes\n"
                "4. Major macro events (interest rates, regulations, ETF flows)\n"
                "5. Large fund movements (Grayscale, BlackRock, etc.)\n\n"
                "For each item, include the SOURCE URL. "
                "If no reliable data found, say 'no data'."
            )

            response1 = _client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt1,
                config=types.GenerateContentConfig(
                    tools=[google_search_tool],
                    temperature=0.2,
                    max_output_tokens=1000,
                ),
            )
            text1 = response1.text if response1.text else ""

            if not text1 or len(text1) < 50:
                print("[SSM] Gemini 응답 부족")
                return self._last_result

            # 2차: 팩트체크 + 방향 판단
            prompt2 = (
                "You are a crypto market analyst. "
                "Fact-check the following analysis. "
                "ONLY keep claims that have verifiable source URLs. "
                "Remove anything without a source.\n\n"
                "Then, based ONLY on verified facts, determine:\n"
                "1. Market direction: BULLISH, BEARISH, or NEUTRAL\n"
                "2. Confidence: 1 (very low) to 5 (very high)\n"
                "3. One-line key reason\n\n"
                "Respond in this EXACT JSON format:\n"
                '{"direction": "BULLISH", "confidence": 3, '
                '"reason": "44B USDT minted, whale accumulation at 67K"}\n\n'
                f"Analysis to fact-check:\n{text1}"
            )

            response2 = _client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt2,
                config=types.GenerateContentConfig(
                    tools=[google_search_tool],
                    temperature=0.1,
                    max_output_tokens=300,
                ),
            )
            text2 = response2.text if response2.text else ""

            # JSON 파싱
            result = self._parse_response(text2)
            result["ts"] = time.time()
            result["raw_analysis"] = text1[:500]  # 원문 일부 보존

            # 파싱 성공한 경우만 결과 갱신 (parse_failed로 좋은 결과 덮어쓰기 방지)
            if result["confidence"] > 0:
                self._last_result = result

            # DB 저장
            self._save_to_db(symbol, result)

            print(f"[SSM] {result['direction']} (confidence={result['confidence']}) "
                  f"— {result['reason'][:60]}")
            return result

        except Exception as e:
            print(f"[SSM] Gemini 호출 실패: {e}")
            return self._last_result

    def get_direction(self) -> dict:
        """최근 SSM 결과 반환 (4시간 이내만 유효)"""
        age = time.time() - self._last_result.get("ts", 0)
        if age > 14400:  # 4시간 초과
            return {"direction": "NEUTRAL", "confidence": 0, "reason": "stale"}
        return self._last_result

    def _parse_response(self, text: str) -> dict:
        """Gemini 응답에서 JSON 추출"""
        default = {"direction": "NEUTRAL", "confidence": 0, "reason": "parse_failed"}

        if not text:
            return default

        # ```json 블록 처리
        clean = text.strip()
        if clean.startswith("```"):
            parts = clean.split("```")
            if len(parts) >= 2:
                clean = parts[1]
                if clean.startswith("json"):
                    clean = clean[4:]
                clean = clean.strip()

        # JSON 추출 (텍스트 중간에 있을 수 있음)
        match = re.search(r'\{[^}]+\}', clean)
        if match:
            try:
                data = json.loads(match.group())
                direction = data.get("direction", "NEUTRAL").upper()
                if direction not in ("BULLISH", "BEARISH", "NEUTRAL"):
                    direction = "NEUTRAL"
                confidence = int(data.get("confidence", 0))
                confidence = max(0, min(5, confidence))
                reason = str(data.get("reason", ""))[:200]
                return {
                    "direction": direction,
                    "confidence": confidence,
                    "reason": reason,
                }
            except (json.JSONDecodeError, ValueError):
                pass

        return default

    def _save_to_db(self, symbol: str, result: dict):
        """ssm_scores 테이블에 저장"""
        conn = get_connection()
        try:
            detail = json.dumps({
                "direction": result["direction"],
                "confidence": result["confidence"],
                "reason": result["reason"],
                "raw": result.get("raw_analysis", "")[:300],
            }, ensure_ascii=False)

            # direction → total_score 매핑
            score_map = {"BULLISH": 1.0, "BEARISH": -1.0, "NEUTRAL": 0.0}
            total = score_map.get(result["direction"], 0.0)

            conn.execute(
                "INSERT INTO ssm_scores "
                "(symbol, trigger_active, momentum_score, sentiment_score, "
                "story_score, value_score, total_score, direction, "
                "score_detail, gemini_calls_used, calculated_at) "
                "VALUES (?, 0, 0, 0, ?, 0, ?, ?, ?, 2, ?)",
                (symbol,
                 float(result["confidence"]),
                 total,
                 result["direction"],
                 detail,
                 datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
        except Exception as e:
            print(f"[SSM] DB 저장 실패: {e}")
        finally:
            conn.close()


if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8")
    from db import init_db
    init_db()

    ssm = SSMEngine()
    print("=== SSM Engine 테스트 ===")
    result = ssm.update("SOLUSDT")
    print(f"\n결과: {json.dumps(result, indent=2, ensure_ascii=False, default=str)}")
