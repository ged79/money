"""⑥ 매크로 이벤트 캘린더 로더 + 이벤트 임박 체크"""
import json
import time
from pathlib import Path

CALENDAR_PATH = Path(__file__).parent.parent / "macro_calendar.json"

# Tier별 사전 알림 시간 (초)
TIER_ALERT_LEAD = {
    1: 4 * 3600,   # Tier 1: 4시간 전
    2: 2 * 3600,   # Tier 2: 2시간 전
    3: 0,           # Tier 3: 발표 시점
}


def load_calendar() -> list[dict]:
    """매크로 캘린더 JSON 로드"""
    if not CALENDAR_PATH.exists():
        print(f"[매크로] 캘린더 파일 없음: {CALENDAR_PATH}")
        return []
    with open(CALENDAR_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def check_upcoming_events() -> list[dict]:
    """임박한 이벤트 체크 (알림 대상 반환)"""
    events = load_calendar()
    now = time.time()
    upcoming = []

    for event in events:
        event_time = event.get("timestamp", 0)
        tier = event.get("tier", 3)
        lead_time = TIER_ALERT_LEAD.get(tier, 0)

        # 이벤트까지 남은 시간
        time_until = event_time - now
        if 0 < time_until <= lead_time:
            hours_left = time_until / 3600
            upcoming.append({
                **event,
                "hours_left": round(hours_left, 1),
            })
            print(f"[매크로] ⚠️ Tier {tier} | {event['name']} | {hours_left:.1f}시간 후")

    if not upcoming:
        print(f"[매크로] 임박한 이벤트 없음")

    return upcoming


if __name__ == "__main__":
    check_upcoming_events()
