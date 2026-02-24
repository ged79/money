"""Engine 6: 매크로 이벤트 가드 - Tier별 L2 진입 제한"""
import time
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from collectors.macro_events import load_calendar

# Tier별 사전 차단 시간 (초)
TIER_BLOCK_LEAD = {
    1: 4 * 3600,   # Tier 1: 4시간 전
    2: 2 * 3600,   # Tier 2: 2시간 전
    3: 0,           # Tier 3: 발표 시점
}

# 발표 후 관찰 쿨다운 (초)
POST_EVENT_COOLDOWN = 3600  # 1시간


def check_macro_block(symbol: str = "BTCUSDT") -> dict:
    """매크로 이벤트 기반 L2 진입 차단 여부 판정"""
    events = load_calendar()
    now = time.time()

    # 가장 가까운 차단 이벤트 찾기
    nearest_block = None

    for event in events:
        event_time = event.get("timestamp", 0)
        tier = event.get("tier", 3)
        lead_time = TIER_BLOCK_LEAD.get(tier, 0)
        name = event.get("name", "Unknown")

        time_until = event_time - now

        # 발표 전 차단: 0 < 남은시간 <= 차단기간
        if 0 < time_until <= lead_time:
            hours_left = time_until / 3600
            candidate = {
                "blocked": True,
                "reason": f"Tier {tier} | {name} in {hours_left:.1f}h",
                "event_name": name,
                "hours_until": round(hours_left, 1),
                "tier": tier,
                "post_event_cooldown": False,
            }
            # 가장 임박한 이벤트 선택
            if nearest_block is None or hours_left < nearest_block["hours_until"]:
                nearest_block = candidate

        # 발표 후 쿨다운: 발표 지나고 1시간 이내
        if -POST_EVENT_COOLDOWN <= time_until <= 0:
            hours_since = abs(time_until) / 3600
            candidate = {
                "blocked": True,
                "reason": f"Tier {tier} | {name} 발표 후 관찰 ({hours_since:.1f}h 경과)",
                "event_name": name,
                "hours_until": 0,
                "tier": tier,
                "post_event_cooldown": True,
            }
            if nearest_block is None:
                nearest_block = candidate

    if nearest_block:
        print(f"[Macro Guard] BLOCKED: {nearest_block['reason']}")
        return nearest_block

    result = {
        "blocked": False,
        "reason": None,
        "event_name": None,
        "hours_until": None,
        "tier": None,
        "post_event_cooldown": False,
    }
    print("[Macro Guard] 제한 없음")
    return result


if __name__ == "__main__":
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8")
    check_macro_block()
