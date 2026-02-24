"""가상 시계 — 백테스트용 시간 시뮬레이션"""
from datetime import date, datetime


class VirtualClock:
    """5분 단위로 전진하는 가상 시계"""

    def __init__(self, start_ts: float):
        self._current = start_ts

    def now(self) -> datetime:
        return datetime.fromtimestamp(self._current)

    def time(self) -> float:
        return self._current

    def today(self) -> date:
        return datetime.fromtimestamp(self._current).date()

    def advance(self, seconds: int):
        self._current += seconds

    @property
    def timestamp(self) -> float:
        return self._current
