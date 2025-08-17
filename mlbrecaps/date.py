from typing import Optional
from pydantic import field_validator
from datetime import date

def parse_date(v: str | date) -> date:
    if isinstance(v, str):
        return date.fromisoformat(v)
    return v

class Date():
    def __init__(self, start_date: date | str, end_date: Optional[date | str] = None):
        self._start_date = parse_date(start_date)
        self._end_date = parse_date(end_date) if end_date else self._start_date

        if self._start_date > self._end_date:
            raise ValueError("start_date must be less than or equal to end_date")

    @property
    def start_date(self) -> date:    
        return self._start_date

    @property
    def end_date(self) -> date:
        return self._end_date    
