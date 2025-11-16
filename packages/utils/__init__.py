from datetime import datetime


def get_window_milliseconds(window_days) -> int:
    return window_days * 24 * 3600 * 1000

def calculate_time_window(window_days: int, processing_date: str) -> tuple[int, int]:
    dt = datetime.fromisoformat(f"{processing_date}T00:00:00+00:00")
    end_timestamp_ms = int(dt.timestamp() * 1000)
    start_timestamp_ms = end_timestamp_ms - get_window_milliseconds(window_days)

    return start_timestamp_ms, end_timestamp_ms