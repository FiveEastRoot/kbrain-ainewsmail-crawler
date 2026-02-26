import datetime
import pytz
from typing import Tuple, Optional
import feedparser
from dateutil import parser as dateutil_parser

KST = pytz.timezone('Asia/Seoul')

def get_collection_window(now: datetime.datetime = None) -> Tuple[datetime.datetime, datetime.datetime]:
    """
    Returns the time window: Yesterday 16:00 KST to Today 16:00 KST.
    If the current time is before 16:00 KST today, it returns the window for the *previous* cycle
    (Day BEFORE Yesterday 16:00 to Yesterday 16:00) to ensure we always have a full closed 24h window
    if running early, or exactly the target 24h window if running after 16:00.
    
    Assuming this runs daily around 16:10 KST.
    """
    if now is None:
        now = datetime.datetime.now(pytz.utc).astimezone(KST)
    else:
        now = now.astimezone(KST)

    # 16시 지났는지 확인
    # 만약 현재가 5월 2일 16시 10분이라면: start = 5월 1일 16:00, end = 5월 2일 16:00
    # 만약 현재가 5월 2일 10시 00분이라면: start = 4월 30일 16:00, end = 5월 1일 16:00 (어제 마감된 윈도우)
    
    if now.hour >= 16:
        end_time = now.replace(hour=16, minute=0, second=0, microsecond=0)
        start_time = end_time - datetime.timedelta(days=1)
    else:
        end_time = now.replace(hour=16, minute=0, second=0, microsecond=0) - datetime.timedelta(days=1)
        start_time = end_time - datetime.timedelta(days=1)
        
    return start_time, end_time

def parse_date_robust(date_string: str) -> Optional[datetime.datetime]:
    """
    Attempts to parse various date strings into a timezone-aware datetime object (UTC).
    """
    if not date_string:
        return None
        
    try:
        # dateutil parser handles ISO, RFC822, etc. robustly
        dt = dateutil_parser.parse(date_string)
        if dt.tzinfo is None:
            # Assume UTC if no timezone is provided, as per most feed standards
            dt = dt.replace(tzinfo=pytz.utc)
        return dt.astimezone(pytz.utc)
    except Exception:
        # fallback string manual checks if needed, but dateutil is usually enough
        return None

def is_within_window(pub_date: datetime.datetime, start_window: datetime.datetime, end_window: datetime.datetime) -> bool:
    """
    Checks if a timezone-aware datetime is within the given window.
    """
    if not pub_date:
        return False
    
    # Ensure all are comparable (UTC or KST)
    pub_date_kst = pub_date.astimezone(KST)
    return start_window <= pub_date_kst <= end_window
