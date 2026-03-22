import datetime
import pytz
from typing import Tuple, Optional
import feedparser
from dateutil import parser as dateutil_parser

KST = pytz.timezone('Asia/Seoul')

def get_collection_window(now: datetime.datetime = None) -> Tuple[datetime.datetime, datetime.datetime]:
    """
    Returns the time window: (now - 24h) ~ now (KST).
    Admin 페이지에서 버튼을 누른 시각을 기준으로 이전 24시간을 수집합니다.
    """
    if now is None:
        now = datetime.datetime.now(pytz.utc).astimezone(KST)
    else:
        now = now.astimezone(KST)

    end_time = now
    start_time = now - datetime.timedelta(hours=24)

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
