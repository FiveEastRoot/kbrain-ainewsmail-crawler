import feedparser
import json
import datetime
import traceback
from typing import Dict, Any

from crawlers.base import BaseCrawler
from core.utils import ensure_https, normalize_url, make_item_uuid, strip_html
from core.time_filter import parse_date_robust, is_within_window
from config import MIN_TEXT_LEN

class RssFullCrawler(BaseCrawler):
    async def crawl(self, source: Dict[str, Any], raw_index: tuple, window: tuple):
        sheet, headers, url_map = raw_index
        start_win, end_win = window
        source_id = source.get("Source_ID", "UNKNOWN")
        max_length = int(source.get("Max_Length", 4000))
        
        feed_url = ensure_https(str(source.get("Target_URL", "")).strip())
        
        try:
            # Using aiohttp to fetch RSS xml is possible, but feedparser.parse can also take a URL.
            # However, feedparser is blocking on network if passed a URL directly.
            # Best practice: fetch raw text async, then pass to feedparser.
            headers_req = {"User-Agent": "Mozilla/5.0 (Python Async RSS_FULL)"}
            async with self.session.get(feed_url, headers=headers_req) as response:
                if response.status != 200:
                    await self.gs.log_event("Crawler", "SOURCE_HTTP_FAIL", source_id, "FAIL", f"HTTP {response.status} - {feed_url}")
                    return
                xml_content = await response.text()
                
            feed = feedparser.parse(xml_content)
            
            if feed.bozo and getattr(feed.bozo_exception, 'getMessage', lambda: '')() != 'unknown encoding':
                 # Not strictly throwing error, bozo is set often on valid feeds with minor standard violations
                 pass
                 
            for entry in feed.entries:
                link = entry.get("link", entry.get("id", ""))
                if not link:
                    continue
                    
                raw_url = normalize_url(link)
                item_uuid = make_item_uuid(raw_url)
                
                # Extract and parse date
                pub_date_str = entry.get("published", entry.get("updated", ""))
                pub_date = parse_date_robust(pub_date_str)
                
                if pub_date:
                    if not is_within_window(pub_date, start_win, end_win):
                        # print(f"Skipping {item_uuid} due to date {pub_date_str}")
                        continue
                else:
                    # If we can't parse a date, we could skip it or include it.
                    # As a conservative fallback for full-extraction, we might include it or log it.
                    # Let's log and process it; the upsert will prevent infinite duplication anyway.
                    await self.gs.log_event("Crawler", "ITEM_NO_DATE_WARN", item_uuid, "WARN", f"{source_id} | Could not parse date: {pub_date_str}")
                
                # Extract text
                # We look at content (Atom) or description (RSS)
                text = ""
                if hasattr(entry, 'content'):
                    text = entry.content[0].value
                elif hasattr(entry, 'summary'):
                    text = entry.summary
                elif hasattr(entry, 'description'):
                    text = entry.description
                    
                text = strip_html(text)
                
                if len(text) > max_length:
                    text = text[:max_length] + " ...[Max_Length cut]"
                    
                if len(text) < MIN_TEXT_LEN:
                    await self.gs.log_event("Crawler", "ITEM_SKIP_SHORT", item_uuid, "SKIP", f"{source_id} | {entry.get('title', '')}")
                    continue
                    
                title = entry.get("title", "")
                
                row_obj = {
                    "Item_UUID": item_uuid,
                    "Collected_At": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "Source_ID": source_id,
                    "Title_Org": title,
                    "Raw_Url": raw_url,
                    "Full_Text": text,
                    "Raw_JSON": json.dumps({"mode": "RSS_FULL", "feedUrl": feed_url, "title": title, "link": raw_url}),
                    "Processed_YN": "N"
                }
                
                await self.gs.upsert_raw_by_url(sheet, headers, url_map, row_obj)
                await self.gs.log_event("Crawler", "ITEM_UPSERT", item_uuid, "OK", f"{source_id} | len={len(text)}")
                
            await self.gs.log_event("Crawler", "SOURCE_DONE", source_id, "OK", f"{source.get('Site_Name', '')} 완료")
            
        except Exception as e:
            err_msg = traceback.format_exc()
            await self.gs.log_event("Crawler", "SOURCE_ERROR", source_id, "FAIL", err_msg[:1000])

