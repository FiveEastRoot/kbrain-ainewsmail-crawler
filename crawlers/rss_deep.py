import feedparser
import json
import datetime
import traceback
import re
from typing import Dict, Any
import asyncio

from crawlers.base import BaseCrawler
from core.utils import ensure_https, normalize_url, make_item_uuid, extract_title_from_md
from core.time_filter import parse_date_robust, is_within_window
from config import MIN_TEXT_LEN, JINA_TIMEOUT_SEC, JINA_DELAY_MS

class RssDeepCrawler(BaseCrawler):
    async def crawl(self, source: Dict[str, Any], raw_index: tuple, window: tuple):
        sheet, headers, url_map = raw_index
        start_win, end_win = window
        source_id = source.get("Source_ID", "UNKNOWN")
        max_length = int(source.get("Max_Length", 4000))
        
        feed_url = ensure_https(str(source.get("Target_URL", "")).strip())
        
        try:
            # 1. Fetch RSS XML
            headers_req = {"User-Agent": "Mozilla/5.0 (Python Async RSS_DEEP)"}
            async with self.session.get(feed_url, headers=headers_req) as response:
                if response.status != 200:
                    await self.gs.log_event("Crawler", "SOURCE_HTTP_FAIL", source_id, "FAIL", f"HTTP {response.status} - {feed_url}")
                    return
                xml_content = await response.text()
                
            feed = feedparser.parse(xml_content)
            
            # List of tasks for concurrent Jina fetching
            jina_tasks = []
            valid_entries = []

            # 2. Filter entries by date first
            for entry in feed.entries:
                link = entry.get("link", entry.get("id", ""))
                if not link:
                    continue
                    
                raw_url = normalize_url(link)
                item_uuid = make_item_uuid(raw_url)
                
                # Check date
                pub_date_str = entry.get("published", entry.get("updated", ""))
                pub_date = parse_date_robust(pub_date_str)
                
                if pub_date:
                    if not is_within_window(pub_date, start_win, end_win):
                         continue
                else:
                    await self.gs.log_event("Crawler", "ITEM_NO_DATE_WARN", item_uuid, "WARN", f"{source_id} | Could not parse date: {pub_date_str}")
                    # Accept anyway as fallback
                
                valid_entries.append((entry, raw_url, item_uuid))
            
            if not valid_entries:
                return # Nothing in window

            # 3. Concurrently fetch valid entries via Jina
            async def process_entry(entry, raw_url, item_uuid):
                try:
                    md_text = await self.jina.read_markdown(
                        raw_url, 
                        self.session, 
                        no_cache=True, 
                        with_links_summary=False, 
                        timeout_sec=JINA_TIMEOUT_SEC
                    )
                    
                    text = re.sub(r'\n{3,}', '\n\n', md_text).strip()
                    
                    # Source-specific cleaning
                    if source_id == "hf_blog":
                        # HF Blog: strip "Back to Articles", author avatars at top
                        # Content starts at first [](url#section) anchor heading
                        m = re.search(r'\[]\(https://huggingface\.co/blog/[^)]+#[^)]+\)\s+\S', text)
                        if m:
                            text = text[m.start():].strip()
                    elif source_id == "nvidia_dev_blog":
                        # NVIDIA Blog: strip huge nav header, content starts after Published Time + title
                        # Look for the first paragraph after the hero image
                        parts = re.split(r'(?m)^(?:Share\s*$|Copy\s+link)', text, maxsplit=1)
                        if len(parts) > 1:
                            text = parts[1].strip()
                        else:
                            # Fallback: skip past the nav/sidebar by finding the article body
                            m = re.search(r'(?m)^(?:By\s+\S|Table of Contents)', text)
                            if m:
                                text = text[m.start():].strip()
                    elif source_id == "geeknews":
                        # GeekNews: content is bullet-point summary between metadata and footer
                        # Strip header: everything before first bullet point "*   "
                        m = re.search(r'(?m)^\*\s+', text)
                        if m:
                            text = text[m.start():].strip()
                        # Strip footer: comments and site nav after "인증 이메일" or voting arrows
                        text = re.split(r'(?m)인증 이메일|^\[▲\]\(javascript:votec', text, maxsplit=1)[0].strip()
                        # Also strip trailing site footer
                        text = re.split(r'\[사이트 이용법\]', text, maxsplit=1)[0].strip()
                    elif source_id == "openai_news":
                        # OpenAI: strip nav header (Research, Safety, etc.)
                        # Content body usually starts after share icons / date line
                        parts = re.split(r'(?m)^(?:Share|Copy link)', text, maxsplit=1)
                        if len(parts) > 1:
                            text = parts[1].strip()
                        else:
                            # Fallback: find first paragraph after nav
                            m = re.search(r'(?m)^[A-Z][a-z].*[.!]$', text)
                            if m:
                                text = text[m.start():].strip()
                        # Strip footer (social links + copyright)
                        text = re.split(r'OpenAI ©|Back to index|\(opens in a new window\)\s*$', text, maxsplit=1)[0].strip()
                    elif source_id == "kisa_notice":
                        # KISA: strip huge Korean gov nav menu
                        # Content starts after "본문 시작" or article title area
                        parts = re.split(r'(?m)등록일\s+\d{4}[.\-]\d{2}[.\-]\d{2}|작성일\s+\d{4}[.\-]\d{2}[.\-]\d{2}|조회수\s+\d+', text, maxsplit=1)
                        if len(parts) > 1:
                            text = parts[1].strip()
                        # Strip footer
                        text = re.split(r'Copyright\(C\)', text, maxsplit=1)[0].strip()
                        text = re.split(r'Now Loading', text, maxsplit=1)[0].strip()
                    
                    if len(text) > max_length:
                        text = text[:max_length] + "\n...[Max_Length cut]"
                        
                    if len(text) < MIN_TEXT_LEN:
                        await self.gs.log_event("Crawler", "ITEM_SKIP_SHORT", item_uuid, "SKIP", f"{source_id} | {entry.get('title', '')}")
                        return
                        
                    title = entry.get("title") or extract_title_from_md(md_text) or ""
                    
                    row_obj = {
                        "Item_UUID": item_uuid,
                        "Collected_At": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "Source_ID": source_id,
                        "Title_Org": title,
                        "Raw_Url": raw_url,
                        "Full_Text": text,
                        "Raw_JSON": json.dumps({"mode": "RSS_DEEP", "feedUrl": feed_url, "title": title, "link": raw_url}),
                        "Processed_YN": "N"
                    }
                    
                    await self.gs.upsert_raw_by_url(sheet, headers, url_map, row_obj)
                    await self.gs.log_event("Crawler", "ITEM_UPSERT", item_uuid, "OK", f"{source_id} | len={len(text)}")
                    
                except Exception as e:
                    await self.gs.log_event("Crawler", "JINA_READ_FAIL", item_uuid, "FAIL", f"{source_id} | {raw_url} | {str(e)}")

            # Run in parallel using gather (semaphore limits internal concurrent Jina calls)
            await asyncio.gather(*(process_entry(*e) for e in valid_entries))
            
            await self.gs.log_event("Crawler", "SOURCE_DONE", source_id, "OK", f"{source.get('Site_Name', '')} 완료")

        except Exception as e:
            err_msg = traceback.format_exc()
            await self.gs.log_event("Crawler", "SOURCE_ERROR", source_id, "FAIL", err_msg[:1000])

