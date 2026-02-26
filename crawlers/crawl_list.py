import json
import datetime
import traceback
import re
import asyncio
from typing import Dict, Any

from crawlers.base import BaseCrawler
from core.utils import (
    ensure_https, normalize_url, make_item_uuid, 
    extract_urls, extract_links, unique_preserve_order, get_host, extract_title_from_md
)
from core.time_filter import parse_date_robust, is_within_window
from config import MIN_TEXT_LEN, JINA_TIMEOUT_SEC, JINA_DELAY_MS, CRAWLLIST_RULES

class CrawlListCrawler(BaseCrawler):
    async def crawl(self, source: Dict[str, Any], raw_index: tuple, window: tuple):
        sheet, headers, url_map = raw_index
        start_win, end_win = window
        source_id = str(source.get("Source_ID", "")).strip()
        max_length = int(source.get("Max_Length", 4000))
        
        list_url = ensure_https(str(source.get("Target_URL", "")).strip())
        rule = CRAWLLIST_RULES.get(source_id, None)
        
        try:
            # 1. Fetch List Page via Jina with links summary
            try:
                list_md = await self.jina.read_markdown(
                    list_url, 
                    self.session, 
                    no_cache=True, 
                    with_links_summary=True,
                    timeout_sec=JINA_TIMEOUT_SEC
                )
            except Exception as e:
                await self.gs.log_event("Crawler", "LIST_READ_FAIL", source_id, "FAIL", f"{list_url} | {str(e)}")
                return
            
            # 2. Extract and Filter URLs
            link_map = extract_links(list_md)
            urls = unique_preserve_order(list(link_map.keys()))
            candidates = self._filter_candidates(source_id, list_url, urls, rule)
            
            if not candidates:
                await self.gs.log_event("Crawler", "LIST_NO_CANDIDATE", source_id, "SKIP", f"0 candidates: {list_url}")
                return
            
            # Limit number of items per source (configurable via Google Sheets "Max_Items" column)
            max_items = int(source.get("Max_Items", 8))
            candidates = candidates[:max_items]

            # 3. Concurrently fetch candidate articles
            async def process_candidate(raw_url):
                item_uuid = make_item_uuid(raw_url)
                list_page_title = link_map.get(raw_url, "")
                try:
                    # Skip if URL already exists in DATA_Raw
                    if raw_url in url_map:
                        return
                    
                    md_text = await self.jina.read_markdown(
                        raw_url, 
                        self.session, 
                        no_cache=True, 
                        with_links_summary=False, 
                        timeout_sec=JINA_TIMEOUT_SEC
                    )
                    
                    # 4. Attempt to find a date in Markdown (Fallback approach)
                    # For List Crawler, we don't have an RSS pubDate.
                    # We might search the text for a date pattern.
                    extracted_date_str = self._extract_date_from_md(md_text)
                    pub_date = parse_date_robust(extracted_date_str)
                    
                    if pub_date:
                        if not is_within_window(pub_date, start_win, end_win):
                            return # Outside window
                    else:
                        # User preferred: Accept if date not found (Upsert will handle duplicates)
                        # We don't skip here.
                        pass
                        
                    # Process Text
                    text = re.sub(r'\n{3,}', '\n\n', md_text).strip()
                    
                    # 5. Apply custom source-specific cleaning logic
                    if source_id == "hf_daily_papers":
                        parts = re.split(r'(?i)^Abstract\s*\n\s*[-=]+\s*$', text, maxsplit=1, flags=re.MULTILINE)
                        if len(parts) > 1:
                            text = "Abstract\n--------\n" + parts[1].strip()
                        else:
                            parts = re.split(r'(?i)^Abstract\s*\n', text, maxsplit=1, flags=re.MULTILINE)
                            if len(parts) > 1:
                                text = "Abstract\n\n" + parts[1].strip()
                        # Strip comment/community/login footer
                        text = re.split(r'(?m)^### Community|^Comment\s*$|Sign up.*to comment|\[- \[x\] Upvote|^Reply\s*$|Upload images.*clicking here', text, maxsplit=1)[0].strip()
                    elif source_id in ("spri_reports", "spri_research"):
                        # Remove huge top menu of SPRi
                        parts = re.split(r'(?i)조회수\s+\d+|작성일\s+[\d\.\-]+', text, maxsplit=1)
                        if len(parts) > 1:
                            text = parts[1].strip()
                        # Strip PDF/HTML download buttons and SNS sharing section
                        text = re.sub(r'!\[Image[^\]]*\]\([^\)]*(?:down_icon|html_icon|sns_icon|copy_link)[^\)]*\)[^\n]*', '', text)
                        text = re.sub(r'PDF\s*다운로드', '', text)
                        text = re.sub(r'\[HTML\s*보기\]\([^\)]+\)', '', text)
                        text = re.split(r'공유\s*열기', text, maxsplit=1)[-1].strip()
                        # Strip SNS share icon links (naver, facebook, twitter, kakao, band, telegram)
                        text = re.sub(r'\*\s+\[!\[Image[^\]]*(?:공유|연결|연동|복사)[^\]]*\]\([^\)]+\)\]\([^\)]+\s*"[^"]*"\)', '', text)
                        text = re.sub(r'\[!\[Image[^\]]*(?:공유|연결|연동|복사)[^\]]*\]\([^\)]+\)\]\([^\)]+\)', '', text)
                        # Strip 글자크기 controls
                        text = re.split(r'글자크기', text, maxsplit=1)[0].strip()
                        text = re.sub(r'\n{3,}', '\n\n', text).strip()
                    elif source_id == "deepmind_blog":
                        # Remove top formatting for Google Blog like Share/Mail buttons
                        parts = re.split(r'(?ims)(?:Share|Copied)\s*\n\s*!\[Image[^\]]*\]\([^\)]+\)\s*\n', text, maxsplit=1)
                        if len(parts) > 1:
                            text = parts[1].strip()
                        else:
                            parts = re.split(r'(?i)Copy link', text, maxsplit=1)
                            if len(parts) > 1:
                                text = parts[1].strip()
                    elif source_id == "nia_aihub":
                        parts = re.split(r'(?i)조회수\s+\d+', text, maxsplit=1)
                        if len(parts) > 1:
                            text = parts[1].strip()
                        # Strip footer: social share buttons, 목록, 다음글/이전글, 대표전화, etc.
                        text = re.split(r'\[트위터\]|\[페이스북\]|\[구글 플러스\]|\[인쇄\]|(?m)^목록\s*$|_\\?_다음글|_\\?_이전글|\[_TOP_\]|대표전화|개인정보처리방침', text, maxsplit=1)[0].strip()
                    
                    if len(text) > max_length:
                        text = text[:max_length] + "\n...[Max_Length cut]"
                        
                    if len(text) < MIN_TEXT_LEN:
                        await self.gs.log_event("Crawler", "ITEM_SKIP_SHORT", item_uuid, "SKIP", f"{source_id} | {raw_url}")
                        return
                        
                    md_title = extract_title_from_md(md_text)
                    title = list_page_title if list_page_title else (md_title or "")
                    
                    row_obj = {
                        "Item_UUID": item_uuid,
                        "Collected_At": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "Source_ID": source_id,
                        "Title_Org": title,
                        "Raw_Url": raw_url,
                        "Full_Text": text,
                        "Raw_JSON": json.dumps({"mode": "CRAWL_LIST_2HOP", "listUrl": list_url, "link": raw_url}),
                        "Processed_YN": "N"
                    }
                    
                    await self.gs.upsert_raw_by_url(sheet, headers, url_map, row_obj)
                    await self.gs.log_event("Crawler", "ITEM_UPSERT", item_uuid, "OK", f"{source_id} | len={len(text)}")
                    
                except Exception as e:
                    await self.gs.log_event("Crawler", "JINA_READ_FAIL", item_uuid, "FAIL", f"{source_id} | {raw_url} | {str(e)}")

            # Process concurrently
            await asyncio.gather(*(process_candidate(normalize_url(c)) for c in candidates))
            
            await self.gs.log_event("Crawler", "SOURCE_DONE", source_id, "OK", f"{source.get('Site_Name', '')} 완료")
            
        except Exception as e:
            err_msg = traceback.format_exc()
            await self.gs.log_event("Crawler", "SOURCE_ERROR", source_id, "FAIL", err_msg[:1000])

    def _filter_candidates(self, source_id: str, list_url: str, urls: list[str], rule: dict) -> list[str]:
        base_host = get_host(list_url)
        out = []
        
        for u in urls:
            # 기본 파일 리소스 제거
            if re.search(r'\.(jpg|jpeg|png|gif|webp|svg|css|js|pdf)(\?|#|$)', u, re.IGNORECASE):
                continue
                
            h = get_host(u)
            if rule and rule.get("host"):
                if h != rule["host"]: continue
            else:
                if h != base_host: continue
                
            # 흔한 네비용 단어 제거
            if re.search(r'/(tag|tags|category|categories|author|about|privacy|terms|login|subscribe)\b', u, re.IGNORECASE):
                continue
                
            out.append(u)
            
        if rule:
            if rule.get("deny"):
                out = [u for u in out if not any(re.search(rx, u, re.IGNORECASE) for rx in rule["deny"])]
            if rule.get("allow"):
                out = [u for u in out if any(re.search(rx, u, re.IGNORECASE) for rx in rule["allow"])]
                
        return unique_preserve_order(out)

    def _extract_date_from_md(self, md_text: str) -> str:
        """
        Attempts to extract a date string from markdown text using regex.
        Looks for common patterns like "Published: 2024-05-01" or "Oct 12, 2023".
        """
        # Very simplistic approach. In real-world, dates can be anywhere.
        # This is a best-effort fallback for CRAWL_LIST where pubDate isn't standard.
        match = re.search(r'(?:Published|Date|작성일|배포일)[:\-\s]*([0-9]{4}[.\-][0-9]{2}[.\-][0-9]{2}|[A-Z][a-z]{2}\s\d{1,2},?\s\d{4})', md_text, re.IGNORECASE)
        if match:
             return match.group(1)
             
        # Just grab the first looking like YYYY-MM-DD or YYYY.MM.DD early in text.
        text_head = md_text[:2000] # look in first 2000 chars
        match2 = re.search(r'\b(20[2-9][0-9][.\-][0-1][0-9][.\-][0-3][0-9])\b', text_head)
        if match2:
            return match2.group(1)
            
        return ""
