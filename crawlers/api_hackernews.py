import json
import datetime
import traceback
import re
from typing import Dict, Any
import asyncio

from crawlers.base import BaseCrawler
from core.utils import ensure_https, normalize_url, make_item_uuid, extract_title_from_md
from core.time_filter import is_within_window
from config import MIN_TEXT_LEN, JINA_TIMEOUT_SEC, JINA_DELAY_MS

HN_API_BASE = "https://hacker-news.firebaseio.com/v0"
MAX_ITEMS_PER_SOURCE = 30  # Increased from 15

# AI/Tech relevance keywords (case-insensitive match against title)
RELEVANCE_KEYWORDS = [
    # AI / ML
    r'\bai\b', r'\bartificial.intelligence\b', r'\bmachine.learning\b', r'\bml\b',
    r'\bdeep.learning\b', r'\bneural.net', r'\btransformer', r'\bllm\b', r'\bgpt\b',
    r'\bchatgpt\b', r'\bopenai\b', r'\banthropic\b', r'\bclaude\b', r'\bgemini\b',
    r'\bgemma\b', r'\bllama\b', r'\bmistral\b', r'\bdiffusion\b', r'\bstable.diffusion\b',
    r'\bagen(t|tic)\b', r'\brag\b', r'\bfine.?tun', r'\bprompt', r'\bembedding',
    r'\bvector.?(db|database|store|search)\b', r'\bnlp\b', r'\bcomputer.vision\b',
    r'\breinforcement.learning\b', r'\brlhf\b', r'\bdpo\b',
    r'\brobot', r'\bautonomous\b', r'\bself.driving\b',
    r'\bgenerat(ive|ion)\b', r'\bfoundation.model\b', r'\bopen.?source.?model\b',
    r'\bmultimodal\b', r'\bspeech', r'\btts\b', r'\bstt\b', r'\bocr\b',
    r'\bimage.gen', r'\bvideo.gen', r'\btext.to',
    # Software Engineering / Dev
    r'\bpython\b', r'\brust\b', r'\btypescript\b', r'\bjavascript\b',
    r'\bgolang\b', r'\bkubernetes\b', r'\bdocker\b', r'\bwasm\b',
    r'\bapi\b', r'\bsdk\b', r'\bopen.?source\b', r'\bgithub\b',
    r'\bcompiler\b', r'\bkernel\b', r'\blinux\b',
    r'\bcloud\b', r'\baws\b', r'\bgcp\b', r'\bazure\b',
    r'\bdatabase\b', r'\bpostgres', r'\bsqlite\b', r'\bredis\b',
    r'\bgpu\b', r'\bnvidia\b', r'\bcuda\b', r'\btpu\b',
    r'\bserverless\b', r'\bedge.comput',
    # Tech Industry / Startups
    r'\bstartup\b', r'\bfunding\b', r'\bseries.[a-d]\b', r'\bipo\b',
    r'\bgoogle\b', r'\bmeta\b', r'\bmicrosoft\b', r'\bapple\b', r'\bamazon\b',
    r'\bdeepseek\b', r'\bdeep.?mind\b', r'\bhugging.?face\b',
    # Data / Security / Infra
    r'\bcyber', r'\bsecurity\b', r'\bprivacy\b', r'\bencrypt',
    r'\bdata.?(science|engineer|pipeline)\b', r'\bmlops\b',
    r'\bscal(e|ing|ability)\b', r'\bperformance\b', r'\bbenchmark\b',
]

RELEVANCE_PATTERN = re.compile('|'.join(RELEVANCE_KEYWORDS), re.IGNORECASE)


def _clean_jina_generic(text: str) -> str:
    """Universal cleaning for Jina markdown output from arbitrary external sites."""
    # Remove "Published Time:" header that Jina prepends
    text = re.sub(r'^Published Time:.*\n+', '', text, count=1)

    # Remove common nav/header patterns
    text = re.sub(r'^\[Skip to (?:main |)content\].*\n*', '', text, flags=re.MULTILINE)
    text = re.sub(r'(?m)^(?:Log ?in|Sign ?up|Subscribe|Newsletter|Menu|Search)\s*$\n*', '', text)

    # Remove markdown image-only lines (nav icons, logos, etc.)
    text = re.sub(r'(?m)^!\[Image \d+[^\]]*\]\([^\)]+\)\s*$\n*', '', text)

    # Remove lines that are just markdown links to nav items
    text = re.sub(r'(?m)^\*\s+\[[^\]]{1,20}\]\(https?://[^\)]+\)\s*$\n*', '', text)

    # Remove "Cookie" / "Privacy" banners
    text = re.sub(r'(?i)(?:cookie|privacy).{0,100}(?:accept|decline|settings|policy).*\n*', '', text)

    # Remove footer social links
    text = re.sub(r'(?m)^(?:Follow us|Share this|©|\(c\)|All rights reserved).*$\n*', '', text, flags=re.IGNORECASE)

    # Collapse excessive whitespace
    text = re.sub(r'\n{3,}', '\n\n', text).strip()

    return text


class ApiHackerNewsCrawler(BaseCrawler):
    async def crawl(self, source: Dict[str, Any], raw_index: tuple, window: tuple):
        sheet, headers, url_map = raw_index
        start_win, end_win = window
        source_id = source.get("Source_ID", "UNKNOWN")
        max_length = int(source.get("Max_Length", 4000))
        min_score = int(source.get("Min_Score", 150))  # Raised default from 50 to 150

        try:
            # 1. Fetch top story IDs
            async with self.session.get(f"{HN_API_BASE}/topstories.json") as resp:
                if resp.status != 200:
                    await self.gs.log_event("Crawler", "SOURCE_HTTP_FAIL", source_id, "FAIL", f"HN API HTTP {resp.status}")
                    return
                story_ids = await resp.json()

            # 2. Fetch each story's metadata and filter
            valid_stories = []
            sem = asyncio.Semaphore(10)

            async def fetch_story(story_id):
                async with sem:
                    try:
                        async with self.session.get(f"{HN_API_BASE}/item/{story_id}.json") as resp:
                            if resp.status != 200:
                                return None
                            return await resp.json()
                    except Exception:
                        return None

            # Check first 200 stories for wider coverage
            tasks = [fetch_story(sid) for sid in story_ids[:200]]
            results = await asyncio.gather(*tasks)

            for story in results:
                if not story or story.get("type") != "story":
                    continue

                # Check time window
                unix_time = story.get("time", 0)
                if not unix_time:
                    continue
                pub_date = datetime.datetime.fromtimestamp(unix_time, tz=datetime.timezone.utc)
                if not is_within_window(pub_date, start_win, end_win):
                    continue

                # Check score threshold
                score = story.get("score", 0)
                if score < min_score:
                    continue

                # Must have an external URL (skip HN self-posts like "Ask HN", "Show HN" without url)
                story_url = story.get("url", "")
                if not story_url:
                    continue

                # Keyword relevance filter: title must match at least one AI/tech keyword
                title = story.get("title", "")
                if not RELEVANCE_PATTERN.search(title):
                    continue

                valid_stories.append(story)

                if len(valid_stories) >= MAX_ITEMS_PER_SOURCE:
                    break

            if not valid_stories:
                await self.gs.log_event("Crawler", "SOURCE_EMPTY", source_id, "SKIP",
                    f"No relevant stories (min_score={min_score}, keyword_filter=ON)")
                return

            self.logger.info(f"HN: {len(valid_stories)} relevant stories found (score≥{min_score})")

            # 3. Fetch full content via Jina for each valid story
            async def process_story(story):
                story_url = story["url"]
                raw_url = normalize_url(story_url)
                item_uuid = make_item_uuid(raw_url)
                hn_title = story.get("title", "")
                score = story.get("score", 0)
                hn_id = story.get("id", "")

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

                    # Apply universal cleaning
                    text = _clean_jina_generic(md_text)

                    if len(text) > max_length:
                        text = text[:max_length] + "\n...[Max_Length cut]"

                    if len(text) < MIN_TEXT_LEN:
                        await self.gs.log_event("Crawler", "ITEM_SKIP_SHORT", item_uuid, "SKIP", f"{source_id} | {hn_title}")
                        return

                    title = hn_title or extract_title_from_md(md_text) or ""

                    row_obj = {
                        "Item_UUID": item_uuid,
                        "Collected_At": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "Source_ID": source_id,
                        "Title_Org": title,
                        "Raw_Url": raw_url,
                        "Full_Text": text,
                        "Raw_JSON": json.dumps({
                            "mode": "API_HN",
                            "hn_id": hn_id,
                            "score": score,
                            "title": title,
                            "link": raw_url,
                            "hn_url": f"https://news.ycombinator.com/item?id={hn_id}"
                        }),
                        "Processed_YN": "N"
                    }

                    await self.gs.upsert_raw_by_url(sheet, headers, url_map, row_obj)
                    await self.gs.log_event("Crawler", "ITEM_UPSERT", item_uuid, "OK", f"{source_id} | score={score} | len={len(text)}")

                except Exception as e:
                    await self.gs.log_event("Crawler", "JINA_READ_FAIL", item_uuid, "FAIL", f"{source_id} | {raw_url} | {str(e)}")

            await asyncio.gather(*(process_story(s) for s in valid_stories))
            await self.gs.log_event("Crawler", "SOURCE_DONE", source_id, "OK", f"HackerNews | {len(valid_stories)} stories processed")

        except Exception as e:
            err_msg = traceback.format_exc()
            await self.gs.log_event("Crawler", "SOURCE_ERROR", source_id, "FAIL", err_msg[:1000])
