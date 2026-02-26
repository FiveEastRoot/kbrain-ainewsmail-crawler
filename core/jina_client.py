import aiohttp
import asyncio
from typing import Optional

from config import JINA_API_KEY, JINA_TIMEOUT_SEC, JINA_DELAY_MS
from core.utils import ensure_https

class JinaClient:
    def __init__(self, api_key: str = JINA_API_KEY):
        self.api_key = api_key
        # Limit concurrent Jina requests to avoid hammering the API
        self.semaphore = asyncio.Semaphore(5)

    async def read_markdown(self, url: str, session: aiohttp.ClientSession, no_cache: bool = True, with_links_summary: bool = False, timeout_sec: int = JINA_TIMEOUT_SEC) -> str:
        """
        Reads a URL using Jina Reader and returns Markdown string asynchronously.
        """
        await asyncio.sleep(JINA_DELAY_MS / 1000.0) # Respect delay per Jina guidelines
        
        final_url = f"https://r.jina.ai/{ensure_https(url)}"
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Python Async Jina Reader)",
            "x-respond-with": "markdown",
            "x-timeout": str(timeout_sec)
        }
        
        if no_cache:
            headers["x-no-cache"] = "true"
        if with_links_summary:
            headers["X-With-Links-Summary"] = "true"
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        timeout = aiohttp.ClientTimeout(total=timeout_sec + 5) # add buffer for network wait

        async with self.semaphore:
            try:
                async with session.get(final_url, headers=headers, timeout=timeout) as response:
                    text = await response.text()
                    if response.status != 200:
                        raise Exception(f"[JINA_HTTP_{response.status}] {text[:250]}")
                    return text
            except Exception as e:
                # Log or re-raise
                raise Exception(f"Jina Request Failed for {url}: {str(e)}")
