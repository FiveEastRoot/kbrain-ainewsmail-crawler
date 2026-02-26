import asyncio
import aiohttp
import logging
from config import TARGET_PHASE

from core.gsheets import GoogleSheetsManager
from core.jina_client import JinaClient
from core.time_filter import get_collection_window, KST

from crawlers.rss_full import RssFullCrawler
from crawlers.rss_deep import RssDeepCrawler
from crawlers.crawl_list import CrawlListCrawler
from crawlers.api_hackernews import ApiHackerNewsCrawler

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Main")

async def main():
    logger.info("Initializing Crawler System...")
    
    gs = GoogleSheetsManager()
    await gs.init()
    
    jina = JinaClient()
    
    # Calculate time window
    start_win, end_win = get_collection_window()
    logger.info(f"Time Window: {start_win.strftime('%Y-%m-%d %H:%M KST')} to {end_win.strftime('%Y-%m-%d %H:%M KST')}")
    window = (start_win, end_win)

    # 1. Read sources and get target ones
    sources = await gs.read_sources()
    targets = [
        s for s in sources 
        if str(s.get("Status", "")).lower() == "active" 
        and int(s.get("Phase", 999)) <= TARGET_PHASE
    ]
    
    if not targets:
        logger.info("No active sources found.")
        return
        
    logger.info(f"Start processing {len(targets)} sources.")

    try:
        # 2. Build Raw URL Index
        raw_index = await gs.build_raw_url_index()
    except Exception as e:
        logger.error(f"Failed to build raw index. Check Google Sheets setup. {e}")
        return

    # Use a single aiohttp session for connection pooling
    async with aiohttp.ClientSession() as session:
        # Initialize Crawlers
        crawler_map = {
            "RSS_FULL": RssFullCrawler(gs, jina, session),
            "RSS_DEEP": RssDeepCrawler(gs, jina, session),
            "CRAWL_LIST": CrawlListCrawler(gs, jina, session),
            "API": ApiHackerNewsCrawler(gs, jina, session)
        }
        
        # 3. Process Sources
        # To avoid overloading Sheets API with too many concurrent upserts/logs across ALL sources,
        # we can process sources sequentially, but *items within a source* are processed concurrently.
        for source in targets:
            fetch_type = str(source.get("Fetch_Type", "")).strip().upper()
            crawler = crawler_map.get(fetch_type)
            
            if not crawler:
                logger.warning(f"Unknown Fetch_Type '{fetch_type}' for source {source.get('Source_ID')}")
                continue
                
            logger.info(f"Crawling source {source.get('Source_ID')} ({source.get('Site_Name')}) using {fetch_type}")
            await crawler.crawl(source, raw_index, window)

    logger.info("Crawler run finished.")

if __name__ == "__main__":
    asyncio.run(main())
