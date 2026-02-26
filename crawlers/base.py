from abc import ABC, abstractmethod
from typing import Dict, Any
import logging

class BaseCrawler(ABC):
    def __init__(self, gs_manager, jina_client, session):
        self.gs = gs_manager
        self.jina = jina_client
        self.session = session
        self.logger = logging.getLogger(self.__class__.__name__)
        
    @abstractmethod
    async def crawl(self, source: Dict[str, Any], raw_index: tuple, window: tuple):
        """
        source: Dictionary containing source configurations (from Google Sheets)
        raw_index: Tuple of (sheet, headers, url_map) for upserting
        window: Tuple of (start_datetime, end_datetime) for 16:00 filtering
        """
        pass
