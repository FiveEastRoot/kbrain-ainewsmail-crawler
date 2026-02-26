import os
from dotenv import load_dotenv

load_dotenv()

# Google Sheets Config
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "service_account.json")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "")  # Or use SPREADSHEET_NAME depends on preference
SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME", "Crawler_Config") # Default name from previous setup if applicable

# Sheet Names (matching CONFIG.SHEETS from GAS)
SHEET_RAW = "DATA_Raw"
SHEET_CONF_SOURCE = "CONF_Sources"
SHEET_LOG = "LOG_History"

# Jina API
JINA_API_KEY = os.getenv("JINA_API_KEY", "")

# Crawler Settings
JINA_TIMEOUT_SEC = 25
JINA_DELAY_MS = 650

# Thresholds
MIN_TEXT_LEN = 120 # Reverting to most conservative limit, though deep requires 200

# Constants
TARGET_PHASE = 1

# List Crawler Host Rules (Copied from 12_Crawler_CRAWL_LIST.gs)
CRAWLLIST_RULES = {
    "deepmind_blog": {
        "host": "deepmind.google",
        "allow": [
            r"^https://deepmind\.google/discover/blog/.*",
            r"^https://deepmind\.google/blog/(?!tags/|tag/|topics/|topic/|authors/|author/|search/)[^\/?#]+/?$"
        ],
        "deny": [
            r"^https://deepmind\.google/blog/?$",
            r"/feed/?$"
        ]
    },
    "hf_daily_papers": {
        "host": "huggingface.co",
        "allow": [
            r"^https://huggingface\.co/papers/\d{4}\.\d{5}(?:v\d+)?/?$"
        ],
        "deny": [
            r"^https://huggingface\.co/papers/?$",
            r"/login",
            r"/settings"
        ]
    },
    "spri_reports": {
        "host": "spri.kr",
        "allow": [
            r"^https://spri\.kr/posts/view/\d+(?:\?code=[^#]+)?$"
        ],
        "deny": []
    },
    "spri_research": {
        "host": "spri.kr",
        "allow": [
            r"^https://spri\.kr/posts/view/\d+(?:\?code=[^#]+)?$"
        ],
        "deny": []
    },
    "nia_aihub": {
        "host": "www.nia.or.kr",
        "allow": [
            r"^https://www\.nia\.or\.kr/site/nia_kor/ex/bbs/View\.do\?cbIdx=99953&bcIdx=\d+.*$"
        ],
        "deny": []
    }
}
