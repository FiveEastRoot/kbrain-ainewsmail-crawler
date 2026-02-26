import json
import os
import uuid
import datetime
import gspread_asyncio
from google.oauth2.service_account import Credentials
from core.time_filter import KST

from config import (
    GOOGLE_SERVICE_ACCOUNT_FILE, 
    SPREADSHEET_ID, 
    SPREADSHEET_NAME,
    SHEET_RAW, 
    SHEET_CONF_SOURCE, 
    SHEET_LOG
)

def get_creds():
    # Requires standard scopes for Sheets
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    if os.path.exists(GOOGLE_SERVICE_ACCOUNT_FILE):
        return Credentials.from_service_account_file(GOOGLE_SERVICE_ACCOUNT_FILE, scopes=scopes)
    else:
        raise FileNotFoundError(f"Service account file not found: {GOOGLE_SERVICE_ACCOUNT_FILE}")

agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)

class GoogleSheetsManager:
    def __init__(self):
        self.client = None
        self.doc = None

    async def init(self):
        self.client = await agcm.authorize()
        if SPREADSHEET_ID:
            self.doc = await self.client.open_by_key(SPREADSHEET_ID)
        else:
            self.doc = await self.client.open(SPREADSHEET_NAME)

    async def read_sources(self) -> list[dict]:
        """Reads CONF_SOURCE and returns list of dictionaries."""
        worksheet = await self.doc.worksheet(SHEET_CONF_SOURCE)
        records = await worksheet.get_all_records()
        return records

    async def build_raw_url_index(self):
        """Returns the worksheet, headers, and a dict mapped by Raw_Url"""
        sheet = await self.doc.worksheet(SHEET_RAW)
        # get_all_values includes headers
        all_values = await sheet.get_all_values()
        
        if not all_values:
            raise ValueError(f"Sheet {SHEET_RAW} is empty")
            
        headers = [str(h).strip() for h in all_values[0]]
        
        try:
            url_col_idx = headers.index("Raw_Url")
        except ValueError:
            raise ValueError(f"Cannot find 'Raw_Url' in headers of {SHEET_RAW}")
            
        url_map = {}
        for row_idx, row in enumerate(all_values[1:], start=2): # 1-based indexing in sheets, +1 for header
            if len(row) > url_col_idx:
                url = str(row[url_col_idx]).strip()
                if url:
                    url_map[url] = row_idx
                    
        return sheet, headers, url_map

    async def upsert_raw_by_url(self, sheet, headers, url_map, data_obj: dict):
        """Upserts a row into DATA_Raw based on Raw_Url."""
        row_data = [str(data_obj.get(h, "")) for h in headers]
        key = data_obj.get("Raw_Url")
        
        if key in url_map:
            row_idx = url_map[key]
            # range: A{row_idx}:Z{row_idx}
            end_col = chr(ord('A') + len(headers) - 1) if len(headers) <= 26 else 'Z' # simple approx
            # Better to use proper col string generation if > 26 columns, but assuming basic for now.
            # Using update for specific row
            await sheet.update(f"A{row_idx}", [row_data])
        else:
            await sheet.append_row(row_data)
            # Rough update of map assuming append goes to last row + 1
            # In highly concurrent scenarios, mapping might be slightly off. 
            # If absolute strictness is needed, batch updates are better.
            
    async def log_event(self, module_name: str, action_type: str, target_uuid: str, status: str, message: str):
        """Logs an event to LOG_History."""
        try:
            worksheet = await self.doc.worksheet(SHEET_LOG)
        except gspread_asyncio.WorksheetNotFound:
            worksheet = await self.doc.add_worksheet(title=SHEET_LOG, rows="1000", cols="7")
            await worksheet.append_row(["Log_UUID", "Timestamp", "Module", "Action_Type", "Target_UUID", "Status", "Message"])
            
        # Timestamp formatted simply
        now_str = datetime.datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
        log_uuid = "LOG_" + str(uuid.uuid4()).replace("-", "")[:8]
        
        row = [
            log_uuid,
            now_str,
            module_name,
            action_type,
            target_uuid or "",
            status,
            message or ""
        ]
        await worksheet.append_row(row)
