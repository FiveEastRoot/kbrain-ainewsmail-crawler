/*******************************
 * 12_Crawler_CRAWL_LIST.gs
 * Phase1 + Fetch_Type=CRAWL_LIST
 * 리스트(Jina) → 링크 추출/필터 → 각 링크(Jina) 2-hop 본문
 *******************************/

const CRAWLLIST_CFG = {
  TARGET_PHASE: 1,
  MAX_ITEMS_PER_SOURCE: 8,
  MIN_TEXT_LEN: 200,
  JINA_TIMEOUT_SEC: 25,
  JINA_DELAY_MS: 700,
  NO_CACHE: true,
  WITH_LINKS_SUMMARY: true, // 리스트에서 링크 뽑을 때만 true
};

// 소스별 “허용 링크 패턴” (핵심: deepmind/hf_daily_papers는 강제 필터)
const CRAWLLIST_RULES = {
  deepmind_blog: {
    host: "deepmind.google",
    allow: [
      /^https:\/\/deepmind\.google\/blog\/(?!tags\/|tag\/|topics\/|topic\/|authors\/|author\/|search\/)[^\/?#]+\/?$/i,
      /^https:\/\/deepmind\.google\/blog\/(?!tags\/|tag\/|topics\/|topic\/|authors\/|author\/|search\/)[^\/?#]+\/[^\/?#]+\/?$/i,
    ],
    deny: [
      /^https:\/\/deepmind\.google\/blog\/?$/i,
      /\/feed\/?$/i
    ]
  },
  hf_daily_papers: {
    host: "huggingface.co",
    allow: [
      /^https:\/\/huggingface\.co\/papers\/\d{4}\.\d{5}(?:v\d+)?\/?$/i
    ],
    deny: [
      /^https:\/\/huggingface\.co\/papers\/?$/i,
      /\/login/i,
      /\/settings/i
    ]
  },
  spri_reports: {
    host: "spri.kr",
    allow: [
      /^https:\/\/spri\.kr\/posts\/view\/\d+(?:\?code=[^#]+)?$/i
    ],
    deny: []
  },
  spri_research: {
    host: "spri.kr",
    allow: [
      /^https:\/\/spri\.kr\/posts\/view\/\d+(?:\?code=[^#]+)?$/i
    ],
    deny: []
  },
  nia_aihub: {
    host: "www.nia.or.kr",
    // View.do?cbIdx=99953&bcIdx=...
    allow: [
      /^https:\/\/www\.nia\.or\.kr\/site\/nia_kor\/ex\/bbs\/View\.do\?cbIdx=99953&bcIdx=\d+.*$/i
    ],
    deny: []
  }
};

function runCrawler_CRAWL_LIST_Phase1() {
  CRAWLLIST_log_("Crawler", "CRAWLER_START", "", "OK", `CRAWL_LIST Phase1 시작: ${new Date().toISOString()}`);

  const sources = CRAWLLIST_readSources_();
  const targets = sources.filter(s =>
    String(s.Status).toLowerCase() === "active" &&
    Number(s.Phase) <= CRAWLLIST_CFG.TARGET_PHASE &&
    String(s.Fetch_Type).trim().toUpperCase() === "CRAWL_LIST"
  );

  if (targets.length === 0) {
    CRAWLLIST_log_("Crawler", "CRAWLER_NO_TARGET", "", "SKIP", "CRAWL_LIST 대상 소스 없음");
    return;
  }

  const rawIndex = CRAWLLIST_buildRawUrlIndex_();

  for (const src of targets) {
    try {
      CRAWLLIST_fetch2Hop_(src, rawIndex);
      CRAWLLIST_log_("Crawler", "SOURCE_DONE", src.Source_ID, "OK", `${src.Site_Name} 완료`);
    } catch (e) {
      CRAWLLIST_log_("Crawler", "SOURCE_ERROR", src.Source_ID, "FAIL", String(e && e.stack ? e.stack : e));
    }
  }

  CRAWLLIST_log_("Crawler", "CRAWLER_END", "", "OK", `CRAWL_LIST Phase1 종료: ${new Date().toISOString()}`);
}

function CRAWLLIST_fetch2Hop_(src, rawIndex) {
  const sourceId = String(src.Source_ID || "").trim();
  const listUrl = CRAWLLIST_ensureHttps_(String(src.Target_URL || "").trim());
  const rule = CRAWLLIST_RULES[sourceId] || null;

  // 1) 리스트 페이지 읽기 (링크 요약 ON)
  Utilities.sleep(CRAWLLIST_CFG.JINA_DELAY_MS);
  const listMd = CRAWLLIST_jinaReadMarkdown_(listUrl, {
    timeoutSec: CRAWLLIST_CFG.JINA_TIMEOUT_SEC,
    noCache: CRAWLLIST_CFG.NO_CACHE,
    withLinksSummary: CRAWLLIST_CFG.WITH_LINKS_SUMMARY
  });

  // 2) 링크 추출 + 필터
  const urls = CRAWLLIST_uniquePreserveOrder_(CRAWLLIST_extractUrls_(listMd));
  const candidates = CRAWLLIST_filterCandidates_(sourceId, listUrl, urls, rule);

  if (candidates.length === 0) {
    CRAWLLIST_log_("Crawler", "LIST_NO_CANDIDATE", sourceId, "SKIP", `후보 링크 0개: ${listUrl}`);
    return;
  }

  const take = Math.min(candidates.length, CRAWLLIST_CFG.MAX_ITEMS_PER_SOURCE);

  // 3) 후보 링크 본문 크롤링
  for (let i = 0; i < take; i++) {
    const rawUrl = CRAWLLIST_normalizeUrl_(candidates[i]);
    const itemUuid = CRAWLLIST_makeItemUUID_(rawUrl);

    Utilities.sleep(CRAWLLIST_CFG.JINA_DELAY_MS);

    let md = "";
    try {
      md = CRAWLLIST_jinaReadMarkdown_(rawUrl, {
        timeoutSec: CRAWLLIST_CFG.JINA_TIMEOUT_SEC,
        noCache: CRAWLLIST_CFG.NO_CACHE,
        withLinksSummary: false
      });
    } catch (e) {
      CRAWLLIST_log_("Crawler", "JINA_READ_FAIL", itemUuid, "FAIL", `${sourceId} | ${rawUrl} | ${String(e)}`);
      continue;
    }

    let text = String(md || "").replace(/\s+/g, " ").trim();
    const limit = src.Max_Length ? Number(src.Max_Length) : 4000;
    if (text.length > limit) text = text.slice(0, limit) + " ...[Max_Length cut]";

    if (!text || text.length < CRAWLLIST_CFG.MIN_TEXT_LEN) {
      CRAWLLIST_log_("Crawler", "ITEM_SKIP_SHORT", itemUuid, "SKIP", `${sourceId} | ${rawUrl}`);
      continue;
    }

    const title = CRAWLLIST_extractTitleFromMd_(md) || "";

    const rowObj = {
      Item_UUID: itemUuid,
      Collected_At: new Date(),
      Source_ID: sourceId,
      Title_Org: title,
      Raw_Url: rawUrl,
      Full_Text: text,
      Raw_JSON: JSON.stringify({ mode: "CRAWL_LIST_2HOP", listUrl, link: rawUrl }),
      Processed_YN: "N"
    };

    CRAWLLIST_upsertRawByUrl_(rawIndex, rowObj);
    CRAWLLIST_log_("Crawler", "ITEM_UPSERT", itemUuid, "OK", `${sourceId} | len=${text.length}`);
  }
}

/*** candidate filter ***/
function CRAWLLIST_filterCandidates_(sourceId, listUrl, urls, rule) {
  const baseHost = CRAWLLIST_host_(listUrl);
  let out = urls.filter(u => {
    // 기본: 파일/리소스 제거
    if (/\.(jpg|jpeg|png|gif|webp|svg|css|js|pdf)(\?|#|$)/i.test(u)) return false;

    // 기본: 동일 host 우선 (rule.host가 있으면 그것 우선)
    const h = CRAWLLIST_host_(u);
    if (rule && rule.host) {
      if (h !== rule.host) return false;
    } else {
      if (h !== baseHost) return false;
    }

    // 흔한 네비/구독/로그인 제거
    if (/\/(tag|tags|category|categories|author|about|privacy|terms|login|subscribe)\b/i.test(u)) return false;

    return true;
  });

  // rule deny/allow 적용
  if (rule && rule.deny && rule.deny.length) {
    out = out.filter(u => !rule.deny.some(rx => rx.test(u)));
  }
  if (rule && rule.allow && rule.allow.length) {
    out = out.filter(u => rule.allow.some(rx => rx.test(u)));
  }

  return CRAWLLIST_uniquePreserveOrder_(out);
}

/*** Jina ***/
function CRAWLLIST_jinaReadMarkdown_(url, opt) {
  const apiKey = PropertiesService.getScriptProperties().getProperty("JINA_API_KEY") || "";
  const finalUrl = "https://r.jina.ai/" + CRAWLLIST_ensureHttps_(url);

  const headers = {
    "User-Agent": "Mozilla/5.0 (GAS Jina Reader)",
    "x-respond-with": "markdown",
    "x-timeout": String((opt && opt.timeoutSec) ? opt.timeoutSec : 20),
  };
  if (opt && opt.noCache) headers["x-no-cache"] = "true";
  if (opt && opt.withLinksSummary) headers["X-With-Links-Summary"] = "true";
  if (apiKey) headers["Authorization"] = "Bearer " + apiKey;

  const res = UrlFetchApp.fetch(finalUrl, {
    method: "get",
    muteHttpExceptions: true,
    followRedirects: true,
    headers
  });

  const code = res.getResponseCode();
  const body = res.getContentText();
  if (code !== 200) throw new Error(`[JINA_HTTP_${code}] ${body.slice(0, 250)}`);
  return body || "";
}

/*** RAW upsert ***/
function CRAWLLIST_buildRawUrlIndex_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(CONFIG.SHEETS.RAW);
  if (!sheet) throw new Error(`시트 없음: ${CONFIG.SHEETS.RAW}`);

  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getLastColumn();
  const headers = sheet.getRange(1,1,1,lastCol).getValues()[0].map(String);

  const urlCol = headers.indexOf("Raw_Url");
  if (urlCol === -1) throw new Error("DATA_Raw에 Raw_Url 컬럼이 없습니다.");

  const map = new Map();
  if (lastRow >= 2) {
    const values = sheet.getRange(2,1,lastRow-1,lastCol).getValues();
    values.forEach((row, idx) => {
      const v = row[urlCol];
      if (v) map.set(String(v), idx + 2);
    });
  }
  return { sheet, headers, map };
}

function CRAWLLIST_upsertRawByUrl_(rawIndex, dataObj) {
  const { sheet, headers, map } = rawIndex;
  const row = headers.map(h => (dataObj[h] !== undefined ? dataObj[h] : ""));
  const key = dataObj["Raw_Url"];

  if (map.has(key)) {
    const r = map.get(key);
    sheet.getRange(r,1,1,headers.length).setValues([row]);
  } else {
    sheet.appendRow(row);
    map.set(key, sheet.getLastRow());
  }
}

/*** sources ***/
function CRAWLLIST_readSources_() {
  if (typeof readDataAsJSON === "function") return readDataAsJSON(CONFIG.SHEETS.CONF_SOURCE);
  return CRAWLLIST_readSheetAsJSON_(CONFIG.SHEETS.CONF_SOURCE);
}

/*** utils ***/
function CRAWLLIST_ensureHttps_(u) {
  if (!u) return "";
  if (u.startsWith("https://")) return u;
  if (u.startsWith("http://")) return "https://" + u.slice(7);
  return "https://" + u;
}

function CRAWLLIST_normalizeUrl_(u) {
  let url = String(u).trim().replace(/^http:\/\//i, "https://").replace(/#.*$/, "");
  const parts = url.split("?");
  if (parts.length === 1) return url;

  const base = parts[0];
  const kept = parts[1].split("&").filter(kv => {
    const k = kv.split("=")[0].toLowerCase();
    return !(k.startsWith("utm_") || k === "fbclid" || k === "gclid");
  });
  return kept.length ? `${base}?${kept.join("&")}` : base;
}

function CRAWLLIST_makeItemUUID_(normalizedUrl) {
  const bytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, normalizedUrl);
  const hex = bytes.map(b => ("0" + ((b < 0 ? b + 256 : b).toString(16))).slice(-2)).join("");
  return "ITEM_" + hex.slice(0, 8);
}

function CRAWLLIST_extractUrls_(text) {
  const s = String(text || "");
  const re = /https?:\/\/[^\s<>()\]]+/g;
  const found = s.match(re) || [];
  return found.map(u => u.replace(/[),.\]]+$/g, ""));
}

function CRAWLLIST_uniquePreserveOrder_(arr) {
  const seen = new Set();
  const out = [];
  for (const x of arr) {
    if (!x) continue;
    if (seen.has(x)) continue;
    seen.add(x);
    out.push(x);
  }
  return out;
}

function CRAWLLIST_extractTitleFromMd_(md) {
  const s = String(md || "");
  const m = s.match(/^#\s+(.+)$/m);
  if (m && m[1]) return m[1].trim();
  const line = s.split("\n").map(x => x.trim()).find(x => x) || "";
  return line.replace(/^#+\s*/, "").slice(0, 140);
}

function CRAWLLIST_host_(u) {
  try { return String(u).match(/^https?:\/\/([^\/]+)/i)[1].toLowerCase(); }
  catch (e) { return ""; }
}

/*** logger ***/
function CRAWLLIST_log_(moduleName, actionType, targetUuid, status, message) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(CONFIG.SHEETS.LOG);
  if (!sh) sh = ss.insertSheet(CONFIG.SHEETS.LOG);

  if (sh.getLastRow() === 0) {
    sh.getRange(1,1,1,7).setValues([["Log_UUID","Timestamp","Module","Action_Type","Target_UUID","Status","Message"]]);
  }

  sh.appendRow([
    "LOG_" + Utilities.getUuid().slice(0, 8),
    new Date(),
    moduleName,
    actionType,
    targetUuid || "",
    status,
    message || ""
  ]);
}

function CRAWLLIST_readSheetAsJSON_(sheetName) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(sheetName);
  if (!sh) throw new Error(`시트 없음: ${sheetName}`);
  const lastRow = sh.getLastRow();
  const lastCol = sh.getLastColumn();
  if (lastRow < 2) return [];
  const headers = sh.getRange(1,1,1,lastCol).getValues()[0].map(String);
  const values = sh.getRange(2,1,lastRow-1,lastCol).getValues();
  return values.map(r => headers.reduce((o,h,i)=> (o[h]=r[i], o), {}));
}
