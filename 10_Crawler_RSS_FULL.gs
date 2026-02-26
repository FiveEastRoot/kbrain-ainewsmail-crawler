/*******************************
 * 10_Crawler_RSS_FULL.gs
 * Phase1 + Fetch_Type=RSS_FULL
 *******************************/

const RSSFULL_CFG = {
  TARGET_PHASE: 1,
  MAX_ITEMS_PER_SOURCE: 10,
  MIN_TEXT_LEN: 120,
};

function runCrawler_RSS_FULL_Phase1() {
  RSSFULL_log_("Crawler", "CRAWLER_START", "", "OK", `RSS_FULL Phase1 시작: ${new Date().toISOString()}`);

  const sources = RSSFULL_readSources_();
  const targets = sources.filter(s =>
    String(s.Status).toLowerCase() === "active" &&
    Number(s.Phase) <= RSSFULL_CFG.TARGET_PHASE &&
    String(s.Fetch_Type).trim().toUpperCase() === "RSS_FULL"
  );

  if (targets.length === 0) {
    RSSFULL_log_("Crawler", "CRAWLER_NO_TARGET", "", "SKIP", "RSS_FULL 대상 소스 없음");
    return;
  }

  const rawIndex = RSSFULL_buildRawUrlIndex_();

  for (const src of targets) {
    try {
      RSSFULL_fetch_(src, rawIndex);
      RSSFULL_log_("Crawler", "SOURCE_DONE", src.Source_ID, "OK", `${src.Site_Name} 완료`);
    } catch (e) {
      RSSFULL_log_("Crawler", "SOURCE_ERROR", src.Source_ID, "FAIL", String(e && e.stack ? e.stack : e));
    }
  }

  RSSFULL_log_("Crawler", "CRAWLER_END", "", "OK", `RSS_FULL Phase1 종료: ${new Date().toISOString()}`);
}

function RSSFULL_fetch_(src, rawIndex) {
  const feedUrl = RSSFULL_ensureHttps_(String(src.Target_URL || "").trim());
  const res = UrlFetchApp.fetch(feedUrl, {
    muteHttpExceptions: true,
    followRedirects: true,
    headers: { "User-Agent": "Mozilla/5.0 (GAS RSS_FULL)" }
  });
  if (res.getResponseCode() !== 200) throw new Error(`HTTP ${res.getResponseCode()} - ${feedUrl}`);

  const { mode, items } = RSSFULL_parseFeed_(res.getContentText());
  const take = Math.min(items.length, RSSFULL_CFG.MAX_ITEMS_PER_SOURCE);

  for (let i = 0; i < take; i++) {
    const parsed = (mode === "RSS") ? RSSFULL_parseRssItem_(items[i]) : RSSFULL_parseAtomEntry_(items[i]);
    if (!parsed.link) continue;

    const rawUrl = RSSFULL_normalizeUrl_(parsed.link);
    const itemUuid = RSSFULL_makeItemUUID_(rawUrl);

    let text = RSSFULL_stripHtml_(parsed.text || "").replace(/\s+/g, " ").trim();
    const limit = src.Max_Length ? Number(src.Max_Length) : 4000;
    if (text.length > limit) text = text.slice(0, limit) + " ...[Max_Length cut]";

    if (!text || text.length < RSSFULL_CFG.MIN_TEXT_LEN) {
      RSSFULL_log_("Crawler", "ITEM_SKIP_SHORT", itemUuid, "SKIP", `${src.Source_ID} | ${parsed.title || ""}`);
      continue;
    }

    const rowObj = {
      Item_UUID: itemUuid,
      Collected_At: new Date(),
      Source_ID: src.Source_ID,
      Title_Org: parsed.title || "",
      Raw_Url: rawUrl,
      Full_Text: text,
      Raw_JSON: JSON.stringify({ mode: "RSS_FULL", feedUrl, title: parsed.title || "", link: rawUrl }),
      Processed_YN: "N"
    };

    RSSFULL_upsertRawByUrl_(rawIndex, rowObj);
    RSSFULL_log_("Crawler", "ITEM_UPSERT", itemUuid, "OK", `${src.Source_ID} | len=${text.length}`);
  }
}

function RSSFULL_readSources_() {
  if (typeof readDataAsJSON === "function") return readDataAsJSON(CONFIG.SHEETS.CONF_SOURCE);
  // fallback: 직접 읽기
  return RSSFULL_readSheetAsJSON_(CONFIG.SHEETS.CONF_SOURCE);
}

function RSSFULL_parseFeed_(xml) {
  const doc = XmlService.parse(xml);
  const root = doc.getRootElement();

  if (root.getName() === "rss") {
    const channel = root.getChild("channel");
    if (!channel) throw new Error("RSS channel 없음");
    return { mode: "RSS", items: channel.getChildren("item") };
  }

  if (root.getName() === "feed") {
    const atomNs = XmlService.getNamespace("http://www.w3.org/2005/Atom");
    return { mode: "ATOM", items: root.getChildren("entry", atomNs) };
  }

  throw new Error(`지원하지 않는 피드 루트: ${root.getName()}`);
}

function RSSFULL_parseRssItem_(item) {
  const title = RSSFULL_safeText_(item, "title");
  const link = RSSFULL_safeText_(item, "link") || RSSFULL_safeText_(item, "guid");

  const contentNs = XmlService.getNamespace("http://purl.org/rss/1.0/modules/content/");
  const encoded = item.getChildText("encoded", contentNs);
  const desc = RSSFULL_safeText_(item, "description");

  return { title, link, text: encoded || desc || "" };
}

function RSSFULL_parseAtomEntry_(entry) {
  const atomNs = XmlService.getNamespace("http://www.w3.org/2005/Atom");
  const title = entry.getChildText("title", atomNs) || "";
  const content = entry.getChildText("content", atomNs) || "";
  const summary = entry.getChildText("summary", atomNs) || "";

  let link = "";
  const links = entry.getChildren("link", atomNs) || [];
  for (const l of links) {
    const rel = l.getAttribute("rel") ? l.getAttribute("rel").getValue() : "";
    const type = l.getAttribute("type") ? l.getAttribute("type").getValue() : "";
    const href = l.getAttribute("href") ? l.getAttribute("href").getValue() : "";
    if (!href) continue;
    if ((!rel || rel === "alternate") && (!type || type.includes("html"))) { link = href; break; }
  }
  if (!link && links.length && links[0].getAttribute("href")) link = links[0].getAttribute("href").getValue();

  return { title, link, text: content || summary || "" };
}

function RSSFULL_safeText_(el, childName) {
  try { return String(el.getChildText(childName) || ""); } catch (e) { return ""; }
}

/*** RAW upsert (Raw_Url) ***/
function RSSFULL_buildRawUrlIndex_() {
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

function RSSFULL_upsertRawByUrl_(rawIndex, dataObj) {
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

/*** utils (prefixed to avoid collision) ***/
function RSSFULL_ensureHttps_(u) {
  if (!u) return "";
  if (u.startsWith("https://")) return u;
  if (u.startsWith("http://")) return "https://" + u.slice(7);
  return "https://" + u;
}
function RSSFULL_normalizeUrl_(u) {
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
function RSSFULL_makeItemUUID_(normalizedUrl) {
  const bytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, normalizedUrl);
  const hex = bytes.map(b => ("0" + ((b < 0 ? b + 256 : b).toString(16))).slice(-2)).join("");
  return "ITEM_" + hex.slice(0, 8);
}
function RSSFULL_stripHtml_(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&#39;/g, "'")
    .replace(/&quot;/g, '"');
}

/*** minimal logger ***/
function RSSFULL_log_(moduleName, actionType, targetUuid, status, message) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(CONFIG.SHEETS.LOG);
  if (!sh) sh = ss.insertSheet(CONFIG.SHEETS.LOG);

  // 헤더 없으면 생성
  if (sh.getLastRow() === 0) {
    sh.getRange(1,1,1,7).setValues([["Log_UUID","Timestamp","Module","Action_Type","Target_UUID","Status","Message"]]);
  }

  const row = [
    "LOG_" + Utilities.getUuid().slice(0, 8),
    new Date(),
    moduleName,
    actionType,
    targetUuid || "",
    status,
    message || ""
  ];
  sh.appendRow(row);
}

/*** fallback sheet reader ***/
function RSSFULL_readSheetAsJSON_(sheetName) {
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
