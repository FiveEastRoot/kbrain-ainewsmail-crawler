/*******************************
 * 11_Crawler_RSS_DEEP.gs  (timeout-safe / chunked)
 * - Phase1 + Fetch_Type=RSS_DEEP
 * - RSS에서 link만 얻고 → Jina로 원문 본문 → DATA_Raw upsert
 * - 6분 제한 회피: 진행상태 저장 + 트리거로 이어달리기
 *
 * 전제: CONFIG.SHEETS.* (RAW, CONF_SOURCE, LOG) 는 이미 있음
 *      (CONFIG 재선언 금지)
 *******************************/

/** ====== 설정 (var + 가드: 중복선언 SyntaxError 방지) ====== **/
var RSSDEEP11_CFG = RSSDEEP11_CFG || {
  TARGET_PHASE: 1,
  MAX_ITEMS_PER_SOURCE: 8,
  MIN_TEXT_LEN: 200,
  JINA_TIMEOUT_SEC: 25,
  JINA_DELAY_MS: 650,
  NO_CACHE: true,
};

var RSSDEEP11_RUN = RSSDEEP11_RUN || {
  STATE_KEY: "RSSDEEP11_STATE_PHASE1",
  MAX_RUNTIME_MS: 330000,      // 5.5분 (6분 제한 대비)
  RESUME_AFTER_MS: 60 * 1000,  // 1분 뒤 재개
  HANDLER: "runCrawler_RSS_DEEP_Phase1_Chunked",
};

/** ====== 엔트리 포인트 ======
 * 수동 실행: runCrawler_RSS_DEEP_Phase1_Chunked()
 * 중단/재개는 자동으로 처리됨
 */
function runCrawler_RSS_DEEP_Phase1_Chunked() {
  var t0 = Date.now();
  RSSDEEP11_log_("Crawler", "CRAWLER_START", "", "OK", "RSS_DEEP Phase1(Chunked) 시작: " + new Date().toISOString());

  var state = RSSDEEP11_loadState_(); // {sourceIdx, itemIdx}
  var sources = RSSDEEP11_readSources_();

  var targets = sources.filter(function (s) {
    return String(s.Status).toLowerCase() === "active" &&
      Number(s.Phase) <= RSSDEEP11_CFG.TARGET_PHASE &&
      String(s.Fetch_Type).trim().toUpperCase() === "RSS_DEEP";
  });

  if (!targets.length) {
    RSSDEEP11_clearState_();
    RSSDEEP11_clearResumeTrigger_();
    RSSDEEP11_log_("Crawler", "CRAWLER_NO_TARGET", "", "SKIP", "RSS_DEEP 대상 소스 없음");
    return;
  }

  var rawIndex = RSSDEEP11_buildRawUrlIndex_();

  for (var sIdx = Number(state.sourceIdx || 0); sIdx < targets.length; sIdx++) {
    var src = targets[sIdx];

    // 소스 시작 시 itemIdx (재개라면 저장된 값, 아니면 0)
    var startItemIdx = (sIdx === Number(state.sourceIdx || 0)) ? Number(state.itemIdx || 0) : 0;

    try {
      var feedUrl = RSSDEEP11_ensureHttps_(String(src.Target_URL || "").trim());
      var res = UrlFetchApp.fetch(feedUrl, {
        muteHttpExceptions: true,
        followRedirects: true,
        headers: { "User-Agent": "Mozilla/5.0 (GAS RSS_DEEP)" }
      });

      if (res.getResponseCode() !== 200) {
        RSSDEEP11_log_("Crawler", "SOURCE_HTTP_FAIL", src.Source_ID, "FAIL", "HTTP " + res.getResponseCode() + " - " + feedUrl);
        continue;
      }

      var parsedFeed = RSSDEEP11_parseFeed_(res.getContentText());
      var items = parsedFeed.items;
      var mode = parsedFeed.mode;

      var take = Math.min(items.length, RSSDEEP11_CFG.MAX_ITEMS_PER_SOURCE);

      for (var i = startItemIdx; i < take; i++) {
        // ⛔ 시간 초과 방지 (아이템 단위로 끊기)
        if (Date.now() - t0 > RSSDEEP11_RUN.MAX_RUNTIME_MS) {
          RSSDEEP11_saveState_({ sourceIdx: sIdx, itemIdx: i });
          RSSDEEP11_scheduleResume_();
          RSSDEEP11_log_("Crawler", "CRAWLER_PAUSE", src.Source_ID, "SKIP",
            "시간 초과 방지 중단. resume sourceIdx=" + sIdx + ", itemIdx=" + i);
          return;
        }

        var it = (mode === "RSS") ? RSSDEEP11_parseRssItem_(items[i]) : RSSDEEP11_parseAtomEntry_(items[i]);
        if (!it.link) continue;

        var rawUrl = RSSDEEP11_normalizeUrl_(it.link);
        var itemUuid = RSSDEEP11_makeItemUUID_(rawUrl);

        Utilities.sleep(RSSDEEP11_CFG.JINA_DELAY_MS);

        var md = "";
        try {
          md = RSSDEEP11_jinaReadMarkdown_(rawUrl, {
            timeoutSec: RSSDEEP11_CFG.JINA_TIMEOUT_SEC,
            noCache: RSSDEEP11_CFG.NO_CACHE,
            withLinksSummary: false
          });
        } catch (e) {
          RSSDEEP11_log_("Crawler", "JINA_READ_FAIL", itemUuid, "FAIL",
            String(src.Source_ID) + " | " + rawUrl + " | " + String(e));
          continue;
        }

        var text = String(md || "").replace(/\s+/g, " ").trim();
        var limit = src.Max_Length ? Number(src.Max_Length) : 4000;
        if (text.length > limit) text = text.slice(0, limit) + " ...[Max_Length cut]";

        if (!text || text.length < RSSDEEP11_CFG.MIN_TEXT_LEN) {
          RSSDEEP11_log_("Crawler", "ITEM_SKIP_SHORT", itemUuid, "SKIP",
            String(src.Source_ID) + " | " + (it.title || ""));
          continue;
        }

        var title = it.title || RSSDEEP11_extractTitleFromMd_(md) || "";

        var rowObj = {
          Item_UUID: itemUuid,
          Collected_At: new Date(),
          Source_ID: src.Source_ID,
          Title_Org: title,
          Raw_Url: rawUrl,
          Full_Text: text,
          Raw_JSON: JSON.stringify({ mode: "RSS_DEEP", feedUrl: feedUrl, title: title, link: rawUrl }),
          Processed_YN: "N"
        };

        RSSDEEP11_upsertRawByUrl_(rawIndex, rowObj);
        RSSDEEP11_log_("Crawler", "ITEM_UPSERT", itemUuid, "OK", String(src.Source_ID) + " | len=" + text.length);
      }

      // 이 소스 완료
      RSSDEEP11_log_("Crawler", "SOURCE_DONE", src.Source_ID, "OK", String(src.Site_Name) + " 완료");

    } catch (e2) {
      RSSDEEP11_log_("Crawler", "SOURCE_ERROR", src.Source_ID, "FAIL", String(e2 && e2.stack ? e2.stack : e2));
    }

    // 다음 소스로 넘어갈 땐 itemIdx는 0으로
    state.itemIdx = 0;
  }

  // 전체 완료
  RSSDEEP11_clearState_();
  RSSDEEP11_clearResumeTrigger_();
  RSSDEEP11_log_("Crawler", "CRAWLER_END", "", "OK", "RSS_DEEP Phase1(Chunked) 종료: " + new Date().toISOString());
}

/** ====== 수동 중지(트리거/상태 초기화) ====== **/
function stopCrawler_RSS_DEEP_Phase1() {
  RSSDEEP11_clearState_();
  RSSDEEP11_clearResumeTrigger_();
  RSSDEEP11_log_("Crawler", "CRAWLER_STOP", "", "OK", "RSS_DEEP Phase1(Chunked) 중지/초기화");
}

/** ====== 상태/트리거 관리 ====== **/
function RSSDEEP11_loadState_() {
  var props = PropertiesService.getScriptProperties();
  var raw = props.getProperty(RSSDEEP11_RUN.STATE_KEY);
  if (!raw) return { sourceIdx: 0, itemIdx: 0 };
  try { return JSON.parse(raw); } catch (e) { return { sourceIdx: 0, itemIdx: 0 }; }
}
function RSSDEEP11_saveState_(obj) {
  PropertiesService.getScriptProperties().setProperty(RSSDEEP11_RUN.STATE_KEY, JSON.stringify(obj));
}
function RSSDEEP11_clearState_() {
  PropertiesService.getScriptProperties().deleteProperty(RSSDEEP11_RUN.STATE_KEY);
}
function RSSDEEP11_scheduleResume_() {
  RSSDEEP11_clearResumeTrigger_();
  ScriptApp.newTrigger(RSSDEEP11_RUN.HANDLER).timeBased().after(RSSDEEP11_RUN.RESUME_AFTER_MS).create();
}
function RSSDEEP11_clearResumeTrigger_() {
  var triggers = ScriptApp.getProjectTriggers();
  triggers.forEach(function (t) {
    if (t.getHandlerFunction && t.getHandlerFunction() === RSSDEEP11_RUN.HANDLER) {
      ScriptApp.deleteTrigger(t);
    }
  });
}

/** ====== 소스 읽기 ====== **/
function RSSDEEP11_readSources_() {
  if (typeof readDataAsJSON === "function") return readDataAsJSON(CONFIG.SHEETS.CONF_SOURCE);
  return RSSDEEP11_readSheetAsJSON_(CONFIG.SHEETS.CONF_SOURCE);
}

/** ====== RSS/ATOM 파싱 ====== **/
function RSSDEEP11_parseFeed_(xml) {
  var doc = XmlService.parse(xml);
  var root = doc.getRootElement();

  if (root.getName() === "rss") {
    var channel = root.getChild("channel");
    if (!channel) throw new Error("RSS channel 없음");
    return { mode: "RSS", items: channel.getChildren("item") };
  }

  if (root.getName() === "feed") {
    var atomNs = XmlService.getNamespace("http://www.w3.org/2005/Atom");
    return { mode: "ATOM", items: root.getChildren("entry", atomNs) };
  }

  throw new Error("지원하지 않는 피드 루트: " + root.getName());
}

function RSSDEEP11_parseRssItem_(item) {
  var title = RSSDEEP11_safeText_(item, "title");
  var link = RSSDEEP11_safeText_(item, "link") || RSSDEEP11_safeText_(item, "guid");
  return { title: title, link: link };
}

function RSSDEEP11_parseAtomEntry_(entry) {
  var atomNs = XmlService.getNamespace("http://www.w3.org/2005/Atom");
  var title = entry.getChildText("title", atomNs) || "";

  var link = "";
  var links = entry.getChildren("link", atomNs) || [];
  for (var i = 0; i < links.length; i++) {
    var l = links[i];
    var rel = l.getAttribute("rel") ? l.getAttribute("rel").getValue() : "";
    var type = l.getAttribute("type") ? l.getAttribute("type").getValue() : "";
    var href = l.getAttribute("href") ? l.getAttribute("href").getValue() : "";
    if (!href) continue;
    if ((!rel || rel === "alternate") && (!type || String(type).indexOf("html") >= 0)) { link = href; break; }
  }
  if (!link && links.length && links[0].getAttribute("href")) link = links[0].getAttribute("href").getValue();

  return { title: title, link: link };
}

function RSSDEEP11_safeText_(el, childName) {
  try { return String(el.getChildText(childName) || ""); } catch (e) { return ""; }
}

/** ====== Jina Reader ======
 * - API 키(선택): Script Properties에 "JINA_API_KEY" 저장
 *   (키 없어도 동작하나, 레이트리밋/안정성은 키가 있으면 좋아짐)
 */
function RSSDEEP11_jinaReadMarkdown_(url, opt) {
  var apiKey = PropertiesService.getScriptProperties().getProperty("JINA_API_KEY") || "";
  var finalUrl = "https://r.jina.ai/" + RSSDEEP11_ensureHttps_(url);

  var headers = {
    "User-Agent": "Mozilla/5.0 (GAS Jina Reader)",
    "x-respond-with": "markdown",
    "x-timeout": String((opt && opt.timeoutSec) ? opt.timeoutSec : 20),
  };
  if (opt && opt.noCache) headers["x-no-cache"] = "true";
  if (opt && opt.withLinksSummary) headers["X-With-Links-Summary"] = "true";
  if (apiKey) headers["Authorization"] = "Bearer " + apiKey;

  var res = UrlFetchApp.fetch(finalUrl, {
    method: "get",
    muteHttpExceptions: true,
    followRedirects: true,
    headers: headers
  });

  var code = res.getResponseCode();
  var body = res.getContentText();
  if (code !== 200) throw new Error("[JINA_HTTP_" + code + "] " + String(body).slice(0, 250));
  return body || "";
}

function RSSDEEP11_extractTitleFromMd_(md) {
  var s = String(md || "");
  var m = s.match(/^#\s+(.+)$/m);
  if (m && m[1]) return m[1].trim();
  var line = s.split("\n").map(function (x) { return String(x).trim(); }).find(function (x) { return x; }) || "";
  return line.replace(/^#+\s*/, "").slice(0, 140);
}

/** ====== DATA_Raw upsert (Raw_Url) ====== **/
function RSSDEEP11_buildRawUrlIndex_() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(CONFIG.SHEETS.RAW);
  if (!sheet) throw new Error("시트 없음: " + CONFIG.SHEETS.RAW);

  var lastRow = sheet.getLastRow();
  var lastCol = sheet.getLastColumn();
  var headers = sheet.getRange(1, 1, 1, lastCol).getValues()[0].map(String);

  var urlCol = headers.indexOf("Raw_Url");
  if (urlCol === -1) throw new Error("DATA_Raw에 Raw_Url 컬럼이 없습니다.");

  var map = new Map();
  if (lastRow >= 2) {
    var values = sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();
    values.forEach(function (row, idx) {
      var v = row[urlCol];
      if (v) map.set(String(v), idx + 2);
    });
  }
  return { sheet: sheet, headers: headers, map: map };
}

function RSSDEEP11_upsertRawByUrl_(rawIndex, dataObj) {
  var sheet = rawIndex.sheet;
  var headers = rawIndex.headers;
  var map = rawIndex.map;

  var row = headers.map(function (h) { return (dataObj[h] !== undefined ? dataObj[h] : ""); });
  var key = dataObj["Raw_Url"];

  if (map.has(key)) {
    var r = map.get(key);
    sheet.getRange(r, 1, 1, headers.length).setValues([row]);
  } else {
    sheet.appendRow(row);
    map.set(key, sheet.getLastRow());
  }
}

/** ====== 유틸 ====== **/
function RSSDEEP11_ensureHttps_(u) {
  if (!u) return "";
  if (String(u).startsWith("https://")) return u;
  if (String(u).startsWith("http://")) return "https://" + String(u).slice(7);
  return "https://" + u;
}

function RSSDEEP11_normalizeUrl_(u) {
  var url = String(u).trim().replace(/^http:\/\//i, "https://").replace(/#.*$/, "");
  var parts = url.split("?");
  if (parts.length === 1) return url;

  var base = parts[0];
  var kept = parts[1].split("&").filter(function (kv) {
    var k = kv.split("=")[0].toLowerCase();
    return !(k.indexOf("utm_") === 0 || k === "fbclid" || k === "gclid");
  });
  return kept.length ? (base + "?" + kept.join("&")) : base;
}

function RSSDEEP11_makeItemUUID_(normalizedUrl) {
  var bytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, normalizedUrl);
  var hex = bytes.map(function (b) {
    var v = b < 0 ? b + 256 : b;
    return ("0" + v.toString(16)).slice(-2);
  }).join("");
  return "ITEM_" + hex.slice(0, 8);
}

/** ====== 로깅(LOG_History) ====== **/
function RSSDEEP11_log_(moduleName, actionType, targetUuid, status, message) {
  // 기존 logEvent_ 있으면 그걸 쓰고 싶다면 아래 2줄을 활성화해도 됨:
  // if (typeof logEvent_ === "function") return logEvent_(moduleName, actionType, targetUuid, status, message);

  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sh = ss.getSheetByName(CONFIG.SHEETS.LOG);
  if (!sh) sh = ss.insertSheet(CONFIG.SHEETS.LOG);

  if (sh.getLastRow() === 0) {
    sh.getRange(1, 1, 1, 7).setValues([["Log_UUID", "Timestamp", "Module", "Action_Type", "Target_UUID", "Status", "Message"]]);
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

/** ====== fallback: 시트 JSON 읽기 ====== **/
function RSSDEEP11_readSheetAsJSON_(sheetName) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sh = ss.getSheetByName(sheetName);
  if (!sh) throw new Error("시트 없음: " + sheetName);
  var lastRow = sh.getLastRow();
  var lastCol = sh.getLastColumn();
  if (lastRow < 2) return [];
  var headers = sh.getRange(1, 1, 1, lastCol).getValues()[0].map(String);
  var values = sh.getRange(2, 1, lastRow - 1, lastCol).getValues();
  return values.map(function (r) {
    var o = {};
    headers.forEach(function (h, i) { o[h] = r[i]; });
    return o;
  });
}
