import hashlib
import re

def ensure_https(url: str) -> str:
    if not url:
        return ""
    url = str(url).strip()
    if url.startswith("https://"):
        return url
    if url.startswith("http://"):
        return "https://" + url[7:]
    return "https://" + url

def normalize_url(u: str) -> str:
    url = str(u).strip()
    url = re.sub(r'^http://', 'https://', url, flags=re.IGNORECASE)
    url = re.sub(r'#.*$', '', url)
    
    parts = url.split("?")
    if len(parts) == 1:
        return url
        
    base = parts[0]
    kept = []
    for kv in parts[1].split("&"):
        k = kv.split("=")[0].lower()
        if not (k.startswith("utm_") or k in ("fbclid", "gclid")):
            kept.append(kv)
            
    if kept:
        return f"{base}?{'&'.join(kept)}"
    return base

def make_item_uuid(normalized_url: str) -> str:
    bytes_data = normalized_url.encode('utf-8')
    digest = hashlib.sha256(bytes_data).hexdigest()
    return "ITEM_" + digest[:8]

def strip_html(html: str) -> str:
    if not html:
        return ""
    text = str(html)
    text = re.sub(r'<script[\s\S]*?</script>', ' ', text, flags=re.IGNORECASE)
    text = re.sub(r'<style[\s\S]*?</style>', ' ', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = text.replace('&nbsp;', ' ')\
               .replace('&amp;', '&')\
               .replace('&lt;', '<')\
               .replace('&gt;', '>')\
               .replace('&#39;', "'")\
               .replace('&quot;', '"')
    return re.sub(r'\s+', ' ', text).strip()
    
def extract_title_from_md(md: str) -> str:
    s = str(md or "")
    
    # 1. Look for standard # Header
    m = re.search(r'^#\s+(.+)$', s, re.MULTILINE)
    if m and m.group(1):
        cleaned = m.group(1).strip()
        if cleaned and not cleaned.lower().startswith("warning: target url returned"):
            return cleaned
            
    # 2. Look for Setext headers: Title\n==== or Title\n----
    m2 = re.search(r'^(.+)\n[=\-]{3,}\s*$', s, re.MULTILINE)
    if m2 and m2.group(1):
        cleaned = m2.group(1).strip()
        if cleaned and not cleaned.lower().startswith("warning: target url returned"):
            return cleaned
            
    # 3. Fallback: first non-empty line that isn't a markdown link or warning
    for line in s.split('\n'):
        line = line.strip()
        if not line:
            continue
        if line.lower().startswith("warning:"):
            continue
        if re.match(r'^\[.*\]\(.*\)$', line): # Skip pure markdown links
            continue
            
        cleaned = re.sub(r'^#+\s*', '', line).strip()
        if cleaned:
            return cleaned[:140]
            
    return ""

def get_host(url: str) -> str:
    try:
        m = re.match(r'^https?://([^/]+)', url, re.IGNORECASE)
        if m:
            return m.group(1).lower()
    except Exception:
        pass
    return ""
    
def extract_urls(text: str) -> list[str]:
    s = str(text or "")
    # Simple regex for http(s) links
    found = re.findall(r'https?://[^\s<>()\]]+', s)
    return [re.sub(r'[),.\]]+$', '', u) for u in found]

def extract_links(text: str) -> dict:
    s = str(text or "")
    link_map = {}
    
    md_links = re.findall(r'\[([^\]]+)\]\((https?://[^\s<>()\]]+)[^\)]*\)', s)
    for title, url in md_links:
        url = re.sub(r'[),.\]]+$', '', url)
        title_clean = title.strip()
        if re.match(r'^!\[|read more|click|here|link', title_clean, re.IGNORECASE):
            continue
        if url not in link_map or len(title_clean) > len(link_map.get(url, "")):
            link_map[url] = title_clean
            
    bare_urls = re.findall(r'https?://[^\s<>()\]]+', s)
    for url in bare_urls:
        url = re.sub(r'[),.\]]+$', '', url)
        if url not in link_map:
            link_map[url] = ""
            
    return link_map

def unique_preserve_order(arr: list[str]) -> list[str]:
    seen = set()
    out = []
    for x in arr:
        if not x or x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out
