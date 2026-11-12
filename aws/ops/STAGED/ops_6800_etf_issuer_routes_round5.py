"""ops 6800 - READ-ONLY round 5: the last open routes before the issuer registry is built.
(a) SPDR Gold (GLD): api.spdrgoldshares.com historical-archive workbook - columns, first/last rows;
(b) DWS (ASHR): is the fund-facts block (outstanding shares, exact net assets) server-rendered on a plain GET, and what
    the NAV-history export returns; (c) Amplify: Firestore collection listing (page size, ordering) for the HACK daily
    documents; (d) Invesco: the dng-api templates on the product-detail page, filled and called for DBC/QQQ/RSP/BKLN/DBA,
    plus the product-detail page in Chrome; (e) USCF: how the site supplies the ALPS MarketingAPI key (header name only -
    the value is never logged) and whether a replay with it answers; (f) VanEck: overview page as-of markup for all four
    funds; (g) Fidelity FBTC and Grayscale GBTC in Chrome (patient). Raw originals embedded (gzip+base64, bounded) as
    fixtures. Nothing is written anywhere."""
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import io
import json
import re
import subprocess
import sys
import time

import requests

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
      "Accept": "application/json, text/html;q=0.9, */*;q=0.8", "Accept-Language": "en-US,en;q=0.9"}
NOISE = re.compile(r"google|adobe|facebook|cookielaw|onetrust|hotjar|doubleclick|bing|linkedin|tiktok|gstatic|clarity|newrelic|datadog|segment|launchdarkly|smetrics|decibel|invoca|milestone|otel|transcend|spotify|braze|adsrvr|taboola|reddit|openai|usercentrics|applicationinsights|amazon-adsystem|qualtrics|demdex|omtrdc|tealium|cdn-cgi|akstat|go-mpulse|typekit|fonts\.|gtm|analytics|pixel|track|beacon|sentry|medallia|evergage|maze\.co|bc0a|woff|openx|ixfd", re.I)
SNIP = re.compile(r".{0,100}(?:Shares\s*Outstanding|Outstanding\s*Shares|sharesOutstanding|Net Assets|Total Net Assets|netAssets|totalNetAssets|\bNAV\b).{0,140}", re.I)
SECRET_HEADERS = re.compile(r"key|token|auth|secret|subscription", re.I)
embedded = {}
MAX = 4000000


def redact(text):
    return re.sub(r"([?&](?:key|apikey|api_key|token)=)[^&\s\"']+", r"\1<redacted>", text)


def get(url, timeout=60, headers=None):
    t0 = time.time()
    try:
        resp = requests.get(url, headers={**UA, **(headers or {})}, timeout=timeout)
        return resp.status_code, resp.content, resp.headers.get("content-type", ""), resp.url, time.time() - t0
    except Exception as exc:  # noqa: BLE001
        return type(exc).__name__, b"", "", url, time.time() - t0


def keep(label, body):
    if body and len(body) <= MAX:
        embedded[label] = base64.b64encode(gzip.compress(body)).decode()


def show(r, label, url, headers=None, embed=True, snips=False, keys=False, max_keys=30):
    code, body, ctype, final, took = get(url, headers=headers)
    r.log("  %-46s HTTP %-4s %4.1fs %-26s %8d B%s" % (label, code, took, ctype[:26], len(body), (" -> " + redact(final[:90])) if final != url else ""))
    text = body[:2000000].decode("utf-8", "ignore")
    if isinstance(code, int) and code == 200 and body:
        if embed:
            keep(label, body)
        if keys:
            s = text.lstrip()
            if s.startswith("{") or s.startswith("["):
                try:
                    j = json.loads(text)
                    r.log("      JSON %s" % (("dict keys " + str(list(j.keys())[:max_keys])) if isinstance(j, dict) else ("list[%d] first %s" % (len(j), json.dumps(j[0])[:600] if j else None))))
                    if isinstance(j, dict):
                        for k, v in list(j.items())[:max_keys]:
                            if not isinstance(v, (dict, list)):
                                r.log("        /%s = %s" % (k, str(v)[:120]))
                except Exception:  # noqa: BLE001
                    r.log("      not JSON: %s" % text[:200])
        if snips:
            plain = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", re.sub(r"<script.*?</script>|<style.*?</style>", " ", text, flags=re.S)))
            for m in list(SNIP.finditer(plain))[:6]:
                r.log("      text: %s" % m.group(0)[:260])
    else:
        t = re.search(r"<title>(.*?)</title>", text, re.S | re.I)
        r.log("      %s" % ((t.group(1).strip()[:100]) if t else text[:160].replace("\n", " ")))
    return code, body, text


def xlsx_peek(r, label, body):
    try:
        import openpyxl  # noqa: F401
    except ImportError:
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "openpyxl"], check=False)
    try:
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(body), read_only=True, data_only=True)
        for ws in wb.worksheets[:4]:
            rows = list(ws.iter_rows(values_only=True))
            r.log("      %s sheet %r: %d rows x %d cols" % (label, ws.title, len(rows), max((len(x) for x in rows), default=0)))
            for i, row in enumerate(rows[:12]):
                r.log("        row %d: %s" % (i + 1, [str(c)[:40] if c is not None else None for c in row][:14]))
            for i, row in enumerate(rows[-3:]):
                r.log("        row %d: %s" % (len(rows) - 3 + i + 1, [str(c)[:40] if c is not None else None for c in row][:14]))
    except Exception as exc:  # noqa: BLE001
        r.warn("      %s workbook: %s %s" % (label, type(exc).__name__, str(exc)[:160]))


def chrome(r, pages, header_hosts=()):
    try:
        try:
            import playwright  # noqa: F401
        except ImportError:
            subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)
        from playwright.sync_api import sync_playwright
    except Exception as exc:  # noqa: BLE001
        r.warn("playwright: %s" % type(exc).__name__)
        return {}
    captured = {}
    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(channel="chrome", headless=True)
        except Exception:  # noqa: BLE001
            subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
            browser = p.chromium.launch(headless=True)
        for label, url, settle_ms, wait_challenge in pages:
            ctx = browser.new_context(viewport={"width": 1366, "height": 2400}, locale="en-US")
            pg = ctx.new_page()
            calls = []

            def on_request(req, calls=calls):
                try:
                    if any(h in req.url for h in header_hosts):
                        hdrs = req.all_headers()
                        captured.setdefault(req.url, hdrs)
                except Exception:  # noqa: BLE001
                    pass

            def on_response(resp, calls=calls):
                try:
                    ct = resp.headers.get("content-type", "")
                    if re.search(r"json|csv|xlsx|octet|text/plain|xml", ct, re.I) and not NOISE.search(resp.url):
                        body = b""
                        try:
                            if resp.status == 200:
                                body = resp.body()
                        except Exception:  # noqa: BLE001
                            pass
                        calls.append((resp.url, resp.status, ct[:30], body))
                except Exception:  # noqa: BLE001
                    pass
            pg.on("request", on_request)
            pg.on("response", on_response)
            t0 = time.time()
            try:
                resp = pg.goto(url, wait_until="domcontentloaded", timeout=60000)
                status = resp.status if resp else None
                if wait_challenge:
                    for _ in range(8):
                        pg.wait_for_timeout(5000)
                        if not re.search(r"moment|checkpoint|denied|attention|verify", pg.title(), re.I):
                            break
                try:
                    pg.wait_for_load_state("networkidle", timeout=20000)
                except Exception:  # noqa: BLE001
                    pass
                for y in (700, 1500, 2500, 3500, 5000, 7000):
                    pg.evaluate("y => window.scrollTo(0, y)", y)
                    pg.wait_for_timeout(500)
                pg.wait_for_timeout(settle_ms)
                html = pg.content()
                keep(label + " | rendered html", html.encode("utf-8"))
                plain = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", re.sub(r"<script.*?</script>|<style.*?</style>", " ", html, flags=re.S)))
                snips = [m.group(0)[:240] for m in SNIP.finditer(plain)][:6]
                r.log("  %s: HTTP %s in %.1fs, title %r, %d bytes, %d calls; text: %s" % (label, status, time.time() - t0, pg.title()[:70], len(html), len(calls), snips))
                for u, st, ct, body in calls[:45]:
                    head = ""
                    if body:
                        t = body[:300000].decode("utf-8", "ignore").lstrip()
                        if t.startswith("{") or t.startswith("["):
                            try:
                                j = json.loads(t)
                                head = ("keys " + str(list(j.keys())[:24])) if isinstance(j, dict) else ("list[%d] %s" % (len(j), json.dumps(j[0])[:400] if j else ""))
                            except Exception:  # noqa: BLE001
                                head = t[:200]
                        else:
                            head = t[:200].replace("\n", " ")
                        if st == 200:
                            keep(label + " | " + redact(u)[:160], body)
                    r.log("    %s %s %s %dB :: %s" % (st, ct, redact(u)[:170], len(body), head[:600]))
            except Exception as exc:  # noqa: BLE001
                r.warn("  %s: %s %s" % (label, type(exc).__name__, str(exc)[:160]))
            ctx.close()
        browser.close()
    return captured


def main():
    from ops_report import report
    now = datetime.now(timezone.utc)
    with report("ops_6800_etf_issuer_routes_round5") as r:
        r.heading("ops 6800 - issuer routes round 5 %s (read-only)" % now.isoformat(timespec="minutes"))
        r.section("1. direct routes")
        r.log("== SPDR Gold (GLD) historical archive")
        code, body, text = show(r, "GLD historical-archive", "https://api.spdrgoldshares.com/api/v1/historical-archive?product=gld&exchange=NYSE&lang=en")
        if isinstance(code, int) and code == 200 and body[:2] == b"PK":
            xlsx_peek(r, "GLD archive", body)
        elif isinstance(code, int) and code == 200:
            r.log("      head: %s" % body[:300])
        r.log("== DWS (ASHR) plain GET: is the fund-facts block server-rendered?")
        code, body, text = show(r, "DWS ASHR page (plain GET)", "https://etf.deutsche-am.com/en-us/ASHR-harvest-csi-300-china-a-shares-etf/", snips=True)
        if isinstance(code, int) and code == 200:
            for kw in ("Outstanding shares", "Net assets", "NAV"):
                hits = [m.start() for m in re.finditer(re.escape(kw), text)]
                r.log("      %r occurrences: %d" % (kw, len(hits)))
                for h in hits[:2]:
                    r.log("        %s" % re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", text[h:h + 1200]))[:260])
        show(r, "DWS ASHR NAV export (Export/etf/ASHR/Performance)", "https://etf.deutsche-am.com/api/pdp/en-us/Export/etf/ASHR/Performance")
        if embedded.get("DWS ASHR NAV export (Export/etf/ASHR/Performance)"):
            raw = gzip.decompress(base64.b64decode(embedded["DWS ASHR NAV export (Export/etf/ASHR/Performance)"]))
            if raw[:2] == b"PK":
                xlsx_peek(r, "DWS export", raw)
            else:
                r.log("      head: %s" % raw[:400])
        r.log("== Amplify Firestore listing (key read from the site's own JS at run time; never logged)")
        code, body, _, _, _ = get("https://amplifyetfs.com/wp-content/plugins/amplify-data/js/amplify-firestore.js")
        js = body.decode("utf-8", "ignore")
        pid = re.search(r'projectId\s*:\s*["\']([^"\']+)["\']', js)
        key = re.search(r'apiKey\s*:\s*["\']([^"\']+)["\']', js)
        r.log("  amplify-firestore.js HTTP %s %d B; projectId %s; apiKey present %s (length %s)" % (code, len(body), pid.group(1) if pid else None, bool(key), len(key.group(1)) if key else None))
        if pid and key:
            base = "https://firestore.googleapis.com/v1/projects/%s/databases/(default)/documents/" % pid.group(1)
            for label, path in [("HACK daily list pageSize=1000", "funds/HACK/daily?pageSize=1000"),
                                ("HACK daily list pageSize=12 orderBy name desc", "funds/HACK/daily?pageSize=12&orderBy=__name__%20desc"),
                                ("HACK daily list pageSize=12 orderBy name", "funds/HACK/daily?pageSize=12&orderBy=__name__"),
                                ("funds collection list", "funds?pageSize=200&mask.fieldPaths=Ticker")]:
                url = base + path + "&key=" + key.group(1)
                c2, b2, _, _, took2 = get(url, timeout=120)
                t2 = b2.decode("utf-8", "ignore")
                summary = ""
                if c2 == 200:
                    try:
                        j = json.loads(t2)
                        docs = j.get("documents", [])
                        names = [d["name"].rsplit("/", 1)[-1] for d in docs]
                        summary = "documents %d, first %s, last %s, nextPageToken %s" % (len(docs), names[:3], names[-3:], bool(j.get("nextPageToken")))
                        if path.startswith("funds/HACK/daily") and docs:
                            summary += "; fields of first: %s" % sorted(docs[0].get("fields", {}).keys())
                        if len(b2) <= MAX:
                            keep("Amplify firestore " + label, b2)
                    except Exception as exc:  # noqa: BLE001
                        summary = "parse %s: %s" % (type(exc).__name__, t2[:200])
                else:
                    summary = re.sub(r"\s+", " ", t2[:300])
                r.log("  %-46s HTTP %-4s %4.1fs %8d B :: %s" % (label, c2, took2, len(b2), redact(summary)))
        r.log("== Invesco product-detail page templates -> dng-api")
        code, body, text = show(r, "Invesco DBC product-detail (plain GET)", "https://www.invesco.com/us/financial-products/etfs/product-detail?audienceType=Investor&ticker=DBC", snips=True)
        templates = []
        if isinstance(code, int) and code == 200:
            page = text.replace("&#34;", '"').replace("&quot;", '"').replace("&amp;", "&").replace("\\/", "/")
            templates = sorted(set(re.findall(r"https://dng-api\.invesco\.com/[A-Za-z0-9_/{}.&;=?%-]+", page)))
            r.log("      dng-api templates (%d):" % len(templates))
            for tpl in templates[:60]:
                r.log("        %s" % tpl[:220])
            for kw in ("sharesOutstanding", "totalNetAssets", "Shares outstanding", "navHistory", "nav-history", "priceHistory", "dailyNav", "pricing"):
                r.log("      %r mentions: %d :: %s" % (kw, len(re.findall(re.escape(kw), page)), [re.sub(r"\s+", " ", page[m.start() - 120:m.end() + 160]) for m in list(re.finditer(re.escape(kw), page))[:2]]))
        filled = []
        for tpl in templates:
            for ticker in ("DBC", "QQQ"):
                u = tpl.replace("{locale}", "en_US").replace("{id}", ticker).replace("{uniqueIdentifier}", "ticker").replace("{productType}", "ETF").replace("{audienceType}", "Investor")
                u = re.sub(r"\{[^}]+\}", "", u)
                if "{" not in u and (ticker, u) not in filled:
                    filled.append((ticker, u))
        for ticker, u in filled[:40]:
            show(r, "dng %s %s" % (ticker, u.split("shareclasses/")[-1][:60] if "shareclasses/" in u else u[-60:]), u, headers={"Accept": "application/json", "Origin": "https://www.invesco.com", "Referer": "https://www.invesco.com/"}, keys=True, max_keys=40)
        r.log("== USCF / ALPS: how the site supplies the key (header name only; value never logged)")
        code, body, text = show(r, "USCF api_key.php", "https://www.uscfinvestments.com/site-template/assets/javascript/api_key.php", embed=False)
        site_key = None
        if isinstance(code, int) and code == 200:
            names = re.findall(r"(?:var|let|const)\s+([A-Za-z_$][\w$]*)\s*=", text)
            r.log("      declared names: %s; bytes %d" % (names[:10], len(text)))
            m = re.search(r"=\s*[\"']([A-Za-z0-9+/=_.-]{16,})[\"']", text)
            site_key = m.group(1) if m else None
            r.log("      key-like literal present: %s (length %s)" % (bool(site_key), len(site_key) if site_key else None))
        code, body, text = show(r, "USCF api_INIT", "https://www.uscfinvestments.com/site-template/assets/javascript/api_INIT-v1.2.2.min.js", embed=True)
        if isinstance(code, int) and code == 200:
            hdr = sorted(set(re.findall(r"setRequestHeader\(\s*[\"']([^\"']+)[\"']", text)) | set(re.findall(r"headers\s*:\s*\{([^}]{0,200})\}", text)) | set(re.findall(r"[\"']([A-Za-z-]*(?:Key|Token|Authorization)[A-Za-z-]*)[\"']\s*:", text)))
            r.log("      header-setting fragments: %s" % [h[:80] for h in hdr][:12])
            r.log("      alps hosts: %s" % sorted(set(re.findall(r"https?://[a-z0-9.-]*alps[a-z0-9./-]*", text)))[:6])
        captured = chrome(r, [("USCF USO (header capture)", "https://www.uscfinvestments.com/uso", 4000, False)], header_hosts=("alpsinc.com",))
        alps_headers = None
        for u, hdrs in captured.items():
            names = sorted(hdrs.keys())
            r.log("  request headers on %s: %s" % (redact(u)[:120], names))
            if any(SECRET_HEADERS.search(k) for k in names):
                alps_headers = {k: v for k, v in hdrs.items() if SECRET_HEADERS.search(k) or k.lower() in ("origin", "referer", "accept")}
        if alps_headers:
            r.log("  replaying ALPS requests with the captured headers (%s)" % sorted(alps_headers.keys()))
            for label, url in [("ALPS dailyprice USO", "https://secure.alpsinc.com/MarketingAPI/api/v1/dailyprice/USO"), ("ALPS dailyprice UNG", "https://secure.alpsinc.com/MarketingAPI/api/v1/dailyprice/UNG"),
                               ("ALPS historicalnav USO", "https://secure.alpsinc.com/MarketingAPI/api/v1/historicalnav/USO/inception-today")]:
                c3, b3, ct3, _, took3 = get(url, headers=alps_headers)
                t3 = b3.decode("utf-8", "ignore")
                leak = any(v and len(v) >= 12 and v in t3 for v in alps_headers.values())
                head = ""
                if c3 == 200 and not leak:
                    keep(label, b3)
                    try:
                        j = json.loads(t3)
                        head = ("list[%d] keys %s" % (len(j), sorted(j[0].keys()) if j and isinstance(j[0], dict) else "")) if isinstance(j, list) else ("dict keys " + str(list(j.keys())[:30]))
                        if isinstance(j, list) and j and isinstance(j[0], dict) and "nav" in j[0]:
                            head += " :: " + json.dumps({k: j[0].get(k) for k in ("symbol", "nav", "navextended", "navtotal", "totalassets", "sharesoutstanding", "shares", "displaydate", "asofdate") if k in j[0]})
                    except Exception:  # noqa: BLE001
                        head = t3[:200]
                r.log("  %-46s HTTP %-4s %4.1fs %8d B leak=%s :: %s" % (label, c3, took3, len(b3), leak, head[:700]))
        else:
            r.warn("  no keyed ALPS request captured in Chrome")
        r.log("== VanEck overview as-of markup (all four)")
        for t, slug in (("GDX", "gold-miners-etf-gdx"), ("SMH", "semiconductor-etf-smh"), ("NLR", "uranium-and-nuclear-etf-nlr"), ("VNM", "vietnam-etf-vnm")):
            code, body, text = show(r, "VanEck %s overview" % t, "https://www.vaneck.com/us/en/investments/%s/overview/" % slug, embed=True)
            if isinstance(code, int) and code == 200:
                items = re.findall(r'item-title[^>]*>\s*([^<]{1,40}?)\s*</div>\s*<div class="item-value">\s*(?:<span class="dollar-sign">\$</span>)?\s*([^<]{1,30}?)\s*(?:<span|</div>).{0,1500}?class="as-of-date">as of ([A-Za-z]+ \d{1,2}, \d{4})</span>', text, re.S)
                r.log("      dated items: %s" % [(a.strip(), b.strip(), c) for a, b, c in items][:8])
                ids = sorted(set(re.findall(r"FundDatasetBlock/Get/\?blockId=(\d+)&(?:amp;)?pageId=(\d+)&(?:amp;)?ticker=([A-Z]+)", text)))
                r.log("      dataset block ids: %s" % ids)
        r.section("2. Chrome captures")
        chrome(r, [("Invesco DBC product-detail", "https://www.invesco.com/us/financial-products/etfs/product-detail?audienceType=Investor&ticker=DBC", 6000, False),
                   ("Invesco QQQ product-detail", "https://www.invesco.com/us/financial-products/etfs/product-detail?audienceType=Investor&ticker=QQQ", 6000, False),
                   ("Fidelity FBTC (fidelity.com/etfs)", "https://www.fidelity.com/etfs/fbtc", 5000, False),
                   ("Fidelity FBTC (institutional snapshot)", "https://institutional.fidelity.com/app/funds-and-products/etf/snapshot/FBTC.html", 5000, False),
                   ("Grayscale GBTC (patient)", "https://www.grayscale.com/funds/grayscale-bitcoin-trust-etf", 5000, True)])
        r.section("originals (gzip+base64)")
        for k, v in embedded.items():
            r.log("<!-- ORIGINAL %s -->\n%s\n<!-- /ORIGINAL -->" % (redact(k), v))
        r.ok("read-only; %d originals embedded" % len(embedded))
    sys.exit(0)


if __name__ == "__main__":
    main()
