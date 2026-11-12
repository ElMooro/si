"""ops 6798 - READ-ONLY round 3 of the issuer audit (ops 6796 / 6797): pin the exact daily NAV + shares-outstanding
routes per issuer. Headless Chrome captures the JSON/XHR calls the issuers' own pages make (Invesco dng-api, Vanguard
investment-retail-webservice, USCF, DWS PDP api, ARK, Grayscale); direct GETs on the routes found so far (VanEck
GetFundData / FundDatasetBlock, First Trust dollarGrowth + price history, Vanguard candidates, Global X and ROBO
server-rendered snapshots for every fund, Amplify's Firestore collection through the public web config its page
ships - the key is used only at runtime and never printed). Also why GLD and MUB fell out of the packet today.
Originals embedded (gzip+base64). Nothing is written anywhere."""
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import json
import re
import subprocess
import sys
import time

import requests

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
      "Accept": "application/json, text/html;q=0.9, */*;q=0.8", "Accept-Language": "en-US,en;q=0.9"}
SNIP = re.compile(r".{0,120}(?:Shares\s*Outstanding|Outstanding\s*Shares|Net Assets|Total Net Assets|\bNAV\b|As of|DATA AS OF).{0,160}", re.I)
NOISE = re.compile(r"google|adobe|facebook|cookielaw|onetrust|hotjar|doubleclick|bing|linkedin|tiktok|gstatic|clarity|newrelic|datadog|segment|launchdarkly|smetrics|decibel|invoca|milestone|otel|transcend|spotify|braze|adsrvr|taboola|reddit|openai|usercentrics|applicationinsights|amazon-adsystem|qualtrics|demdex|omtrdc|tealium|cdn-cgi|akstat|go-mpulse|typekit|fonts\.|gtm|analytics|pixel|track|beacon|sentry|cloudfront\.net/.*\.js", re.I)
embedded = {}


def get(url, timeout=40, headers=None):
    t0 = time.time()
    try:
        resp = requests.get(url, headers={**UA, **(headers or {})}, timeout=timeout)
        return resp.status_code, resp.content, resp.headers.get("content-type", ""), resp.url, time.time() - t0
    except Exception as exc:  # noqa: BLE001
        return type(exc).__name__, b"", "", url, time.time() - t0


def walk(obj, path="", hits=None, depth=0):
    if hits is None:
        hits = []
    if depth > 8 or len(hits) > 50:
        return hits
    if isinstance(obj, dict):
        for k, v in obj.items():
            if re.search(r"share|nav|asset|outstanding|date|aum|price", str(k), re.I) and not isinstance(v, (dict, list)):
                hits.append((path + "/" + str(k), str(v)[:50]))
            walk(v, path + "/" + str(k), hits, depth + 1)
    elif isinstance(obj, list):
        for i, v in enumerate(obj[:4]):
            walk(v, path + "[%d]" % i, hits, depth + 1)
    return hits


def show(r, label, url, headers=None, embed=True, text_snips=False):
    code, body, ctype, final, took = get(url, headers=headers)
    text = body[:1500000].decode("utf-8", "ignore")
    r.log("  %-44s HTTP %-4s %4.1fs %-28s %8d B%s" % (label, code, took, ctype[:28], len(body), (" -> " + final[:80]) if final != url else ""))
    if not body or not isinstance(code, int) or code >= 400:
        t = re.search(r"<title>(.*?)</title>", text, re.S | re.I)
        r.log("      %s" % ((t.group(1).strip()[:100]) if t else text[:160].replace("\n", " ")))
        return text
    s = text.lstrip()
    if s.startswith("{") or s.startswith("["):
        try:
            j = json.loads(text)
            r.log("      JSON %s" % (("dict keys " + str(list(j.keys())[:24])) if isinstance(j, dict) else ("list[%d] first %s" % (len(j), json.dumps(j[0])[:400] if j else None))))
            for p, v in walk(j)[:36]:
                r.log("        %s = %s" % (p, v))
            if embed and len(body) <= 600000:
                embedded[label] = base64.b64encode(gzip.compress(body)).decode()
        except Exception:  # noqa: BLE001
            r.log("      JSON unparseable: %s" % text[:200])
        return text
    if text_snips:
        plain = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", re.sub(r"<script.*?</script>|<style.*?</style>", " ", text, flags=re.S)))
        for m in list(SNIP.finditer(plain))[:6]:
            r.log("      text: %s" % m.group(0)[:300])
    return text


def chrome_capture(r, pages):
    try:
        try:
            import playwright  # noqa: F401
        except ImportError:
            subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)
        from playwright.sync_api import sync_playwright
    except Exception as exc:  # noqa: BLE001
        r.warn("playwright: %s" % type(exc).__name__)
        return
    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(channel="chrome", headless=True)
        except Exception:  # noqa: BLE001
            subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
            browser = p.chromium.launch(headless=True)
        for label, url, settle_ms, retry in pages:
            ctx = browser.new_context(viewport={"width": 1366, "height": 2200}, locale="en-US")
            pg = ctx.new_page()
            calls = []

            def on_response(resp, calls=calls):
                try:
                    ct = resp.headers.get("content-type", "")
                    if re.search(r"json|csv|xlsx|octet|text/plain", ct, re.I) and not NOISE.search(resp.url):
                        body = b""
                        try:
                            if resp.status == 200:
                                body = resp.body()
                        except Exception:  # noqa: BLE001
                            pass
                        calls.append((resp.url, resp.status, ct[:30], body))
                except Exception:  # noqa: BLE001
                    pass
            pg.on("response", on_response)
            t0 = time.time()
            try:
                resp = pg.goto(url, wait_until="domcontentloaded", timeout=60000)
                try:
                    pg.wait_for_load_state("networkidle", timeout=20000)
                except Exception:  # noqa: BLE001
                    pass
                for y in (600, 1400, 2400, 3600, 5000):
                    pg.evaluate("y => window.scrollTo(0, y)", y)
                    pg.wait_for_timeout(500)
                pg.wait_for_timeout(settle_ms)
                status = resp.status if resp else None
                if retry and status and status >= 400:
                    pg.wait_for_timeout(8000)
                    resp = pg.reload(wait_until="domcontentloaded", timeout=60000)
                    pg.wait_for_timeout(settle_ms)
                    status = resp.status if resp else None
                html = pg.content()
                plain = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", re.sub(r"<script.*?</script>|<style.*?</style>", " ", html, flags=re.S)))
                snips = [m.group(0)[:260] for m in SNIP.finditer(plain)][:5]
                r.log("  %s: HTTP %s in %.1fs, title %r, %d calls captured; text: %s" % (label, status, time.time() - t0, pg.title()[:70], len(calls), snips))
                for u, st, ct, body in calls[:40]:
                    head = ""
                    if body:
                        t = body[:200000].decode("utf-8", "ignore").lstrip()
                        if t.startswith("{") or t.startswith("["):
                            try:
                                j = json.loads(t)
                                head = ("keys " + str(list(j.keys())[:14])) if isinstance(j, dict) else ("list[%d] %s" % (len(j), json.dumps(j[0])[:200] if j else ""))
                                hits = walk(j)[:12]
                                if hits:
                                    head += " | " + "; ".join("%s=%s" % h for h in hits)
                            except Exception:  # noqa: BLE001
                                head = t[:160]
                        else:
                            head = t[:160].replace("\n", " ")
                        if len(body) <= 600000 and st == 200:
                            embedded[label + " | " + u[:150]] = base64.b64encode(gzip.compress(body)).decode()
                    r.log("    %s %s %s %dB :: %s" % (st, ct, u[:170], len(body), head[:600]))
            except Exception as exc:  # noqa: BLE001
                r.warn("  %s: %s %s" % (label, type(exc).__name__, str(exc)[:160]))
            ctx.close()
        browser.close()


def main():
    from ops_report import report
    now = datetime.now(timezone.utc)
    with report("ops_6798_etf_issuer_routes_round3") as r:
        r.heading("ops 6798 - issuer routes round 3 %s (read-only)" % now.isoformat(timespec="minutes"))
        r.section("0. GLD and MUB in today's packet")
        try:
            packet = requests.get("%s/data/etf-true-flows.json?t=%d" % (PROXY, int(time.time())), timeout=90).json()
            for t in ("GLD", "MUB", "SLV", "IBIT"):
                e = (packet.get("by_etf") or {}).get(t) or {}
                r.log("  %s: source_status %s | source_error %s | identity %s" % (t, e.get("source_status"), json.dumps(e.get("source_error"))[:300], json.dumps(e.get("identity"))[:200]))
            codes = packet.get("source_status_codes") or {}
            r.log("  non-vendor status codes: %s" % json.dumps({k: v for k, v in codes.items() if not k.startswith("vendor_")})[:600])
        except Exception as exc:  # noqa: BLE001
            r.warn("packet: %s" % type(exc).__name__)

        r.section("1. direct routes")
        r.log("== VanEck")
        show(r, "FundListingUs/GetFundData", "https://www.vaneck.com/Main/FundListingUs/GetFundData")
        show(r, "FundDatasetBlock GDX", "https://www.vaneck.com/Main/FundDatasetBlock/Get/?blockId=351321&pageId=233083&ticker=GDX")
        show(r, "GDX overview page text", "https://www.vaneck.com/us/en/investments/gold-miners-etf-gdx/overview/", text_snips=True, embed=False)
        r.log("== First Trust")
        show(r, "api dollarGrowth CIBR", "https://api.ftportfolios.com/api/dollarGrowth/UnitedStates/chart/144587/11/11/2026")
        show(r, "CIBR price history page", "https://www.ftportfolios.com/Retail/Etf/EtfPriceHistory.aspx?Ticker=CIBR", text_snips=True, embed=False)
        show(r, "CIBR summary text", "https://www.ftportfolios.com/Retail/Etf/EtfSummary.aspx?Ticker=CIBR", text_snips=True, embed=False)
        r.log("== Vanguard")
        for label, url in [("VOO delayed-price", "https://investor.vanguard.com/api/investment-retail-webservice/VOO/price/delayed-price"),
                           ("VOO price history", "https://investor.vanguard.com/api/investment-retail-webservice/VOO/price/history"),
                           ("VOO price-history (dash)", "https://investor.vanguard.com/api/investment-retail-webservice/VOO/price-history"),
                           ("VOO overview", "https://investor.vanguard.com/api/investment-retail-webservice/VOO/overview"),
                           ("VOO profile", "https://investor.vanguard.com/api/investment-retail-webservice/VOO/profile"),
                           ("VOO fund-facts", "https://investor.vanguard.com/api/investment-retail-webservice/VOO/fund-facts"),
                           ("VOO price", "https://investor.vanguard.com/api/investment-retail-webservice/VOO/price"),
                           ("VOO daily-nav", "https://investor.vanguard.com/api/investment-retail-webservice/VOO/price/daily-nav"),
                           ("etfportal extrapi standard", "https://etfportal.vanguard.com/extrapi/v2/us/standard/"),
                           ("price.js component", "https://funds-web-components.cdn.web.vanguard.com/price.js"),
                           ("overview.js component", "https://funds-web-components.cdn.web.vanguard.com/overview.js")]:
            text = show(r, label, url, headers={"Referer": "https://investor.vanguard.com/investment-products/etfs/profile/voo", "Origin": "https://investor.vanguard.com"}, embed=not url.endswith(".js"))
            if url.endswith(".js") and text:
                found = sorted(set(re.findall(r"""["'`](/api/[A-Za-z0-9_./${}-]{4,120}|https?://[a-z0-9.-]*vanguard\.com/[A-Za-z0-9_./${}?=&-]{4,140})["'`]""", text)))[:40]
                r.log("      routes in script: %s" % found)
        show(r, "advisors VOO text", "https://advisors.vanguard.com/investments/products/voo/vanguard-sp-500-etf", text_snips=True, embed=False)
        r.log("== Global X (every fund, server-rendered snapshot)")
        for t in ("lit", "ura", "finx", "driv", "argt", "botz"):
            show(r, "Global X %s" % t.upper(), "https://www.globalxetfs.com/funds/%s" % t, text_snips=True, embed=False)
        r.log("== ROBO")
        text = show(r, "ROBO page", "https://roboglobaletfs.com/robo", text_snips=True, embed=False)
        m = re.search(r'<script[^>]*id="__NUXT_DATA__"[^>]*>(.*?)</script>', text, re.S)
        if m:
            try:
                arr = json.loads(m.group(1))
                strs = [x for x in arr if isinstance(x, str)]
                idx = {s: i for i, s in enumerate(arr) if isinstance(s, str)}
                for key in ("Shares Outstanding", "NAV", "Net Assets", "DATA AS OF", "Total Net Assets"):
                    i = idx.get(key)
                    r.log("      NUXT_DATA %r at %s; neighbours %s" % (key, i, [arr[j] for j in range(max(0, (i or 0) - 3), min(len(arr), (i or 0) + 6))] if i is not None else None))
                r.log("      NUXT_DATA has %d entries; numeric-looking strings near 'Shares Outstanding': %s" % (len(arr), [s for s in strs if re.fullmatch(r"[\d,]{6,}", s)][:10]))
            except Exception as exc:  # noqa: BLE001
                r.log("      NUXT_DATA unparsed: %s" % type(exc).__name__)
        r.log("== Amplify (HACK) - server-rendered snapshot + Firestore")
        show(r, "HACK page text", "https://amplifyetfs.com/hack/", text_snips=True, embed=False)
        code, body, ctype, final, took = get("https://amplifyetfs.com/wp-content/plugins/amplify-data/js/amplify-firestore.js")
        js = body.decode("utf-8", "ignore")
        cfg = {k: re.search(r'%s\s*:\s*["\']([^"\']+)["\']' % k, js) for k in ("projectId", "apiKey", "databaseURL", "appId")}
        r.log("  amplify-firestore.js: HTTP %s %d B; config keys present: %s; collections mentioned: %s" % (code, len(body), {k: bool(v) for k, v in cfg.items()}, sorted(set(re.findall(r'collection\(\s*[^,)]+,\s*["\']([a-z_]+)["\']', js)) | set(re.findall(r'["\'](funds|daily|yields|holdings|performance_monthly|dimensions)["\']', js)))))
        if cfg["projectId"] and cfg["apiKey"]:
            project = cfg["projectId"].group(1)
            key = cfg["apiKey"].group(1)
            for label, path in [("firestore funds/HACK", "funds/HACK"), ("firestore funds/HACK/daily (page 5)", "funds/HACK/daily?pageSize=5"), ("firestore funds/HACK/daily latest", "funds/HACK/daily?pageSize=3&orderBy=AsOfDate%20desc")]:
                url = "https://firestore.googleapis.com/v1/projects/%s/databases/(default)/documents/%s%skey=%s" % (project, path, "&" if "?" in path else "?", key)
                c2, b2, ct2, _, took2 = get(url)
                t2 = b2.decode("utf-8", "ignore")
                r.log("  %s: HTTP %s %.1fs %d B :: %s" % (label, c2, took2, len(b2), re.sub(r"key=[^&\s]+", "key=***", t2[:700]).replace("\n", " ")))
                if c2 == 200 and len(b2) <= 600000:
                    embedded["amplify " + label] = base64.b64encode(gzip.compress(b2)).decode()
        r.log("== Grayscale (direct)")
        show(r, "GBTC page", "https://www.grayscale.com/funds/grayscale-bitcoin-trust-etf", text_snips=True, embed=False)
        show(r, "grayscale api gbtc", "https://www.grayscale.com/api/v1/funds/gbtc", embed=True)

        r.section("2. headless Chrome captures")
        chrome_capture(r, [("Invesco QQQ", "https://www.invesco.com/us/en/financial-products/etfs/invesco-qqq", 4000, False),
                           ("Invesco RSP", "https://www.invesco.com/us/en/financial-products/etfs/invesco-sp-500-equal-weight", 3000, False),
                           ("Vanguard VOO", "https://investor.vanguard.com/investment-products/etfs/profile/voo", 6000, False),
                           ("USCF USO", "https://www.uscfinvestments.com/uso", 4000, False),
                           ("DWS ASHR", "https://etf.deutsche-am.com/en-us/ASHR-harvest-csi-300-china-a-shares-etf/", 5000, False),
                           ("ARK ARKK", "https://www.ark-funds.com/funds/arkk", 5000, False),
                           ("Grayscale GBTC", "https://www.grayscale.com/funds/grayscale-bitcoin-trust-etf", 6000, True),
                           ("Schwab SCHD", "https://www.schwabassetmanagement.com/products/schd", 3000, True)])

        r.section("originals (gzip+base64)")
        for k, v in embedded.items():
            r.log("<!-- ORIGINAL %s -->\n%s\n<!-- /ORIGINAL -->" % (k, v))
        r.ok("read-only; %d originals embedded" % len(embedded))
    sys.exit(0)


if __name__ == "__main__":
    main()
