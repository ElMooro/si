"""ops 6799 - READ-ONLY round 4: the last discovery questions and the raw originals the parsers will be written against.
(a) Vanguard: the advisors product page in Chrome (its JSON calls) - does Vanguard publish daily shares outstanding or
net assets anywhere; (b) Invesco: the real product pages in Chrome (DBC, QQQ home) - the dng-api calls; (c) DWS: the
pdp api components (fund facts / pricing details) by name; (d) ARK: Cloudflare challenge with patience; (e) SPDR Gold
(GLD): spdrgoldshares.com historical workbook; (f) ALPS MarketingAPI (USCF): dailyprice + historicalnav for USO and
UNG; VanEck: each fund's overview page -> FundDatasetBlock ids -> the JSON; First Trust: CIBR summary + price history
pages; Global X: BOTZ raw page; ROBO raw page; Amplify HACK raw page + Firestore latest docs. Raw originals embedded
(gzip+base64, bounded) as fixtures. Nothing is written anywhere."""
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
sys.path.insert(0, str(ROOT / "aws" / "shared"))

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
      "Accept": "application/json, text/html;q=0.9, */*;q=0.8", "Accept-Language": "en-US,en;q=0.9"}
NOISE = re.compile(r"google|adobe|facebook|cookielaw|onetrust|hotjar|doubleclick|bing|linkedin|tiktok|gstatic|clarity|newrelic|datadog|segment|launchdarkly|smetrics|decibel|invoca|milestone|otel|transcend|spotify|braze|adsrvr|taboola|reddit|openai|usercentrics|applicationinsights|amazon-adsystem|qualtrics|demdex|omtrdc|tealium|cdn-cgi|akstat|go-mpulse|typekit|fonts\.|gtm|analytics|pixel|track|beacon|sentry|medallia|evergage|maze\.co|bc0a|woff|openx|ixfd", re.I)
SNIP = re.compile(r".{0,100}(?:Shares\s*Outstanding|Outstanding\s*Shares|sharesOutstanding|Net Assets|Total Net Assets|netAssets|totalNetAssets|\bNAV\b).{0,140}", re.I)
embedded = {}
MAX = 1500000


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


def show(r, label, url, headers=None, embed=True, snips=False, keys=False):
    code, body, ctype, final, took = get(url, headers=headers)
    r.log("  %-46s HTTP %-4s %4.1fs %-26s %8d B%s" % (label, code, took, ctype[:26], len(body), (" -> " + final[:70]) if final != url else ""))
    text = body[:2000000].decode("utf-8", "ignore")
    if isinstance(code, int) and code == 200 and body:
        if embed:
            keep(label, body)
        if keys:
            s = text.lstrip()
            if s.startswith("{") or s.startswith("["):
                try:
                    j = json.loads(text)
                    r.log("      JSON %s" % (("dict keys " + str(list(j.keys())[:30])) if isinstance(j, dict) else ("list[%d] first %s" % (len(j), json.dumps(j[0])[:500] if j else None))))
                except Exception:  # noqa: BLE001
                    r.log("      not JSON: %s" % text[:200])
        if snips:
            plain = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", re.sub(r"<script.*?</script>|<style.*?</style>", " ", text, flags=re.S)))
            for m in list(SNIP.finditer(plain))[:5]:
                r.log("      text: %s" % m.group(0)[:260])
            raw_hits = [m.group(0) for m in re.finditer(r".{0,80}(?:sharesOutstanding|shares_outstanding|SharesOutstanding|Shares Outstanding)[^<]{0,120}", text)][:4]
            for h in raw_hits:
                r.log("      raw: %s" % re.sub(r"\s+", " ", h)[:220])
    else:
        t = re.search(r"<title>(.*?)</title>", text, re.S | re.I)
        r.log("      %s" % ((t.group(1).strip()[:100]) if t else text[:160].replace("\n", " ")))
    return code, body, text


def chrome(r, pages):
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
        for label, url, settle_ms, wait_challenge in pages:
            ctx = browser.new_context(viewport={"width": 1366, "height": 2400}, locale="en-US")
            pg = ctx.new_page()
            calls = []

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
            pg.on("response", on_response)
            t0 = time.time()
            try:
                resp = pg.goto(url, wait_until="domcontentloaded", timeout=60000)
                status = resp.status if resp else None
                if wait_challenge:
                    for _ in range(6):
                        pg.wait_for_timeout(5000)
                        if "moment" not in pg.title().lower() and "checkpoint" not in pg.title().lower() and "denied" not in pg.title().lower():
                            break
                try:
                    pg.wait_for_load_state("networkidle", timeout=20000)
                except Exception:  # noqa: BLE001
                    pass
                for y in (700, 1500, 2500, 3500, 5000, 7000):
                    pg.evaluate("y => window.scrollTo(0, y)", y)
                    pg.wait_for_timeout(600)
                pg.wait_for_timeout(settle_ms)
                html = pg.content()
                keep(label + " | rendered html", html.encode("utf-8"))
                plain = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", re.sub(r"<script.*?</script>|<style.*?</style>", " ", html, flags=re.S)))
                snips = [m.group(0)[:240] for m in SNIP.finditer(plain)][:6]
                r.log("  %s: HTTP %s in %.1fs, title %r, %d bytes, %d calls; text: %s" % (label, status, time.time() - t0, pg.title()[:70], len(html), len(calls), snips))
                for u, st, ct, body in calls[:40]:
                    head = ""
                    if body:
                        t = body[:300000].decode("utf-8", "ignore").lstrip()
                        if t.startswith("{") or t.startswith("["):
                            try:
                                j = json.loads(t)
                                head = ("keys " + str(list(j.keys())[:20])) if isinstance(j, dict) else ("list[%d] %s" % (len(j), json.dumps(j[0])[:300] if j else ""))
                            except Exception:  # noqa: BLE001
                                head = t[:200]
                        else:
                            head = t[:200].replace("\n", " ")
                        if st == 200:
                            keep(label + " | " + u[:160], body)
                    r.log("    %s %s %s %dB :: %s" % (st, ct, u[:170], len(body), head[:500]))
            except Exception as exc:  # noqa: BLE001
                r.warn("  %s: %s %s" % (label, type(exc).__name__, str(exc)[:160]))
            ctx.close()
        browser.close()


def main():
    from ops_report import report
    now = datetime.now(timezone.utc)
    with report("ops_6799_etf_issuer_fixtures") as r:
        r.heading("ops 6799 - issuer routes round 4 + fixtures %s (read-only)" % now.isoformat(timespec="minutes"))
        r.section("1. direct originals")
        r.log("== USCF via ALPS MarketingAPI")
        for t in ("USO", "UNG"):
            show(r, "ALPS dailyprice %s" % t, "https://secure.alpsinc.com/MarketingAPI/api/v1/dailyprice/%s" % t, keys=True)
            show(r, "ALPS historicalnav %s" % t, "https://secure.alpsinc.com/MarketingAPI/api/v1/historicalnav/%s/inception-today" % t, keys=False)
        r.log("== VanEck: FundDatasetBlock ids from each overview page")
        for t, slug in (("GDX", "gold-miners-etf-gdx"), ("SMH", "semiconductor-etf-smh"), ("NLR", "uranium-and-nuclear-etf-nlr"), ("VNM", "vietnam-etf-vnm")):
            code, body, text = show(r, "VanEck %s overview" % t, "https://www.vaneck.com/us/en/investments/%s/overview/" % slug, embed=False)
            ids = sorted(set(re.findall(r"FundDatasetBlock/Get/\?blockId=(\d+)&(?:amp;)?pageId=(\d+)&(?:amp;)?ticker=([A-Z]+)", text)))
            r.log("      dataset block ids: %s" % ids)
            for block, page, tick in ids[:1]:
                show(r, "VanEck FundDatasetBlock %s" % t, "https://www.vaneck.com/Main/FundDatasetBlock/Get/?blockId=%s&pageId=%s&ticker=%s" % (block, page, tick), keys=True)
        r.log("== First Trust CIBR")
        show(r, "First Trust CIBR summary", "https://www.ftportfolios.com/Retail/Etf/EtfSummary.aspx?Ticker=CIBR", snips=True)
        show(r, "First Trust CIBR price history", "https://www.ftportfolios.com/Retail/Etf/EtfPriceHistory.aspx?Ticker=CIBR", snips=True)
        r.log("== Global X / ROBO / Amplify raw pages")
        show(r, "Global X BOTZ raw", "https://www.globalxetfs.com/funds/botz", snips=True)
        show(r, "ROBO raw", "https://roboglobaletfs.com/robo", snips=True)
        show(r, "Amplify HACK raw", "https://amplifyetfs.com/hack/", snips=True)
        code, body, _, _, _ = get("https://amplifyetfs.com/wp-content/plugins/amplify-data/js/amplify-firestore.js")
        js = body.decode("utf-8", "ignore")
        pid = re.search(r'projectId\s*:\s*["\']([^"\']+)["\']', js)
        key = re.search(r'apiKey\s*:\s*["\']([^"\']+)["\']', js)
        if pid and key:
            for label, path in [("Amplify firestore HACK daily 2026-11-11", "funds/HACK/daily/2026-11-11"), ("Amplify firestore HACK daily 2026-11-10", "funds/HACK/daily/2026-11-10"), ("Amplify firestore HACK fund_metadata", "funds/HACK/fund_metadata?pageSize=3")]:
                url = "https://firestore.googleapis.com/v1/projects/%s/databases/(default)/documents/%s%skey=%s" % (pid.group(1), path, "&" if "?" in path else "?", key.group(1))
                c2, b2, _, _, took2 = get(url)
                r.log("  %-46s HTTP %-4s %4.1fs %8d B :: %s" % (label, c2, took2, len(b2), re.sub(r"\s+", " ", b2.decode("utf-8", "ignore")[:600])))
                if c2 == 200:
                    keep(label, b2)
        r.log("== SPDR Gold (GLD) - the trust's own site")
        for label, url in [("spdrgoldshares historical data page", "https://www.spdrgoldshares.com/usa/historical-data/"), ("spdrgoldshares GLD archive xls", "https://www.spdrgoldshares.com/assets/dynamic/GLD/GLD_US_archive_EN.csv"),
                           ("spdrgoldshares GLD archive csv (alt)", "https://www.spdrgoldshares.com/assets/dynamic/GLD/GLD_US_archive_EN.xls"), ("spdrgoldshares usa home", "https://www.spdrgoldshares.com/usa/")]:
            code, body, text = show(r, label, url, snips=True)
            if "historical" in label and isinstance(code, int) and code == 200:
                links = sorted(set(re.findall(r'href="([^"]+\.(?:xls|xlsx|csv))"', text, re.I)))
                r.log("      file links: %s" % links[:10])
                for link in links[:3]:
                    u = link if link.startswith("http") else "https://www.spdrgoldshares.com" + link
                    show(r, "GLD file %s" % u.split("/")[-1], u)
        r.log("== DWS ASHR pdp components by name")
        for comp in ("fundFacts", "fundfacts", "keyFacts", "pricingDetails", "pricingdetails", "priceDetails", "navHistory", "navhistory", "priceHistory", "overview", "fundOverview", "shareClass", "details"):
            show(r, "DWS ASHR %s" % comp, "https://etf.deutsche-am.com/api/pdp/en-us/etf/ASHR-harvest-csi-300-china-a-shares-etf/%s" % comp, keys=True, embed=False)
        r.section("2. Chrome captures")
        chrome(r, [("Vanguard advisors VOO", "https://advisors.vanguard.com/investments/products/voo/vanguard-sp-500-etf", 5000, False),
                   ("Invesco DBC", "https://www.invesco.com/us/en/financial-products/etfs/invesco-db-commodity-index", 6000, False),
                   ("Invesco QQQ home", "https://www.invesco.com/qqq-etf/en/home.html", 6000, False),
                   ("DWS ASHR fund facts tab", "https://etf.deutsche-am.com/en-us/ASHR-harvest-csi-300-china-a-shares-etf/#fund-facts", 6000, False),
                   ("ARK ARKK (challenge)", "https://www.ark-funds.com/funds/arkk", 5000, True)])
        r.section("originals (gzip+base64)")
        for k, v in embedded.items():
            r.log("<!-- ORIGINAL %s -->\n%s\n<!-- /ORIGINAL -->" % (k, v))
        r.ok("read-only; %d originals embedded" % len(embedded))
    sys.exit(0)


if __name__ == "__main__":
    main()
