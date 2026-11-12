"""ops 6797 - READ-ONLY round 2 of the issuer audit (ops 6796): where each issuer's own site gets its daily NAV and
shares outstanding. For every issuer of the 48 uncovered funds: fetch the product page(s), extract the embedded data
blobs (__NEXT_DATA__ / __NUXT__ / window.__INITIAL_* / data-* attributes) and every URL that looks like a data route;
fetch the page's own data scripts and read their API bases; show the text around 'Shares Outstanding' and 'NAV';
call the candidate JSON routes (Invesco dng-api, DWS api, Vanguard fund-id routes, Global X / VanEck discovered
routes); open the bot-walled issuers (Schwab, Grayscale) in headless Chrome. Originals of successful JSON / CSV
answers are embedded (gzip+base64) for the parsers. Nothing is written anywhere."""
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

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
      "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.9,*/*;q=0.8", "Accept-Language": "en-US,en;q=0.9"}
ROUTE = re.compile(r"""["'](https?://[^"'\s<>]{10,220}|/[^"'\s<>]{6,200})["']""")
ROUTE_WORDS = re.compile(r"api|json|csv|xlsx|download|export|nav|history|holdings|fund|price|data", re.I)
SNIP = re.compile(r".{0,140}(?:Shares\s*Outstanding|sharesOutstanding|shares_outstanding|Outstanding\s*Shares|Net Asset Value|\bNAV\b).{0,200}", re.I)
BLOB = re.compile(r"<script[^>]*id=\"(__NEXT_DATA__|__NUXT_DATA__)\"[^>]*>(.*?)</script>|window\.(__NUXT__|__INITIAL_STATE__|__PRELOADED_STATE__|__DATA__)\s*=\s*(\{.*?\})\s*;?\s*</script>", re.S)
embedded = {}


def get(url, timeout=40, headers=None):
    t0 = time.time()
    try:
        resp = requests.get(url, headers={**UA, **(headers or {})}, timeout=timeout)
        return resp.status_code, resp.content, resp.headers.get("content-type", ""), resp.url, time.time() - t0
    except Exception as exc:  # noqa: BLE001
        return type(exc).__name__, b"", "", url, time.time() - t0


def routes(text, host):
    out = set()
    for m in ROUTE.finditer(text):
        u = m.group(1)
        if ROUTE_WORDS.search(u) and not re.search(r"\.(png|jpe?g|svg|gif|woff2?|ttf|css|ico)(\?|$)", u, re.I) and not re.search(r"google|facebook|twitter|linkedin|adobe|doubleclick|gstatic|youtube|hotjar|cookielaw|onetrust|tiktok|bing", u, re.I):
            out.add(u if u.startswith("http") else host + u)
    return sorted(out)


def walk(obj, path="", hits=None, depth=0):
    """every key/value whose key mentions shares / nav / asset / date, with its path"""
    if hits is None:
        hits = []
    if depth > 9 or len(hits) > 60:
        return hits
    if isinstance(obj, dict):
        for k, v in obj.items():
            if re.search(r"share|nav|asset|outstanding|date|price|aum", str(k), re.I) and not isinstance(v, (dict, list)):
                hits.append((path + "/" + str(k), str(v)[:60]))
            walk(v, path + "/" + str(k), hits, depth + 1)
    elif isinstance(obj, list):
        for i, v in enumerate(obj[:5]):
            walk(v, path + "[%d]" % i, hits, depth + 1)
    return hits


def inspect(r, label, url, headers=None, embed_kind=None, script_grep=False):
    code, body, ctype, final, took = get(url, headers=headers)
    text = body[:900000].decode("utf-8", "ignore")
    host = re.match(r"https?://[^/]+", final or url).group(0) if re.match(r"https?://[^/]+", final or url) else ""
    r.log("  %-40s HTTP %-4s %4.1fs %-32s %7d B%s" % (label, code, took, ctype[:32], len(body), (" -> " + final[:90]) if final != url else ""))
    if not body or not isinstance(code, int) or code >= 400:
        if body:
            t = re.search(r"<title>(.*?)</title>", text, re.S | re.I)
            r.log("      title %s" % ((t.group(1).strip()[:80]) if t else text[:80].replace("\n", " ")))
        return text
    stripped = text.lstrip()
    if stripped.startswith("{") or stripped.startswith("["):
        try:
            j = json.loads(text)
            r.log("      JSON %s" % (("dict keys " + str(list(j.keys())[:20])) if isinstance(j, dict) else ("list[%d] first %s" % (len(j), json.dumps(j[0])[:300] if j else None))))
            for p, v in walk(j)[:40]:
                r.log("        %s = %s" % (p, v))
            if len(body) <= 400000:
                embedded[label] = base64.b64encode(gzip.compress(body)).decode()
        except Exception:  # noqa: BLE001
            r.log("      JSON unparseable: %s" % text[:200])
        return text
    if "html" not in ctype.lower() and not stripped.startswith("<"):
        lines = text.splitlines()
        r.log("      %d lines; first: %s" % (len(lines), [ln[:140] for ln in lines[:4]]))
        if len(body) <= 400000 and (embed_kind or len(lines) > 3):
            embedded[label] = base64.b64encode(gzip.compress(body)).decode()
        return text
    if script_grep:
        found = sorted(set(re.findall(r"""["'`](https?://[^"'`\s]{10,160}|/[A-Za-z0-9_./-]*(?:api|json|data|fund|nav|price)[A-Za-z0-9_./{}-]*)["'`]""", text, re.I)))[:30]
        r.log("      script routes: %s" % [f[:120] for f in found])
        return text
    t = re.search(r"<title>(.*?)</title>", text, re.S | re.I)
    r.log("      title %s" % ((t.group(1).strip()[:80]) if t else ""))
    for m in BLOB.finditer(text):
        raw = m.group(2) or m.group(4)
        name = m.group(1) or m.group(3)
        try:
            j = json.loads(raw)
            r.log("      blob %s: keys %s" % (name, list(j.keys())[:12] if isinstance(j, dict) else type(j).__name__))
            for p, v in walk(j)[:40]:
                r.log("        %s = %s" % (p, v))
        except Exception:  # noqa: BLE001
            r.log("      blob %s: %d chars (unparsed) %s" % (name, len(raw), raw[:160]))
    plain = re.sub(r"<script.*?</script>", " ", text, flags=re.S)
    plain = re.sub(r"<[^>]+>", " ", plain)
    plain = re.sub(r"\s+", " ", plain)
    snippets = [m.group(0) for m in SNIP.finditer(plain)]
    for s in snippets[:4]:
        r.log("      text: %s" % s[:320])
    rs = routes(text, host)
    r.log("      routes (%d): %s" % (len(rs), [u[:120] for u in rs[:24]]))
    return text


def main():
    from ops_report import report
    now = datetime.now(timezone.utc)
    with report("ops_6797_etf_issuer_routes_round2") as r:
        r.heading("ops 6797 - issuer data routes, round 2 %s (read-only)" % now.isoformat(timespec="minutes"))

        r.section("Invesco (QQQ RSP TAN DBC DBA BKLN) - dng-api")
        inspect(r, "QQQ page templates", "https://www.invesco.com/us/en/financial-products/etfs/invesco-qqq", script_grep=False)
        page = get("https://www.invesco.com/us/en/financial-products/etfs/invesco-db-commodity-index")[1].decode("utf-8", "ignore")
        templates = sorted(set(re.findall(r"https://dng-api\.invesco\.com/[A-Za-z0-9_/{}.&;=?-]+", page.replace("&#34;", '"').replace("&amp;", "&"))))
        r.log("  dng-api templates on the DBC page (%d): %s" % (len(templates), [t[:150] for t in templates[:30]]))
        for label, url in [("QQQ performance/standard", "https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses/QQQ/performance/standard?idType=ticker"),
                           ("QQQ fund facts", "https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses/QQQ/fundFacts?idType=ticker"),
                           ("QQQ nav history", "https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses/QQQ/navHistory?idType=ticker"),
                           ("QQQ pricing", "https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses/QQQ/pricing?idType=ticker"),
                           ("QQQ daily pricing history", "https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses/QQQ/dailyPricingHistory?idType=ticker"),
                           ("QQQ premium/discount", "https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses/QQQ/frequencyPremiumDiscounts?idType=ticker"),
                           ("QQQ shareclass root", "https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses/QQQ?idType=ticker"),
                           ("product search", "https://dng-api.invesco.com/product/search?country=US&productType=ETF")]:
            inspect(r, label, url, headers={"Accept": "application/json", "Origin": "https://www.invesco.com", "Referer": "https://www.invesco.com/"})

        r.section("Vanguard (17 funds)")
        inspect(r, "advisors VOO page", "https://advisors.vanguard.com/investments/products/voo/vanguard-sp-500-etf")
        for label, url in [("investor VOO profile (id routes)", "https://investor.vanguard.com/investment-products/etfs/profile/voo"),
                           ("fund 0968 price-history", "https://investor.vanguard.com/investment-products/etfs/profile/api/0968/price-history"),
                           ("fund 0968 price", "https://investor.vanguard.com/investment-products/etfs/profile/api/0968/price"),
                           ("ngf-next-gen 0968", "https://personal1.vanguard.com/ngf-next-gen-funds-webapp/fund/0968/price-history"),
                           ("advisors api product", "https://advisors.vanguard.com/investments/products/api/0968/profile"),
                           ("vanguard open api fund list", "https://api.vanguard.com/rs/ire/01/pe/fund/0968/profile.json")]:
            inspect(r, label, url)

        r.section("Global X (BOTZ LIT URA FINX DRIV ARGT) - Next.js")
        text = inspect(r, "BOTZ page", "https://www.globalxetfs.com/funds/botz")
        apis = sorted(set(re.findall(r"https?://[a-z0-9.-]*globalx[a-z0-9.-]*/[^\s\"'<>\\]{3,160}", text)))
        r.log("  globalx hosts/routes: %s" % [a[:140] for a in apis[:30]])
        for label, url in [("fund-data api botz", "https://www.globalxetfs.com/api/funds/botz"), ("funds api", "https://www.globalxetfs.com/api/funds"),
                           ("nav history botz", "https://www.globalxetfs.com/funds/botz/nav-history"), ("holdings botz csv", "https://www.globalxetfs.com/api/funds/botz/holdings/download")]:
            inspect(r, label, url, headers={"Accept": "application/json"})

        r.section("VanEck (NLR SMH GDX VNM) - React")
        inspect(r, "fund-pages-common.js", "https://www.vaneck.com/Static/us/js/dist/fund-pages-common.js", script_grep=True)
        inspect(r, "fund-page-scripts.js", "https://www.vaneck.com/static/us/js/dist/fund-page-scripts.js", script_grep=True)
        for label, url in [("GDX overview json", "https://www.vaneck.com/us/en/investments/gold-miners-etf-gdx/overview/?format=json"),
                           ("vaneck fund data api", "https://www.vaneck.com/api/v1/fund/gdx/overview"), ("vaneck etf nav history", "https://www.vaneck.com/us/en/investments/gold-miners-etf-gdx/nav-history/")]:
            inspect(r, label, url, headers={"Accept": "application/json"})

        r.section("ARK (ARKK)")
        inspect(r, "ARKK page", "https://www.ark-funds.com/funds/arkk")
        for label, url in [("ARK holdings csv (current path)", "https://assets.ark-funds.com/fund-documents/funds-etf-csv/ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv"),
                           ("ARK fund facts api", "https://www.ark-funds.com/api/funds/arkk")]:
            inspect(r, label, url)

        r.section("First Trust (CIBR) - ASP.NET pages")
        inspect(r, "CIBR summary", "https://www.ftportfolios.com/Retail/Etf/EtfSummary.aspx?Ticker=CIBR")
        for label, url in [("CIBR NAV history", "https://www.ftportfolios.com/Retail/Etf/EtfHistoricalNav.aspx?Ticker=CIBR"), ("CIBR daily pricing", "https://www.ftportfolios.com/retail/etf/etfdailypricing.aspx?Ticker=CIBR"),
                           ("First Trust ETF list", "https://www.ftportfolios.com/Retail/Etf/EtfList.aspx")]:
            inspect(r, label, url)

        r.section("Amplify (HACK) - Firestore data plugin")
        inspect(r, "HACK page", "https://amplifyetfs.com/hack/")
        inspect(r, "data-dictionary.json", "https://amplifyetfs.com/wp-content/plugins/amplify-data/data-dictionary.json")
        inspect(r, "amplify-data-loader.js", "https://amplifyetfs.com/wp-content/plugins/amplify-data/amplify-data-loader.js", script_grep=True)

        r.section("USCF (USO UNG) - keyed JS api")
        inspect(r, "USO page", "https://www.uscfinvestments.com/uso")
        inspect(r, "api_INIT js", "https://www.uscfinvestments.com/assets/javascript/api_INIT-v1.2.2.min.js", script_grep=True)
        inspect(r, "fund_map.js", "https://www.uscfinvestments.com/assets/javascript/fund_map.js", script_grep=True)
        inspect(r, "api_key.php", "https://www.uscfinvestments.com/assets/javascript/api_key.php", script_grep=True)

        r.section("Xtrackers / DWS (ASHR)")
        inspect(r, "ASHR page", "https://etf.deutsche-am.com/en-us/ASHR-harvest-csi-300-china-a-shares-etf/")
        for label, url in [("dws api root", "https://etf.deutsche-am.com/api/"), ("dws fund api ASHR", "https://etf.deutsche-am.com/api/fund/ASHR"),
                           ("dws nav history csv", "https://etf.deutsche-am.com/etfdata/export/US/ENG/CSV/Product/Historical/NAV/ASHR/"),
                           ("dws product data", "https://etf.deutsche-am.com/api/product/US/ASHR")]:
            inspect(r, label, url, headers={"Accept": "application/json"})

        r.section("ROBO Global (ROBO) - Nuxt payload")
        text = inspect(r, "ROBO summary", "https://roboglobaletfs.com/robo")
        nuxt = re.search(r"window\.__NUXT__\s*=\s*(.*?);</script>", text, re.S)
        if nuxt:
            blob = nuxt.group(1)
            r.log("  __NUXT__ %d chars; mentions: shares %s nav %s; sample around 'Shares Outstanding': %s" % (len(blob), blob.count("Shares Outstanding"), blob.count("NAV"), blob[max(0, blob.find("Shares Outstanding") - 200):blob.find("Shares Outstanding") + 200].replace("\n", " ")))
        for label, url in [("ROBO api fund", "https://roboglobaletfs.com/api/fund/robo"), ("ROBO nav history csv", "https://roboglobaletfs.com/robo/nav-history.csv")]:
            inspect(r, label, url)

        r.section("Grayscale (GBTC) and Schwab (SCHD SCHG SCHP): walled - headless Chrome")
        try:
            try:
                import playwright  # noqa: F401
            except ImportError:
                subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                try:
                    browser = p.chromium.launch(channel="chrome", headless=True)
                except Exception:  # noqa: BLE001
                    subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
                    browser = p.chromium.launch(headless=True)
                ctx = browser.new_context(viewport={"width": 1366, "height": 900}, locale="en-US")
                pg = ctx.new_page()
                seen = []
                pg.on("response", lambda resp: seen.append((resp.url, resp.status, resp.headers.get("content-type", "")[:30])))
                for label, url in [("Schwab SCHD", "https://www.schwabassetmanagement.com/products/schd"), ("Grayscale GBTC", "https://www.grayscale.com/funds/grayscale-bitcoin-trust-etf"),
                                   ("Vanguard investor VOO", "https://investor.vanguard.com/investment-products/etfs/profile/voo"), ("Global X BOTZ", "https://www.globalxetfs.com/funds/botz"),
                                   ("VanEck GDX", "https://www.vaneck.com/us/en/investments/gold-miners-etf-gdx/overview/")]:
                    seen.clear()
                    t0 = time.time()
                    try:
                        resp = pg.goto(url, wait_until="domcontentloaded", timeout=60000)
                        try:
                            pg.wait_for_load_state("networkidle", timeout=25000)
                        except Exception:  # noqa: BLE001
                            pass
                        html = pg.content()
                        plain = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", re.sub(r"<script.*?</script>", " ", html, flags=re.S)))
                        snippets = [m.group(0)[:300] for m in SNIP.finditer(plain)][:4]
                        data_calls = [(u[:150], s, c) for u, s, c in seen if re.search(r"json|api|csv|xlsx|graphql", u + c, re.I) and not re.search(r"google|adobe|facebook|cookielaw|onetrust|hotjar|doubleclick|bing|linkedin|tiktok|gstatic|clarity|newrelic|datadog|segment|launchdarkly", u, re.I)][:25]
                        r.log("  %s: HTTP %s in %.1fs, title %r, %d bytes; text: %s" % (label, resp.status if resp else None, time.time() - t0, pg.title()[:60], len(html), snippets))
                        r.log("    data calls (%d): %s" % (len(data_calls), data_calls))
                    except Exception as exc:  # noqa: BLE001
                        r.warn("  %s: %s %s" % (label, type(exc).__name__, str(exc)[:160]))
                ctx.close()
                browser.close()
        except Exception as exc:  # noqa: BLE001
            r.warn("chrome: %s %s" % (type(exc).__name__, str(exc)[:160]))

        r.section("originals (gzip+base64)")
        for k, v in embedded.items():
            r.log("<!-- ORIGINAL %s -->\n%s\n<!-- /ORIGINAL -->" % (k, v))
        r.ok("read-only; %d originals embedded" % len(embedded))
    sys.exit(0)


if __name__ == "__main__":
    main()
