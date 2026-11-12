"""ops 6796 - READ-ONLY audit before covering the ETF creations engine's uncovered funds (Khalid 2026-11-12 11:21 ET:
"lets do this: ETF creations engine's 47 funds without a native issuer source (Vanguard, Invesco, ARK, Schwab...)").

(1) The exact gap list from the live packet (data/etf-true-flows.json: by_etf[t].source_status ==
'no_reviewed_native_history'), grouped by issuer. (2) For every issuer, the official data routes their own sites use
for daily NAV and shares outstanding - catalog / product-list downloads, per-fund NAV-history downloads, per-fund JSON
widgets, the product page HTML - probed from the GitHub runner with a browser User-Agent: HTTP status, content type,
size, bot-wall markers, whether the body carries 'shares outstanding' / 'NAV' / dated observations, the header rows
of CSV / xlsx downloads, the keys of JSON answers. Small originals are embedded (gzip+base64) so parsers can be written
against the real files. Nothing is written anywhere; nothing is invoked.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import csv
import gzip
import io
import json
import re
import sys
import time
import zipfile

import requests

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))

PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
      "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.9,*/*;q=0.8", "Accept-Language": "en-US,en;q=0.9"}
WALL = re.compile(r"Access Denied|Reference #\d|Pardon Our Interruption|captcha|Request unsuccessful|Incapsula|bot detection|cf-browser-verification", re.I)
SHARES = re.compile(r"shares\s*outstanding|sharesOutstanding|shares_outstanding|Shares Outstanding", re.I)
NAVRE = re.compile(r"\bNAV\b|net asset value|navPrice|\"nav\"", re.I)
DATE = re.compile(r"\b(20\d\d-\d\d-\d\d|\d{1,2}/\d{1,2}/20\d\d|[A-Z][a-z]{2} \d{1,2}, 20\d\d)\b")

# issuer -> probes: (label, url, kind) ; kind: page / json / csv / xlsx / list
PROBES = {
    "Vanguard": [
        ("product list (all funds) json", "https://investor.vanguard.com/investment-products/list/funddetail", "json"),
        ("VOO profile page", "https://investor.vanguard.com/investment-products/etfs/profile/voo", "page"),
        ("VOO price-history api", "https://investor.vanguard.com/investment-products/etfs/profile/api/voo/price-history", "json"),
        ("VOO price api", "https://investor.vanguard.com/investment-products/etfs/profile/api/voo/price", "json"),
        ("VOO profile api", "https://investor.vanguard.com/investment-products/etfs/profile/api/voo/profile", "json"),
        ("fund 0968 price history (legacy api)", "https://api.vanguard.com/rs/ire/01/pe/fund/0968/price/daily/.json?start-date=2026-10-01&end-date=2026-11-12", "json"),
        ("fund 0968 profile (legacy api)", "https://api.vanguard.com/rs/ire/01/pe/fund/0968/profile.json", "json"),
        ("advisors fund data", "https://advisors.vanguard.com/investments/products/voo/vanguard-sp-500-etf", "page"),
    ],
    "Invesco": [
        ("QQQ product detail page", "https://www.invesco.com/us/financial-products/etfs/product-detail?audienceType=Investor&ticker=QQQ", "page"),
        ("QQQ holdings download", "https://www.invesco.com/us/financial-products/etfs/holdings/main/holdings/0?audienceType=Investor&action=download&ticker=QQQ", "csv"),
        ("QQQ NAV history download", "https://www.invesco.com/us/financial-products/etfs/performance/prices/main/performance/0?audienceType=Investor&action=download&ticker=QQQ", "csv"),
        ("ETF product list download", "https://www.invesco.com/us/financial-products/etfs/product-list?audienceType=Investor&action=download", "xlsx"),
        ("QQQ etf json (us-product api)", "https://www.invesco.com/us/financial-products/etfs/product-detail/api?ticker=QQQ", "json"),
        ("DB commodity DBC detail", "https://www.invesco.com/us/financial-products/etfs/product-detail?audienceType=Investor&ticker=DBC", "page"),
    ],
    "Global X": [
        ("BOTZ fund page", "https://www.globalxetfs.com/funds/botz/", "page"),
        ("BOTZ holdings download", "https://www.globalxetfs.com/funds/botz/?download_full_holdings=true", "csv"),
        ("BOTZ NAV history download", "https://www.globalxetfs.com/funds/botz/?download_nav_history=true", "csv"),
        ("BOTZ daily nav api", "https://www.globalxetfs.com/wp-json/globalx/v1/fund/botz", "json"),
        ("explore funds list", "https://www.globalxetfs.com/explore/", "page"),
    ],
    "VanEck": [
        ("GDX overview page", "https://www.vaneck.com/us/en/investments/gold-miners-etf-gdx/overview/", "page"),
        ("GDX nav history download", "https://www.vaneck.com/us/en/investments/gold-miners-etf-gdx/overview/?download=historical-nav", "csv"),
        ("vaneck fund api gdx", "https://www.vaneck.com/api/fund/gdx/", "json"),
        ("vaneck etf list", "https://www.vaneck.com/us/en/etf-mutual-fund-finder/etfs/", "page"),
    ],
    "ARK": [
        ("ARKK fund page", "https://ark-funds.com/funds/arkk/", "page"),
        ("ARKK holdings csv", "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv", "csv"),
        ("ARK fund data json", "https://ark-funds.com/wp-json/ark/v1/funds/arkk", "json"),
    ],
    "Schwab": [
        ("SCHD product page", "https://www.schwabassetmanagement.com/products/schd", "page"),
        ("SCHD nav history download", "https://www.schwabassetmanagement.com/products/schd/nav-history/download", "csv"),
        ("SCHD api", "https://www.schwabassetmanagement.com/api/product/schd", "json"),
    ],
    "First Trust": [
        ("CIBR summary page", "https://www.ftportfolios.com/Retail/Etf/EtfSummary.aspx?Ticker=CIBR", "page"),
        ("CIBR historical NAV page", "https://www.ftportfolios.com/Retail/etf/etfhistoricalnav.aspx?Ticker=CIBR", "page"),
        ("CIBR historical NAV download", "https://www.ftportfolios.com/Retail/Etf/EtfHistoricalNavExport.aspx?Ticker=CIBR", "csv"),
    ],
    "Amplify": [
        ("HACK fund page", "https://amplifyetfs.com/hack/", "page"),
        ("HACK fund data csv", "https://amplifyetfs.com/wp-content/uploads/hack-daily-data.csv", "csv"),
    ],
    "USCF": [
        ("USO fund page", "https://www.uscfinvestments.com/uso", "page"),
        ("USO holdings/NAV csv", "https://www.uscfinvestments.com/holdings/uso", "page"),
        ("USCF daily data", "https://www.uscfinvestments.com/daily-data", "page"),
    ],
    "Xtrackers (DWS)": [
        ("ASHR fund page", "https://etf.dws.com/en-us/ASHR-xtrackers-harvest-csi-300-china-a-shares-etf/", "page"),
        ("ASHR nav history download", "https://etf.dws.com/etfdata/export/US/ENG/CSV/Product/Historical/NAV/ASHR", "csv"),
    ],
    "Grayscale": [
        ("GBTC fund page", "https://www.grayscale.com/funds/grayscale-bitcoin-trust-etf", "page"),
        ("GBTC historical data csv", "https://www.grayscale.com/api/funds/GBTC/historical", "json"),
    ],
    "ROBO Global": [
        ("ROBO fund page", "https://roboglobaletfs.com/robo", "page"),
        ("ROBO fund page (fund site)", "https://www.roboglobaletfs.com/robo/", "page"),
    ],
    "Fidelity": [
        ("FBTC institutional quote", "https://institutional.fidelity.com/prgw/digital/fundresearch/etf/quote/FBTC", "page"),
        ("FBTC fund research", "https://fundresearch.fidelity.com/mutual-funds/summary/316092808", "page"),
    ],
    "iPath / Barclays (VXX, ETN)": [
        ("VXX ETN page", "https://www.ipathetn.com/US/16/en/details.app?instrumentId=342447", "page"),
    ],
}


def fetch(url, timeout=40):
    t0 = time.time()
    try:
        resp = requests.get(url, headers=UA, timeout=timeout, allow_redirects=True)
        return resp.status_code, resp.content, resp.headers.get("content-type", ""), resp.url, time.time() - t0
    except Exception as exc:  # noqa: BLE001
        return type(exc).__name__, b"", "", url, time.time() - t0


def describe(body, ctype, kind):
    """what the answer carries: a wall? shares / NAV mentions? dates? csv header? json keys? xlsx sheets?"""
    text = body[:400000].decode("utf-8", "ignore")
    out = {"bytes": len(body), "wall": bool(WALL.search(text[:20000])), "shares_mention": bool(SHARES.search(text)), "nav_mention": bool(NAVRE.search(text))}
    dates = DATE.findall(text)
    out["dates"] = len(dates)
    out["last_dates"] = sorted(set(dates))[-3:] if dates else []
    stripped = text.lstrip()
    if stripped.startswith("{") or stripped.startswith("["):
        try:
            j = json.loads(text)
            out["json"] = ("dict keys %s" % list(j.keys())[:16]) if isinstance(j, dict) else ("list[%d] first %s" % (len(j), json.dumps(j[0])[:160] if j else None))
        except Exception:  # noqa: BLE001
            out["json"] = "unparseable"
    elif body[:2] == b"PK":
        try:
            z = zipfile.ZipFile(io.BytesIO(body))
            out["xlsx_entries"] = [n for n in z.namelist() if n.startswith("xl/worksheets")][:6]
            from risk_sources import Xlsx
            rows = Xlsx(body).rows(0, max_rows=6)
            out["xlsx_rows"] = [[str(v)[:18] if v is not None else "" for v in r[:10]] for r in rows]
        except Exception as exc:  # noqa: BLE001
            out["xlsx"] = "unreadable: %s" % type(exc).__name__
    elif "html" in ctype.lower() or stripped.startswith("<"):
        m = re.search(r"<title>(.*?)</title>", text, re.S | re.I)
        out["title"] = (m.group(1).strip()[:80] if m else "")
        snippet = re.search(r".{0,80}[Ss]hares\s*[Oo]utstanding.{0,160}", re.sub(r"<[^>]+>", " ", text))
        out["shares_snippet"] = re.sub(r"\s+", " ", snippet.group(0))[:240] if snippet else ""
        scripts = re.findall(r'<script[^>]+src="([^"]+)"', text)
        out["script_hints"] = [s for s in scripts if re.search(r"api|data|fund|chart", s, re.I)][:4]
        apis = sorted(set(re.findall(r"https?://[^\s\"']+(?:api|json|download|export)[^\s\"']*", text)))[:8]
        out["api_hints"] = [a[:120] for a in apis]
    else:
        lines = text.splitlines()
        if lines:
            out["first_lines"] = [ln[:160] for ln in lines[:4]]
            try:
                rows = list(csv.reader(io.StringIO("\n".join(lines[:5]))))
                out["csv_header"] = rows[0][:14] if rows else []
            except Exception:  # noqa: BLE001
                pass
            out["n_lines"] = len(lines)
    return out


def main():
    from ops_report import report
    now = datetime.now(timezone.utc)
    embedded = {}
    with report("ops_6796_etf_native_gaps_audit") as r:
        r.heading("ops 6796 - ETF creations engine: the uncovered funds and their issuers' official data routes %s (read-only)" % now.isoformat(timespec="minutes"))
        r.section("1. the gap list from the live packet")
        try:
            packet = requests.get("%s/data/etf-true-flows.json?t=%d" % (PROXY, int(time.time())), timeout=90).json()
            by = packet.get("by_etf") or {}
            gaps = sorted(t for t, e in by.items() if e.get("source_status") == "no_reviewed_native_history")
            covered = sorted(t for t, e in by.items() if e.get("source_status") != "no_reviewed_native_history")
            issuers = {}
            for t in covered:
                issuers.setdefault(((by[t].get("identity") or {}).get("issuer") or "?"), []).append(t)
            r.log("packet %s generated %s | %d funds | covered %d: %s | gaps %d: %s" % (packet.get("version"), packet.get("generated_at"), len(by), len(covered), {k: len(v) for k, v in issuers.items()}, len(gaps), " ".join(gaps)))
            cat_of = {t: e.get("category") for t, e in by.items()}
            r.log("gaps by category: %s" % json.dumps({c: [t for t in gaps if cat_of[t] == c] for c in sorted(set(cat_of[t] for t in gaps))}))
        except Exception as exc:  # noqa: BLE001
            r.warn("packet: %s %s" % (type(exc).__name__, str(exc)[:160]))
        r.section("2. issuer data routes from the runner")
        for issuer, probes in PROBES.items():
            r.log("== %s" % issuer)
            for label, url, kind in probes:
                code, body, ctype, final, took = fetch(url)
                info = describe(body, ctype, kind) if body else {}
                r.log("  %-38s HTTP %-4s %4.1fs %-40s %s%s" % (label, code, took, ctype[:40], json.dumps(info, default=str)[:900], (" -> " + final[:80]) if final != url else ""))
                if isinstance(code, int) and code == 200 and body and not info.get("wall") and (kind in ("csv", "xlsx", "json") or info.get("json") or info.get("csv_header") or info.get("xlsx_rows")) and len(body) <= 600000:
                    embedded[issuer + " | " + label] = base64.b64encode(gzip.compress(body)).decode()
        r.section("originals (gzip+base64, one per line: label \\t blob)")
        for k, v in embedded.items():
            r.log("<!-- ORIGINAL %s -->\n%s\n<!-- /ORIGINAL -->" % (k, v))
        r.ok("read-only audit complete: %d originals embedded" % len(embedded))
    sys.exit(0)


if __name__ == "__main__":
    main()
