"""
justhodl-13f-positions — Parse 13F infotables, extract holdings,
                         compute BUYS / SELLS / NEW / EXIT vs prior quarter

Why a separate Lambda?
----------------------
justhodl-sec-13f tracks WHEN funds file. This Lambda parses WHAT they
own — the actual positions, and crucially the position changes vs
their previous filing.

What "Big Investors are Buying / Selling" means:
  NEW       Position appears in latest filing but wasn't there last quarter
  ADD       Position grew vs last quarter (>5% increase)
  TRIM      Position shrunk vs last quarter (>5% decrease)
  EXIT      Position disappeared
  HOLD      Position roughly unchanged

Output:
  data/13f-positions.json — per-fund positions + per-stock aggregate

Schema:
  {
    "generated_at": ...,
    "as_of_quarter": "2025-Q4",
    "funds_parsed": int,
    "funds_failed": int,
    "by_fund": {
      "BERKSHIRE": {
        "name": "Berkshire Hathaway",
        "filed_at": "2026-02-17",
        "period_of_report": "2025-12-31",
        "total_value_usd": 380_500_000_000,
        "n_positions": 41,
        "top_positions": [
          {"name": "APPLE INC", "cusip": "037833100", "ticker": "AAPL",
           "value_usd": 75_000_000_000, "shares": 300_000_000,
           "pct_of_portfolio": 19.7,
           "change": "ADD", "prior_value": 70_000_000_000, "value_delta_pct": 7.1},
          ...
        ],
        "changes_summary": {
          "new": [...],   "exit": [...],
          "adds": [...],  "trims": [...]
        }
      }
    },
    "aggregate_by_ticker": {
      "AAPL": {
        "name": "Apple Inc",
        "n_funds_holding": 12,
        "total_value": 250_000_000_000,
        "n_funds_adding": 5,
        "n_funds_trimming": 2,
        "n_funds_new_position": 1,
        "n_funds_exiting": 0,
        "net_action_score": +4,    # adds + new - trims - exits
      },
      ...
    },
    "most_bought": [{ticker, name, n_funds_buying, total_added_value}, ...],
    "most_sold":   [{ticker, name, n_funds_selling, total_trimmed_value}, ...],
    "consensus_holds": [{ticker, n_funds_holding, ...}, ...],
    "rare_picks":  [{ticker, only_held_by: [funds]}, ...]
  }

Caching strategy:
  - Each fund's parsed positions are cached at:
        13f-cache/{FUND_KEY}/{accession}.json
    so we never re-parse the same filing twice.
  - When we see the same accession as the previous run, skip parse.
  - On first run, parse all 18 funds' latest filings.

Schedule:
  rate(6 hours) — checks for new filings 4× daily.
  13F filings hit SEC during ~Feb 15, May 15, Aug 15, Nov 15. Outside those
  windows the Lambda just confirms cache is current.
"""
from __future__ import annotations
import json
import os
import re
import time
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

import boto3
from botocore.exceptions import ClientError

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/13f-positions.json")
S3_CACHE_PREFIX = os.environ.get("S3_CACHE_PREFIX", "13f-cache/")
S3_FILINGS_KEY = os.environ.get("S3_FILINGS_KEY", "data/institutional-positions.json")
USER_AGENT = os.environ.get("USER_AGENT", "JustHodl Research raafouis@gmail.com")
FMP_KEY = os.environ.get("FMP_KEY", "")
MAX_PARALLEL = int(os.environ.get("MAX_PARALLEL", "3"))   # SEC rate-limits aggressively

s3 = boto3.client("s3")

WATCHLIST = {
    "BERKSHIRE":      "0001067983",
    "BRIDGEWATER":    "0001350694",
    "RENAISSANCE":    "0001037389",
    "AQR":            "0001167557",
    "TWO_SIGMA":      "0001179392",
    "CITADEL":        "0001423053",
    "MILLENNIUM":     "0001273087",
    "PERSHING":       "0001336528",
    "GREENLIGHT":     "0001079114",
    "SOROS":          "0001029160",
    "TIGER_GLOBAL":   "0001167483",
    "COATUE":         "0001135730",
    "BAUPOST":        "0001061165",
    "ELLIOTT":        "0001286922",
    "SCION":          "0001649339",
    "DURATION":       "0001582202",
    "POINT72":        "0001603466",
    "LONE_PINE":      "0001061768",
}

FUND_DISPLAY_NAMES = {
    "BERKSHIRE":      "Berkshire Hathaway",
    "BRIDGEWATER":    "Bridgewater Associates",
    "RENAISSANCE":    "Renaissance Technologies",
    "AQR":            "AQR Capital",
    "TWO_SIGMA":      "Two Sigma",
    "CITADEL":        "Citadel Advisors",
    "MILLENNIUM":     "Millennium Mgmt",
    "PERSHING":       "Pershing Square (Ackman)",
    "GREENLIGHT":     "Greenlight (Einhorn)",
    "SOROS":          "Soros Fund",
    "TIGER_GLOBAL":   "Tiger Global",
    "COATUE":         "Coatue",
    "BAUPOST":        "Baupost (Klarman)",
    "ELLIOTT":        "Elliott Mgmt",
    "SCION":          "Scion (Burry)",
    "DURATION":       "Duration Capital",
    "POINT72":        "Point72 (Cohen)",
    "LONE_PINE":      "Lone Pine",
}

# Hard-coded CUSIP → ticker overrides for top names where FMP fails or is slow
CUSIP_OVERRIDES = {
    "037833100": ("AAPL", "Apple Inc"),
    "594918104": ("MSFT", "Microsoft Corp"),
    "02079K305": ("GOOGL", "Alphabet Inc Class A"),
    "02079K107": ("GOOG", "Alphabet Inc Class C"),
    "023135106": ("AMZN", "Amazon.com Inc"),
    "67066G104": ("NVDA", "Nvidia Corp"),
    "30303M102": ("META", "Meta Platforms Inc"),
    "88160R101": ("TSLA", "Tesla Inc"),
    "11135F101": ("AVGO", "Broadcom Inc"),
    "92826C839": ("V", "Visa Inc"),
    "57636Q104": ("MA", "Mastercard Inc"),
    "46625H100": ("JPM", "JPMorgan Chase"),
    "060505104": ("BAC", "Bank of America"),
    "949746101": ("WFC", "Wells Fargo"),
    "172967424": ("C", "Citigroup Inc"),
    "00287Y109": ("ABBV", "AbbVie Inc"),
    "478160104": ("JNJ", "Johnson & Johnson"),
    "58933Y105": ("MRK", "Merck & Co"),
    "717081103": ("PFE", "Pfizer Inc"),
    "00206R102": ("T", "AT&T Inc"),
    "92343V104": ("VZ", "Verizon Communications"),
    "30231G102": ("XOM", "ExxonMobil"),
    "166764100": ("CVX", "Chevron Corp"),
    "92189F106": ("VST", "Vistra Corp"),
    "931142103": ("WMT", "Walmart Inc"),
    "20825C104": ("KO", "Coca-Cola Co"),
    "742718109": ("PG", "Procter & Gamble"),
    "84265V105": ("GE", "GE Aerospace"),
    "459200101": ("IBM", "IBM Corp"),
    "00724F101": ("ADBE", "Adobe Inc"),
    "036752103": ("ORCL", "Oracle Corp"),
    "55617P104": ("MA", "Mastercard"),
    "832696405": ("JD", "JD.com"),
    "G87110105": ("STLA", "Stellantis"),
    "017175100": ("ALL", "Allstate Corp"),
    "032037103": ("AMP", "Ameriprise Financial"),
    "02005N100": ("ALLY", "Ally Financial"),
    "025816109": ("AXP", "American Express"),
    "031162100": ("AMGN", "Amgen Inc"),
    "036752103": ("ORCL", "Oracle Corp"),
    "056752108": ("BAX", "Baxter International"),
    "071813109": ("BAC", "Bank of America"),
    "097023105": ("BA", "Boeing Co"),
    "12572Q105": ("CMI", "Cummins Inc"),
    "126650100": ("CVS", "CVS Health"),
    "166764100": ("CVX", "Chevron"),
    "191216100": ("KO", "Coca-Cola Co"),
    "203363101": ("CMCSA", "Comcast Corp"),
    "247361702": ("DAL", "Delta Air Lines"),
    "254687106": ("DIS", "Walt Disney"),
    "278642103": ("EBAY", "eBay Inc"),
    "29355A107": ("ET", "Energy Transfer"),
    "302491303": ("FDX", "FedEx"),
    "316773100": ("FITB", "Fifth Third"),
    "337738108": ("FCX", "Freeport-McMoRan"),
    "369604103": ("GE", "GE Aerospace"),
    "375558103": ("GILD", "Gilead Sciences"),
    "370334104": ("GS", "Goldman Sachs"),
    "375558103": ("GILD", "Gilead Sciences"),
    "446150104": ("HUM", "Humana Inc"),
    "459200101": ("IBM", "IBM"),
    "464287200": ("IVV", "iShares Core S&P 500 ETF"),
    "464287655": ("IBB", "iShares Biotech ETF"),
    "464287200": ("IVV", "iShares Core S&P 500"),
    "478160104": ("JNJ", "Johnson & Johnson"),
    "532457108": ("LLY", "Eli Lilly"),
    "539830109": ("LMT", "Lockheed Martin"),
    "57636Q104": ("MA", "Mastercard"),
    "58155Q103": ("MCD", "McDonald's"),
    "58933Y105": ("MRK", "Merck"),
    "594918104": ("MSFT", "Microsoft"),
    "609207105": ("MS", "Morgan Stanley"),
    "61744J101": ("MS", "Morgan Stanley"),
    "654106103": ("NKE", "Nike"),
    "67066G104": ("NVDA", "Nvidia"),
    "693718108": ("PEP", "PepsiCo"),
    "713448108": ("PEP", "PepsiCo"),
    "742718109": ("PG", "Procter & Gamble"),
    "747525103": ("QCOM", "Qualcomm"),
    "78462F103": ("SPY", "SPDR S&P 500 ETF"),
    "808513105": ("SCHW", "Charles Schwab"),
    "84265V105": ("GE", "GE Aerospace"),
    "853157101": ("SQ", "Block Inc"),
    "87612E106": ("TGT", "Target Corp"),
    "884903105": ("TXN", "Texas Instruments"),
    "888559302": ("TMUS", "T-Mobile US"),
    "893830409": ("TMO", "Thermo Fisher"),
    "92343V104": ("VZ", "Verizon"),
    "924016105": ("V", "Visa"),
    "927804103": ("VOO", "Vanguard S&P 500"),
    "931142103": ("WMT", "Walmart"),
    "94106L109": ("WBA", "Walgreens Boots"),
    "94755P101": ("WCN", "Waste Connections"),
    "98138H101": ("WYNN", "Wynn Resorts"),
    # Berkshire's known top holdings + common 13F names
    "025816109": ("AXP", "American Express"),
    "191216100": ("KO", "Coca-Cola"),
    "20825C104": ("COP", "ConocoPhillips"),
    "532457108": ("LLY", "Eli Lilly"),
    "742718109": ("PG", "Procter & Gamble"),
    "713448108": ("PEP", "PepsiCo"),
    "742737109": ("PG", "Procter & Gamble"),  # alt cusip
    "92345Y106": ("VRTX", "Vertex Pharmaceuticals"),
    "92556H206": ("VICI", "VICI Properties"),
    "743315103": ("PM", "Philip Morris"),
    "740189105": ("PPG", "PPG Industries"),
    "733174100": ("POOL", "Pool Corp"),
    "722816107": ("PNC", "PNC Financial"),
    "709599105": ("PFE", "Pfizer"),  # alt
    "67103H107": ("OXY", "Occidental Petroleum"),
    "655664408": ("NOC", "Northrop Grumman"),
    "654106103": ("NKE", "Nike"),
    "63938C108": ("NFLX", "Netflix"),
    "615369105": ("MCO", "Moody's"),
    "60855R100": ("MOH", "Molina Healthcare"),
    "60628P303": ("MMM", "3M Company"),
    "59491610Z": ("MSFT", "Microsoft"),  # alt
    "58155Q103": ("MCD", "McDonald's"),
    "574599106": ("MSI", "Motorola Solutions"),
    "55354G100": ("MMC", "Marsh McLennan"),
    "54153E105": ("LLY", "Eli Lilly"),  # alt
    "509151107": ("LMT", "Lockheed Martin"),
    "501797309": ("LRCX", "Lam Research"),
    "493267108": ("KEYS", "Keysight Technologies"),
    "478366107": ("JNPR", "Juniper"),
    "459200101": ("IBM", "IBM"),
    "44891N208": ("HUBS", "HubSpot"),
    "443510607": ("HUM", "Humana"),
    "437076102": ("HD", "Home Depot"),
    "404119109": ("HCA", "HCA Healthcare"),
    "37733W105": ("GLW", "Corning"),
    "369604103": ("GE", "General Electric"),  # alt
    "369550108": ("HCA", "HCA Healthcare"),
    "353484103": ("FFIV", "F5 Networks"),
    "344849104": ("FCX", "Freeport-McMoRan"),
    "30303M102": ("META", "Meta Platforms"),
    "29786A106": ("FAST", "Fastenal"),
    "28176E108": ("EBAY", "eBay"),
    "278642103": ("EA", "Electronic Arts"),
    "247361702": ("DE", "Deere"),
    "24906P109": ("DHR", "Danaher"),
    "247203802": ("DEO", "Diageo"),
    "166764100": ("CVX", "Chevron"),
    "172967424": ("C", "Citigroup"),  # alt
    "172967101": ("C", "Citigroup"),
    "169656105": ("CHKP", "Check Point"),
    "164045100": ("CHTR", "Charter Communications"),
    "1592525095": ("CHWY", "Chewy"),
    "12572Q105": ("CME", "CME Group"),
    "125509106": ("CI", "Cigna"),
    "125896100": ("CMS", "CMS Energy"),
    "126650100": ("CVS", "CVS Health"),
    "126408103": ("CSX", "CSX Corporation"),
    "127190304": ("CAH", "Cardinal Health"),
    "108102206": ("BMY", "Bristol-Myers Squibb"),
    "097023105": ("BA", "Boeing"),
    "075887109": ("BSX", "Boston Scientific"),
    "071813109": ("BKNG", "Booking Holdings"),
    "06367W103": ("BK", "BNY Mellon"),
    "045327103": ("APH", "Amphenol"),
    "032654105": ("AMP", "Ameriprise Financial"),
    "037411105": ("APD", "Air Products"),
    "030490103": ("AKAM", "Akamai"),
    "025537101": ("AME", "Ametek"),
    "00724F101": ("ADBE", "Adobe"),
    "00191U102": ("AAPL", "Apple Inc"),  # alt sometimes
    "00130H105": ("ALB", "Albemarle"),
    "00079N105": ("AAP", "Advance Auto Parts"),
    # Common ETF / fund holdings
    "78462F103": ("SPY", "SPDR S&P 500"),
    "464287200": ("IWM", "iShares Russell 2000"),
    "464287465": ("IVV", "iShares Core S&P 500"),
    "46428P457": ("AGG", "iShares Aggregate Bond"),
    "464287317": ("EFA", "iShares MSCI EAFE"),
    "464287481": ("EEM", "iShares MSCI Emerging Markets"),
    "46090E103": ("ICLN", "iShares Global Clean Energy"),
    "46428793": ("IBB", "iShares Biotech"),
    "46137V357": ("IEF", "iShares 7-10 Yr Treasury"),
}


def _fetch(url, timeout=20):
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT, "Accept": "*/*",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def get_s3_json(key, default=None):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return default


def put_s3_json(key, body):
    s3.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=json.dumps(body, default=str).encode(),
        ContentType="application/json", CacheControl="no-cache",
    )


def get_filing_index_dir(cik: str, accession: str):
    """Return the SEC archive directory URL for a filing."""
    cik_int = str(int(cik))
    acc_clean = accession.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_clean}/"


def find_infotable_xml(filing_dir: str):
    """Locate the infotable XML in a 13F filing directory.

    Returns (url, xml_text), or (None, None).

    Strategy:
      1. Try filing_dir/infotable.xml directly (standard SEC filename)
      2. Else fetch index.json + iterate xml files until one has <infoTable>
      3. Fallback to primary_doc.xml in case it's combined
    """
    # Step 1: try infotable.xml directly (the standard filename)
    direct_url = filing_dir + "infotable.xml"
    try:
        text = _fetch(direct_url, timeout=15).decode("utf-8", errors="ignore")
        if "<infoTable" in text or "<ns1:infoTable" in text:
            return direct_url, text
    except Exception:
        pass   # fall through

    # Step 2: walk index.json
    try:
        raw = _fetch(filing_dir + "index.json")
        idx = json.loads(raw.decode("utf-8"))
        items = idx.get("directory", {}).get("item", [])
        xml_files = [i.get("name", "") for i in items if i.get("name", "").endswith(".xml")]

        # Probe each in order, skipping primary_doc and any we've already tried
        for name in xml_files:
            if name == "primary_doc.xml" or name == "infotable.xml":
                continue
            url = filing_dir + name
            try:
                text = _fetch(url, timeout=15).decode("utf-8", errors="ignore")
                if "<infoTable" in text or "<ns1:infoTable" in text:
                    return url, text
            except Exception:
                continue

        # Step 3: fallback to primary_doc.xml
        if "primary_doc.xml" in xml_files:
            url = filing_dir + "primary_doc.xml"
            text = _fetch(url, timeout=15).decode("utf-8", errors="ignore")
            if "<infoTable" in text or "<ns1:infoTable" in text:
                return url, text
    except Exception as e:
        print(f"  index probe fail {filing_dir}: {e}")
    return None, None


def parse_infotable(xml_text: str):
    """Parse infotable XML into list of position dicts.

    Returns: [{name, cusip, value_usd, shares, share_type}, ...]
    Note: <value> is in THOUSANDS per SEC instructions; we multiply by 1000.

    Handles two namespace patterns commonly seen in SEC 13F filings:
      Pattern A: prefixed namespaces (<ns1:infoTable>)
      Pattern B: default namespace (xmlns="..." on root, plain <infoTable>)

    Strategy: aggressively strip ALL namespace declarations and prefixes
    so ElementTree treats elements as bare tags.
    """
    # Step 1: strip namespace prefixes from element tags (ns1:foo → foo)
    cleaned = re.sub(r"<(/?)\w+:", r"<\1", xml_text)
    # Step 2: strip default namespace declarations from any element
    # so ET.iter("infoTable") matches without {namespace}infoTable.
    cleaned = re.sub(r'\s+xmlns(:\w+)?="[^"]*"', "", cleaned)
    # Step 3: strip prefixed attributes (xsi:schemaLocation, etc.) that
    # would now be "unbound" since their namespace declaration is gone.
    # Match attribute names like `xsi:schemaLocation="..."` inside tags.
    cleaned = re.sub(r'\s+\w+:\w+="[^"]*"', "", cleaned)

    positions = []
    try:
        root = ET.fromstring(cleaned)
    except ET.ParseError as e:
        print(f"  parse fail: {e}")
        return positions

    # Find all infoTable elements (now namespace-free)
    for it in root.iter("infoTable"):
        try:
            name = (it.findtext("nameOfIssuer") or "").strip()
            title = (it.findtext("titleOfClass") or "").strip()
            cusip = (it.findtext("cusip") or "").strip().upper()
            value_str = it.findtext("value") or "0"
            shrs_node = it.find("shrsOrPrnAmt")
            shares = "0"
            stype = "SH"
            if shrs_node is not None:
                shares = (shrs_node.findtext("sshPrnamt") or "0").strip()
                stype = (shrs_node.findtext("sshPrnamtType") or "SH").strip()

            # SEC 13F-HR <value>: documented as "in thousands of dollars" but
            # many filers report whole dollars. Auto-detect by sanity-checking
            # the implied per-share price.
            value_int = int(float(value_str))
            shares_int = int(float(shares))
            value_usd = value_int   # default: dollars
            if shares_int > 0 and value_int > 0:
                price_if_thousands = (value_int * 1000) / shares_int
                price_if_dollars = value_int / shares_int
                t_plausible = 0.5 <= price_if_thousands <= 5000
                d_plausible = 0.5 <= price_if_dollars <= 5000
                if t_plausible and not d_plausible:
                    value_usd = value_int * 1000
                elif d_plausible and t_plausible:
                    # Both plausible — pick whichever yields a price closer to $50
                    # (typical median equity price). Most legitimate $0.50-$50
                    # are large-cap stocks so we lean dollar.
                    if abs(price_if_dollars - 50) < abs(price_if_thousands - 50):
                        value_usd = value_int
                    else:
                        value_usd = value_int * 1000

            if not name or not cusip:
                continue
            positions.append({
                "name": name,
                "title": title,
                "cusip": cusip,
                "value_usd": value_usd,
                "shares": shares_int,
                "share_type": stype,
            })
        except (ValueError, TypeError, AttributeError):
            continue

    # Multiple infoTable rows can exist for the same name+cusip (different
    # voting authority configurations). Collapse to one per cusip.
    by_cusip = defaultdict(lambda: {
        "name": "", "title": "", "cusip": "",
        "value_usd": 0, "shares": 0, "share_type": "SH",
    })
    for p in positions:
        key = p["cusip"]
        agg = by_cusip[key]
        agg["name"] = p["name"]
        agg["title"] = p["title"]
        agg["cusip"] = p["cusip"]
        agg["value_usd"] += p["value_usd"]
        agg["shares"] += p["shares"]
        agg["share_type"] = p["share_type"]

    return list(by_cusip.values())


def cusip_to_ticker(cusip: str, name: str):
    """Resolve CUSIP → ticker via override map.

    NOTE: FMP API lookups were originally used as fallback but were causing
    Lambda timeouts (each call ~200-500ms × thousands of CUSIPs per run).
    Now we only resolve via the static override map (94 top names) during
    Lambda execution. Tickers for positions outside the override map are
    null, and the frontend displays the issuer name instead. This keeps
    the Lambda fast and reliable; ticker resolution can be done lazily
    on-demand for the few that matter (top positions per fund).
    """
    if cusip in CUSIP_OVERRIDES:
        ticker, full_name = CUSIP_OVERRIDES[cusip]
        return ticker, full_name
    return None, name


def cusip_to_ticker_via_fmp(cusip: str, name: str):
    """Optional FMP lookup for individual cusips.
    Used during async resolve, NOT during parse. Returns (ticker, name) or (None, name).
    """
    if not FMP_KEY:
        return None, name
    try:
        url = f"https://financialmodelingprep.com/api/v3/cusip/{cusip}?apikey={FMP_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read())
        if isinstance(data, list) and data:
            t = data[0].get("ticker") or data[0].get("symbol")
            if t:
                return t.upper(), data[0].get("company") or name
    except Exception:
        pass
    return None, name


def parse_one_fund(fund_key: str, cik: str, latest_filing: dict, prior_filing: dict = None):
    """Parse a fund's latest filing + compare to prior. Returns full output dict."""
    accession = latest_filing.get("accession")
    if not accession:
        return {"fund_key": fund_key, "error": "no_accession"}

    # Cache check — version-tagged so unit fixes invalidate old cache
    PARSER_VERSION = "v2"   # bump when parser logic changes (units, fields, etc.)
    cache_key = f"{S3_CACHE_PREFIX}{fund_key}/{accession.replace('-', '')}_{PARSER_VERSION}.json"
    cached = get_s3_json(cache_key)
    if cached and cached.get("positions") and cached.get("parser_version") == PARSER_VERSION:
        positions = cached["positions"]
        print(f"  {fund_key}: using cached positions ({len(positions)})")
    else:
        # Parse fresh
        filing_dir = get_filing_index_dir(cik, accession)
        url, xml_text = find_infotable_xml(filing_dir)
        if not xml_text:
            return {"fund_key": fund_key, "error": "infotable_not_found"}
        positions = parse_infotable(xml_text)
        if not positions:
            return {"fund_key": fund_key, "error": "parse_returned_empty"}

        # Resolve tickers
        for p in positions:
            ticker, full_name = cusip_to_ticker(p["cusip"], p["name"])
            p["ticker"] = ticker
            p["resolved_name"] = full_name

        # Cache
        try:
            put_s3_json(cache_key, {
                "fund_key": fund_key,
                "accession": accession,
                "parser_version": PARSER_VERSION,
                "positions": positions,
                "cached_at": datetime.now(timezone.utc).isoformat(),
            })
        except Exception as e:
            print(f"  cache write fail {fund_key}: {e}")

    # Compute portfolio totals
    total_value = sum(p.get("value_usd", 0) for p in positions)

    # Sort by value desc and assign pct_of_portfolio
    positions.sort(key=lambda p: -p.get("value_usd", 0))
    for p in positions:
        p["pct_of_portfolio"] = round(100 * p["value_usd"] / max(total_value, 1), 2)

    # Compute changes vs prior filing
    prior_positions = []
    if prior_filing and prior_filing.get("accession"):
        prior_acc = prior_filing["accession"]
        prior_cache_key = f"{S3_CACHE_PREFIX}{fund_key}/{prior_acc.replace('-', '')}_{PARSER_VERSION}.json"
        prior_cached = get_s3_json(prior_cache_key)
        if prior_cached and prior_cached.get("parser_version") == PARSER_VERSION:
            prior_positions = prior_cached.get("positions", [])
        else:
            # Parse prior on-the-fly
            prior_dir = get_filing_index_dir(cik, prior_acc)
            _, prior_xml = find_infotable_xml(prior_dir)
            if prior_xml:
                prior_positions = parse_infotable(prior_xml)
                for p in prior_positions:
                    ticker, full_name = cusip_to_ticker(p["cusip"], p["name"])
                    p["ticker"] = ticker
                    p["resolved_name"] = full_name
                # Cache
                try:
                    put_s3_json(prior_cache_key, {
                        "fund_key": fund_key,
                        "accession": prior_acc,
                        "positions": prior_positions,
                        "cached_at": datetime.now(timezone.utc).isoformat(),
                    })
                except Exception:
                    pass

    prior_by_cusip = {p["cusip"]: p for p in prior_positions}

    # Tag each position with change vs prior
    new_pos, exits, adds, trims, holds = [], [], [], [], []
    for p in positions:
        prior = prior_by_cusip.get(p["cusip"])
        if not prior:
            p["change"] = "NEW"
            p["prior_value"] = 0
            p["value_delta_pct"] = None
            new_pos.append(p)
        else:
            prior_val = prior.get("value_usd", 0)
            cur_val = p["value_usd"]
            if prior_val > 0:
                delta_pct = (cur_val - prior_val) / prior_val * 100
            else:
                delta_pct = 0
            p["prior_value"] = prior_val
            p["value_delta_pct"] = round(delta_pct, 2)
            if delta_pct > 5:
                p["change"] = "ADD"
                adds.append(p)
            elif delta_pct < -5:
                p["change"] = "TRIM"
                trims.append(p)
            else:
                p["change"] = "HOLD"
                holds.append(p)

    # EXITS = in prior but not in current
    cur_cusips = {p["cusip"] for p in positions}
    for prior_p in prior_positions:
        if prior_p["cusip"] not in cur_cusips:
            exit_entry = {**prior_p, "change": "EXIT", "value_usd": 0,
                         "shares": 0, "prior_value": prior_p.get("value_usd", 0)}
            exits.append(exit_entry)

    return {
        "fund_key": fund_key,
        "fund_name": FUND_DISPLAY_NAMES.get(fund_key, fund_key),
        "filed_at": latest_filing.get("filed_at"),
        "period_of_report": latest_filing.get("period_of_report"),
        "accession": accession,
        "n_positions": len(positions),
        "total_value_usd": total_value,
        "positions": positions,
        "top_positions": positions[:25],
        "changes_summary": {
            "new": sorted(new_pos, key=lambda p: -p["value_usd"])[:15],
            "exits": sorted(exits, key=lambda p: -p["prior_value"])[:15],
            "adds": sorted(adds, key=lambda p: -p["value_usd"])[:15],
            "trims": sorted(trims, key=lambda p: -(p["prior_value"] - p["value_usd"]))[:15],
            "n_new": len(new_pos),
            "n_exits": len(exits),
            "n_adds": len(adds),
            "n_trims": len(trims),
            "n_holds": len(holds),
        },
    }


def aggregate_by_ticker(fund_results):
    """Aggregate positions across all funds, by ticker."""
    by_ticker = {}
    for fund_data in fund_results:
        if fund_data.get("error"):
            continue
        fund_key = fund_data["fund_key"]
        for p in fund_data.get("positions", []):
            tkr = p.get("ticker") or p.get("cusip", "?")[:9]
            if tkr not in by_ticker:
                by_ticker[tkr] = {
                    "ticker": p.get("ticker"),
                    "cusip": p.get("cusip"),
                    "name": p.get("resolved_name") or p.get("name"),
                    "n_funds_holding": 0,
                    "total_value": 0,
                    "n_funds_adding": 0,
                    "n_funds_trimming": 0,
                    "n_funds_new_position": 0,
                    "n_funds_exiting": 0,
                    "fund_actions": [],   # which funds did what
                }
            agg = by_ticker[tkr]
            agg["n_funds_holding"] += 1
            agg["total_value"] += p.get("value_usd", 0)
            change = p.get("change", "HOLD")
            if change == "ADD":
                agg["n_funds_adding"] += 1
            elif change == "TRIM":
                agg["n_funds_trimming"] += 1
            elif change == "NEW":
                agg["n_funds_new_position"] += 1
            agg["fund_actions"].append({
                "fund": fund_key,
                "fund_name": FUND_DISPLAY_NAMES.get(fund_key, fund_key),
                "value": p.get("value_usd", 0),
                "shares": p.get("shares", 0),
                "change": change,
                "pct_of_portfolio": p.get("pct_of_portfolio", 0),
                "delta_pct": p.get("value_delta_pct"),
            })

        # Also tally exits separately (these don't appear in positions)
        for ex in fund_data.get("changes_summary", {}).get("exits", []):
            tkr = ex.get("ticker") or ex.get("cusip", "?")[:9]
            if tkr not in by_ticker:
                by_ticker[tkr] = {
                    "ticker": ex.get("ticker"),
                    "cusip": ex.get("cusip"),
                    "name": ex.get("resolved_name") or ex.get("name"),
                    "n_funds_holding": 0, "total_value": 0,
                    "n_funds_adding": 0, "n_funds_trimming": 0,
                    "n_funds_new_position": 0, "n_funds_exiting": 0,
                    "fund_actions": [],
                }
            by_ticker[tkr]["n_funds_exiting"] += 1
            by_ticker[tkr]["fund_actions"].append({
                "fund": fund_key,
                "fund_name": FUND_DISPLAY_NAMES.get(fund_key, fund_key),
                "value": 0,
                "shares": 0,
                "change": "EXIT",
                "delta_pct": -100,
            })

    # Compute net_action_score for each
    for tkr, agg in by_ticker.items():
        agg["net_action_score"] = (
            agg["n_funds_adding"] + 2 * agg["n_funds_new_position"]
            - agg["n_funds_trimming"] - 2 * agg["n_funds_exiting"]
        )

    return by_ticker


def lambda_handler(event, context):
    started = time.time()

    # Step 1: load filings index from sec-13f Lambda's output
    filings_index = get_s3_json(S3_FILINGS_KEY, {})
    by_fund_meta = filings_index.get("by_fund", {})
    if not by_fund_meta:
        return {"statusCode": 502,
                "body": json.dumps({"error": "no filings index — run sec-13f first"})}

    # Step 2: load PRIOR run's positions to get prior accession per fund
    prior_run = get_s3_json(S3_KEY, {})
    prior_by_fund = {}
    for f in prior_run.get("by_fund", {}).values():
        prior_by_fund[f.get("fund_key")] = {
            "accession": f.get("accession"),
            "filed_at": f.get("filed_at"),
            "period_of_report": f.get("period_of_report"),
        }

    # Step 3: parse each fund's latest filing in parallel
    fund_results = []
    fund_inputs = []
    for fund_key, cik in WATCHLIST.items():
        meta = by_fund_meta.get(fund_key, {})
        latest = meta.get("latest_filing")
        if not latest or not latest.get("accession"):
            print(f"  {fund_key}: no latest filing in index")
            continue
        # Find prior — try to detect from our prior run if accession differs
        prior = None
        prior_meta = prior_by_fund.get(fund_key)
        if prior_meta and prior_meta.get("accession") != latest["accession"]:
            # We have a prior we know about
            prior = prior_meta
        fund_inputs.append((fund_key, cik, latest, prior))

    print(f"Parsing {len(fund_inputs)} funds in parallel (max {MAX_PARALLEL})…")
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as pool:
        futures = [pool.submit(parse_one_fund, *args) for args in fund_inputs]
        for fut in as_completed(futures):
            try:
                result = fut.result(timeout=120)
                fund_results.append(result)
            except Exception as e:
                fund_results.append({"error": str(e)})

    successful = [r for r in fund_results if not r.get("error")]
    failed = [r for r in fund_results if r.get("error")]

    # Step 3.5: async-resolve FMP CUSIPs for top positions per fund.
    # We deliberately skip CUSIP→ticker FMP lookups during parse (timeout
    # risk). Now resolve only the top 30 positions per fund (where it
    # matters for visualization). Caps the FMP calls at 30 × 18 = 540
    # max, with parallel workers, finishing in <30s.
    cusips_to_resolve = set()
    for fund in successful:
        for p in (fund.get("top_positions") or [])[:30]:
            if not p.get("ticker") and p.get("cusip"):
                cusips_to_resolve.add((p["cusip"], p.get("name", "")))

    print(f"Resolving {len(cusips_to_resolve)} unique top CUSIPs via FMP…")
    resolved_map = {}    # cusip → (ticker, name)
    if cusips_to_resolve and FMP_KEY:
        with ThreadPoolExecutor(max_workers=8) as pool:
            futs = {pool.submit(cusip_to_ticker_via_fmp, c, n): c
                    for c, n in cusips_to_resolve}
            for fut in as_completed(futs):
                cusip = futs[fut]
                try:
                    tk, nm = fut.result(timeout=10)
                    if tk:
                        resolved_map[cusip] = (tk, nm)
                except Exception:
                    pass

    print(f"  resolved {len(resolved_map)}/{len(cusips_to_resolve)} via FMP")

    # Apply resolved tickers back to all positions across all funds
    for fund in successful:
        for p in (fund.get("top_positions") or []):
            if not p.get("ticker") and p.get("cusip") in resolved_map:
                tk, nm = resolved_map[p["cusip"]]
                p["ticker"] = tk
                if nm:
                    p["resolved_name"] = nm
        for p in (fund.get("changes_summary", {}).get("new_positions") or []):
            if not p.get("ticker") and p.get("cusip") in resolved_map:
                p["ticker"], p["resolved_name"] = resolved_map[p["cusip"]]
        for p in (fund.get("changes_summary", {}).get("exited_positions") or []):
            if not p.get("ticker") and p.get("cusip") in resolved_map:
                p["ticker"], p["resolved_name"] = resolved_map[p["cusip"]]

    # Step 4: aggregate by ticker
    by_ticker = aggregate_by_ticker(successful)

    # Step 5: rankings
    most_bought = sorted(
        by_ticker.values(),
        key=lambda x: -((x["n_funds_adding"] + x["n_funds_new_position"]) * 100 + x["total_value"] / 1e9),
    )[:25]
    most_sold = sorted(
        by_ticker.values(),
        key=lambda x: -((x["n_funds_trimming"] + x["n_funds_exiting"]) * 100),
    )[:25]
    consensus_holds = sorted(
        by_ticker.values(),
        key=lambda x: (-x["n_funds_holding"], -x["total_value"]),
    )[:30]

    rare_picks = [
        x for x in by_ticker.values()
        if x["n_funds_holding"] == 1 and x["total_value"] > 50_000_000
    ]
    rare_picks.sort(key=lambda x: -x["total_value"])
    rare_picks = rare_picks[:20]

    # Step 6: write output
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "as_of_quarter": successful[0].get("period_of_report") if successful else None,
        "funds_total": len(WATCHLIST),
        "funds_parsed": len(successful),
        "funds_failed": len(failed),
        "fund_errors": [{"fund_key": f.get("fund_key"), "error": f.get("error")} for f in failed],
        "by_fund": {f["fund_key"]: f for f in successful},
        "aggregate_by_ticker": by_ticker,
        "most_bought": most_bought,
        "most_sold": most_sold,
        "consensus_holds": consensus_holds,
        "rare_picks": rare_picks,
        "fetch_duration_s": round(time.time() - started, 1),
    }

    put_s3_json(S3_KEY, output)
    print(f"13F positions: {len(successful)} funds parsed | "
          f"{len(by_ticker)} unique tickers | "
          f"top buy: {most_bought[0]['ticker'] if most_bought else '?'}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({
            "ok": True,
            "funds_parsed": len(successful),
            "funds_failed": len(failed),
            "tickers_aggregated": len(by_ticker),
        }),
    }
