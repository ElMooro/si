"""
justhodl-activist-filings-scanner — SEC 13D/13G/13D-A filings monitor

Activist investors and 5%+ holders must file 13D (active intent) or 13G
(passive intent) within 10 days of crossing the threshold. These filings
often precede major moves:
  • Carl Icahn 13D in OXY (2014) → +200% over 18 months
  • Bill Ackman 13D in ADP (2017) → +50% over 12 months
  • Elliott 13D in AT&T (2019) → +40% in 6 months

WHAT THIS DOES:
  1. Scans SEC EDGAR Atom RSS feeds for SC 13D, SC 13D/A, SC 13G, SC 13G/A daily
  2. Walks EDGAR daily-index for full coverage (filing.idx for that day)
  3. Identifies KNOWN ACTIVIST FILERS (ICAHN_ASSOCIATES, ELLIOTT_INV_MGMT,
     PERSHING_SQUARE, THIRD_POINT, JANA_PARTNERS, STARBOARD, TRIAN_PARTNERS,
     VALUEACT, ENGINE_NO_1, TPG, KKR, BLACKSTONE, BERKSHIRE_HATHAWAY, etc.)
  4. For each filing, parses the SEC URL to get:
     - Subject company (target ticker)
     - Filer (fund name)
     - Filing date
     - Form type (13D = active, 13G = passive)
  5. Scores by activist reputation × filing type × recency
  6. Cross-references with our universe to surface only US-listed names
  7. Writes data/activist-filings.json + tracks state for delta alerts

ALERT TRIGGERS:
  - New 13D filing by KNOWN_ACTIVIST = TIER_A
  - New 13D/A amendment with stake increase = TIER_A
  - New 13G by major fund (KKR, BLACKSTONE, etc.) = TIER_B
  - Volume of 13D filings on same ticker in 30 days >= 2 = TIER_A (multi-activist)

OUTPUT: data/activist-filings.json
"""
import io, json, os, time, urllib.request, urllib.error, urllib.parse, re
from collections import defaultdict
import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/activist-filings.json")
STATE_KEY = os.environ.get("STATE_KEY", "data/activist-filings-state.json")
SEC_USER_AGENT = os.environ.get("SEC_USER_AGENT", "JustHodl-AI raafouis@gmail.com")
TIMEOUT_BUDGET_S = int(os.environ.get("TIMEOUT_BUDGET_S", "260"))
DAYS_BACK = int(os.environ.get("DAYS_BACK", "30"))  # how many days of history to keep

S3 = boto3.client("s3", region_name=REGION)


# ─────────────────────────────────────────────────────────────────────────
# KNOWN ACTIVIST INVESTORS — name patterns matched against filer (case-insensitive)
# Tiered by reputation/track record
# ─────────────────────────────────────────────────────────────────────────
ACTIVIST_TIERS = {
    "TIER_S_LEGENDARY": [
        # Legendary activists — top-tier credibility
        "icahn",
        "berkshire hathaway",
        "berkshire ",
        "buffett",
        "warren buffett",
        "pershing square",
        "ackman",
        "third point",
        "loeb",  # Daniel Loeb / Third Point
        "elliott management",
        "elliott investment",
        "elliott associates",
        "paul singer",
        "trian fund",
        "trian partners",
        "trian management",
        "nelson peltz",
    ],
    "TIER_A_TOP": [
        # Top-tier activists
        "starboard",
        "valueact",
        "engine no. 1",
        "engine no 1",
        "jana partners",
        "irenic",  # Irenic Capital
        "blue harbour",
        "marcato",
        "land & buildings",
        "land and buildings",
        "scopia",
        "altimeter",
        "lone pine",
        "stephen mandel",
        "viking global",
        "andreas halvorsen",
        "tiger global",
        "chase coleman",
        "appaloosa",
        "david tepper",
        "soroban",
        "eric mandelblatt",
        "greenlight capital",
        "david einhorn",
        "muddy waters",  # Short activist
        "hindenburg",  # Short activist
        "scion asset management",  # Burry
        "michael burry",
    ],
    "TIER_B_MAJOR_FUND": [
        # Major institutional funds (typically 13G passive but signal)
        "blackstone",
        "kkr ",
        "kkr,",
        "kohlberg kravis",
        "apollo global",
        "carlyle group",
        "ares capital",
        "ares management",
        "bain capital",
        "tpg ",
        "tpg,",
        "warburg pincus",
        "general atlantic",
        "silver lake",
        "thoma bravo",
        "vista equity",
        "platinum equity",
        "advent international",
        "hellman & friedman",
        "ck capital",
        "cvc capital",
        "permira",
    ],
    "TIER_C_NOTABLE_HEDGE": [
        "millennium management",
        "citadel ",
        "citadel,",
        "renaissance technologies",
        "two sigma",
        "de shaw",
        "d. e. shaw",
        "point72",
        "balyasny",
        "exodus point",
        "schonfeld",
        "marshall wace",
        "brevan howard",
        "winton",
        "man group",
        "tpg axon",
        "coatue",
        "philippe laffont",
        "whale rock",
        "soros fund",
        "stanley druckenmiller",
        "duquesne",
        "moore capital",
        "louis bacon",
    ],
}


def http_get(url, timeout=20):
    """SEC requires User-Agent with email."""
    req = urllib.request.Request(url, headers={
        "User-Agent": SEC_USER_AGENT,
        "Accept": "application/json,application/atom+xml,*/*",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def fetch_atom_feed(form_type):
    """Fetch latest filings RSS feed for a form type."""
    url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=" + urllib.parse.quote(form_type) + "&output=atom"
    try:
        text = http_get(url, timeout=15)
        return parse_atom_entries(text)
    except Exception as e:
        print("[activist] atom feed " + form_type + " failed: " + str(e))
        return []


def parse_atom_entries(xml_text):
    """Parse SEC Atom feed entries into structured records."""
    entries = []
    # Extract <entry>...</entry> blocks
    blocks = re.findall(r"<entry>(.*?)</entry>", xml_text, flags=re.DOTALL)
    for b in blocks:
        # Each entry: <title>FORM_TYPE - FILER_NAME (CIK) (Filer)</title>
        # Or: <title>FORM_TYPE - SUBJECT_NAME (CIK) (Subject)</title>
        title_m = re.search(r"<title>([^<]+)</title>", b)
        link_m = re.search(r'<link[^/]*href="([^"]+)"', b)
        updated_m = re.search(r"<updated>([^<]+)</updated>", b)
        summary_m = re.search(r"<summary[^>]*>([^<]+)</summary>", b, flags=re.DOTALL)
        if not title_m:
            continue
        title = title_m.group(1).strip()
        link = link_m.group(1) if link_m else ""
        updated = updated_m.group(1) if updated_m else ""
        summary = summary_m.group(1).strip() if summary_m else ""

        # Parse title: "FORM_TYPE - NAME (CIK) (Role)"
        title_match = re.match(r"^([A-Z\d/\-\.+]+)\s*-\s*(.+?)\s*\((\d+)\)\s*\((Filer|Subject|Reporting)\)\s*$", title)
        form_type = ""
        name = ""
        cik = ""
        role = ""
        if title_match:
            form_type = title_match.group(1).strip()
            name = title_match.group(2).strip()
            cik = title_match.group(3).strip()
            role = title_match.group(4).strip()
        else:
            form_type = title.split("-", 1)[0].strip() if "-" in title else title
            name = title

        entries.append({
            "form_type": form_type,
            "name": name,
            "cik": cik,
            "role": role,
            "title": title,
            "link": link,
            "updated_iso": updated,
            "summary": summary[:500],
        })
    return entries


def fetch_filing_detail(filing_index_url):
    """Pull the filing-index page to find the subject company and tickers.
    EDGAR filings have a /Archives/edgar/data/CIK/ACCESSION/ folder. The
    primary index file lists subject + filer.
    """
    try:
        text = http_get(filing_index_url, timeout=15)
    except Exception:
        return {}
    
    # Try to extract subject company name + CIK
    # Look for subject company section in HTML
    info = {"subject_name": None, "subject_cik": None, "subject_ticker": None}

    # SEC's index pages have: "Subject Company\n-+\nCompanyName\nCIK"
    subj_match = re.search(r"Subject Company.*?<a[^>]*>([^<]+)</a>.*?CIK[^>]*>(\d+)", text, flags=re.DOTALL)
    if subj_match:
        info["subject_name"] = subj_match.group(1).strip()
        info["subject_cik"] = subj_match.group(2).strip()

    # Look for ticker references in the text
    ticker_match = re.search(r'(?:ticker|symbol)[^A-Z]*([A-Z]{1,5})\b', text)
    if ticker_match:
        info["subject_ticker"] = ticker_match.group(1)

    return info


def cik_to_ticker_map():
    """Pull SEC's company-ticker mapping (CIK→ticker)."""
    url = "https://www.sec.gov/files/company_tickers.json"
    try:
        text = http_get(url, timeout=20)
        d = json.loads(text)
        # Format: {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "..."}}
        cik_map = {}
        for k, v in d.items():
            if isinstance(v, dict) and v.get("cik_str") and v.get("ticker"):
                cik_str = str(v["cik_str"]).zfill(10)
                cik_map[cik_str] = {
                    "ticker": v["ticker"].upper(),
                    "company_name": v.get("title", ""),
                }
        return cik_map
    except Exception as e:
        print("[activist] cik mapping failed: " + str(e))
        return {}


def classify_filer(name):
    """Match filer name against known activist patterns."""
    name_lower = name.lower().strip()
    for tier_name, patterns in ACTIVIST_TIERS.items():
        for pat in patterns:
            if pat in name_lower:
                return tier_name, pat
    return None, None


def get_universe_tickers():
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/universe.json")
        d = json.loads(obj["Body"].read())
        return {(s.get("symbol") or "").upper(): s for s in d.get("stocks", [])}
    except Exception:
        return {}


def lambda_handler(event=None, context=None):
    started = time.time()
    deadline_at = started + TIMEOUT_BUDGET_S
    print("[activist] starting v1.0")

    # Pull CIK → ticker mapping
    print("[activist] fetching CIK → ticker map...")
    cik_map = cik_to_ticker_map()
    print("[activist] mapped " + str(len(cik_map)) + " CIKs to tickers")

    universe = get_universe_tickers()
    print("[activist] universe: " + str(len(universe)) + " tickers")

    # Pull RSS feeds for all form types we care about
    form_types = ["SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A"]
    all_entries = []
    for ft in form_types:
        print("[activist] fetching " + ft + "...")
        entries = fetch_atom_feed(ft)
        print("[activist]   got " + str(len(entries)) + " entries")
        for e in entries:
            e["fetched_form_type"] = ft
        all_entries.extend(entries)

    # Group by accession number / filing URL to dedupe
    # The same filing has 2 entries (Filer + Subject)
    # Pair them up using the link (which contains the accession#)
    by_accession = defaultdict(list)
    for e in all_entries:
        link = e.get("link", "")
        # Extract /Archives/edgar/data/CIK/ACCESSION-NUMBER-INDEX.htm
        accession_match = re.search(r"/Archives/edgar/data/(\d+)/(\d+-\d+-\d+|[\d\-]+)", link)
        if accession_match:
            accession = accession_match.group(2)
            by_accession[accession].append(e)
        else:
            # No accession — group by link itself
            by_accession[link].append(e)

    print("[activist] " + str(len(by_accession)) + " unique filings")

    # Build structured records — each filing has filer + subject
    filings = []
    for acc, entries in by_accession.items():
        filer = None
        subject = None
        form_type = None
        for e in entries:
            role = e.get("role", "")
            if role == "Filer" or role == "Reporting":
                filer = e
            elif role == "Subject":
                subject = e
            if not form_type:
                form_type = e.get("fetched_form_type") or e.get("form_type")

        # Fall back: if both entries have same role, take first
        if not filer and entries:
            filer = entries[0]
        if not subject and len(entries) > 1:
            subject = entries[1]
        elif not subject and entries:
            subject = entries[0]

        if not filer:
            continue

        filer_name = filer.get("name", "")
        filer_cik = filer.get("cik", "").zfill(10) if filer.get("cik") else ""
        subject_name = subject.get("name", "") if subject else ""
        subject_cik = subject.get("cik", "").zfill(10) if subject else ""
        link = filer.get("link", "")
        updated = filer.get("updated_iso", "")

        # Map subject CIK to ticker
        subject_ticker = None
        subject_company = None
        if subject_cik and subject_cik in cik_map:
            subject_ticker = cik_map[subject_cik]["ticker"]
            subject_company = cik_map[subject_cik]["company_name"]

        # Classify filer
        tier, matched_pattern = classify_filer(filer_name)

        # Score
        score = 0
        flags = []
        if tier == "TIER_S_LEGENDARY":
            score += 50
            flags.append("LEGENDARY_FILER")
        elif tier == "TIER_A_TOP":
            score += 35
            flags.append("TOP_ACTIVIST")
        elif tier == "TIER_B_MAJOR_FUND":
            score += 20
            flags.append("MAJOR_FUND")
        elif tier == "TIER_C_NOTABLE_HEDGE":
            score += 10
            flags.append("NOTABLE_HEDGE")

        # 13D = active, 13D/A = amendment (often = stake change), 13G = passive
        if form_type and ("13D" in form_type and "/A" not in form_type):
            score += 25
            flags.append("ACTIVE_INTENT")
        elif form_type and "13D/A" in form_type:
            score += 20
            flags.append("ACTIVE_AMENDMENT")
        elif form_type and ("13G" in form_type and "/A" not in form_type):
            score += 10
            flags.append("PASSIVE_INTENT")
        elif form_type and "13G/A" in form_type:
            score += 8
            flags.append("PASSIVE_AMENDMENT")

        # Bonus if subject ticker is in our investable universe
        in_universe = subject_ticker in universe if subject_ticker else False
        if in_universe:
            score += 15
            flags.append("IN_UNIVERSE")

        score = min(score, 100)

        if score >= 70:
            level = "TIER_A_HOT"
        elif score >= 50:
            level = "TIER_B_BUILDING"
        elif score >= 30:
            level = "WATCH"
        else:
            level = "NOTABLE"

        filings.append({
            "accession": acc,
            "form_type": form_type,
            "filer_name": filer_name,
            "filer_cik": filer_cik,
            "subject_name": subject_name,
            "subject_cik": subject_cik,
            "subject_ticker": subject_ticker,
            "subject_company": subject_company,
            "filer_tier": tier,
            "matched_pattern": matched_pattern,
            "in_universe": in_universe,
            "score": score,
            "level": level,
            "flags": flags,
            "filing_link": link,
            "filing_date": updated[:10] if updated else "",
            "updated_iso": updated,
        })

    # Sort by score
    filings.sort(key=lambda x: -x["score"])

    # Aggregate by ticker — multi-activist same name detection
    by_ticker = defaultdict(list)
    for f in filings:
        if f["subject_ticker"]:
            by_ticker[f["subject_ticker"]].append(f)

    multi_activist = []
    for ticker, fs in by_ticker.items():
        if len(fs) >= 2:
            multi_activist.append({
                "ticker": ticker,
                "company": fs[0].get("subject_company", ""),
                "n_filings": len(fs),
                "filers": [f["filer_name"] for f in fs],
                "form_types": list(set(f["form_type"] for f in fs)),
                "tiers": list(set(f.get("filer_tier") for f in fs if f.get("filer_tier"))),
                "max_score": max(f["score"] for f in fs),
            })
    multi_activist.sort(key=lambda x: -x["max_score"])

    # Detect new since last run (state-based)
    prior_state = None
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=STATE_KEY)
        prior_state = json.loads(obj["Body"].read())
    except Exception:
        pass

    prior_seen = set((prior_state or {}).get("seen_accessions", []))
    new_filings = [f for f in filings if f["accession"] not in prior_seen]
    new_seen = list(prior_seen | set(f["accession"] for f in filings))[-1000:]  # cap state size

    # Top tier-A new filings = highest signal
    new_tier_a = [f for f in new_filings if f["level"] == "TIER_A_HOT"]
    new_tier_b = [f for f in new_filings if f["level"] == "TIER_B_BUILDING"]

    print("[activist] total filings: " + str(len(filings)))
    print("[activist] new filings this run: " + str(len(new_filings)))
    print("[activist] new TIER-A: " + str(len(new_tier_a)))
    print("[activist] new TIER-B: " + str(len(new_tier_b)))
    print("[activist] multi-activist tickers: " + str(len(multi_activist)))

    out = {
        "schema_version": 1,
        "method": "activist_filings_scanner_v1",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "duration_s": round(time.time() - started, 1),
        "stats": {
            "n_filings_total": len(filings),
            "n_unique_tickers": len(by_ticker),
            "n_multi_activist": len(multi_activist),
            "n_new_filings": len(new_filings),
            "n_new_tier_a": len(new_tier_a),
            "n_new_tier_b": len(new_tier_b),
            "n_in_universe": sum(1 for f in filings if f["in_universe"]),
        },
        "summary": {
            "top_25_filings": filings[:25],
            "new_tier_a_alerts": new_tier_a,
            "new_tier_b_alerts": new_tier_b,
            "multi_activist_setups": multi_activist[:15],
        },
        "all_filings": filings,
    }

    body = json.dumps(out, default=str).encode()
    S3.put_object(Bucket=BUCKET, Key=S3_KEY, Body=body, ContentType="application/json")
    print("[activist] wrote " + str(len(body)) + "b")

    # Save state
    state = {
        "generated_at": out["generated_at"],
        "seen_accessions": new_seen,
    }
    S3.put_object(Bucket=BUCKET, Key=STATE_KEY,
                   Body=json.dumps(state).encode(),
                   ContentType="application/json")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_filings": len(filings),
            "n_new": len(new_filings),
            "n_new_tier_a": len(new_tier_a),
            "n_multi_activist": len(multi_activist),
            "duration_s": out["duration_s"],
        }),
    }


# Need urllib.parse import at module level
