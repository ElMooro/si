"""
justhodl-activist-filings-scanner v2 — SEC EDGAR daily-index edition

DATA SOURCE (MUCH BETTER than Atom RSS):
  EDGAR daily-index master.idx files give us EVERY filing of EVERY type per day.
  Format (pipe-delimited):  CIK|Company Name|Form Type|Date Filed|File Name
  
  Yesterday's master.idx had 192 13D/G filings (vs 1 in the Atom feed).

PIPELINE:
  1. Walk back N business days, fetch each master.YYYYMMDD.idx
  2. Filter to SCHEDULE 13D, SCHEDULE 13D/A, SCHEDULE 13G, SCHEDULE 13G/A
  3. For each filing, extract:
     - Filer CIK + name (from idx)
     - Accession number (from filename)
     - Filing URL (.txt → primary doc)
  4. Parse each 13D doc to extract SUBJECT company + ticker
  5. Map filer CIK to known activist patterns (TIER_S/A/B/C)
  6. Cross-reference subject ticker with our investable universe
  7. Score: filer_tier × form_type × in_universe
  8. Detect multi-activist setups (>=2 filings same ticker in N days)

OUTPUT:
  data/activist-filings.json
  data/activist-filings-state.json (seen accessions for delta detection)
"""
import io, json, os, time, urllib.request, urllib.error, urllib.parse, re
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict, Counter
import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/activist-filings.json")
STATE_KEY = os.environ.get("STATE_KEY", "data/activist-filings-state.json")
SEC_USER_AGENT = os.environ.get("SEC_USER_AGENT", "JustHodl-AI raafouis@gmail.com")
TIMEOUT_BUDGET_S = int(os.environ.get("TIMEOUT_BUDGET_S", "260"))
DAYS_BACK = int(os.environ.get("DAYS_BACK", "5"))  # business days back to scan
N_WORKERS = int(os.environ.get("N_WORKERS", "10"))
RESOLVE_SUBJECTS = os.environ.get("RESOLVE_SUBJECTS", "1") == "1"

S3 = boto3.client("s3", region_name=REGION)


# ─────────────────────────────────────────────────────────────────────────
# KNOWN ACTIVIST INVESTORS — name patterns (lowercase substring match against filer)
# ─────────────────────────────────────────────────────────────────────────
ACTIVIST_TIERS = {
    "TIER_S_LEGENDARY": [
        "icahn",
        "berkshire hathaway",
        "warren buffett",
        "cascade investment",  # Bill Gates' family office
        "pershing square",
        "ackman",
        "third point",
        "elliott management",
        "elliott investment",
        "paul singer",
        "trian fund",
        "trian partners",
        "trian management",
        "nelson peltz",
        "michael burry",
        "scion asset",
    ],
    "TIER_A_TOP": [
        "starboard",
        "valueact",
        "engine no. 1",
        "engine no 1",
        "jana partners",
        "irenic",
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
        "greenlight capital",
        "david einhorn",
        "muddy waters",  # short activist
        "hindenburg",  # short activist
        "carson block",
        "orbimed",  # biotech specialist
        "baker bros",
        "lundbeck",
    ],
    "TIER_B_MAJOR_FUND": [
        "blackstone",
        "kkr ",
        "kkr,",
        "kkr.",
        "kohlberg kravis",
        "apollo global",
        "carlyle group",
        "ares capital",
        "ares management",
        "bain capital",
        "tpg ",
        "tpg,",
        "tpg.",
        "warburg pincus",
        "general atlantic",
        "silver lake",
        "thoma bravo",
        "vista equity",
        "platinum equity",
        "advent international",
        "hellman & friedman",
        "cvc capital",
        "permira",
        "leonard green",
        "msd partners",
        "tpg axon",
    ],
    "TIER_C_NOTABLE_HEDGE": [
        "millennium management",
        "citadel ",
        "citadel,",
        "citadel.",
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
        "coatue",
        "philippe laffont",
        "whale rock",
        "soros fund",
        "stanley druckenmiller",
        "duquesne",
        "moore capital",
        "louis bacon",
        "glenview capital",
        "larry robbins",
        "pzena investment",
        "davis selected",
    ],
}


def http_get(url, timeout=20):
    req = urllib.request.Request(url, headers={
        "User-Agent": SEC_USER_AGENT,
        "Accept": "*/*",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def cik_to_ticker_map():
    """Pull SEC's CIK → ticker mapping."""
    url = "https://www.sec.gov/files/company_tickers.json"
    try:
        text = http_get(url, timeout=20)
        d = json.loads(text)
        cik_map = {}
        for k, v in d.items():
            if isinstance(v, dict) and v.get("cik_str") and v.get("ticker"):
                cik_str = str(v["cik_str"]).zfill(10)
                # Also store unpadded for flexibility
                cik_map[cik_str] = {
                    "ticker": v["ticker"].upper(),
                    "company_name": v.get("title", ""),
                }
                cik_map[str(v["cik_str"])] = cik_map[cik_str]
        return cik_map
    except Exception as e:
        print("[activist] cik mapping failed: " + str(e))
        return {}


def fetch_master_idx(date_obj):
    """Fetch master.YYYYMMDD.idx for one trading day. Returns parsed list of filings."""
    date_str = time.strftime("%Y%m%d", date_obj)
    year = time.strftime("%Y", date_obj)
    qtr = ((date_obj.tm_mon - 1) // 3) + 1
    url = "https://www.sec.gov/Archives/edgar/daily-index/" + year + "/QTR" + str(qtr) + "/master." + date_str + ".idx"
    try:
        text = http_get(url, timeout=25)
    except urllib.error.HTTPError as e:
        # Index may not exist for weekends/holidays
        if e.code == 404:
            return None
        raise
    except Exception as e:
        print("[activist] master.idx " + date_str + " failed: " + str(e))
        return None

    filings = []
    for line in text.splitlines():
        parts = line.split("|")
        if len(parts) != 5:
            continue
        cik, company, form, filed, filename = [p.strip() for p in parts]
        if not cik.isdigit():
            continue
        if "13D" in form or "13G" in form:
            # Filename like edgar/data/1045942/0000921895-26-001162.txt
            # Build the index page URL: /Archives/edgar/data/CIK/ACCESSION-INDEX.htm
            accession = ""
            m = re.search(r"(\d{10}-\d{2}-\d{6})", filename)
            if m:
                accession = m.group(1)
            else:
                # Fallback: last token before .txt
                m2 = re.search(r"/(\d+\-\d+\-\d+)\.txt$", filename)
                if m2:
                    accession = m2.group(1)
            
            filings.append({
                "filer_cik": cik.zfill(10),
                "filer_cik_raw": cik,
                "filer_name": company,
                "form_type": form,
                "filed_date": filed,
                "txt_filename": filename,
                "accession": accession,
                "filing_url_txt": "https://www.sec.gov/Archives/" + filename,
                "index_url": "https://www.sec.gov/Archives/" + filename.replace(".txt", "-index.htm"),
            })
    return filings


def fetch_subject_from_filing(filing):
    """Pull the SEC index page (.htm or .txt header) to extract subject CIK + name.
    
    13D/G filings include a SUBJECT COMPANY section with CIK.
    The fastest source is the .txt SGML header at the top of the .txt file.
    """
    # Fetch first 10KB of the text file to get the SGML header
    url = filing["filing_url_txt"]
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": SEC_USER_AGENT,
            "Range": "bytes=0-15000",
        })
        with urllib.request.urlopen(req, timeout=15) as r:
            text = r.read().decode("utf-8", "replace")
    except Exception:
        return None

    # SGML header in 13D/G has:
    # <SUBJECT-COMPANY>
    #   ...
    #   COMPANY CONFORMED NAME:    Genco Shipping & Trading Ltd
    #   CENTRAL INDEX KEY:         0001326200
    #   ...
    
    # Find SUBJECT-COMPANY block
    subj_block_m = re.search(r"SUBJECT[\s\-]*COMPANY[:\s]*(.+?)(?:FILED[\s\-]*BY|FILER|</SEC-HEADER>)",
                              text, flags=re.DOTALL | re.IGNORECASE)
    if not subj_block_m:
        return None
    subj_block = subj_block_m.group(1)

    name_m = re.search(r"COMPANY CONFORMED NAME[:\s]+([^\r\n]+)", subj_block, flags=re.IGNORECASE)
    cik_m = re.search(r"CENTRAL INDEX KEY[:\s]+(\d+)", subj_block, flags=re.IGNORECASE)
    
    if not (name_m or cik_m):
        return None
    
    return {
        "subject_name": name_m.group(1).strip() if name_m else "",
        "subject_cik": cik_m.group(1).strip().zfill(10) if cik_m else "",
    }


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
    print("[activist-v2] starting v2.0 — daily-index edition")

    # Load CIK→ticker map
    cik_map = cik_to_ticker_map()
    print("[activist-v2] cik map: " + str(len(cik_map)) + " entries")

    universe = get_universe_tickers()
    print("[activist-v2] universe: " + str(len(universe)) + " tickers")

    # Walk back DAYS_BACK business days
    all_filings = []
    days_collected = 0
    days_back = 0
    while days_collected < DAYS_BACK and days_back < DAYS_BACK * 2 + 5:
        check_dt = time.gmtime(time.time() - days_back * 86400)
        wday = check_dt.tm_wday
        if wday >= 5:
            days_back += 1
            continue
        date_str = time.strftime("%Y-%m-%d", check_dt)
        if time.time() > deadline_at:
            break
        filings = fetch_master_idx(check_dt)
        if filings is not None:
            print("[activist-v2] " + date_str + ": " + str(len(filings)) + " 13D/G filings")
            all_filings.extend(filings)
            days_collected += 1
        days_back += 1

    print("[activist-v2] total raw filings: " + str(len(all_filings)))

    # Resolve subject company for each filing (parallel, with deadline)
    subjects_resolved = 0
    if RESOLVE_SUBJECTS and all_filings:
        def resolve_one(filing):
            if time.time() > deadline_at:
                return filing
            try:
                subj = fetch_subject_from_filing(filing)
                if subj:
                    filing.update(subj)
            except Exception:
                pass
            return filing

        with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
            futures = {pool.submit(resolve_one, f): f for f in all_filings}
            for fut in as_completed(futures):
                try:
                    f = fut.result()
                    if f.get("subject_cik"):
                        subjects_resolved += 1
                except Exception:
                    continue

    print("[activist-v2] subjects resolved: " + str(subjects_resolved) + "/" + str(len(all_filings)))

    # Enrich each filing with classifier + ticker mapping + score
    enriched = []
    for f in all_filings:
        tier, matched_pattern = classify_filer(f["filer_name"])

        subject_ticker = None
        subject_company = None
        if f.get("subject_cik") and f["subject_cik"] in cik_map:
            subject_ticker = cik_map[f["subject_cik"]]["ticker"]
            subject_company = cik_map[f["subject_cik"]]["company_name"]
        elif f.get("subject_cik"):
            # Try unpadded
            unpadded = f["subject_cik"].lstrip("0")
            if unpadded in cik_map:
                subject_ticker = cik_map[unpadded]["ticker"]
                subject_company = cik_map[unpadded]["company_name"]

        in_universe = subject_ticker in universe if subject_ticker else False

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

        ft = f.get("form_type", "")
        if "13D" in ft and "/A" not in ft:
            score += 25
            flags.append("ACTIVE_INTENT")
        elif "13D/A" in ft:
            score += 20
            flags.append("ACTIVE_AMENDMENT")
        elif "13G" in ft and "/A" not in ft:
            score += 10
            flags.append("PASSIVE_INTENT")
        elif "13G/A" in ft:
            score += 8
            flags.append("PASSIVE_AMENDMENT")

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

        enriched.append({
            "accession": f.get("accession"),
            "form_type": ft,
            "filer_name": f["filer_name"],
            "filer_cik": f["filer_cik"],
            "subject_name": f.get("subject_name", ""),
            "subject_cik": f.get("subject_cik", ""),
            "subject_ticker": subject_ticker,
            "subject_company": subject_company,
            "filer_tier": tier,
            "matched_pattern": matched_pattern,
            "in_universe": in_universe,
            "score": score,
            "level": level,
            "flags": flags,
            "filing_url": f.get("filing_url_txt"),
            "filed_date": f.get("filed_date"),
        })

    enriched.sort(key=lambda x: -x["score"])

    # Multi-activist setups (≥2 filings on same ticker)
    by_ticker = defaultdict(list)
    for f in enriched:
        if f["subject_ticker"]:
            by_ticker[f["subject_ticker"]].append(f)
    multi_activist = []
    for ticker, fs in by_ticker.items():
        if len(fs) >= 2:
            multi_activist.append({
                "ticker": ticker,
                "company": fs[0].get("subject_company", ""),
                "n_filings": len(fs),
                "filers": list(set(f["filer_name"] for f in fs))[:6],
                "form_types": list(set(f["form_type"] for f in fs)),
                "tiers": list(set(f.get("filer_tier") for f in fs if f.get("filer_tier"))),
                "max_score": max(f["score"] for f in fs),
                "in_universe": fs[0].get("in_universe", False),
            })
    multi_activist.sort(key=lambda x: -x["max_score"])

    # State delta tracking
    prior_state = None
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=STATE_KEY)
        prior_state = json.loads(obj["Body"].read())
    except Exception:
        pass

    prior_seen = set((prior_state or {}).get("seen_accessions", []))
    new_filings = [f for f in enriched if f["accession"] not in prior_seen]
    new_seen = list(prior_seen | set(f["accession"] for f in enriched if f["accession"]))[-2000:]

    new_tier_a = [f for f in new_filings if f["level"] == "TIER_A_HOT"]
    new_tier_b = [f for f in new_filings if f["level"] == "TIER_B_BUILDING"]

    print("[activist-v2] total enriched: " + str(len(enriched)))
    print("[activist-v2] new this run: " + str(len(new_filings)))
    print("[activist-v2] new TIER-A: " + str(len(new_tier_a)) + ", TIER-B: " + str(len(new_tier_b)))
    print("[activist-v2] multi-activist tickers: " + str(len(multi_activist)))

    # Filer-tier breakdown stats
    tier_counts = Counter(f["filer_tier"] for f in enriched if f["filer_tier"])

    out = {
        "schema_version": 2,
        "method": "activist_filings_daily_index_v2",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "duration_s": round(time.time() - started, 1),
        "stats": {
            "n_filings_total": len(enriched),
            "n_subjects_resolved": subjects_resolved,
            "n_unique_tickers": len(by_ticker),
            "n_multi_activist": len(multi_activist),
            "n_new_filings": len(new_filings),
            "n_new_tier_a": len(new_tier_a),
            "n_new_tier_b": len(new_tier_b),
            "n_in_universe": sum(1 for f in enriched if f["in_universe"]),
            "tier_counts": dict(tier_counts),
            "days_collected": days_collected,
        },
        "summary": {
            "top_25_filings": enriched[:25],
            "top_25_in_universe": [f for f in enriched if f["in_universe"]][:25],
            "new_tier_a_alerts": new_tier_a,
            "new_tier_b_alerts": new_tier_b,
            "multi_activist_setups": multi_activist[:15],
        },
        "all_filings": enriched,
    }

    body = json.dumps(out, default=str).encode()
    S3.put_object(Bucket=BUCKET, Key=S3_KEY, Body=body, ContentType="application/json")
    print("[activist-v2] wrote " + str(len(body)) + "b")

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
            "n_filings": len(enriched),
            "n_resolved": subjects_resolved,
            "n_new_tier_a": len(new_tier_a),
            "n_multi_activist": len(multi_activist),
            "duration_s": out["duration_s"],
        }),
    }
