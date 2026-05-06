"""
justhodl-activist-filings-scanner v3 — EDGAR Atom RSS + full-text search

PROVEN WORKING STRATEGY (after diagnosing 403 + EFTS pagination quirks):
  1. SEC RSS Atom feeds: 4 form types (SC 13D/13D-A/13G/13G-A) — gives latest
     ~40 filings of each — refreshed every minute by SEC
  2. EDGAR full-text search /LATEST/search-index?q=...&forms=... (no date
     filter, client-side filter by file_date) — gives broader history
  3. display_names field often includes inline ticker like "Apple Inc. (AAPL)"
  4. company_tickers.json for CIK→ticker fallback
  5. Atom RSS gives entries with form_type/filer_name/cik/role/link/date

WHAT THIS DOES:
  • Scans 4 RSS feeds for fresh 13D/G filings (last ~24h coverage)
  • Pulls EFTS history search (broader 30-day window) and filters client-side
  • Resolves subject ticker via display_names regex + CIK fallback
  • Identifies KNOWN ACTIVIST FILERS (4 tiers: legendary/top/major/notable)
  • Scores each filing by activist tier × form type × universe match
  • Detects multi-activist setups (>=2 activists on same ticker)
  • State-tracks accession numbers for delta alerts

OUTPUT: data/activist-filings.json
"""
import io, json, os, time, re, urllib.request, urllib.error, urllib.parse
from collections import defaultdict
import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/activist-filings.json")
STATE_KEY = os.environ.get("STATE_KEY", "data/activist-filings-state.json")
SEC_USER_AGENT = os.environ.get("SEC_USER_AGENT", "JustHodl-AI raafouis@gmail.com")
TIMEOUT_BUDGET_S = int(os.environ.get("TIMEOUT_BUDGET_S", "260"))
DAYS_BACK = int(os.environ.get("DAYS_BACK", "30"))

S3 = boto3.client("s3", region_name=REGION)


# ─────────────────────────────────────────────────────────────────────────
# KNOWN ACTIVIST INVESTORS — 4 tiers
# ─────────────────────────────────────────────────────────────────────────
ACTIVIST_TIERS = {
    "TIER_S_LEGENDARY": [
        "icahn", "berkshire hathaway", "berkshire ", "buffett",
        "pershing square", "ackman", "third point", "loeb",
        "elliott management", "elliott investment", "elliott associates",
        "paul singer", "trian fund", "trian partners", "trian management",
        "nelson peltz",
    ],
    "TIER_A_TOP": [
        "starboard", "valueact", "engine no. 1", "engine no 1",
        "jana partners", "irenic", "blue harbour", "marcato",
        "land & buildings", "land and buildings",
        "scopia", "altimeter", "lone pine", "stephen mandel",
        "viking global", "andreas halvorsen", "tiger global", "chase coleman",
        "appaloosa", "david tepper", "soroban", "eric mandelblatt",
        "greenlight capital", "david einhorn",
        "muddy waters", "hindenburg",
        "scion asset management", "michael burry",
    ],
    "TIER_B_MAJOR_FUND": [
        "blackstone", "kkr ", "kkr,", "kohlberg kravis",
        "apollo global", "carlyle group",
        "ares capital", "ares management",
        "bain capital", "tpg ", "tpg,",
        "warburg pincus", "general atlantic", "silver lake",
        "thoma bravo", "vista equity", "platinum equity",
        "advent international", "hellman & friedman",
        "ck capital", "cvc capital", "permira",
    ],
    "TIER_C_NOTABLE_HEDGE": [
        "millennium management", "citadel ", "citadel,",
        "renaissance technologies", "two sigma",
        "de shaw", "d. e. shaw", "point72", "balyasny",
        "exodus point", "schonfeld", "marshall wace",
        "brevan howard", "winton", "man group",
        "tpg axon", "coatue", "philippe laffont", "whale rock",
        "soros fund", "stanley druckenmiller", "duquesne",
        "moore capital", "louis bacon",
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


# ─── Atom RSS ────────────────────────────────────────────────────────────

def fetch_atom_feed(form_type):
    """Fetch latest filings for a form type via Atom RSS."""
    url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=" + urllib.parse.quote(form_type) + "&output=atom&count=40"
    try:
        text = http_get(url, timeout=15)
        return parse_atom_entries(text, form_type)
    except Exception as e:
        print("[activist] atom feed " + form_type + " failed: " + str(e))
        return []


def parse_atom_entries(xml_text, fetched_form_type):
    """Parse SEC Atom feed entries."""
    entries = []
    blocks = re.findall(r"<entry>(.*?)</entry>", xml_text, flags=re.DOTALL)
    for b in blocks:
        title_m = re.search(r"<title>([^<]+)</title>", b)
        link_m = re.search(r'<link[^/]*href="([^"]+)"', b)
        updated_m = re.search(r"<updated>([^<]+)</updated>", b)
        summary_m = re.search(r"<summary[^>]*>(.*?)</summary>", b, flags=re.DOTALL)
        if not title_m:
            continue
        title = title_m.group(1).strip()
        link = link_m.group(1) if link_m else ""
        updated = updated_m.group(1) if updated_m else ""
        summary = (summary_m.group(1) if summary_m else "").strip()
        # Decode HTML entities in summary
        summary = summary.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")

        title_match = re.match(r"^([A-Z\d/\-\.+\s]+?)\s*-\s*(.+?)\s*\((\d+)\)\s*\((Filer|Subject|Reporting)\)\s*$", title)
        form_type = fetched_form_type
        name = ""
        cik = ""
        role = ""
        if title_match:
            form_type = title_match.group(1).strip() or fetched_form_type
            name = title_match.group(2).strip()
            cik = title_match.group(3).strip()
            role = title_match.group(4).strip()
        else:
            name = title

        entries.append({
            "form_type": form_type,
            "fetched_form_type": fetched_form_type,
            "name": name,
            "cik": cik,
            "role": role,
            "title": title,
            "link": link,
            "updated_iso": updated,
            "summary": summary[:500],
            "source": "rss",
        })
    return entries


# ─── EFTS full-text search ───────────────────────────────────────────────

def fetch_efts_search(form_type_query, q_text, max_pages=4, max_retries=3):
    """Use EDGAR full-text search. The dateRange filter breaks results, so
    we omit it and filter client-side by file_date.
    
    Retries on transient 500 errors (SEC server occasionally returns these).
    """
    all_hits = []
    for page in range(max_pages):
        offset = page * 100
        forms_encoded = form_type_query.replace(" ", "+").replace("/", "%2F")
        url = ("https://efts.sec.gov/LATEST/search-index?"
               "q=" + urllib.parse.quote('"' + q_text + '"') +
               "&forms=" + forms_encoded +
               "&from=" + str(offset))
        success = False
        for attempt in range(max_retries):
            try:
                text = http_get(url, timeout=20)
                d = json.loads(text)
                hits = (d.get("hits") or {}).get("hits") or []
                if not hits:
                    return all_hits
                all_hits.extend(hits)
                success = True
                break
            except urllib.error.HTTPError as e:
                if e.code == 500 and attempt < max_retries - 1:
                    print("[activist] efts " + form_type_query + " p" + str(page) +
                           " 500 retry " + str(attempt+1) + "/" + str(max_retries))
                    time.sleep(2 + attempt)
                    continue
                print("[activist] efts " + form_type_query + " p" + str(page) +
                       " HTTP " + str(e.code))
                break
            except Exception as e:
                print("[activist] efts " + form_type_query + " p" + str(page) +
                       " err: " + str(e))
                if attempt < max_retries - 1:
                    time.sleep(1 + attempt)
                    continue
                break
        if not success:
            break
    return all_hits


def parse_efts_hit(hit):
    """Parse an EFTS hit into a structured filing record (filer + subject combined)."""
    src = hit.get("_source", {})
    form = src.get("form", "")
    file_date = src.get("file_date", "")
    ciks = src.get("ciks") or []
    display_names = src.get("display_names") or []
    adsh = src.get("adsh", "")

    # display_names typically: [subject_company, filer1, filer2, ...]
    # In EFTS, the SUBJECT is always the first CIK
    # Filers come after
    subject_name = None
    subject_cik = None
    subject_ticker = None
    filers = []
    for i, name in enumerate(display_names):
        cik = ciks[i] if i < len(ciks) else ""
        # Extract ticker from "Company Name (TICK)" pattern
        ticker_m = re.search(r"\(([A-Z]{1,5}(?:,\s*[A-Z]{1,5})*)\)", name)
        ticker = None
        if ticker_m:
            tickers = [t.strip() for t in ticker_m.group(1).split(",")]
            ticker = tickers[0] if tickers else None
        # Strip the (TICK) and (CIK ...) suffixes for clean name
        clean_name = re.sub(r"\s*\([A-Z][^)]*\)\s*$", "", name).strip()
        clean_name = re.sub(r"\s*\([A-Z][^)]*\)\s*", " ", clean_name).strip()
        clean_name = re.sub(r"\s*\(CIK\s+\d+\)\s*$", "", clean_name).strip()
        
        if i == 0:
            subject_name = clean_name
            subject_cik = cik
            subject_ticker = ticker
        else:
            filers.append({"name": clean_name, "cik": cik, "ticker": ticker})

    # Build the link
    if adsh and ciks:
        adsh_no_dash = adsh.replace("-", "")
        link = "https://www.sec.gov/Archives/edgar/data/" + ciks[0] + "/" + adsh_no_dash + "/" + adsh + "-index.htm"
    else:
        link = ""

    return {
        "form_type": form,
        "filing_date": file_date,
        "subject_name": subject_name,
        "subject_cik": subject_cik,
        "subject_ticker": subject_ticker,
        "filers": filers,  # list of {name, cik, ticker}
        "accession": adsh,
        "link": link,
        "source": "efts",
    }


# ─── CIK → ticker mapping ────────────────────────────────────────────────

def cik_to_ticker_map():
    """SEC's company_tickers.json — proven working."""
    url = "https://www.sec.gov/files/company_tickers.json"
    try:
        text = http_get(url, timeout=20)
        d = json.loads(text)
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
        print("[activist] cik_to_ticker_map failed: " + str(e))
        return {}


def classify_filer(name):
    """Match filer name against known activist patterns."""
    if not name:
        return None, None
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


# ─── Scoring ─────────────────────────────────────────────────────────────

def score_filing(form_type, filer_tier, in_universe):
    score = 0
    flags = []
    if filer_tier == "TIER_S_LEGENDARY":
        score += 50; flags.append("LEGENDARY_FILER")
    elif filer_tier == "TIER_A_TOP":
        score += 35; flags.append("TOP_ACTIVIST")
    elif filer_tier == "TIER_B_MAJOR_FUND":
        score += 20; flags.append("MAJOR_FUND")
    elif filer_tier == "TIER_C_NOTABLE_HEDGE":
        score += 10; flags.append("NOTABLE_HEDGE")

    if form_type and ("13D" in form_type and "/A" not in form_type):
        score += 25; flags.append("ACTIVE_INTENT")
    elif form_type and "13D/A" in form_type:
        score += 20; flags.append("ACTIVE_AMENDMENT")
    elif form_type and ("13G" in form_type and "/A" not in form_type):
        score += 10; flags.append("PASSIVE_INTENT")
    elif form_type and "13G/A" in form_type:
        score += 8; flags.append("PASSIVE_AMENDMENT")

    if in_universe:
        score += 15; flags.append("IN_UNIVERSE")

    score = min(score, 100)
    if score >= 70:
        level = "TIER_A_HOT"
    elif score >= 50:
        level = "TIER_B_BUILDING"
    elif score >= 30:
        level = "WATCH"
    else:
        level = "NOTABLE"
    return score, level, flags


# ─── Main ────────────────────────────────────────────────────────────────

def lambda_handler(event=None, context=None):
    started = time.time()
    deadline_at = started + TIMEOUT_BUDGET_S
    print("[activist] v3.0 starting")

    cik_map = cik_to_ticker_map()
    print("[activist] CIK map: " + str(len(cik_map)) + " tickers")

    universe = get_universe_tickers()
    print("[activist] universe: " + str(len(universe)) + " tickers")

    # ── Atom RSS pulls ──
    rss_entries = []
    for ft in ["SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A"]:
        if time.time() > deadline_at:
            break
        entries = fetch_atom_feed(ft)
        print("[activist] RSS " + ft + ": " + str(len(entries)) + " entries")
        rss_entries.extend(entries)

    # Group RSS entries by accession (Filer + Subject pair)
    by_accession = defaultdict(list)
    for e in rss_entries:
        link = e.get("link", "")
        # Accession: 18-digit hyphenated number e.g. 0001140361-26-018419
        accession_match = re.search(r"(\d{10}-\d{2}-\d{6})", link)
        if accession_match:
            accession = accession_match.group(2)
            by_accession[accession].append(e)
        else:
            by_accession[link].append(e)

    print("[activist] RSS unique filings: " + str(len(by_accession)))

    # ── EFTS pulls (broader 30d backfill) ──
    efts_filings = []
    if time.time() < deadline_at - 30:
        for query_form, q_text in [
            ("SC 13D", "schedule 13D"),
            ("SC 13D/A", "schedule 13D"),
            ("SC 13G", "schedule 13G"),
            ("SC 13G/A", "schedule 13G"),
        ]:
            if time.time() > deadline_at:
                break
            hits = fetch_efts_search(query_form, q_text, max_pages=2)
            print("[activist] EFTS " + query_form + ": " + str(len(hits)) + " hits")
            for h in hits:
                parsed = parse_efts_hit(h)
                # Filter client-side by file_date
                fd = parsed.get("filing_date", "")
                if fd:
                    cutoff = time.strftime("%Y-%m-%d", time.gmtime(time.time() - DAYS_BACK * 86400))
                    if fd >= cutoff:
                        efts_filings.append(parsed)

    print("[activist] EFTS filtered to last " + str(DAYS_BACK) + "d: " + str(len(efts_filings)))

    # ── Build unified filing records ──
    filings = []
    seen_accessions = set()

    # First, EFTS filings (already have subject + filer parsed)
    for ef in efts_filings:
        acc = ef.get("accession")
        if not acc or acc in seen_accessions:
            continue
        seen_accessions.add(acc)
        # Subject ticker fallback via CIK
        if not ef.get("subject_ticker") and ef.get("subject_cik"):
            cik_padded = ef["subject_cik"].zfill(10)
            if cik_padded in cik_map:
                ef["subject_ticker"] = cik_map[cik_padded]["ticker"]
                if not ef.get("subject_name"):
                    ef["subject_name"] = cik_map[cik_padded]["company_name"]

        # Score against the highest-tier filer
        best_tier = None
        best_pattern = None
        filer_names = []
        for f in ef.get("filers", []):
            filer_names.append(f.get("name", ""))
            tier, pat = classify_filer(f.get("name", ""))
            if tier:
                # Promote if higher tier
                tier_order = {"TIER_S_LEGENDARY": 4, "TIER_A_TOP": 3, "TIER_B_MAJOR_FUND": 2, "TIER_C_NOTABLE_HEDGE": 1}
                if best_tier is None or tier_order.get(tier, 0) > tier_order.get(best_tier, 0):
                    best_tier = tier
                    best_pattern = pat

        in_universe = ef.get("subject_ticker") and ef["subject_ticker"] in universe
        score, level, flags = score_filing(ef.get("form_type"), best_tier, in_universe)

        filings.append({
            "accession": acc,
            "form_type": ef.get("form_type"),
            "filing_date": ef.get("filing_date"),
            "subject_name": ef.get("subject_name"),
            "subject_cik": ef.get("subject_cik"),
            "subject_ticker": ef.get("subject_ticker"),
            "subject_company": ef.get("subject_name"),
            "filer_name": ", ".join(filer_names[:2]) if filer_names else None,
            "all_filers": ef.get("filers", []),
            "filer_tier": best_tier,
            "matched_pattern": best_pattern,
            "in_universe": in_universe,
            "score": score,
            "level": level,
            "flags": flags,
            "filing_link": ef.get("link"),
            "source": "efts",
        })

    # Now, RSS entries (most recent — may overlap with EFTS but we dedupe by acc)
    for acc, entries in by_accession.items():
        if acc in seen_accessions:
            continue
        # Try to extract accession from link
        clean_acc = acc
        if "-" not in clean_acc and len(clean_acc) > 12:
            # Looks like a hash — skip
            continue
        seen_accessions.add(clean_acc)

        filer = None
        subject = None
        form_type = None
        for e in entries:
            r = e.get("role", "")
            if r in ("Filer", "Reporting"):
                filer = e
            elif r == "Subject":
                subject = e
            if not form_type:
                form_type = e.get("fetched_form_type") or e.get("form_type")
        if not filer and entries:
            filer = entries[0]
        if not subject:
            subject = entries[1] if len(entries) > 1 else (entries[0] if entries else None)

        filer_name = (filer.get("name") if filer else "") or ""
        filer_cik = (filer.get("cik") if filer else "") or ""
        if filer_cik:
            filer_cik = filer_cik.zfill(10)
        subject_name = (subject.get("name") if subject else "") or ""
        subject_cik = (subject.get("cik") if subject else "") or ""
        if subject_cik:
            subject_cik = subject_cik.zfill(10)
        link = (filer.get("link") if filer else "") or ""
        updated = (filer.get("updated_iso") if filer else "") or ""

        # Map subject CIK → ticker
        subject_ticker = None
        subject_company = subject_name
        if subject_cik and subject_cik in cik_map:
            subject_ticker = cik_map[subject_cik]["ticker"]
            subject_company = cik_map[subject_cik]["company_name"]

        # Try to extract ticker from subject_name like "COMPANY (TICK)"
        if not subject_ticker:
            tm = re.search(r"\(([A-Z]{1,5})\)\s*$", subject_name)
            if tm:
                subject_ticker = tm.group(1)

        tier, pat = classify_filer(filer_name)
        in_universe = bool(subject_ticker) and subject_ticker in universe
        score, level, flags = score_filing(form_type, tier, in_universe)

        filings.append({
            "accession": clean_acc,
            "form_type": form_type,
            "filing_date": updated[:10] if updated else "",
            "subject_name": subject_name,
            "subject_cik": subject_cik,
            "subject_ticker": subject_ticker,
            "subject_company": subject_company,
            "filer_name": filer_name,
            "filer_cik": filer_cik,
            "filer_tier": tier,
            "matched_pattern": pat,
            "in_universe": in_universe,
            "score": score,
            "level": level,
            "flags": flags,
            "filing_link": link,
            "source": "rss",
        })

    filings.sort(key=lambda x: -x["score"])

    # ── Multi-activist tickers ──
    by_ticker = defaultdict(list)
    for f in filings:
        if f.get("subject_ticker"):
            by_ticker[f["subject_ticker"]].append(f)
    multi_activist = []
    for ticker, fs in by_ticker.items():
        if len(fs) >= 2:
            multi_activist.append({
                "ticker": ticker,
                "company": fs[0].get("subject_company") or fs[0].get("subject_name"),
                "n_filings": len(fs),
                "filers": list({f.get("filer_name", "") for f in fs}),
                "form_types": list({f.get("form_type", "") for f in fs}),
                "tiers": list({f.get("filer_tier") for f in fs if f.get("filer_tier")}),
                "max_score": max(f["score"] for f in fs),
            })
    multi_activist.sort(key=lambda x: -x["max_score"])

    # ── State-based delta detection ──
    prior_state = None
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=STATE_KEY)
        prior_state = json.loads(obj["Body"].read())
    except Exception:
        pass
    prior_seen = set((prior_state or {}).get("seen_accessions", []))
    new_filings = [f for f in filings if f["accession"] not in prior_seen]
    new_seen = list(prior_seen | set(f["accession"] for f in filings))[-2000:]

    new_tier_a = [f for f in new_filings if f["level"] == "TIER_A_HOT"]
    new_tier_b = [f for f in new_filings if f["level"] == "TIER_B_BUILDING"]

    n_classified = sum(1 for f in filings if f.get("filer_tier"))
    n_in_universe = sum(1 for f in filings if f["in_universe"])

    print("[activist] total: " + str(len(filings)) + ", classified: " + str(n_classified) +
           ", in_universe: " + str(n_in_universe))
    print("[activist] new this run: " + str(len(new_filings)) + " (TIER-A: " + str(len(new_tier_a)) + ")")
    print("[activist] multi-activist tickers: " + str(len(multi_activist)))

    out = {
        "schema_version": 3,
        "method": "activist_filings_v3_atom_plus_efts",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "duration_s": round(time.time() - started, 1),
        "stats": {
            "n_filings_total": len(filings),
            "n_classified_by_tier": n_classified,
            "n_in_universe": n_in_universe,
            "n_unique_tickers": len(by_ticker),
            "n_multi_activist": len(multi_activist),
            "n_new_filings": len(new_filings),
            "n_new_tier_a": len(new_tier_a),
            "n_new_tier_b": len(new_tier_b),
        },
        "summary": {
            "top_25_overall": filings[:25],
            "tier_a_classified": [f for f in filings if f["level"] == "TIER_A_HOT"][:15],
            "tier_b_classified": [f for f in filings if f["level"] == "TIER_B_BUILDING"][:15],
            "in_universe_filings": [f for f in filings if f["in_universe"]][:25],
            "multi_activist_setups": multi_activist[:15],
            "new_alerts_this_run": new_tier_a[:10],
        },
        "all_filings": filings,
    }

    body = json.dumps(out, default=str).encode()
    S3.put_object(Bucket=BUCKET, Key=S3_KEY, Body=body, ContentType="application/json")
    print("[activist] wrote " + str(len(body)) + "b")

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
            "n_classified": n_classified,
            "n_in_universe": n_in_universe,
            "n_new": len(new_filings),
            "n_multi_activist": len(multi_activist),
            "duration_s": out["duration_s"],
        }),
    }
