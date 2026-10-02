"""
justhodl-biotech-catalysts Lambda
==================================
Free data pipeline over the clinicaltrials.gov API v2 (no API key required).

For ~30 major biotech/pharma sponsors it pulls sponsored clinical trials,
categorizes them by phase and status, and extracts upcoming trial
completion dates plus "PDUFA likely within 6-12 months" flags (Phase 3
trials completed within the last 12 months).

Writes: s3://justhodl-dashboard-live/data/biotech-catalysts.json

FAIL-SOFT: every network/parse step is wrapped in try/except. The handler
never raises; it always attempts to write the output file (possibly
partial or empty) and returns a status summary.
"""

import json
import time
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone

import boto3

BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/biotech-catalysts.json"

API_BASE = "https://clinicaltrials.gov/api/v2/studies"
USER_AGENT = "justhodl.ai biotech-catalysts pipeline (+https://justhodl.ai)"

PAGE_SIZE = 100
MAX_PAGES = 3          # cap per sponsor to stay inside Lambda time
HTTP_TIMEOUT = 25
REQUEST_SLEEP = 0.25   # politeness delay between API calls
MAX_CATALYSTS_PER_TICKER = 15
PDUFA_WINDOW_MONTHS = 12


# (sponsor query term, ticker, match fragments against the lead sponsor name)
SPONSORS = [
    ("Moderna", "MRNA", ["moderna"]),
    ("BioNTech", "BNTX", ["biontech"]),
    ("Regeneron", "REGN", ["regeneron"]),
    ("Vertex", "VRTX", ["vertex"]),
    ("Biogen", "BIIB", ["biogen"]),
    ("Gilead", "GILD", ["gilead"]),
    ("Amgen", "AMGN", ["amgen"]),
    ("Ionis", "IONS", ["ionis"]),
    ("Incyte", "INCY", ["incyte"]),
    ("Sarepta", "SRPT", ["sarepta"]),
    ("Alnylam", "ALNY", ["alnylam"]),
    ("Novavax", "NVAX", ["novavax"]),
    ("CRISPR Therapeutics", "CRSP", ["crispr"]),
    ("Intellia", "NTLA", ["intellia"]),
    ("Beam Therapeutics", "BEAM", ["beam"]),
    ("Ultragenyx", "RARE", ["ultragenyx"]),
    ("BioMarin", "BMRN", ["biomarin"]),
    ("Exelixis", "EXEL", ["exelixis"]),
    ("Seagen", "SGEN", ["seagen"]),
    ("Merck", "MRK", ["merck"]),
    ("Pfizer", "PFE", ["pfizer"]),
    ("Eli Lilly", "LLY", ["lilly"]),
    ("Bristol Myers", "BMY", ["bristol"]),
    ("AstraZeneca", "AZN", ["astrazeneca"]),
    ("Novartis", "NVS", ["novartis"]),
    ("Roche", "RHHBY", ["roche", "genentech"]),
    ("Sanofi", "SNY", ["sanofi"]),
    ("GSK", "GSK", ["gsk", "glaxosmithkline"]),
    ("Takeda", "TAK", ["takeda"]),
    ("Telix Pharmaceuticals", "TLX", ["telix"]),
]


def _http_get_json(url):
    """GET JSON with one retry; raises on persistent failure."""
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    last_err = None
    for attempt in range(2):
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as exc:  # noqa: BLE001 - fail-soft retry
            last_err = exc
            if attempt == 0:
                time.sleep(2)
    raise last_err


def _fetch_sponsor_studies(query_term):
    """Fetch up to MAX_PAGES pages of studies for one sponsor query."""
    studies = []
    page_token = None
    for _page in range(MAX_PAGES):
        params = {
            "query.term": query_term,
            "pageSize": str(PAGE_SIZE),
        }
        if page_token:
            params["pageToken"] = page_token
        url = API_BASE + "?" + urllib.parse.urlencode(params)
        data = _http_get_json(url)
        for s in data.get("studies", []):
            studies.append(s)
        page_token = data.get("nextPageToken")
        if not page_token:
            break
        time.sleep(REQUEST_SLEEP)
    return studies


def _lead_sponsor_name(study):
    try:
        mods = study["protocolSection"]["sponsorCollaboratorsModule"]
        return (mods.get("leadSponsor") or {}).get("name") or ""
    except Exception:  # noqa: BLE001 - defensive navigation
        return ""


def _sponsor_matches(study, fragments):
    name = _lead_sponsor_name(study).lower()
    if not name:
        return True  # no sponsor info; keep it (the query term matched)
    return any(frag in name for frag in fragments)


def _phase_number(phases):
    """Highest trial phase as an int: PHASE1->1 ... PHASE4->4. None if N/A."""
    best = None
    for p in phases or []:
        p = (p or "").upper()
        if p == "EARLY_PHASE1":
            n = 0
        elif p.startswith("PHASE"):
            try:
                n = int(p.replace("PHASE", ""))
            except ValueError:
                continue
        else:
            continue  # "NA" or unknown
        if best is None or n > best:
            best = n
    return best


def _phase_label(n):
    if n is None:
        return "N/A"
    if n == 0:
        return "Early Phase 1"
    return "Phase %d" % n


def _status_bucket(overall_status):
    s = (overall_status or "").upper()
    if s == "RECRUITING":
        return "recruiting"
    if s in ("ACTIVE_NOT_RECRUITING", "ENROLLING_BY_INVITATION",
             "NOT_YET_RECRUITING"):
        return "active"
    if s == "COMPLETED":
        return "completed"
    if s in ("TERMINATED", "WITHDRAWN", "SUSPENDED", "NO_LONGER_AVAILABLE"):
        return "ended"
    return "unknown"


def _parse_ym(date_str):
    """Return (year, month) for 'YYYY-MM-DD' or 'YYYY-MM', else None."""
    if not date_str:
        return None
    try:
        parts = str(date_str).split("-")
        return (int(parts[0]), int(parts[1]))
    except Exception:  # noqa: BLE001 - bad date format
        return None


def _month_index(ym):
    return ym[0] * 12 + ym[1]


def _study_completion_ym(study):
    """Primary completion date preferred, fallback to study completion date."""
    try:
        sm = study["protocolSection"]["statusModule"]
        for key in ("primaryCompletionDateStruct", "completionDateStruct"):
            d = (sm.get(key) or {}).get("date")
            ym = _parse_ym(d)
            if ym:
                return ym
    except Exception:  # noqa: BLE001 - defensive navigation
        pass
    return None


def _build_ticker_payload(sponsor, ticker, fragments, studies, today_ym):
    counts = {"phase1": 0, "phase2": 0, "phase3": 0, "phase4": 0,
              "other_phase": 0}
    status_counts = {"recruiting": 0, "active": 0, "completed": 0,
                     "ended": 0, "unknown": 0}
    catalysts = []
    seen_nct = set()

    for study in studies:
        try:
            if not _sponsor_matches(study, fragments):
                continue
            proto = study.get("protocolSection", {})
            ident = proto.get("identificationModule", {})
            nct_id = ident.get("nctId") or "unknown"
            title = (ident.get("briefTitle") or "")[:220]

            design = proto.get("designModule", {})
            phase_n = _phase_number(design.get("phases"))

            if phase_n == 1:
                counts["phase1"] += 1
            elif phase_n == 2:
                counts["phase2"] += 1
            elif phase_n == 3:
                counts["phase3"] += 1
            elif phase_n == 4:
                counts["phase4"] += 1
            else:
                counts["other_phase"] += 1

            status_mod = proto.get("statusModule", {})
            overall = status_mod.get("overallStatus")
            bucket = _status_bucket(overall)
            status_counts[bucket] = status_counts.get(bucket, 0) + 1

            comp_ym = _study_completion_ym(study)
            if comp_ym and nct_id not in seen_nct:
                seen_nct.add(nct_id)
                comp_str = "%04d-%02d" % comp_ym
                phase_lbl = _phase_label(phase_n)
                # PDUFA heuristic: Phase 3 completed within the last 12 months
                if (phase_n == 3 and bucket == "completed"
                        and _month_index(today_ym) - _month_index(comp_ym)
                        <= PDUFA_WINDOW_MONTHS):
                    catalysts.append({
                        "nct_id": nct_id,
                        "title": title,
                        "phase": phase_lbl,
                        "completion_date": comp_str,
                        "catalyst_type": "pdufa_likely",
                        "note": "Phase 3 completed; PDUFA decision likely "
                                "within 6-12 months",
                    })
                # Upcoming completion: this month or later
                elif _month_index(comp_ym) >= _month_index(today_ym):
                    catalysts.append({
                        "nct_id": nct_id,
                        "title": title,
                        "phase": phase_lbl,
                        "completion_date": comp_str,
                        "catalyst_type": "trial_completion",
                    })
        except Exception:  # noqa: BLE001 - one bad study must not kill the run
            continue

    catalysts.sort(key=lambda c: c["completion_date"])
    total = sum(counts.values())

    return {
        "sponsor": sponsor,
        "ticker": ticker,
        "studies_analyzed": total,
        "phase1_count": counts["phase1"],
        "phase2_count": counts["phase2"],
        "phase3_count": counts["phase3"],
        "phase4_count": counts["phase4"],
        "other_phase_count": counts["other_phase"],
        "status_counts": status_counts,
        "completed_phase3_count": sum(
            1 for c in catalysts if c["catalyst_type"] == "pdufa_likely"),
        "upcoming_catalysts": catalysts[:MAX_CATALYSTS_PER_TICKER],
    }


def _write_s3(payload):
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=BUCKET,
        Key=S3_KEY,
        Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )


def lambda_handler(event, context):
    """Entry point. Always writes data/biotech-catalysts.json (fail-soft)."""
    generated_at = datetime.now(timezone.utc).isoformat()
    today = datetime.now(timezone.utc).date()
    today_ym = (today.year, today.month)

    by_ticker = {}
    errors = []
    sponsors_failed = []

    for sponsor, ticker, fragments in SPONSORS:
        try:
            studies = _fetch_sponsor_studies(sponsor)
            by_ticker[ticker] = _build_ticker_payload(
                sponsor, ticker, fragments, studies, today_ym)
        except Exception as exc:  # noqa: BLE001 - fail-soft per sponsor
            sponsors_failed.append(ticker)
            errors.append("sponsor %s (%s): %s" % (sponsor, ticker, exc))
            # keep a placeholder so the ticker is still present in output
            by_ticker[ticker] = {
                "sponsor": sponsor,
                "ticker": ticker,
                "studies_analyzed": 0,
                "phase1_count": 0,
                "phase2_count": 0,
                "phase3_count": 0,
                "phase4_count": 0,
                "other_phase_count": 0,
                "status_counts": {},
                "completed_phase3_count": 0,
                "upcoming_catalysts": [],
                "fetch_error": str(exc)[:300],
            }
        time.sleep(REQUEST_SLEEP)

    payload = {
        "generated_at": generated_at,
        "pipeline": "justhodl-biotech-catalysts",
        "source": "clinicaltrials.gov API v2 (free, no key)",
        "sponsors_targeted": len(SPONSORS),
        "tickers_covered": len(by_ticker),
        "sponsors_failed": sponsors_failed,
        "errors": errors[:50],
        "by_ticker": by_ticker,
    }

    s3_status = "not_attempted"
    try:
        _write_s3(payload)
        s3_status = "ok"
    except Exception as exc:  # noqa: BLE001 - still return payload on S3 failure
        s3_status = "failed: %s" % exc

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "s3": "s3://%s/%s" % (BUCKET, S3_KEY),
            "s3_status": s3_status,
            "generated_at": generated_at,
            "tickers_covered": len(by_ticker),
            "sponsors_failed": sponsors_failed,
            "error_count": len(errors),
        }),
        "payload": payload,  # returned too, so a failed S3 write is not silent
    }


if __name__ == "__main__":
    # Local smoke test (does not need AWS creds to exercise the fetch logic;
    # S3 write will fail-soft and the payload is printed instead).
    result = lambda_handler({}, None)
    print(json.dumps(json.loads(result["body"]), indent=2)[:2000])
