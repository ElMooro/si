"""
ops/833 - Spin-Off Desk preflight: audit + data-source probe.

Two jobs, one run:

  A) FRESHNESS AUDIT of the recently-shipped opportunity sleeves
     (best-ideas, dividend-growth, merger-arb, trend-engine).
     Confirms each S3 output exists, is fresh, and carries real rows.

  B) DATA-SOURCE PROBE for the next build - the Spin-Off &
     Special-Situations Desk. Spin-offs register with the SEC on
     Form 10 / Form 10-12B. We probe SEC EDGAR (full-text search +
     current-filings feed) and FMP /stable/ corporate-event
     endpoints to confirm a real, free, no-guessing data spine
     before writing a line of engine code (doctrine #23/#27).

Writes aws/ops/reports/833_spinoff_preflight.json.
"""
import json
import urllib.request
import urllib.error
from datetime import datetime, timezone

import boto3

S3_BUCKET = "justhodl-dashboard-live"
REGION = "us-east-1"
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
# SEC requires a descriptive User-Agent with contact info.
UA = "JustHodl.AI research raafouis@gmail.com"

s3 = boto3.client("s3", region_name=REGION)


def get_json(url, headers=None, timeout=25):
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read().decode("utf-8", "replace")
    return r.status, raw


def main():
    rep = {
        "ops": 833,
        "ts": datetime.now(timezone.utc).isoformat(),
        "subject": "Spin-Off Desk preflight - opportunity-stack freshness + EDGAR/FMP probe",
        "freshness": {},
        "edgar_probes": {},
        "fmp_probes": {},
        "build_guidance": {},
    }

    # ---- A) freshness audit -------------------------------------------
    now = datetime.now(timezone.utc)
    targets = {
        "best-ideas": "data/best-ideas.json",
        "dividend-growth": "data/dividend-growth.json",
        "merger-arb": "data/merger-arb.json",
        "trend-engine": "data/trend-engine.json",
    }
    for name, key in targets.items():
        info = {"key": key}
        try:
            head = s3.head_object(Bucket=S3_BUCKET, Key=key)
            age_h = (now - head["LastModified"]).total_seconds() / 3600.0
            info["exists"] = True
            info["age_hours"] = round(age_h, 1)
            info["bytes"] = head["ContentLength"]
            obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
            doc = json.loads(obj["Body"].read())
            # surface the largest list found at top level
            biggest = 0
            for k, v in doc.items():
                if isinstance(v, list):
                    biggest = max(biggest, len(v))
            info["max_list_len"] = biggest
            info["top_keys"] = list(doc.keys())[:14]
            info["healthy"] = bool(biggest > 0 and age_h < 72)
        except Exception as e:
            info["exists"] = False
            info["error"] = f"{type(e).__name__}: {e}"
            info["healthy"] = False
        rep["freshness"][name] = info

    # ---- B1) SEC EDGAR probes -----------------------------------------
    # Full-text search (EFTS) - registrations for spin-offs.
    for label, url in [
        ("efts_10-12B",
         "https://efts.sec.gov/LATEST/search-index?q=&forms=10-12B"),
        ("efts_form10_spinoff",
         'https://efts.sec.gov/LATEST/search-index?q=%22spin-off%22&forms=10-12B'),
        ("browse_current_10-12B",
         "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent"
         "&type=10-12B&company=&dateb=&owner=include&count=40&output=atom"),
    ]:
        p = {"url": url}
        try:
            status, raw = get_json(url, headers={"User-Agent": UA})
            p["status"] = status
            p["len"] = len(raw)
            p["sample"] = raw[:900]
            # try to count hits if JSON
            try:
                j = json.loads(raw)
                hits = j.get("hits", {}).get("hits")
                if hits is not None:
                    p["hit_count"] = len(hits)
                    if hits:
                        src = hits[0].get("_source", {})
                        p["first_hit"] = {
                            "display_names": src.get("display_names"),
                            "file_date": src.get("file_date"),
                            "form": src.get("root_forms") or src.get("file_type"),
                        }
                    p["total"] = j.get("hits", {}).get("total")
            except Exception:
                p["json"] = False
        except urllib.error.HTTPError as e:
            p["status"] = e.code
            p["error"] = f"HTTPError {e.code}"
            try:
                p["body"] = e.read().decode("utf-8", "replace")[:300]
            except Exception:
                pass
        except Exception as e:
            p["error"] = f"{type(e).__name__}: {e}"
        rep["edgar_probes"][label] = p

    # ---- B2) FMP corporate-event probes -------------------------------
    for label, url in [
        ("fmp_ipo_calendar",
         f"https://financialmodelingprep.com/stable/ipos-calendar?apikey={FMP_KEY}"),
        ("fmp_company_notes",
         f"https://financialmodelingprep.com/stable/company-notes?symbol=AAPL&apikey={FMP_KEY}"),
        ("fmp_mergers",
         f"https://financialmodelingprep.com/stable/mergers-acquisitions-latest?page=0&apikey={FMP_KEY}"),
    ]:
        p = {"url": url.split("apikey=")[0] + "apikey=***"}
        try:
            status, raw = get_json(url, headers={"User-Agent": UA})
            p["status"] = status
            try:
                j = json.loads(raw)
                p["type"] = type(j).__name__
                p["count"] = len(j) if isinstance(j, list) else None
                p["sample"] = j[:2] if isinstance(j, list) else str(j)[:400]
            except Exception:
                p["sample_raw"] = raw[:300]
        except urllib.error.HTTPError as e:
            p["status"] = e.code
            p["error"] = f"HTTPError {e.code}"
        except Exception as e:
            p["error"] = f"{type(e).__name__}: {e}"
        rep["fmp_probes"][label] = p

    # ---- guidance -----------------------------------------------------
    edgar_ok = any(
        rep["edgar_probes"].get(k, {}).get("status") == 200
        for k in rep["edgar_probes"]
    )
    rep["build_guidance"] = {
        "edgar_usable": edgar_ok,
        "stale_or_broken": [
            n for n, i in rep["freshness"].items() if not i.get("healthy")
        ],
        "note": "If EDGAR EFTS returns 200 with hits, build the Spin-Off "
                "Desk on the 10-12B full-text feed as the no-guess spine.",
    }

    body = json.dumps(rep, indent=2, default=str)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key="ops/reports/833_spinoff_preflight.json",
        Body=body.encode(),
        ContentType="application/json",
    )
    with open("aws/ops/reports/833_spinoff_preflight.json", "w") as f:
        f.write(body)
    print(body)


if __name__ == "__main__":
    main()
