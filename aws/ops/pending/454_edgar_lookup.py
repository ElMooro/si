#!/usr/bin/env python3
"""Step 454 — Resolve dropped CIKs via SEC EDGAR + multi-endpoint FMP probe.

STRATEGY (4-stage funnel):
  Stage A: SEC EDGAR full-text search by fund name (forms=13F-HR)
           → returns authoritative CIKs of 13F filers matching the name
  Stage B: For each found CIK, query SEC submissions API to confirm
           recent 13F-HR filings (filter dormant/inactive filers)
  Stage C: Validate against FMP holder-performance-summary
  Stage D: Validate against FMP extract endpoint (the one that powers
           our actual screener integration) — many funds pass Stage C
           but fail Stage D (FMP performance data exists but not holdings)

OUTPUT: Per-fund report showing which stage each one cleared.
        Ready-to-paste Python tuples for the surviving funds.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/454_edgar_lookup.json"
NAME = "justhodl-tmp-454"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = r'''
import json, urllib.request, urllib.parse, time
from concurrent.futures import ThreadPoolExecutor
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
FMP_BASE = "https://financialmodelingprep.com/stable"
SEC_BASE = "https://efts.sec.gov/LATEST/search-index"
SEC_SUBS = "https://data.sec.gov/submissions"
# SEC requires a User-Agent with real contact info per their fair-use policy
UA = "JustHodl Research raafouis@gmail.com"

# Funds to lookup. (display_name, search_query — quoted phrase for exact match)
FUNDS_TO_RESOLVE = [
    ("Trian Fund Management",           "Trian Fund Management"),
    ("Caxton Associates",                "Caxton Associates"),
    ("Duquesne Family Office",           "Duquesne Family Office"),
    ("Moore Capital Management",         "Moore Capital Management"),
    ("Brevan Howard",                    "Brevan Howard"),
    ("WorldQuant",                       "WorldQuant"),
    ("Balyasny Asset Management",        "Balyasny"),
    ("Verition Fund Management",         "Verition Fund"),
    ("T. Rowe Price",                    "T. Rowe Price Associates"),
    ("Capital Group / Capital Research", "Capital Research and Management"),
    ("Janus Henderson",                  "Janus Henderson"),
    ("Brookfield Asset Management",      "Brookfield Asset"),
    ("Polen Capital Management",         "Polen Capital"),
    ("Ariel Investments",                "Ariel Investments"),
    ("Yacktman Asset Management",        "Yacktman"),
    ("Akre Capital Management",          "Akre Capital"),
    ("Cooperman / Omega",                "Omega Advisors"),
    ("Lee Cooperman",                    "Lee Cooperman"),
    ("GAMCO / Gabelli",                  "GAMCO Investors"),
    ("Gabelli Funds",                    "Gabelli Funds"),
    ("Miller Value Partners",            "Miller Value"),
    ("Davidson Kempner",                 "Davidson Kempner Capital Management"),
    ("Farallon Capital",                 "Farallon Capital Management"),
    ("TCI Fund Management",              "TCI Fund Management"),
    ("Egerton Capital",                  "Egerton Capital"),
    ("Lansdowne Partners",               "Lansdowne Partners"),
    ("Marshall Wace",                    "Marshall Wace"),
    ("First Eagle Investment Mgmt",      "First Eagle Investment Management"),
    ("Blue Ridge Capital",               "Blue Ridge Capital"),
    ("Whale Rock Capital",               "Whale Rock Capital"),
    ("Sands Capital Ventures",           "Sands Capital Ventures"),
    ("Oaktree Capital Management",       "Oaktree Capital Management"),
    ("Appaloosa Management",             "Appaloosa Management"),
    ("Tepper",                           "Appaloosa LP"),
    ("Kynikos Associates",               "Kynikos Associates"),
    ("Chanos",                           "Jim Chanos"),
    ("King Street Capital",              "King Street Capital"),
    ("Anchorage Capital (alt)",          "Anchorage Capital"),
    ("Hound Partners",                   "Hound Partners"),
    ("Pinpoint Asset Management",        "Pinpoint Asset"),
    ("Element Capital",                  "Element Capital Management"),
    ("Glenview Capital",                 "Glenview Capital"),
    ("Point72",                          "Point72 Asset Management"),
    ("Greenlight Capital (alt)",         "Greenlight Capital"),
    ("Elliott Investment Mgmt (alt)",    "Elliott Investment Management"),
    ("Davidson Kempner (alt2)",          "Davidson Kempner Partners"),
    ("Bridgewater Pure Alpha",           "Bridgewater Pure Alpha"),
    # Tiger cubs / growth
    ("Maverick Capital (alt)",           "Maverick Capital Management"),
    ("Schonfeld Strategic",              "Schonfeld Strategic"),
    # Distressed
    ("D1 Capital Partners",              "D1 Capital Partners"),
    ("Hudson Bay Capital",               "Hudson Bay Capital"),
    ("Marathon Asset Management",        "Marathon Asset Management"),
    # PE-adjacent
    ("KKR & Co",                         "KKR & Co"),
    ("Blackstone",                       "Blackstone Inc"),
    ("Apollo Global",                    "Apollo Global Management"),
    # Smaller activists
    ("Engaged Capital",                  "Engaged Capital"),
    ("Land & Buildings",                 "Land & Buildings"),
    ("ValueAct (alt)",                   "ValueAct Holdings"),
    # Concentrated growth
    ("Polen Capital (alt)",              "Polen Focus Growth"),
    ("Akre Focus",                       "Akre Focus"),
    ("Sequoia Capital Fund Mgmt",        "Ruane Cunniff Goldfarb"),
]


def sec_search(query):
    """SEC EDGAR full-text search for 13F-HR filers matching the query."""
    params = {"q": '"' + query + '"', "forms": "13F-HR"}
    url = SEC_BASE + "?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": UA, "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read().decode("utf-8"))
        hits = (data.get("hits") or {}).get("hits") or []
        # Each hit has _source.ciks: list, _source.display_names: list
        results = []
        seen_ciks = set()
        for h in hits[:8]:
            src = h.get("_source") or {}
            ciks = src.get("ciks") or []
            names = src.get("display_names") or []
            form = src.get("form") or src.get("forms")
            for i, cik in enumerate(ciks):
                cik_str = cik.zfill(10) if cik.isdigit() else cik
                if cik_str in seen_ciks: continue
                seen_ciks.add(cik_str)
                name = names[i] if i < len(names) else (names[0] if names else "?")
                results.append({
                    "cik": cik_str,
                    "name": name,
                    "form": form,
                    "filing_date": src.get("file_date"),
                })
                if len(results) >= 6: break
            if len(results) >= 6: break
        return results
    except Exception as e:
        return {"error": str(e)[:200]}


def sec_submissions(cik):
    """Confirm a CIK has recent 13F-HR filings."""
    cik_str = cik.zfill(10) if cik.isdigit() else cik
    url = f"{SEC_SUBS}/CIK{cik_str}.json"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read().decode("utf-8"))
        recent = (data.get("filings") or {}).get("recent") or {}
        forms = recent.get("form") or []
        dates = recent.get("filingDate") or []
        # Find most recent 13F-HR
        for i, f in enumerate(forms):
            if f and "13F" in f.upper():
                return {
                    "filer_name": data.get("name"),
                    "sic": data.get("sic"),
                    "latest_13f_date": dates[i] if i < len(dates) else None,
                    "latest_13f_form": f,
                }
        return None
    except Exception as e:
        return {"error": str(e)[:150]}


def fmp_perf(cik):
    """Check FMP holder-performance-summary."""
    url = f"{FMP_BASE}/institutional-ownership/holder-performance-summary?cik={cik}&apikey={FMP}"
    try:
        with urllib.request.urlopen(
            urllib.request.Request(url, headers={"User-Agent":"JH/1.0"}),
            timeout=10) as r:
            d = json.loads(r.read().decode("utf-8"))
        if not isinstance(d, list) or not d: return None
        d.sort(key=lambda x: x.get("date",""), reverse=True)
        latest = d[0]
        return {
            "mv": latest.get("marketValue"),
            "size": latest.get("portfolioSize"),
            "name": latest.get("investorName"),
            "date": latest.get("date","")[:10],
        }
    except Exception:
        return None


def fmp_extract(cik):
    """Check FMP extract — the endpoint our smart-money-holdings Lambda
    actually uses. This is the ground truth for our integration."""
    # Try most recent 2 quarters (Q4 2025 and Q3 2025)
    for year, quarter in [(2025, 4), (2025, 3)]:
        url = f"{FMP_BASE}/institutional-ownership/extract?cik={cik}&year={year}&quarter={quarter}&apikey={FMP}"
        try:
            with urllib.request.urlopen(
                urllib.request.Request(url, headers={"User-Agent":"JH/1.0"}),
                timeout=10) as r:
                d = json.loads(r.read().decode("utf-8"))
            if isinstance(d, list) and d and isinstance(d[0], dict):
                # Ensure at least one record has a tradeable symbol
                n_with_sym = sum(1 for x in d if x.get("symbol"))
                if n_with_sym > 0:
                    return {"year": year, "quarter": quarter,
                            "n_records": len(d), "n_with_sym": n_with_sym}
        except Exception:
            continue
    return None


def resolve_one(args):
    label, query = args
    rec = {"label": label, "query": query}

    # Stage A: SEC EDGAR full-text search
    sec_hits = sec_search(query)
    if isinstance(sec_hits, dict) and "error" in sec_hits:
        rec["sec_err"] = sec_hits["error"]
        return rec
    rec["sec_hits"] = sec_hits or []
    if not sec_hits:
        rec["status"] = "no_sec_match"
        return rec

    # Stage B + C + D for each top hit (max 4 to limit calls)
    candidates = []
    for hit in sec_hits[:4]:
        cik = hit["cik"]
        # Stage B: confirm 13F-HR filings
        subs = sec_submissions(cik)
        # Be polite — SEC rate limit
        time.sleep(0.12)
        if not subs or (isinstance(subs, dict) and subs.get("error")):
            candidates.append({**hit, "stage_b": "fail",
                                 "stage_b_err": subs.get("error") if subs else "no_subs"})
            continue
        # Stage C: FMP performance summary
        perf = fmp_perf(cik)
        # Stage D: FMP extract endpoint
        extract = fmp_extract(cik)
        candidates.append({
            **hit,
            "sec_name": subs.get("filer_name"),
            "latest_13f": subs.get("latest_13f_date"),
            "fmp_perf": perf,
            "fmp_extract": extract,
            "score": (
                (3 if extract else 0) +    # extract is the gold standard
                (2 if perf else 0) +        # perf data is good
                (1 if subs else 0)          # at least files 13F
            ),
        })

    # Sort candidates by score
    candidates.sort(key=lambda c: -(c.get("score") or 0))
    rec["candidates"] = candidates
    best = candidates[0] if candidates else None
    if best:
        rec["best_cik"] = best.get("cik")
        rec["best_name"] = best.get("sec_name") or best.get("name")
        rec["best_score"] = best.get("score")
        rec["best_works_in_extract"] = bool(best.get("fmp_extract"))
        rec["best_works_in_perf"] = bool(best.get("fmp_perf"))
        if best.get("score", 0) >= 3:
            rec["status"] = "READY"
        elif best.get("score", 0) >= 2:
            rec["status"] = "PARTIAL"
        else:
            rec["status"] = "WEAK"
    else:
        rec["status"] = "no_candidate"
    return rec


def lambda_handler(event, context):
    results = []
    # Resolve serially to be polite to SEC EDGAR (10/sec limit)
    for args in FUNDS_TO_RESOLVE:
        results.append(resolve_one(args))
        time.sleep(0.15)  # ~7/sec

    # Build ready-to-paste lists
    ready_extract = []  # cleared stage D
    partial_perf = []   # cleared stage C only
    weak = []

    for r in results:
        if r.get("status") == "READY":
            ready_extract.append({
                "label": r["label"],
                "cik": r["best_cik"],
                "name": r["best_name"],
                "extract_info": r.get("candidates", [{}])[0].get("fmp_extract"),
            })
        elif r.get("status") == "PARTIAL":
            partial_perf.append({
                "label": r["label"],
                "cik": r["best_cik"],
                "name": r["best_name"],
            })
        else:
            weak.append({"label": r["label"], "status": r.get("status")})

    return {"statusCode": 200, "body": json.dumps({
        "n_funds": len(FUNDS_TO_RESOLVE),
        "n_ready": len(ready_extract),
        "n_partial": len(partial_perf),
        "n_weak": len(weak),
        "ready_extract": ready_extract,
        "partial_perf": partial_perf,
        "weak": weak,
        "all_results": results,
    }, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=1024, Timeout=900, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
    except Exception:
        out["raw"] = body[:30000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
