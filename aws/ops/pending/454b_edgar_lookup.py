#!/usr/bin/env python3
"""Step 454b — SEC EDGAR lookup with tight budget.

Improvements over 454:
  - 25 most-important funds (was 60)
  - Top 2 SEC candidates per fund (was 4)
  - Parallel SEC submissions + FMP calls (was serial)
  - Internal 600s hard cap so we always return partial results
  - More conservative pacing (200ms between SEC searches)

Total budget: ~25 funds × 2s = 50s SEC search + 25 × 4 × 0.5s parallel
              = ~110s total, well under 15-min workflow limit.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/454b_edgar_lookup.json"
NAME = "justhodl-tmp-454b"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = r'''
import json, urllib.request, urllib.parse, time
from concurrent.futures import ThreadPoolExecutor, as_completed
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
FMP_BASE = "https://financialmodelingprep.com/stable"
SEC_BASE = "https://efts.sec.gov/LATEST/search-index"
SEC_SUBS = "https://data.sec.gov/submissions"
UA = "JustHodl Research raafouis@gmail.com"

FUNDS_TO_RESOLVE = [
    ("Trian Fund Management",       "Trian Fund Management"),
    ("Caxton Associates",           "Caxton Associates"),
    ("Duquesne Family Office",      "Duquesne Family Office"),
    ("Moore Capital Management",    "Moore Capital Management"),
    ("Brevan Howard",               "Brevan Howard"),
    ("WorldQuant",                  "WorldQuant"),
    ("Balyasny Asset",              "Balyasny Asset Management"),
    ("T. Rowe Price",               "T. Rowe Price Associates"),
    ("Capital Research",            "Capital Research and Management"),
    ("Polen Capital",               "Polen Capital Management"),
    ("Ariel Investments",           "Ariel Investments"),
    ("Yacktman",                    "Yacktman Asset Management"),
    ("Akre Capital",                "Akre Capital Management"),
    ("Omega Advisors",              "Omega Advisors"),
    ("GAMCO",                       "GAMCO Investors"),
    ("Miller Value",                "Miller Value Partners"),
    ("Farallon Capital",            "Farallon Capital Management"),
    ("TCI Fund",                    "TCI Fund Management"),
    ("First Eagle",                 "First Eagle Investment Management"),
    ("Whale Rock",                  "Whale Rock Capital Management"),
    ("Oaktree",                     "Oaktree Capital Management"),
    ("Appaloosa",                   "Appaloosa LP"),
    ("Glenview",                    "Glenview Capital Management"),
    ("Point72",                     "Point72 Asset Management"),
    ("D1 Capital",                  "D1 Capital Partners"),
]


def sec_search(query):
    params = {"q": '"' + query + '"', "forms": "13F-HR"}
    url = SEC_BASE + "?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": UA, "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read().decode("utf-8"))
        hits = (data.get("hits") or {}).get("hits") or []
        results = []; seen = set()
        for h in hits[:6]:
            src = h.get("_source") or {}
            ciks = src.get("ciks") or []
            names = src.get("display_names") or []
            for i, cik in enumerate(ciks):
                cs = cik.zfill(10) if cik.isdigit() else cik
                if cs in seen: continue
                seen.add(cs)
                name = names[i] if i < len(names) else (names[0] if names else "?")
                results.append({"cik": cs, "name": name,
                                 "filing_date": src.get("file_date")})
                if len(results) >= 2: break
            if len(results) >= 2: break
        return results
    except Exception as e:
        return None


def fmp_perf(cik):
    url = f"{FMP_BASE}/institutional-ownership/holder-performance-summary?cik={cik}&apikey={FMP}"
    try:
        with urllib.request.urlopen(
            urllib.request.Request(url, headers={"User-Agent":"JH/1.0"}),
            timeout=8) as r:
            d = json.loads(r.read().decode("utf-8"))
        if not isinstance(d, list) or not d: return None
        d.sort(key=lambda x: x.get("date",""), reverse=True)
        return {"mv": d[0].get("marketValue"), "size": d[0].get("portfolioSize"),
                "name": d[0].get("investorName")}
    except Exception:
        return None


def fmp_extract(cik):
    for year, quarter in [(2025, 4), (2025, 3)]:
        url = f"{FMP_BASE}/institutional-ownership/extract?cik={cik}&year={year}&quarter={quarter}&apikey={FMP}"
        try:
            with urllib.request.urlopen(
                urllib.request.Request(url, headers={"User-Agent":"JH/1.0"}),
                timeout=8) as r:
                d = json.loads(r.read().decode("utf-8"))
            if isinstance(d, list) and d and isinstance(d[0], dict):
                n_with_sym = sum(1 for x in d if x.get("symbol"))
                if n_with_sym > 0:
                    return {"year": year, "quarter": quarter,
                            "n_records": len(d), "n_with_sym": n_with_sym}
        except Exception:
            continue
    return None


def validate_candidate(cand):
    """Run FMP perf+extract in parallel for one candidate."""
    cik = cand["cik"]
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_perf = ex.submit(fmp_perf, cik)
        f_extract = ex.submit(fmp_extract, cik)
        perf = f_perf.result()
        extract = f_extract.result()
    score = (3 if extract else 0) + (2 if perf else 0)
    return {**cand, "fmp_perf": perf, "fmp_extract": extract, "score": score}


def resolve_one(args):
    label, query = args
    started = time.time()
    sec_hits = sec_search(query)
    if not sec_hits:
        return {"label": label, "query": query, "status": "no_sec_hits"}
    # Validate candidates in parallel
    with ThreadPoolExecutor(max_workers=2) as ex:
        validated = list(ex.map(validate_candidate, sec_hits))
    validated.sort(key=lambda c: -(c.get("score") or 0))
    best = validated[0] if validated else None
    if best and best.get("score", 0) >= 3:
        status = "READY"
    elif best and best.get("score", 0) >= 2:
        status = "PARTIAL"
    else:
        status = "WEAK"
    return {
        "label": label, "query": query, "status": status,
        "elapsed_s": round(time.time() - started, 1),
        "best_cik": best.get("cik") if best else None,
        "best_name": (best.get("fmp_perf") or {}).get("name") if best else None,
        "best_score": best.get("score") if best else 0,
        "extract_ok": bool(best and best.get("fmp_extract")),
        "perf_ok": bool(best and best.get("fmp_perf")),
        "extract_records": (best.get("fmp_extract") or {}).get("n_records") if best else None,
        "fmp_size": (best.get("fmp_perf") or {}).get("size") if best else None,
        "fmp_mv": (best.get("fmp_perf") or {}).get("mv") if best else None,
        "candidates": validated,
    }


def lambda_handler(event, context):
    started = time.time()
    results = []
    HARD_CAP = 600  # 10-min internal cap
    for args in FUNDS_TO_RESOLVE:
        if time.time() - started > HARD_CAP:
            results.append({"label": args[0], "status": "timeout_cap"})
            continue
        results.append(resolve_one(args))
        time.sleep(0.25)  # respect SEC 10/sec

    ready = [r for r in results if r.get("status") == "READY"]
    partial = [r for r in results if r.get("status") == "PARTIAL"]
    weak = [r for r in results if r.get("status") in ("WEAK", "no_sec_hits", "timeout_cap")]

    return {"statusCode": 200, "body": json.dumps({
        "elapsed_s": round(time.time() - started, 1),
        "n_funds": len(FUNDS_TO_RESOLVE),
        "n_ready": len(ready),
        "n_partial": len(partial),
        "n_weak": len(weak),
        "ready": ready,
        "partial": partial,
        "weak": weak,
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
