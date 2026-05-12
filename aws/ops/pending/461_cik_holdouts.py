#!/usr/bin/env python3
"""Step 461 — Second-pass CIK resolution.

Tries broader/variant searches for the 7 funds that didn't resolve in 454b:
  - Polen, Yacktman, Akre   (no SEC hit at all)
  - T. Rowe Price, Ariel, Point72, WorldQuant   (SEC returned wrong filer)

STRATEGY:
  1. Multiple search query variants per fund (with/without "Capital",
      "Management", "Asset", "LLC", etc.)
  2. For each search, validate ALL hits (not just top 2) against FMP extract
  3. Add a name-similarity filter: SEC's display_name must contain the fund's
      core name token (e.g. "Polen" must appear in display_name)
  4. Try alternative endpoints if extract fails: maybe holder-performance
      works even when extract doesn't (some funds have perf data but FMP
      doesn't have their full 13F holdings)
"""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/461_cik_holdouts.json"
NAME = "justhodl-tmp-461"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = r'''
import json, urllib.request, urllib.parse, time, re
from concurrent.futures import ThreadPoolExecutor
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
FMP_BASE = "https://financialmodelingprep.com/stable"
SEC_BASE = "https://efts.sec.gov/LATEST/search-index"
UA = "JustHodl Research raafouis@gmail.com"

# (label, [search_variants], required_token_for_match)
# The required_token MUST appear in SEC's display_name to count as a real match.
# This filters out the cross-matches we saw in 454b.
FUNDS_TO_RESOLVE = [
    ("Polen Capital",        ["Polen Capital", "Polen Capital Management"], "polen"),
    ("Yacktman",              ["Yacktman Asset", "Yacktman Asset Management"], "yacktman"),
    ("Akre Capital",          ["Akre Capital Management", "Akre Capital LP"], "akre"),
    ("Point72",               ["Point72 Asset Management LP", "Point72 Asset Management"], "point72"),
    ("T. Rowe Price",         ["T Rowe Price Associates Inc",
                                 "T Rowe Price Investment Management",
                                 "Price T Rowe Associates"], "rowe price"),
    ("Ariel Investments",     ["Ariel Investments LLC", "Ariel Capital Management"], "ariel"),
    ("WorldQuant",            ["WorldQuant LLC", "World Quant"], "worldquant"),
    # Add a few more we never tried in 454b that could be valuable
    ("Engine No. 1",          ["Engine No. 1", "Engine No 1"], "engine no"),
    ("Druckenmiller Capital", ["Druckenmiller Capital", "Druckenmiller"], "druckenmill"),
    ("Pinpoint Asset",        ["Pinpoint Asset Management"], "pinpoint"),
    ("Hound Partners",        ["Hound Partners LLC"], "hound"),
    ("D.E. Shaw Group",       ["D E Shaw Group", "D E Shaw & Co"], "shaw"),
    ("Coatue Tech",            ["Coatue Tech LP"], "coatue"),
    ("Lone Pine Master",       ["Lone Pine Master Fund"], "lone pine"),
    ("Element Capital",        ["Element Capital Management"], "element capital"),
    ("Citadel Multi-Strat",    ["Citadel Multi-Strategy"], "citadel"),
    ("BlackRock",              ["BlackRock Inc", "BlackRock Fund Advisors"], "blackrock"),
    ("Vanguard",               ["Vanguard Group", "Vanguard Fiduciary"], "vanguard"),
    ("State Street",           ["State Street Corp"], "state street"),
    ("Fidelity",               ["FMR LLC", "Fidelity Management"], "fmr"),
    ("Sequoia",                ["Ruane Cunniff", "Sequoia Fund"], "sequoia"),
    ("Sands Cap Ventures",    ["Sands Capital Ventures"], "sands"),
    ("Sands Cap Investment",  ["Sands Capital Investment"], "sands capital"),
    ("Whale Rock LP",          ["Whale Rock Capital Master"], "whale rock"),
    ("Blue Ridge",             ["Blue Ridge Capital Holdings"], "blue ridge"),
]


def sec_search(query):
    params = {"q": '"' + query + '"', "forms": "13F-HR"}
    url = SEC_BASE + "?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": UA, "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read().decode("utf-8"))
        hits = (data.get("hits") or {}).get("hits") or []
        results = []
        seen = set()
        for h in hits[:10]:  # consider more candidates this pass
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
                if len(results) >= 5: break
            if len(results) >= 5: break
        return results
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
            if isinstance(d, list) and d:
                n_with_sym = sum(1 for x in d if x.get("symbol"))
                if n_with_sym > 0:
                    return {"year": year, "quarter": quarter, "n_records": len(d),
                            "n_with_sym": n_with_sym}
        except Exception:
            continue
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
        latest = d[0]
        return {"mv": latest.get("marketValue"),
                "size": latest.get("portfolioSize"),
                "name": latest.get("investorName")}
    except Exception:
        return None


def name_matches(display_name, required_token):
    """Check that the SEC display_name contains the required substring (case-insensitive)."""
    if not display_name or not required_token: return False
    return required_token.lower() in display_name.lower()


def resolve_one(args):
    label, queries, required_token = args
    started = time.time()
    rec = {"label": label, "required_token": required_token, "tried_queries": []}

    candidates = []
    seen_ciks = set()
    for q in queries:
        hits = sec_search(q)
        rec["tried_queries"].append({"q": q, "n_hits": len(hits) if hits else 0})
        if not hits: continue
        for h in hits:
            if h["cik"] in seen_ciks: continue
            seen_ciks.add(h["cik"])
            # NAME MATCH FILTER — must contain required token in display_name
            if not name_matches(h.get("name") or "", required_token):
                continue
            candidates.append({**h, "matched_query": q})
        time.sleep(0.15)  # SEC rate limit

    rec["filtered_candidates"] = candidates

    if not candidates:
        rec["status"] = "no_name_match"
        return rec

    # Validate against FMP — both extract AND perf in parallel
    def validate(c):
        cik = c["cik"]
        with ThreadPoolExecutor(max_workers=2) as ex:
            f_extract = ex.submit(fmp_extract, cik)
            f_perf = ex.submit(fmp_perf, cik)
            extract = f_extract.result()
            perf = f_perf.result()
        return {**c, "fmp_extract": extract, "fmp_perf": perf,
                "score": (3 if extract else 0) + (2 if perf else 0)}

    with ThreadPoolExecutor(max_workers=3) as ex:
        validated = list(ex.map(validate, candidates))
    validated.sort(key=lambda c: -(c.get("score") or 0))

    best = validated[0] if validated else None
    if best and best.get("score", 0) >= 3:
        status = "READY"
    elif best and best.get("score", 0) >= 2:
        status = "PARTIAL_perf_only"
    else:
        status = "WEAK"

    rec["status"] = status
    rec["candidates"] = validated
    rec["elapsed_s"] = round(time.time() - started, 1)
    if best:
        rec["best_cik"] = best.get("cik")
        rec["best_sec_name"] = best.get("name")
        rec["best_fmp_name"] = (best.get("fmp_perf") or {}).get("name")
        rec["best_size"] = (best.get("fmp_perf") or {}).get("size")
        rec["best_mv_b"] = round(((best.get("fmp_perf") or {}).get("mv") or 0) / 1e9, 2)
        rec["best_extract_n"] = (best.get("fmp_extract") or {}).get("n_records")
    return rec


def lambda_handler(event, context):
    started = time.time()
    results = []
    HARD_CAP = 600
    for args in FUNDS_TO_RESOLVE:
        if time.time() - started > HARD_CAP:
            results.append({"label": args[0], "status": "timeout"})
            continue
        results.append(resolve_one(args))

    ready = [r for r in results if r.get("status") == "READY"]
    partial = [r for r in results if r.get("status") == "PARTIAL_perf_only"]
    weak = [r for r in results if r.get("status") not in ("READY", "PARTIAL_perf_only")]

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
    _time.sleep(2)
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
