#!/usr/bin/env python3
"""Step 462 — Find Maverick Capital's real CIK + a few more variant searches.

The Akre/Maverick mislabeling means we need Maverick's actual CIK.
Lee Ainslie's Maverick is a Tiger cub with ~$10B AUM, files 13F.

Also try a few more variant searches we haven't:
  - Polen Growth (instead of Polen Capital)
  - Sequoia (Ruane / Cunniff / Goldfarb)
  - WorldQuant Millennium (their parent)
  - Lone Pine Capital (already exists but verify)
"""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/462_maverick_etc.json"
NAME = "justhodl-tmp-462"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = r'''
import json, urllib.request, urllib.parse, time
from concurrent.futures import ThreadPoolExecutor
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
FMP_BASE = "https://financialmodelingprep.com/stable"
SEC_BASE = "https://efts.sec.gov/LATEST/search-index"
UA = "JustHodl Research raafouis@gmail.com"

# (label, [queries], required_token)
PROBE_LIST = [
    ("Maverick Capital",        ["Maverick Capital", "Maverick Capital Management",
                                    "Maverick Fund", "Maverick Holdings"], "maverick"),
    ("Polen Capital",           ["Polen Capital Management LLC", "Polen Growth"], "polen"),
    ("Sequoia Fund",            ["Ruane Cunniff", "Ruane, Cunniff", "Sequoia Fund Inc",
                                    "Sequoia Fund"], "ruane"),
    ("Sequoia Heritage",        ["Sequoia Heritage", "Sequoia Capital Heritage"], "sequoia"),
    ("Light Street",             ["Light Street Capital"], "light street"),
    ("ARK Genomic",              ["ARK Genomic"], "ark"),
    ("Magnetar Capital",         ["Magnetar Capital"], "magnetar"),
    ("Two Sigma Advisers",       ["Two Sigma Advisers"], "two sigma"),
    ("PIMCO",                    ["Pacific Investment Management", "PIMCO"], "pacific"),
    ("Pershing Square Holdings", ["Pershing Square Holdings"], "pershing"),
    ("Renaissance Inst",         ["Renaissance Institutional", "RIEF"], "renaissance"),
    ("Citadel Securities",       ["Citadel Securities"], "citadel"),
    ("Soros Capital",            ["Soros Capital"], "soros"),
    ("Carl Icahn Foundation",    ["Icahn Capital LP", "High River Limited"], "icahn"),
    ("Druckenmiller Foundation", ["Druckenmiller Foundation"], "druckenmiller"),
    ("Pabrai Funds",             ["Pabrai Investment", "Pabrai Funds"], "pabrai"),
    ("Joel Greenblatt / Gotham", ["Gotham Capital", "Gotham Asset Management"], "gotham"),
    ("Mohnish Pabrai",           ["Dalal Street LLC"], "dalal"),
    ("Wally Weitz Funds",        ["Weitz Investment Management"], "weitz"),
    ("Mason Hawkins / Longleaf", ["Southeastern Asset Management", "Longleaf"], "southeastern"),
    ("Bill Nygren / Oakmark",    ["Harris Associates LP", "Oakmark Funds"], "harris associates"),
    ("Bruce Berkowitz / Fairholme", ["Fairholme Capital"], "fairholme"),
    ("BlackRock Asset Mgmt",     ["BlackRock Fund Advisors"], "blackrock"),
    ("State Street",             ["SSGA Funds Management", "State Street Global"], "state street"),
    ("Pictet Asset Management",  ["Pictet Asset Management"], "pictet"),
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
        results = []
        seen = set()
        for h in hits[:8]:
            src = h.get("_source") or {}
            ciks = src.get("ciks") or []
            names = src.get("display_names") or []
            for i, cik in enumerate(ciks):
                cs = cik.zfill(10) if cik.isdigit() else cik
                if cs in seen: continue
                seen.add(cs)
                name = names[i] if i < len(names) else (names[0] if names else "?")
                results.append({"cik": cs, "name": name})
                if len(results) >= 4: break
            if len(results) >= 4: break
        return results
    except Exception:
        return None


def fmp_extract(cik):
    for year, quarter in [(2025, 4), (2025, 3)]:
        url = f"{FMP_BASE}/institutional-ownership/extract?cik={cik}&year={year}&quarter={quarter}&apikey={FMP}"
        try:
            with urllib.request.urlopen(
                urllib.request.Request(url, headers={"User-Agent":"JH/1.0"}),
                timeout=7) as r:
                d = json.loads(r.read().decode("utf-8"))
            if isinstance(d, list) and d:
                n_with_sym = sum(1 for x in d if x.get("symbol"))
                if n_with_sym > 0:
                    return {"n_records": len(d)}
        except Exception:
            continue
    return None


def fmp_perf(cik):
    url = f"{FMP_BASE}/institutional-ownership/holder-performance-summary?cik={cik}&apikey={FMP}"
    try:
        with urllib.request.urlopen(
            urllib.request.Request(url, headers={"User-Agent":"JH/1.0"}),
            timeout=7) as r:
            d = json.loads(r.read().decode("utf-8"))
        if not isinstance(d, list) or not d: return None
        d.sort(key=lambda x: x.get("date",""), reverse=True)
        return {"mv": d[0].get("marketValue"), "size": d[0].get("portfolioSize"),
                "name": d[0].get("investorName")}
    except Exception:
        return None


def name_matches(display_name, token):
    return token.lower() in (display_name or "").lower()


def resolve_one(args):
    label, queries, token = args
    started = time.time()
    candidates = []
    seen = set()
    for q in queries:
        hits = sec_search(q) or []
        for h in hits:
            if h["cik"] in seen: continue
            seen.add(h["cik"])
            if not name_matches(h.get("name") or "", token): continue
            candidates.append(h)
        time.sleep(0.12)

    if not candidates:
        return {"label": label, "status": "no_name_match",
                "elapsed_s": round(time.time()-started, 1)}

    def validate(c):
        extract = fmp_extract(c["cik"])
        perf = fmp_perf(c["cik"])
        return {**c, "fmp_extract": extract, "fmp_perf": perf,
                "score": (3 if extract else 0) + (2 if perf else 0)}

    with ThreadPoolExecutor(max_workers=3) as ex:
        validated = list(ex.map(validate, candidates))
    validated.sort(key=lambda c: -(c.get("score") or 0))
    best = validated[0]
    return {
        "label": label,
        "status": "READY" if best.get("score", 0) >= 3 else
                  ("PARTIAL" if best.get("score", 0) >= 2 else "WEAK"),
        "elapsed_s": round(time.time()-started, 1),
        "best_cik": best.get("cik"),
        "best_sec_name": best.get("name"),
        "best_fmp_name": (best.get("fmp_perf") or {}).get("name"),
        "best_size": (best.get("fmp_perf") or {}).get("size"),
        "best_mv_b": round(((best.get("fmp_perf") or {}).get("mv") or 0) / 1e9, 2),
        "best_extract_n": (best.get("fmp_extract") or {}).get("n_records"),
        "n_candidates": len(validated),
    }


def lambda_handler(event, context):
    started = time.time()
    results = []
    for args in PROBE_LIST:
        if time.time() - started > 540:
            results.append({"label": args[0], "status": "timeout"})
            continue
        results.append(resolve_one(args))

    ready = [r for r in results if r.get("status") == "READY"]
    partial = [r for r in results if r.get("status") == "PARTIAL"]
    weak = [r for r in results if r.get("status") not in ("READY", "PARTIAL")]
    return {"statusCode": 200, "body": json.dumps({
        "elapsed_s": round(time.time()-started, 1),
        "n_funds": len(PROBE_LIST),
        "n_ready": len(ready), "n_partial": len(partial), "n_weak": len(weak),
        "ready": ready, "partial": partial, "weak": weak,
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
