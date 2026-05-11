#!/usr/bin/env python3
"""Step 452 — Validate a comprehensive curated CIK list.

Instead of pattern-matching against discovered filers (which only returns
recent filers, not famous historical ones), directly hit
holder-performance-summary?cik=X for a long list of known CIKs.

If FMP returns 1+ records with marketValue > 0, the CIK is valid for our
purposes. Output a sorted, validated list ready to paste into the Lambda.

CANDIDATE CIKs (curated from public sources — SEC EDGAR, 13F filings):
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/452_cik_validation.json"
NAME = "justhodl-tmp-452"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = r'''
import json, urllib.request
from concurrent.futures import ThreadPoolExecutor
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
BASE = "https://financialmodelingprep.com/stable"

# Curated CIK candidates — best-guess from public sources.
# Format: (cik_padded_to_10, display_label, attribution)
CANDIDATES = [
    # ── Already validated v2 (sanity baseline) ──
    ("0001067983", "Berkshire Hathaway", "Buffett"),
    ("0001336528", "Pershing Square Capital", "Ackman"),
    ("0001037389", "Renaissance Technologies", "Simons"),
    ("0001167483", "Tiger Global Management", "Coleman"),
    ("0001135730", "Coatue Management", "Laffont"),
    ("0001423053", "Citadel Advisors", "Griffin"),
    ("0001179392", "Two Sigma Investments", "quant"),
    ("0001541617", "Millennium Management", "Englander"),
    ("0001103804", "Viking Global Investors", "Halvorsen"),
    ("0001029160", "Soros Fund Management", "Soros"),
    ("0001031972", "Baupost Group", "Klarman"),
    ("0001040273", "Third Point", "Loeb"),
    ("0001418814", "ValueAct Capital", "activist"),
    ("0001061768", "Lone Pine Capital", "Mandel"),
    ("0001112520", "Maverick Capital", "Tiger cub"),
    ("0001350694", "Bridgewater Associates", "Dalio"),
    ("0001009207", "D.E. Shaw", "Shaw"),
    ("0000820027", "Tudor Investment", "PTJ"),
    # ── Just discovered in step 451 ──
    ("0001374170", "Norges Bank Investment Mgmt", "Norway sovereign"),
    ("0001020066", "Sands Capital Management", "growth"),
    ("0001036325", "Davis Selected Advisers", "value"),
    ("0001346824", "ARK Investment Management", "Cathie Wood"),
    ("0000732905", "Tweedy, Browne", "legendary value"),
    ("0001313893", "Maple Capital", ""),
    # ── Activists / deep value ──
    ("0001412082", "Trian Fund Management", "Peltz"),
    ("0001541996", "Starboard Value LP", "Smith"),
    ("0000813762", "Icahn Capital LP", "Carl Icahn"),
    ("0000921669", "Icahn Capital Management", "Carl Icahn alt"),
    ("0001067187", "Icahn Enterprises L.P.", "Icahn vehicle"),
    ("0001821781", "Engine No. 1", "activist"),
    # ── Macro funds ──
    ("0001045111", "Caxton Associates LLC", "Kovner"),
    ("0001167581", "Duquesne Family Office", "Druckenmiller"),
    ("0000835887", "Moore Capital Management", "Moore"),
    ("0001027459", "Brevan Howard Asset Management LLP", "macro"),
    ("0001616730", "Element Capital Management LLC", "Talpins"),
    ("0001102648", "Bluecrest Capital", "Platt"),
    ("0001321655", "Discovery Capital Management", "Citrone"),
    # ── Quants ──
    ("0001167557", "AQR Capital Management LLC", "Asness"),
    ("0001358109", "WorldQuant LLC", "Tulchinsky"),
    ("0001354521", "Balyasny Asset Management", "Balyasny"),
    ("0001706209", "Verition Fund Management", "multi-mgr"),
    ("0001603466", "Schonfeld Strategic Advisors", "Schonfeld"),
    ("0001624153", "Hutchin Hill Capital", "quant"),
    # ── Growth / VC-adjacent / mutual ──
    ("0001113169", "T. Rowe Price Group", "T Rowe"),
    ("0000882540", "Capital Research & Management", "Capital Group"),
    ("0000902219", "Wellington Management", "Wellington"),
    ("0001049656", "Janus Henderson Group", "Janus"),
    ("0000910389", "Brookfield Asset Management", "Brookfield"),
    ("0001275061", "Polen Capital Management", "Polen"),
    # ── Value / classics ──
    ("0001020475", "Ariel Investments LLC", "Rogers"),
    ("0000936800", "Cooperman / Omega Advisors", "Cooperman"),
    ("0001226018", "Yacktman Asset Management", "Yacktman"),
    ("0001351017", "Akre Capital Management", "Akre"),
    ("0000091500", "Sequoia Fund", "Ruane Cunniff"),
    ("0001274683", "Weitz Investment Management", "Wally Weitz"),
    ("0001620266", "Miller Value Partners", "Bill Miller"),
    ("0001236628", "GAMCO Investors", "Gabelli"),
    ("0000040738", "Gabelli Funds LLC", "Gabelli alt"),
    ("0001047644", "Davidson Kempner Capital Management", "DK"),
    ("0001056604", "Farallon Capital Management", "Farallon"),
    ("0001436073", "TCI Fund Management", "Chris Hohn"),
    ("0001081541", "Egerton Capital", "Egerton"),
    ("0001113613", "Lansdowne Partners", "Lansdowne"),
    ("0001405986", "Marshall Wace LLP", "Marshall Wace"),
    ("0001020848", "First Eagle Investment Management", "First Eagle"),
    ("0000855538", "Tweedy Browne Co LLC", "Tweedy Browne alt"),
    # ── Tiger cubs & growth ──
    ("0001145717", "Blue Ridge Capital LLC", "Tiger cub"),
    ("0001275148", "Whale Rock Capital Management", "tech growth"),
    ("0001512699", "Sands Capital Mgmt (alt)", "growth alt"),
    ("0001167274", "Glenview Capital Management", "Robbins"),
    ("0001079114", "Greenlight Capital Inc", "Einhorn"),
    ("0001048445", "Elliott Investment Management", "Singer"),
    ("0001296958", "Point72 Asset Management", "Cohen"),
    # ── Distressed / credit ──
    ("0001119312", "Oaktree Capital Management LP", "Marks"),
    ("0001071611", "Appaloosa LP", "Tepper"),
    ("0001008797", "Kynikos Associates", "Chanos short-seller"),
    ("0001423902", "King Street Capital Management", "King Street"),
    ("0001345471", "Anchorage Capital", "Anchorage"),
    ("0001536411", "Canyon Capital Advisors", "Canyon"),
    # ── Hound Partners (Tiger cub) ──
    ("0001345366", "Hound Partners LLC", "Tiger cub"),
    # ── Polish probes for variants ──
    ("0001346824", "Ark Invest", "Wood alt"),
    ("0001599056", "Pinpoint Asset Management", "Russell"),
    ("0001517137", "Starboard Value alt", "Smith alt"),
    ("0001541997", "Starboard Value LP alt", "Smith alt2"),
    ("0001345099", "Trian Fund Management alt", "Peltz alt"),
    ("0001428203", "Greenlight Capital alt", "Einhorn alt"),
]


def fetch_json(path):
    url = BASE + path + ("&" if "?" in path else "?") + "apikey=" + FMP
    try:
        r = urllib.request.urlopen(
            urllib.request.Request(url, headers={"User-Agent":"JH/1.0"}),
            timeout=10)
        return json.loads(r.read().decode("utf-8"))
    except Exception:
        return None


def validate(args):
    cik, label, attrib = args
    d = fetch_json(f"/institutional-ownership/holder-performance-summary?cik={cik}")
    if not isinstance(d, list) or not d:
        return {"cik": cik, "label": label, "attrib": attrib,
                "valid": False, "reason": "no data"}
    d.sort(key=lambda r: r.get("date",""), reverse=True)
    latest = d[0]
    mv = latest.get("marketValue") or 0
    size = latest.get("portfolioSize") or 0
    if mv <= 0:
        return {"cik": cik, "label": label, "attrib": attrib,
                "valid": False, "reason": "zero mv"}
    return {
        "cik": cik,
        "label": label,
        "attrib": attrib,
        "official_name": latest.get("investorName"),
        "valid": True,
        "date": latest.get("date","")[:10],
        "market_value": mv,
        "market_value_b": round(mv/1e9, 2),
        "portfolio_size": size,
        "added": latest.get("securitiesAdded"),
        "removed": latest.get("securitiesRemoved"),
        "qoq_pct": latest.get("changeInMarketValuePercentage"),
        "perf_pct": latest.get("performancePercentage"),
    }


def lambda_handler(event, context):
    valid = []; invalid = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        for r in ex.map(validate, CANDIDATES):
            if r.get("valid"):
                valid.append(r)
            else:
                invalid.append(r)
    valid.sort(key=lambda v: -(v.get("market_value") or 0))
    return {"statusCode": 200, "body": json.dumps({
        "n_candidates": len(CANDIDATES),
        "n_valid": len(valid),
        "n_invalid": len(invalid),
        "valid": valid,
        "invalid": invalid,
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
                            MemorySize=1024, Timeout=600, Code={"ZipFile": zb})
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
        out["raw"] = body[:20000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
