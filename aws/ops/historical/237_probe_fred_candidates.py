#!/usr/bin/env python3
"""Step 237 — probe candidate FRED IDs.

Step 236 confirmed:
  - WGMMNS / WPMMNS / WTMMNS all return HTTP 400 (don't exist on FRED)
  - INTGSBJPM193N is on FRED but discontinued (last data 2017-05-01)
  - DGS10/SOFR/BUSLOANS/DTWEXBGS work fine
  - IR3TBB01EZM156N (Eurozone 3M) works fine — it's just JPY that's broken

Probe a list of candidate IDs to find which ones currently exist on FRED.
After this, we update the catalogs with the working IDs.
"""
import io
import json
import sys
import time
import zipfile
from ops_report import report
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
PROBE_NAME = "justhodl-tmp-fred-probe-237"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=300))

PROBE_CODE = '''
import json, os, urllib.request, urllib.parse
def lambda_handler(event, context):
    series = event["series_id"]
    api_key = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")
    params = {"series_id": series, "api_key": api_key, "file_type": "json", "limit": 5, "sort_order": "desc"}
    url = "https://api.stlouisfed.org/fred/series/observations?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            body = r.read().decode()
            data = json.loads(body)
            obs = data.get("observations", [])
            return {"ok": True, "n_obs": len(obs),
                    "latest_date": obs[0].get("date") if obs else None,
                    "latest_value": obs[0].get("value") if obs else None}
    except urllib.error.HTTPError as e:
        if e.code == 400:
            return {"ok": False, "status": 400, "verdict": "DOES_NOT_EXIST"}
        return {"ok": False, "status": e.code, "error": str(e)[:200]}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200], "type": type(e).__name__}
'''


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", PROBE_CODE)
    buf.seek(0)
    return buf.read()


# Candidate FRED series IDs for each category
CANDIDATES = {
    # ── Money Market Funds: gov / prime / tax-exempt / total ──
    "MMF": [
        # Quarterly (Z.1 financial accounts)
        "MMMFFAQ027S",       # Money market funds total liabilities, quarterly
        "BOGZ1FL633034005Q", # MMF total assets quarterly
        "MMFFGTAQ027S",      # Government MMF? guess
        "MMFFPRQ027S",       # Prime MMF? guess
        "MMFFGOVTAQ",        # Gov MMF? guess
        # Monthly  
        "ICILVL",            # ICI levels?
        "MMFGOVT",           # Gov MMF guess
        "MMFPRIME",          # Prime MMF guess
        # Weekly
        "WMMFNS",            # Total MMF Weekly (the ORIGINAL series — was it actually discontinued?)
        "WIMFSL",            # Institutional MMF Weekly (legacy, ended 2021)
        # Try retail series alternatives
        "WTRMNS",            # Retail MMF Weekly?
        "WMFINS",            # Inst MMF Weekly?
    ],
    # ── Japan 3M rate (alternate to discontinued INTGSBJPM193N) ──
    "JPY_3M": [
        "IR3TIB01JPM156N",     # OECD: Interbank Rates 3M, Japan, monthly
        "IR3TBB01JPM156N",     # OECD: T-Bills 3M, Japan, monthly (same pattern as EUR)
        "IRSTCI01JPM156N",     # Short Term Interest Rates: Immediate Rates (Call Money), Japan, monthly
        "INTDSRJPM193N",       # Interest Rates: Discount Rate, Japan, monthly (older)
        "IR3TBC01JPM156N",     # T-Bill rate Japan monthly?
        "IRLTLT01JPM156N",     # Long-term, Japan
        "INTGSTJPM193N",       # Govt securities Japan?
        "INTGSBJPM193N",       # Original — confirm it still works (sanity)
    ],
}


with report("probe_fred_candidates") as r:
    r.heading("Probe candidate FRED IDs for MMF + Japan 3M")

    # Spin up probe Lambda
    try:
        lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError:
        pass
    lam.create_function(
        FunctionName=PROBE_NAME,
        Runtime="python3.11",
        Role=ROLE_ARN,
        Handler="lambda_function.lambda_handler",
        Code={"ZipFile": build_zip()},
        Timeout=30,
        MemorySize=256,
        Architectures=["x86_64"],
    )
    time.sleep(4)

    findings = {"working": [], "broken": []}

    for category, sids in CANDIDATES.items():
        r.section(f"Category: {category}")
        for sid in sids:
            try:
                resp = lam.invoke(FunctionName=PROBE_NAME, InvocationType="RequestResponse",
                                  Payload=json.dumps({"series_id": sid}))
                result = json.loads(resp["Payload"].read())
            except Exception as e:
                r.warn(f"  ✗ {sid:25s}  invoke error: {e}")
                continue

            if result.get("ok"):
                n = result.get("n_obs", 0)
                ld = result.get("latest_date") or "—"
                lv = result.get("latest_value") or "—"
                if n > 0:
                    # Check if data is recent (last 60d) — discontinued series have old data
                    from datetime import datetime
                    try:
                        dt = datetime.fromisoformat(ld)
                        age_days = (datetime.now() - dt).days
                        recent = age_days < 90
                    except Exception:
                        age_days = -1
                        recent = False
                    mark = "✅" if recent else "⚠"
                    r.log(f"  {mark} {sid:25s}  latest={ld} ({age_days}d ago)  value={lv}")
                    if recent:
                        findings["working"].append((category, sid, ld, lv))
                else:
                    r.log(f"  ⚠ {sid:25s}  exists but no data")
            else:
                verdict = result.get("verdict") or result.get("status") or result.get("type", "?")
                r.log(f"  ✗ {sid:25s}  {verdict}")
                findings["broken"].append((category, sid, verdict))
            time.sleep(0.5)  # gentle pacing to avoid 429

    # Cleanup probe
    try:
        lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError:
        pass

    r.section("RESULTS — these series are LIVE + RECENT and safe to use")
    by_cat = {}
    for cat, sid, ld, lv in findings["working"]:
        by_cat.setdefault(cat, []).append((sid, ld, lv))
    for cat, items in by_cat.items():
        r.log(f"  {cat}:")
        for sid, ld, lv in items:
            r.log(f"    {sid:25s}  latest={ld}  value={lv}")
    if not findings["working"]:
        r.warn("  ✗ No working candidates found in any category")
    r.log("")
    r.log("Done")
