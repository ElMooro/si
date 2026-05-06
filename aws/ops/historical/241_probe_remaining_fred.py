#!/usr/bin/env python3
"""Step 241 — probe candidate FRED IDs for the two remaining gaps:
  - IG_BBB_OAS  (currently BAMLC0A4CMTRIV → unavailable)
  - rate_diff_eur_3m  (currently IR3TBB01EZM156N → unavailable)

Mirrors step 237's pattern: spin up a probe Lambda, call FRED for each
candidate, report n_obs + latest_date + age_in_days. Recent (<90d) wins.
"""
import io
import json
import sys
import time
import zipfile
from datetime import datetime
from ops_report import report
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
PROBE_NAME = "justhodl-tmp-fred-probe-241"
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
            data = json.loads(r.read().decode())
            obs = data.get("observations", [])
            return {"ok": True, "n_obs": len(obs),
                    "latest_date": obs[0].get("date") if obs else None,
                    "latest_value": obs[0].get("value") if obs else None}
    except urllib.error.HTTPError as e:
        if e.code == 400:
            return {"ok": False, "verdict": "DOES_NOT_EXIST"}
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


CANDIDATES = {
    # ── IG BBB OAS — credit fear gauge for investment-grade BBB ──
    "IG_BBB_OAS": [
        # ICE BofA series naming convention: BAMLC<level><tenor><quality>
        "BAMLC0A4CBBB",        # ICE BofA US Corp BBB OAS (short label)
        "BAMLC0A4CBBBEY",      # …Effective Yield variant
        "BAMLC0A4CBBBSYTW",    # …Semi-annual yield to worst
        "BAMLC0A4CBBBTRIV",    # Total return index value? guess
        "BAMLC0A4CMTRIV",      # Original — sanity (we expect failure)
        # Alternates
        "BAMLC4A0C710YEY",     # 7-10Y corp
        "BAMLEMUBCRPIUSEY",    # EM corp BBB
        "BAMLH0A1HYBB",        # BB high-yield (related)
    ],
    # ── EUR 3M rate — for rate_diff_eur_3m XCC signal ──
    "EUR_3M": [
        "IR3TIB01EZM156N",   # OECD: Interbank Rates 3M, Eurozone, monthly
        "IR3TBB01EZM156N",   # T-Bills 3M Eurozone (current, broken)
        "IRSTCI01EZM156N",   # Short Term Immediate Rate (Call Money), Eurozone
        "IRLTLT01EZM156N",   # Long-term, Eurozone (sanity, should work)
        "INTGSBEZM193N",     # Government securities Eurozone — probably old
        "ECBESTRVOLWGTTRMDMNRT",  # ESTR weighted rate (modern)
        "EUR3MTD156N",       # 3M Euribor guess
        "EURIBOR3MD",        # 3M Euribor daily?
    ],
}


with report("probe_remaining_fred_ids") as r:
    r.heading("Probe candidates for IG_BBB_OAS + EUR 3M")

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

    findings = {}

    for category, sids in CANDIDATES.items():
        r.section(f"Category: {category}")
        for sid in sids:
            try:
                resp = lam.invoke(
                    FunctionName=PROBE_NAME,
                    InvocationType="RequestResponse",
                    Payload=json.dumps({"series_id": sid}),
                )
                result = json.loads(resp["Payload"].read())
            except Exception as e:
                r.warn(f"  ✗ {sid:30s}  invoke error: {e}")
                continue

            if result.get("ok") and result.get("n_obs", 0) > 0:
                ld = result.get("latest_date") or "—"
                lv = result.get("latest_value") or "—"
                try:
                    dt = datetime.fromisoformat(ld)
                    age_days = (datetime.now() - dt).days
                except Exception:
                    age_days = -1
                recent = age_days >= 0 and age_days < 90
                mark = "✅" if recent else "⚠"
                r.log(f"  {mark} {sid:30s}  latest={ld} ({age_days}d ago)  value={lv}")
                if recent:
                    findings.setdefault(category, []).append((sid, ld, lv))
            elif result.get("ok"):
                r.log(f"  ⚠ {sid:30s}  exists but no obs")
            else:
                verdict = result.get("verdict") or result.get("status") or result.get("type", "?")
                r.log(f"  ✗ {sid:30s}  {verdict}")
            time.sleep(0.5)

    try:
        lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError:
        pass

    r.section("WINNERS — live + recent series, safe to use in catalog")
    if not findings:
        r.warn("  no live recent series found in any category — investigate manually")
    for cat, items in findings.items():
        r.log(f"  {cat}:")
        for sid, ld, lv in items:
            r.log(f"    {sid:30s}  latest={ld}  value={lv}")
    r.log("")
    r.log("Done")
