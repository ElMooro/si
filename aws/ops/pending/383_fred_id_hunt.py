#!/usr/bin/env python3
"""Step 383 — Bulk test candidate FRED IDs to find correct replacements."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/383_fred_id_hunt.json"
NAME = "justhodl-tmp-fred-hunt"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

# Candidate IDs to test, organized by what we're looking for.
# I'll test multiple variants per slot and pick the one that returns data.
CANDIDATES = {
    # Replacement for MBST (DISCONTINUED) — Fed MBS holdings
    "fed_mbs_held": ["WSHOMCB", "MBST", "WSHOMBSL", "WMBSEC"],
    # Replacement for EXCSRESNW (DISCONTINUED post-2020)
    "excess_reserves": ["EXCSRESNW", "EXCSRESNS", "WLODL", "EXCRESNS"],
    # Replacement for REQRESNS (DISCONTINUED)
    "required_reserves": ["REQRESNS", "REQUIRES", "RQRSL"],
    # Replacement for WLEMCBL — Other Loans (BTFP / emergency)
    "other_loans": ["WLEMCBL", "WLLCL", "OTHLT", "H41RESPPALDOTOTLNWW", "OTHL"],
    # Treasury holdings TOTAL (replacement for WSHOMCB if that's MBS not Treasuries)
    "treasury_total": ["WSHOTSL", "TREAST", "WSHOTSLM"],
    # Bank Term Funding Program
    "btfp": ["H41RESPPALDKLOAOLNWW", "BTFP", "WLODL"],
    # ── Euro HY by quality ──
    "euro_hy_bb": ["BAMLHE10HYBBOAS", "BAMLHE1HYBBEY", "BAMLHE1HYBBOAS", "BAMLHE10HYBB"],
    "euro_hy_b":  ["BAMLHE20HYBOAS",  "BAMLHE20HYBEY",  "BAMLHE2HYBOAS"],
    "euro_hy_ccc":["BAMLHE30HYCDOAS", "BAMLHE3HYCDOAS", "BAMLHE30HYCD"],
    # ── SLOOS demand by category ──
    "sloos_ci_large_demand": ["DRSDCILM", "SUBLPDCILMNQ", "SUBLPDCILMQ", "DRGSCILM"],
    "sloos_ci_small_demand": ["DRSDCIS", "SUBLPDCISNQ", "SUBLPDCISQ", "DRGSCIS"],
    "sloos_cre_tightening": ["SUBLPDCRENQ", "DRTSCRE", "SUBTPDCRENQ", "SUBLPCRE"],
    "sloos_cre_demand": ["SUBLPDCRENQ", "DRSDCRE", "DRGSCRE"],
    "sloos_mortgage_demand": ["SUBLPDHMNQ", "DRSDPM", "SUBLPDPMNQ", "DRGSPM"],
    "sloos_mortgage_tightening_prime": ["DRTSPM", "SUBLPDPMNQ", "DRGSPMTNQ"],
    "sloos_other_consumer": ["STDSOTHER", "DRTSCOL", "DRTSCOLT", "DRTSC"],
    "sloos_subprime_mort": ["DRTSSP", "STDSOTHER"],
    # NEW SLOOS — willingness to lend, special questions
    "sloos_willingness_consumer": ["DRIWCIL", "DRIWNCIL"],
}

DIAG_CODE = '''
import json, urllib.request

FRED_KEY = "2f057499936072679d8843d7fce99989"

def test(sid):
    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={sid}&api_key={FRED_KEY}&file_type=json"
           f"&sort_order=desc&limit=2")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JH-IDhunt/1.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read().decode("utf-8"))
        obs = data.get("observations", [])
        if obs and obs[0].get("value") not in (".", "", None):
            return {"valid": True, "latest_date": obs[0].get("date"),
                    "latest_value": obs[0].get("value"),
                    "n_obs_returned": len(obs)}
        return {"valid": False, "reason": "no_data"}
    except urllib.error.HTTPError as e:
        return {"valid": False, "reason": f"HTTP {e.code}: {e.reason}"}
    except Exception as e:
        return {"valid": False, "reason": str(e)[:100]}

CANDIDATES = ''' + json.dumps(CANDIDATES) + '''

def lambda_handler(event, context):
    out = {}
    for slot, ids in CANDIDATES.items():
        slot_results = []
        winner = None
        for sid in ids:
            r = test(sid)
            slot_results.append({"sid": sid, **r})
            if r.get("valid") and not winner:
                winner = sid
        out[slot] = {"winner": winner, "all_tests": slot_results}
    return {"statusCode": 200, "body": json.dumps(out)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG_CODE)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=256, Timeout=180, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed else parsed
    except Exception:
        out["raw"] = body[:5000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
