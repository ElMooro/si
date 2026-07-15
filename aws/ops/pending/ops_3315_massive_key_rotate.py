"""ops 3315 — Khalid has THREE Massive keys; only 'beautiful_chandrasekhar'
(...ptM) was ever stored in SSM and it's not Benzinga-entitled (403). Test
all three against the Benzinga ratings endpoint via Massive
(api.polygon.io/benzinga/v1/ratings). Whichever returns 200-with-data is
the Benzinga-entitled key -> rotate it into SSM /justhodl/massive-api-key,
then force-run justhodl-analyst-actions and confirm the feed populates.

Security note: these are Massive API keys (rotatable in Khalid's dashboard).
They pass through this script once; the WINNER is stored in SSM (private).
A follow-up commit scrubs the raw values from this file's git-tracked copy.
Report output prints only fingerprints (name + last3), never full keys.

Verdict PASS = a key authorizes Benzinga AND after rotation the engine
writes non-empty counts.
"""
import json
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path

import boto3

from ops_report import report

REGION = "us-east-1"
SSM = boto3.client("ssm", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/analyst-actions.json"
SSM_PATH = "/justhodl/massive-api-key"

CANDIDATES = {
    "beautiful_chandrasekhar": "ch6CGKm7oMtKWfMp0SI7pP3uLI7q_ptM",
    "desperate_lamarr": "Out4PAHPLWSG6uoeQVSgGUsyN2AnVFPI",
    "Default": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d",
}


def benzinga_test(key):
    url = (f"https://api.polygon.io/benzinga/v1/ratings?limit=2&order=desc"
           f"&apiKey={key}")
    req = urllib.request.Request(url, headers={"User-Agent": "jh-ops-3315"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            body = json.loads(r.read())
            results = body.get("results") or body.get("ratings") or []
            return {"http": r.status, "status": body.get("status"),
                    "n": len(results)}
    except urllib.error.HTTPError as e:
        d = ""
        try:
            d = json.loads(e.read().decode())["status"]
        except Exception:
            pass
        return {"http": e.code, "status": d}
    except Exception as e:
        return {"http": None, "err": f"{type(e).__name__}"}


with report("3315_massive_key_rotate") as rep:
    rep.section("TEST ALL THREE KEYS vs BENZINGA")
    winner_name = winner_key = None
    for name, k in CANDIDATES.items():
        res = benzinga_test(k)
        rep.kv(**{f"{name} (…{k[-3:]})": res})
        if res.get("http") == 200 and res.get("n", 0) > 0 and not winner_key:
            winner_name, winner_key = name, k

    if not winner_key:
        rep.section("VERDICT")
        rep.fail("None of the 3 Massive keys are Benzinga-entitled (all "
                 "403/empty). The Benzinga add-on is not attached to any key "
                 "on this Massive account — needs enabling in the Massive "
                 "dashboard (per-key entitlement toggle), or the add-on lives "
                 "on a different Massive account.")
        rep.kv(RESULT="NO_ENTITLED_KEY")
        sys.exit(1)

    rep.ok(f"ENTITLED KEY = '{winner_name}' (…{winner_key[-3:]})")

    # rotate into SSM
    rep.section("ROTATE SSM")
    SSM.put_parameter(Name=SSM_PATH, Value=winner_key, Type="SecureString",
                      Overwrite=True)
    rep.ok(f"{SSM_PATH} updated to '{winner_name}'")

    # also set env directly on analyst-actions so it doesn't depend on SSM
    # cold-cache and to make the fix immediate + explicit
    rep.section("SET ENGINE ENV")
    try:
        cfg = LAM.get_function_configuration(
            FunctionName="justhodl-analyst-actions")
        env = (cfg.get("Environment") or {}).get("Variables") or {}
        env["MASSIVE_API_KEY"] = winner_key
        LAM.update_function_configuration(
            FunctionName="justhodl-analyst-actions",
            Environment={"Variables": env})
        # wait for update to settle
        for _ in range(20):
            s = LAM.get_function_configuration(
                FunctionName="justhodl-analyst-actions")
            if s.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
        rep.ok("MASSIVE_API_KEY set on justhodl-analyst-actions env")
    except Exception as e:
        rep.warn(f"env set failed (SSM fallback still works): {e}")

    # force-run + verify
    rep.section("FORCE RUN + VERIFY")
    r = LAM.invoke(FunctionName="justhodl-analyst-actions",
                   InvocationType="RequestResponse", Payload=b"{}")
    resp = json.loads(r["Payload"].read().decode())
    rep.kv(invoke_status=r.get("StatusCode"),
           function_error=r.get("FunctionError"), response=resp)
    time.sleep(3)
    try:
        doc = json.loads(S3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read())
        counts = doc.get("counts", {})
        total = sum(counts.values()) if counts else 0
        rep.kv(generated_at=doc.get("generated_at"), counts=counts,
               n_most_bullish=len(doc.get("most_bullish", [])),
               n_top_picks=len(doc.get("top_picks", [])))
        rep.section("VERDICT")
        if total > 0:
            rep.ok(f"FEED POPULATED — {total} analyst signals harvested. "
                   "analyst-actions.html will render on next load.")
            rep.kv(RESULT="FIXED", winner=winner_name, total_signals=total)
        else:
            rep.warn("Key authorized but harvest still 0 — could be a genuinely "
                     "quiet 7-day window; recheck after next market session.")
            rep.kv(RESULT="AUTHORIZED_BUT_EMPTY", winner=winner_name)
    except Exception as e:
        rep.fail(f"feed read failed post-run: {e}")
        rep.kv(RESULT="RUN_OK_VERIFY_FAILED")
        sys.exit(1)
