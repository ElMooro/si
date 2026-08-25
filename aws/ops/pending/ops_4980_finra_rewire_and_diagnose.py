"""ops_4980 -- FINRA: new credential rewire + the REAL error.

Khalid's portal: OLD b288c3b3... EXPIRED 8/25 1:25PM; NEW
c2de60df7bdf4bfabe21 created today, PENDING EMAIL ACTIVATION,
secret value not yet shared. Meanwhile v6's zero-delta signature
(cat=0 inv=0 frozen through a forced rediscover) = the engine
invoke is crashing before touching state -- Event invokes swallow
it. Get the truth:

  P0 rewire vault+env to the NEW client_id (old marked expired)
  P1 RequestResponse invoke {"rediscover":true} with LogType=Tail
     -> print FunctionError + payload + decoded log tail (the
     crash line, verbatim)
  P2 state truth after the sync invoke
"""
import base64
import gzip
import json
import sys
import time
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-finra-full"
TBL = "justhodl-api-keys"
NEW_CID = "c2de60df7bdf4bfabe21"
OLD_CID = "b288c3b3ba12401faf1d"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)


def gj(key, default=None):
    try:
        raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:
        return default


with report("ops_4980_finra_rewire_and_diagnose") as R:
    fails = []
    R.section("P0 rewire to new client_id")
    try:
        desc = ddb.describe_table(TableName=TBL)["Table"]
        hk = desc["KeySchema"][0]["AttributeName"]
        item = {hk: {"S": "finra"},
                "provider": {"S": "finra"},
                "client_id": {"S": NEW_CID},
                "old_client_id": {"S": OLD_CID + " (expired "
                                       "8/25/26)"},
                "status": {"S": "pending-email-activation; secret "
                                "value pending (portal Reset shows "
                                "it once)"},
                "note": {"S": "rewired ops 4980"}}
        ddb.put_item(TableName=TBL, Item=item)
        R.log("  vault: finra item -> new client_id")
    except Exception as e:
        R.log("  vault: %s" % str(e)[:100])
        fails.append("P0-vault")
    try:
        env = lam.get_function_configuration(
            FunctionName=FN)["Environment"]["Variables"]
        env["FINRA_CLIENT_ID"] = NEW_CID
        env.pop("FINRA_CLIENT_SECRET", None)
        env["FINRA_CRED_NOTE"] = ("new cred pending activation+"
                                  "secret; old expired 8/25")
        lam.update_function_configuration(
            FunctionName=FN, Environment={"Variables": env})
        R.log("  env: FINRA_CLIENT_ID -> new; stale secret "
              "cleared")
        time.sleep(20)
    except Exception as e:
        R.log("  env: %s" % str(e)[:100])
        fails.append("P0-env")

    R.section("P1 sync invoke with log tail")
    err_seen = None
    try:
        resp = lam.invoke(
            FunctionName=FN, InvocationType="RequestResponse",
            LogType="Tail",
            Payload=json.dumps({"rediscover": True,
                                "no_chain": True}).encode())
        fe = resp.get("FunctionError")
        payload = resp["Payload"].read().decode("utf-8", "replace")
        R.log("  FunctionError=%s" % fe)
        R.log("  payload: %s" % payload[:400])
        tail = base64.b64decode(
            resp.get("LogResult") or b"").decode("utf-8", "replace")
        for ln in tail.splitlines()[-25:]:
            R.log("  | %s" % ln[:150])
        err_seen = fe
    except Exception as e:
        R.log("  invoke err: %s" % str(e)[:150])
        fails.append("P1")

    R.section("P2 state truth")
    st = gj("data/warm/finra-full/_state/state.json") or {}
    R.log("  phase=%s universe=%d invalid=%d have=%d failures=%s"
          % (st.get("phase"), len(st.get("universe") or {}),
             len(st.get("invalid") or {}),
             len(st.get("have") or {}),
             json.dumps(st.get("failures") or {})[:300]))
    for k, v in list((st.get("invalid") or {}).items())[:8]:
        R.log("    invalid %s: %s" % (k, v))

    moved = bool((st.get("universe") or {}) or
                 (st.get("invalid") or {}))
    if err_seen or (not moved and "P1" not in fails):
        R.log("ops 4980 RED: engine faulted or still inert -- "
              "crash line above is the fix target")
        sys.exit(1)
    if fails:
        R.log("ops 4980 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(universe=len(st.get("universe") or {}),
         invalid=len(st.get("invalid") or {}),
         banked=len(st.get("have") or {}))
    R.log("ops 4980 GREEN -- discovery moving; new client_id "
          "wired (activation + secret still pending on Khalid)")
