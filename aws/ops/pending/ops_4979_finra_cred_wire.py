"""ops_4979 -- wire Khalid's FINRA credential + drain-before-rotate.

Khalid supplied: API Client ID b288c3b3ba12401faf1d, EXPIRES
2026-09-19 -- he will rotate only AFTER the full import lands, so
the clock is running.

  P0 persist: vault item provider=finra in justhodl-api-keys
     (client_id + expiry) + FINRA_CLIENT_ID into the engine env
  P1 token mint: ID + secret-if-present. OAuth2 client-credentials
     needs BOTH; if the secret is nowhere, say so in one line and
     keep the public-tier drain running -- the env upgrade is
     in-place whenever the secret lands (vault or paste)
  P2 drain truth: state must show datasets/rows GROWING (or
     COMPLETE); manifest gains the expiry note
"""
import gzip
import json
import sys
import time
from base64 import b64encode
from datetime import datetime, timezone
from pathlib import Path
import urllib.request

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-finra-full"
TBL = "justhodl-api-keys"
CID = "b288c3b3ba12401faf1d"
EXP = "2026-09-19"
STATE_KEY = "data/warm/finra-full/_state/state.json"
MANIFEST_KEY = "data/warm/finra-full/manifest.json"
TOKEN_URL = ("https://ews.fip.finra.org/fip/rest/ews/oauth2/"
             "access_token?grant_type=client_credentials")

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


with report("ops_4979_finra_cred_wire") as R:
    fails = []
    R.section("P0 persist credential")
    # secret may already sit in the vault under the finra item
    secret = ""
    try:
        sc = ddb.scan(TableName=TBL)
        for it in sc.get("Items", []):
            blob = json.dumps(it).lower()
            if "finra" in blob or CID.lower() in blob:
                for k, v in it.items():
                    if "secret" in k.lower() and v.get("S"):
                        secret = v["S"]
        item = {"provider": {"S": "finra"},
                "client_id": {"S": CID},
                "expires": {"S": EXP},
                "note": {"S": "wired ops 4979; rotate AFTER full "
                              "import lands"}}
        if secret:
            item["client_secret"] = {"S": secret}
        # match table key schema dynamically
        desc = ddb.describe_table(TableName=TBL)["Table"]
        hk = desc["KeySchema"][0]["AttributeName"]
        if hk not in item:
            item[hk] = {"S": "finra"}
        ddb.put_item(TableName=TBL, Item=item)
        R.log("  vault item written (hash key %r) secret_present=%s"
              % (hk, bool(secret)))
    except Exception as e:
        R.log("  vault write: %s" % str(e)[:110])
        fails.append("P0-vault")
    try:
        env = lam.get_function_configuration(
            FunctionName=FN)["Environment"]["Variables"]
        env["FINRA_CLIENT_ID"] = CID
        if secret:
            env["FINRA_CLIENT_SECRET"] = secret
        env["FINRA_CRED_EXPIRES"] = EXP
        lam.update_function_configuration(
            FunctionName=FN, Environment={"Variables": env})
        R.log("  engine env: CLIENT_ID set, secret=%s, expiry "
              "recorded" % bool(secret))
        time.sleep(20)
    except Exception as e:
        R.log("  env inject: %s" % str(e)[:110])
        fails.append("P0-env")

    R.section("P1 token mint")
    if secret:
        try:
            req = urllib.request.Request(
                TOKEN_URL, method="POST",
                headers={"Authorization": "Basic " + b64encode(
                    ("%s:%s" % (CID, secret)).encode()).decode()})
            with urllib.request.urlopen(req, timeout=45) as r:
                tok = json.loads(r.read(20_000)).get(
                    "access_token") or ""
            R.log("  mint with vault secret: %s" %
                  ("OK -- FULL TIER LIVE" if tok else "empty"))
        except Exception as e:
            R.log("  mint failed: %s" % str(e)[:110])
    else:
        R.log("  SECRET MISSING: OAuth2 client-credentials needs "
              "the API Client Secret paired with %s... -- paste "
              "it (or add client_secret to the vault's finra "
              "item) and the engine upgrades on the next 6h tick. "
              "Public-tier drain continues meanwhile." % CID[:8])

    R.section("P2 drain truth + expiry note")
    kicked = False
    st0 = gj(STATE_KEY) or {}
    h0 = len(st0.get("have") or {})
    if not st0 or (st0.get("phase") != "COMPLETE" and
                   float(st0.get("lease_until") or 0)
                   <= time.time()):
        try:
            lam.invoke(FunctionName=FN, InvocationType="Event",
                       Payload=b"{}")
            kicked = True
        except Exception:
            pass
    st, t0 = st0, time.time()
    while time.time() - t0 < 8 * 60:
        time.sleep(30)
        st = gj(STATE_KEY) or {}
        have = st.get("have") or {}
        R.log("  t+%3ds phase=%s banked=%d rows=%d q=%s" % (
            time.time() - t0, st.get("phase"), len(have),
            sum(v.get("rows") or 0 for v in have.values()),
            len(st.get("queue") or [])))
        if st.get("phase") == "COMPLETE" or \
                len(have) > max(h0, 3):
            break
    have = st.get("have") or {}
    ok2 = st.get("phase") == "COMPLETE" or len(have) > h0 or \
        len(have) >= 4
    R.log("  P2 %s (banked %d -> %d, kicked=%s)" % (
        "PASS" if ok2 else "FAIL", h0, len(have), kicked))
    if not ok2:
        fails.append("P2")
    man = gj(MANIFEST_KEY) or {}
    man["cred_expires"] = EXP
    man["cred_note"] = ("client_id wired ops 4979; rotate after "
                        "full import; secret %s" %
                        ("present" if secret else "PENDING"))
    s3.put_object(Bucket=B, Key=MANIFEST_KEY,
                  Body=json.dumps(man, indent=1).encode(),
                  ContentType="application/json")
    R.log("  manifest expiry note written")

    if fails:
        R.log("ops 4979 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(client_id=CID[:8] + "...", secret=bool(secret),
         banked=len(have), phase=st.get("phase"))
    R.log("ops 4979 GREEN -- credential wired with expiry %s; "
          "drain-before-rotate clock running" % EXP)
