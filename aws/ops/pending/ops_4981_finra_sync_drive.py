"""ops_4981 -- FINRA sync-drive + the async verdict + Khalid's
"try this one" mint.

Events provably vanish (sync 4980 moved state; v6/v7/v8 Event
kicks froze at t+0). Schedules fire Events, so this MUST be
settled. Plan:

  P0 config dump: reserved concurrency + EventInvokeConfig; reset
     EventInvokeConfig to sane retry/age
  P0b mint attempts with the ACTIVATED client c2de60df... x
      [no secret, id-as-secret] -> print FINRA's verbatim answer
      (expected: needs the real secret from the portal's Reset)
  P1 SYNC rediscover (budget_s=150, no_chain) -> universe MUST
     populate (v1.0.5 trust-catalog)
  P2 SYNC drain links x5 (budget_s=150) -> banked/rows GROW
  P3 one bare Event + 90s as_of watch -> async verdict for the
     schedules; if dead, schedules move to sync-driving via a
     wrapper later
"""
import base64
import gzip
import json
import sys
import time
import urllib.error
import urllib.request
from base64 import b64encode
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-finra-full"
CID = "c2de60df7bdf4bfabe21"
STATE_KEY = "data/warm/finra-full/_state/state.json"
TOKEN_URL = ("https://ews.fip.finra.org/fip/rest/ews/oauth2/"
             "access_token?grant_type=client_credentials")

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def gj(key, default=None):
    try:
        raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:
        return default


def sync(payload, tail=False):
    kw = dict(FunctionName=FN, InvocationType="RequestResponse",
              Payload=json.dumps(payload).encode())
    if tail:
        kw["LogType"] = "Tail"
    r = lam.invoke(**kw)
    body = r["Payload"].read().decode("utf-8", "replace")
    lt = base64.b64decode(r.get("LogResult") or b"").decode(
        "utf-8", "replace") if tail else ""
    return r.get("FunctionError"), body, lt


with report("ops_4981_finra_sync_drive") as R:
    fails = []
    R.section("P0 function config")
    try:
        cfg = lam.get_function_configuration(FunctionName=FN)
        R.log("  timeout=%s mem=%s state=%s lastmod=%s" % (
            cfg.get("Timeout"), cfg.get("MemorySize"),
            cfg.get("State"), cfg.get("LastModified")))
        try:
            rc = lam.get_function_concurrency(FunctionName=FN)
            R.log("  reserved_concurrency=%s" %
                  rc.get("ReservedConcurrentExecutions"))
        except Exception as e:
            R.log("  reserved_concurrency: %s" % str(e)[:60])
        try:
            ev = lam.get_function_event_invoke_config(
                FunctionName=FN)
            R.log("  event_invoke_config=%s" % json.dumps(
                {k: v for k, v in ev.items()
                 if k in ("MaximumRetryAttempts",
                          "MaximumEventAgeInSeconds",
                          "DestinationConfig")})[:200])
        except lam.exceptions.ResourceNotFoundException:
            R.log("  event_invoke_config: none (defaults)")
        lam.put_function_event_invoke_config(
            FunctionName=FN, MaximumRetryAttempts=2,
            MaximumEventAgeInSeconds=3600)
        R.log("  event_invoke_config reset to sane defaults")
    except Exception as e:
        R.log("  config: %s" % str(e)[:100])

    R.section("P0b mint attempts (Khalid: try this one)")
    for label, sec in [("no-secret", ""), ("id-as-secret", CID)]:
        try:
            req = urllib.request.Request(
                TOKEN_URL, method="POST",
                headers={"Authorization": "Basic " + b64encode(
                    ("%s:%s" % (CID, sec)).encode()).decode()})
            with urllib.request.urlopen(req, timeout=30) as r_:
                js = json.loads(r_.read(20_000))
            R.log("  %s -> 200 token=%s" % (
                label, bool(js.get("access_token"))))
        except urllib.error.HTTPError as e:
            R.log("  %s -> HTTP %s %s" % (
                label, e.code, (e.read(150) or b"").decode(
                    "utf-8", "replace")[:120]))
        except Exception as e:
            R.log("  %s -> %s" % (label, str(e)[:100]))

    R.section("P1 sync rediscover")
    fe, body, lt = sync({"rediscover": True, "no_chain": True,
                         "budget_s": 150}, tail=True)
    R.log("  FunctionError=%s payload=%s" % (fe, body[:260]))
    if fe:
        for ln in lt.splitlines()[-14:]:
            R.log("  | %s" % ln[:150])
    st = gj(STATE_KEY) or {}
    uni = len(st.get("universe") or {})
    R.log("  universe=%d invalid=%d phase=%s" % (
        uni, len(st.get("invalid") or {}), st.get("phase")))
    if uni < 6:
        fails.append("P1")

    R.section("P2 sync drain x5")
    last = 0
    for i in range(5):
        fe, body, _ = sync({"no_chain": True, "budget_s": 150})
        st = gj(STATE_KEY) or {}
        have = st.get("have") or {}
        rows = sum(v.get("rows") or 0 for v in have.values())
        R.log("  link%d err=%s banked=%d rows=%d q=%d" % (
            i + 1, fe, len(have), rows,
            len(st.get("queue") or [])))
        if fe:
            break
        if st.get("phase") == "COMPLETE":
            break
        last = rows
    have = st.get("have") or {}
    rows = sum(v.get("rows") or 0 for v in have.values())
    for k, v in list((st.get("failures") or {}).items())[:6]:
        R.log("    fail %s: %s" % (k, json.dumps(v)[:110]))
    if len(have) < 3 and st.get("phase") != "COMPLETE":
        fails.append("P2")

    R.section("P3 async verdict")
    as_of0 = st.get("as_of")
    lam.invoke(FunctionName=FN, InvocationType="Event",
               Payload=json.dumps({"no_chain": True,
                                   "budget_s": 60}).encode())
    verdict = "DEAD"
    t0 = time.time()
    while time.time() - t0 < 100:
        time.sleep(15)
        st2 = gj(STATE_KEY) or {}
        if st2.get("as_of") and st2.get("as_of") != as_of0:
            verdict = "ALIVE (%.0fs)" % (time.time() - t0)
            break
    R.log("  async events: %s" % verdict)

    if fails:
        R.log("ops 4981 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(universe=uni, banked=len(have), rows=rows,
         async_events=verdict)
    R.log("ops 4981 GREEN -- FINRA draining under sync links; "
          "async verdict recorded for the schedules")
