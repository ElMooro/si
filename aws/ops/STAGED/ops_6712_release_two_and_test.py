"""ops 6712 - turn macro-financial-intelligence and treasury-api back on (Khalid, 2026-11-02 11:08 ET: "turn these on"),
step 1: lift the governor's operator hold and run each once; ofrapi stays held (see below).

ops 6711: both write outside the live bucket (the Oct 20 review's "missing key" was the wrong bucket):
  * treasury-api: Treasury FiscalData auctions (real), API Gateway TreasuryMarketAPI + Function URL; its scheduled update
    ({"source":"aws.events"} -> openbb-lambda-data/treasury_historical_comprehensive.json) last ran 2026-05-28 - the
    rule treasury-auto-update no longer exists.
  * macro-financial-intelligence: FRED (WSHOMCB 90 d, DFF / T10Y2Y / DGS10 / DGS2) -> macro-financial-intelligence/
    latest_brief.json, last written 2026-10-16 at 159 bytes (an empty brief); its rule macro-financial-daily-execution
    is DISABLED (retire plan).
  * ofrapi is NOT released: its "42 OFR indicators" are generated with random.gauss around hand-set targets
    (generate_base_fsi_data / generate_correlated_data) - synthetic, against the real-data-only rule.  Khalid decides.

A. {"action":"release_cap"} for the two (operator holds, stage CAP); reserved concurrency read back.
B. treasury-api: the scheduled path once ({"source":"aws.events"}) -> the history file's age, update count and latest
   auction; and the API path once ({"rawPath":"/auctions/current"}) -> status and latest auction date.
C. macro-financial-intelligence: the scheduled path once ({}) -> latest_brief.json content (series filled or empty);
   whether its role may read the SSM FRED key /justhodl/fred/api-key (IAM simulation) - the fix chooses SSM if so.
Invocations are synchronous with no SDK retry (a retried sync invoke re-runs the engine).  No key value is read or printed.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
GOVERNOR = "justhodl-spend-governor"
TWO = ["macro-financial-intelligence", "treasury-api"]
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, read_timeout=120)
ONCE = Config(retries={"max_attempts": 1}, read_timeout=900, connect_timeout=10)


def gov(lam, payload):
    for _ in range(8):                                   # the governor has reserved concurrency 1: a running tick answers 429
        try:
            resp = lam.invoke(FunctionName=GOVERNOR, InvocationType="RequestResponse", Payload=json.dumps(payload).encode())
            return json.loads(resp["Payload"].read() or b"{}")
        except lam.exceptions.TooManyRequestsException:
            time.sleep(30)
    raise RuntimeError("governor busy")


def run(lam1, fn, payload, r):
    t0 = time.time()
    resp = lam1.invoke(FunctionName=fn, InvocationType="RequestResponse", LogType="Tail", Payload=json.dumps(payload).encode())
    body = resp["Payload"].read().decode("utf-8", "replace")
    tail = base64.b64decode(resp.get("LogResult") or b"").decode("utf-8", "replace")
    r.log("%s %s -> HTTP %s %s in %.1f s | payload %s" % (fn, json.dumps(payload), resp.get("StatusCode"), resp.get("FunctionError") or "",
                                                       time.time() - t0, body[:700]))
    r.log("   log tail: %s" % " | ".join(l.strip() for l in tail.splitlines() if l.strip() and not l.startswith(("START", "END")))[-1200:])
    return resp, body


def main():
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    lam1 = boto3.client("lambda", region_name=REGION, config=ONCE)
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    iam = boto3.client("iam", config=CFG)
    with report("ops_6712_release_two_and_test") as r:
        r.heading("ops 6712 - release macro-financial-intelligence + treasury-api from the operator hold and run each once")
        r.section("A. governor release (operator holds)")
        for fn in TWO:
            out = gov(lam, {"action": "release_cap", "function": fn, "confirm": fn,
                            "reason": "ops 6712: Khalid 2026-11-02 'turn these on' (Oct 20 retire-plan hold lifted)"})
            rc = lam.get_function_concurrency(FunctionName=fn).get("ReservedConcurrentExecutions")
            (r.ok if not out.get("error") and rc != 0 else r.warn)("%s: %s | reserved concurrency now %s" % (fn, json.dumps(out), rc))
        held = lam.get_function_concurrency(FunctionName="ofrapi").get("ReservedConcurrentExecutions")
        r.log("ofrapi stays held (synthetic data): reserved concurrency %s" % held)

        r.section("B. treasury-api: scheduled update + one API answer")
        key = "treasury_historical_comprehensive.json"
        before = s3.head_object(Bucket="openbb-lambda-data", Key=key)["LastModified"]
        run(lam1, "treasury-api", {"source": "aws.events"}, r)
        h = s3.head_object(Bucket="openbb-lambda-data", Key=key)
        doc = json.loads(s3.get_object(Bucket="openbb-lambda-data", Key=key)["Body"].read())
        first = (doc.get("updates") or [{}])[0]
        latest = {k: (v.get("latest_auction") or {}).get("auction_date") for k, v in (first.get("instruments") or {}).items()}
        (r.ok if h["LastModified"] > before else r.warn)("history file: written %s (was %s), %d B, total_updates %s, last_updated %s, instruments %d, latest auction dates %s" % (
            h["LastModified"].isoformat()[:19], before.isoformat()[:19], h["ContentLength"], doc.get("total_updates"), doc.get("last_updated"),
            len(first.get("instruments") or {}), json.dumps(latest)[:500]))
        resp, body = run(lam1, "treasury-api", {"rawPath": "/auctions/current"}, r)
        try:
            ans = json.loads(json.loads(body).get("body") or "{}")
            r.log("   /auctions/current: keys %s | auction %s" % (sorted(ans)[:15], json.dumps({k: ans.get(k) for k in ("auction_date", "security_type", "security_term", "high_yield", "bid_to_cover_ratio")} if isinstance(ans, dict) else ans)[:400]))
        except Exception as e:  # noqa: BLE001
            r.warn("   /auctions/current unparsed: %s" % type(e).__name__)

        r.section("C. macro-financial-intelligence: one scheduled run, its brief, its route to the SSM FRED key")
        cfg = lam.get_function_configuration(FunctionName="macro-financial-intelligence")
        bucket = ((cfg.get("Environment") or {}).get("Variables") or {}).get("S3_BUCKET")
        run(lam1, "macro-financial-intelligence", {}, r)
        h = s3.head_object(Bucket=bucket, Key="latest_brief.json")
        brief = json.loads(s3.get_object(Bucket=bucket, Key="latest_brief.json")["Body"].read())
        r.log("latest_brief.json: written %s, %d B: %s" % (h["LastModified"].isoformat()[:19], h["ContentLength"], json.dumps(brief)[:900]))
        filled = bool(brief.get("inds")) and (brief.get("fails") or {}).get("status") != "no_data"
        (r.ok if filled else r.warn)("FRED series filled: %s (inds %d, fails %s)" % (filled, len(brief.get("inds") or {}), (brief.get("fails") or {}).get("status", "ok")))
        role_arn = cfg["Role"]
        param = "arn:aws:ssm:%s:%s:parameter/justhodl/fred/api-key" % (REGION, ACCOUNT)
        try:
            sim = iam.simulate_principal_policy(PolicySourceArn=role_arn, ActionNames=["ssm:GetParameter"], ResourceArns=[param])
            dec = [(x["EvalActionName"], x["EvalDecision"]) for x in sim["EvaluationResults"]]
        except Exception as e:  # noqa: BLE001
            dec = "simulation failed: %s" % type(e).__name__
        r.log("role %s ssm:GetParameter on /justhodl/fred/api-key: %s" % (role_arn.split("/")[-1], dec))
        pols = iam.list_attached_role_policies(RoleName=role_arn.split("/")[-1]).get("AttachedPolicies", [])
        inline = iam.list_role_policies(RoleName=role_arn.split("/")[-1]).get("PolicyNames", [])
        r.log("role policies: attached %s | inline %s" % ([p["PolicyName"] for p in pols], inline))
        r.ok("ops 6712 complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
