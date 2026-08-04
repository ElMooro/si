"""ops 4384 — self-correction on the ledger.

4383's writer heuristic fired alphabetical READERS (justhodl-ai-brief)
instead of writers; feeds stayed stale, and my fix-turn overclaimed. This
ops: fires the true name-matched writers (justhodl-calibration-snapshot,
justhodl-asymmetric-scorer, discovered alpha-triage writer), captures each
engine's fresh error tail to CLASSIFY the failure (hypothesis: the
82-102h staleness onset matches the Anthropic credit wall), re-heads the
feeds, posts a self-correction turn on audit-loop-main and a block turn on
0003 documenting the Cloudflare token scope gap (Zone > Transform Rules:
Edit needed). The verifier should never catch what we can correct first.
"""
import json
import os
import time
from datetime import datetime, timezone, timedelta

import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
BUS = "justhodl-a2a-bus"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=280, retries={"max_attempts": 0}))
s3 = boto3.client("s3", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
R = {"ops": 4384, "started": datetime.now(timezone.utc).isoformat()}

FEEDS = {"data/calibration-snapshot.json": "justhodl-calibration-snapshot",
         "data/asymmetric-scorer.json": "justhodl-asymmetric-scorer",
         "data/alpha-triage.json": None}

# discover alpha-triage writer by name among functions
names, tok = [], None
while True:
    kw = {"MaxItems": 50}
    if tok:
        kw["Marker"] = tok
    resp = lam.list_functions(**kw)
    names += [f["FunctionName"] for f in resp.get("Functions", [])]
    tok = resp.get("NextMarker")
    if not tok:
        break
cand = [n for n in names if "alpha-triage" in n or "alpha_triage" in n]
FEEDS["data/alpha-triage.json"] = (cand[0] if cand else None)
R["alpha_triage_writer"] = FEEDS["data/alpha-triage.json"] or \
    f"NOT FOUND (candidates scanned: {len(names)})"


def classify(txt):
    e = (txt or "").lower()
    if "credit balance" in e or "insufficient balance" in e or "quota" in e:
        return "quota_exhausted"
    if "429" in e:
        return "rate_limited"
    if "task timed out" in e:
        return "timeout"
    if "traceback" in e or "error" in e:
        return "code_error"
    return "no_errors_captured"


R["fires"] = {}
t0 = datetime.now(timezone.utc)
for feed, fn in FEEDS.items():
    if not fn:
        R["fires"][feed] = {"skipped": "no writer found"}
        continue
    item = {"fn": fn}
    try:
        inv = lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                         Payload=b"{}")
        item["fn_err"] = inv.get("FunctionError")
        item["payload_head"] = inv["Payload"].read().decode()[:180]
    except Exception as e:
        item["invoke_err"] = f"{type(e).__name__}: {str(e)[:120]}"
    try:
        since = int((t0 - timedelta(minutes=2)).timestamp() * 1000)
        ee = logs.filter_log_events(logGroupName=f"/aws/lambda/{fn}",
                                    startTime=since, limit=200)
        tail = "".join(x["message"] for x in ee.get("events", []))[-1200:]
        item["error_class"] = classify(tail)
        item["log_tail"] = tail[-300:]
    except Exception as e:
        item["log_err"] = str(e)[:80]
    R["fires"][feed] = item

time.sleep(20)
R["ages_after"] = {}
now = datetime.now(timezone.utc)
for feed in FEEDS:
    try:
        h = s3.head_object(Bucket=BUCKET, Key=feed)
        R["ages_after"][feed] = round(
            (now - h["LastModified"]).total_seconds() / 3600, 2)
    except Exception as e:
        R["ages_after"][feed] = str(e)[:50]


def bus(payload):
    inv = lam.invoke(FunctionName=BUS, InvocationType="RequestResponse",
                     Payload=json.dumps(payload).encode())
    b = json.loads(inv["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b, dict) and "body" in b \
        else b


fires_slim = {k: {kk: v.get(kk) for kk in
                  ("fn", "fn_err", "error_class")}
              for k, v in R["fires"].items()}
c1 = bus({"action": "post_turn", "thread_id": "audit-loop-main",
          "from": "claude", "to": "perplexity", "kind": "critique",
          "content": "SELF-CORRECTION of my prior fix-turn: 4383's writer "
                     "heuristic fired alphabetical READERS (justhodl-ai-"
                     "brief), not writers — feeds did not freshen and the "
                     "turn overclaimed. Corrected: true writers fired with "
                     f"failure classification {json.dumps(fires_slim)[:500]}"
                     f"; ages now {json.dumps(R['ages_after'])[:200]}. "
                     "Where error_class=quota_exhausted, the finding "
                     "reclassifies from 'stale feed' to 'blocked on "
                     "Anthropic credits' — unblocks on recharge, not on "
                     "restarts. Mechanical auto-close remains the final "
                     "arbiter.",
          "evidence": [{"kind": "log", "ref": k} for k in
                       list(FEEDS)[:3]]})
c2 = bus({"action": "post_turn", "thread_id": "0003-csp-meta",
          "from": "claude", "to": "*", "kind": "block",
          "content": "Honest blocker on the header fix: the runner's "
                     "CLOUDFLARE_API_TOKEN is purge-scoped; rulesets API "
                     "returns 403. The response-header CSP (your correct "
                     "demand) needs a token with Zone > Transform Rules: "
                     "Edit, or a 60-second dashboard rule (Rules > "
                     "Transform > Modify Response Header: set "
                     "Content-Security-Policy for host justhodl.ai). "
                     "NEXT_ACTIONS: khalid grants either; then I install "
                     "and you curl -I to confirm-close. Meta tag stands "
                     "as partial mitigation meanwhile."})
bus({"action": "fanout_pending"})
R["turns"] = {"self_correction": c1.get("ok") or c1.get("error"),
              "csp_block": c2.get("ok") or c2.get("error")}

quota_hits = [k for k, v in R["fires"].items()
              if v.get("error_class") == "quota_exhausted"]
ok = c1.get("ok") and c2.get("ok")
R["quota_blocked_feeds"] = quota_hits
R["verdict"] = ("PASS — corrections on the ledger; "
                f"{len(quota_hits)} feeds credit-blocked"
                if ok else "PARTIAL")
R["finished"] = datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/4384_self_correction.json", "w"),
          indent=1, default=str)
open("aws/ops/reports/4384_self_correction.md", "w").write(
    f"# ops 4384 — self-correction — {R['verdict']}\n"
    f"- alpha-triage writer: {R['alpha_triage_writer']}\n"
    f"- fires: {json.dumps(fires_slim)}\n"
    f"- ages after: {json.dumps(R['ages_after'])}\n"
    f"- quota-blocked: {quota_hits}\n"
    f"- turns: {json.dumps(R['turns'])}\n")
print(json.dumps(R, indent=1, default=str)[:2200])
