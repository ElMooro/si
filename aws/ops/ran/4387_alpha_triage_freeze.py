"""ops 4387 — alpha-triage retirement, executed as FREEZE (Khalid: retire).

Pre-cut recon found the feed is load-bearing config: signals_emit reads
its RETIRE list to suppress noise families fleet-wide at emission;
inverse-harvester builds its book from its INVERT verdicts. Deleting it
would silently un-suppress noise across the fleet. Correct retirement:
(1) stamp data/alpha-triage.json status:frozen (ALL verdicts/families
preserved byte-for-byte, only metadata added), (2) register it in
data/audit/exemptions.json so the loop stops flagging staleness on
frozen config, (3) hot-update the loop with exemption support, (4) open
bus thread 0006 with the revised plan INCLUDING the self-correction of my
earlier strip-and-delete recommendation — the verifier should never catch
what we can correct first. Page copy already says 'frozen (writer
retired)'.
"""
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
LOOP = "justhodl-audit-loop"
BUS = "justhodl-a2a-bus"
KEY = "data/alpha-triage.json"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=280, retries={"max_attempts": 0}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 4387, "started": datetime.now(timezone.utc).isoformat()}

# 1 — freeze stamp (preserve everything)
try:
    doc = json.loads(s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
    R["pre"] = {"keys": sorted(doc.keys())[:10],
                "families": len(doc.get("families") or {}),
                "verdicts": len(doc.get("verdicts") or [])}
    doc["status"] = "frozen"
    doc["frozen_at"] = datetime.now(timezone.utc).isoformat(
        timespec="seconds")
    doc["frozen_note"] = ("Writer retired (no producer among 784 fns); "
                          "verdicts stand as fleet suppression/inversion "
                          "config per Khalid decision, ops 4387. Consumers:"
                          " signals_emit._suppress_set, inverse-harvester,"
                          " proven-alpha, alpha-families.html.")
    s3.put_object(Bucket=BUCKET, Key=KEY,
                  Body=json.dumps(doc, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=86400")
    R["frozen"] = True
except Exception as e:
    R["freeze_err"] = f"{type(e).__name__}: {str(e)[:150]}"

# 2 — exemptions registry
try:
    ex = {}
    try:
        ex = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/audit/exemptions.json")["Body"].read())
    except Exception:
        pass
    lst = set(ex.get("stale_exempt") or [])
    lst.add(KEY)
    ex = {"stale_exempt": sorted(lst),
          "updated": datetime.now(timezone.utc).isoformat(),
          "note": "frozen-config feeds: staleness is by design"}
    s3.put_object(Bucket=BUCKET, Key="data/audit/exemptions.json",
                  Body=json.dumps(ex).encode(),
                  ContentType="application/json")
    R["exemptions"] = ex["stale_exempt"]
except Exception as e:
    R["exempt_err"] = str(e)[:120]

# 3 — hot-update loop with exemption support
try:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.write("aws/lambdas/justhodl-audit-loop/source/"
                "lambda_function.py", "lambda_function.py")
        if os.path.exists("aws/shared/_sentry_lite.py"):
            z.write("aws/shared/_sentry_lite.py", "_sentry_lite.py")
    lam.update_function_code(FunctionName=LOOP, ZipFile=buf.getvalue())
    for _ in range(24):
        if lam.get_function_configuration(FunctionName=LOOP).get(
                "LastUpdateStatus") == "Successful":
            break
        time.sleep(5)
    R["loop"] = "exemption support deployed"
except Exception as e:
    R["loop_err"] = str(e)[:150]


# 4 — bus: thread 0006 with the self-correcting revised plan
def bus(payload):
    inv = lam.invoke(FunctionName=BUS, InvocationType="RequestResponse",
                     Payload=json.dumps(payload).encode())
    b = json.loads(inv["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b, dict) and "body" in b \
        else b


t1 = bus({"action": "open_thread",
          "thread_id": "0006-alpha-triage-retirement",
          "topic": "alpha-triage retirement — freeze-not-delete "
                   "(Khalid decision, plan revised by recon)",
          "turn": {"from": "claude", "to": "perplexity", "kind": "propose",
                   "content": "SELF-CORRECTION FIRST: my earlier "
                              "recommendation ('strip the reference and "
                              "tombstone the object; nothing missed it in "
                              "18 days') was WRONG. Recon found the feed "
                              "is load-bearing config: signals_emit."
                              "_suppress_set() silences noise families "
                              "fleet-wide from its RETIRE list on every "
                              "emission, and inverse-harvester derives its "
                              "book from its INVERT verdicts. Deletion "
                              "would have silently un-suppressed noise "
                              "fleet-wide. Executed instead: object "
                              "stamped status:frozen with all verdicts "
                              "preserved; data/audit/exemptions.json "
                              "registers it so the loop stops flagging "
                              "by-design staleness; page copy now reads "
                              "'frozen (writer retired)'. Verify: fetch "
                              "the feed and confirm frozen stamp + intact "
                              "families/verdicts; fetch exemptions; then "
                              "verdict:confirmed and I will resolve into "
                              "the decision ledger.",
                   "evidence": [
                       {"kind": "log", "ref": KEY,
                        "snippet": "frozen"},
                       {"kind": "log",
                        "ref": "data/audit/exemptions.json",
                        "snippet": "alpha-triage"},
                       {"kind": "file",
                        "ref": "aws/shared/signals_emit.py",
                        "snippet": "_suppress_set"}]}})
bus({"action": "post_turn", "thread_id": "audit-loop-main",
     "from": "claude", "to": "perplexity", "kind": "question",
     "content": "Finding 996e1e22ad8b (alpha-triage stale) disposition: "
                "frozen-config, see thread 0006. The exemption means the "
                "loop will not re-flag; the finding closes by policy, not "
                "by freshness."})
bus({"action": "fanout_pending"})
R["thread_0006"] = (t1.get("first_turn") or {}).get("ok") or t1.get("error")

ok = R.get("frozen") and KEY in (R.get("exemptions") or []) and \
    R.get("loop") and R.get("thread_0006") is True
R["verdict"] = ("PASS — frozen with verdicts intact, exempted, "
                "self-correction on the ledger" if ok
                else "PARTIAL — see fields")
R["finished"] = datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/4387_alpha_triage_freeze.json", "w"),
          indent=1, default=str)
open("aws/ops/reports/4387_alpha_triage_freeze.md", "w").write(
    f"# ops 4387 — alpha-triage freeze — {R['verdict']}\n"
    f"- pre: {json.dumps(R.get('pre'))}\n"
    f"- frozen: {R.get('frozen')} | exemptions: {R.get('exemptions')}\n"
    f"- loop: {R.get('loop') or R.get('loop_err')}\n"
    f"- thread 0006 first turn ok: {R.get('thread_0006')}\n")
print(json.dumps(R, indent=1, default=str)[:1600])
