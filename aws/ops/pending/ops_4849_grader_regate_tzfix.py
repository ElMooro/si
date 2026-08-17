"""ops/4849 -- grader regate after tz fix (4848 captured the
root cause: naive-vs-aware datetime; v1.0.1 slices week keys to
YYYY-MM-DD) (4847: invoke fired, no
doc in 3 min => runtime crash).
 (1) tail CloudWatch for /aws/lambda/justhodl-beaters-grader --
     print the last error lines.
 (2) SYNCHRONOUS re-invoke: FunctionError + payload traceback
     printed verbatim (root cause lands in this report).
 (3) if healthy: poll OUT + full truths (bank identity, accruing
     ETA, weights gate).
"""
import gzip
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
FN = "justhodl-beaters-grader"
B = "justhodl-dashboard-live"
SRC_KEY = "data/spx-beaters.json"
BANK_KEY = "spx-beaters/listings-history.json"
OUT_KEY = "data/beaters-learned-weights.json"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=150,
                                 retries={"max_attempts": 1}))
logs = boto3.client("logs", region_name=REGION)
FAILED = []


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def main():
    with report("ops 4849 -- grader regate tz-fixed") as rep:
        rep.heading("1. CloudWatch tail")
        try:
            streams = logs.describe_log_streams(
                logGroupName="/aws/lambda/%s" % FN,
                orderBy="LastEventTime", descending=True,
                limit=2).get("logStreams") or []
            for st in streams[:2]:
                evs = logs.get_log_events(
                    logGroupName="/aws/lambda/%s" % FN,
                    logStreamName=st["logStreamName"],
                    limit=40).get("events") or []
                for e in evs:
                    msg = e["message"].rstrip()
                    if any(k in msg for k in
                           ("Error", "error", "Trace", "REPORT",
                            "Task timed out", "raise")):
                        rep.log("  %s" % msg[:180])
        except ClientError as e:
            rep.warn("logs unavailable: %s" % e)

        rep.heading("2. synchronous invoke (root cause capture)")
        r = lam.invoke(FunctionName=FN,
                       InvocationType="RequestResponse",
                       Payload=b"{}")
        payload = r["Payload"].read().decode("utf-8", "replace")
        ferr = r.get("FunctionError")
        rep.log("  StatusCode=%s FunctionError=%s"
                % (r.get("StatusCode"), ferr))
        rep.log("  payload: %s" % payload[:800])
        if ferr:
            rep.fail("lambda still erroring -- traceback above")
            sys.exit(1)
        rep.ok("synchronous invoke clean")

        rep.heading("3. truths")
        try:
            doc = sread(OUT_KEY)
            bank = sread(BANK_KEY)
            src = sread(SRC_KEY)
        except ClientError as e:
            rep.fail("outputs unreadable: %s" % e)
            sys.exit(1)
        wk = (src.get("as_of") or "")[:10]
        if doc.get("status") == "LIVE" and wk in bank.get(
                "weeks", {}):
            rep.ok("  LIVE; week %s banked" % wk)
        else:
            rep.fail("  status=%s weeks=%s"
                     % (doc.get("status"),
                        sorted(bank.get("weeks", {}))))
            FAILED.append("bank")
        snap = (bank.get("weeks", {}).get(wk) or {}).get(
            "buckets") or {}
        for bname, rows in (src.get("buckets") or {}).items():
            exp = min(40, len(rows))
            got = len(snap.get(bname) or [])
            if got == exp:
                rep.ok("  bucket %-18s banked %d == min(40,%d)"
                       % (bname, got, len(rows)))
            else:
                rep.fail("  bucket %s got=%d exp=%d"
                         % (bname, got, exp))
                FAILED.append("b")
        age = (datetime.fromisoformat(doc["as_of"])
               - datetime.fromisoformat(wk)).days
        acc = doc.get("accruing") or {}
        eta = (datetime.fromisoformat(wk)
               + timedelta(days=28)).date().isoformat()
        if age < 28:
            if doc.get("n_graded_rows") == 0 \
                    and acc.get("first_grade_eta") == eta:
                rep.ok("  age %dd -> accruing, ETA %s"
                       % (age, eta))
            else:
                rep.fail("  accruing wrong: %s" % acc)
                FAILED.append("acc")
        if doc.get("note") and doc.get("n_graded_rows", -1) >= 0:
            rep.ok("  weights: n=%d, consumption-deferred note "
                   "present" % doc["n_graded_rows"])
        else:
            rep.fail("  weights block malformed")
            FAILED.append("w")
        rep.log("  banked weeks=%d" % len(bank.get("weeks", {})))

        rep.heading("4. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("Fusion 4 grader LIVE -- claims banked, grading "
               "clock running")


if __name__ == "__main__":
    main()
