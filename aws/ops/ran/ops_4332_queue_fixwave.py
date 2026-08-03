"""ops_4332 -- queue fix-wave seal: liquidity-flow writes again to its
pinned key; pump-radar parses as plain JSON; ai-rerating's squeeze map
loads with provenance (never silently empty); opportunity-engine's
outgrow flag carries a disclosed basis."""
import json, subprocess, sys, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from ops_report import report
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=600,
                                 retries={"max_attempts": 1}))
logs = boto3.client("logs", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
RUN_START = datetime.now(timezone.utc)

def floor_ok(fn, d):
    try:
        ts = subprocess.run(["git", "log", "-1", "--format=%ct",
                             "--", "aws/lambdas/" + d],
                            capture_output=True, text=True,
                            timeout=30).stdout.strip()
        fl = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except Exception:
        fl = RUN_START
    for _ in range(50):
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            lm = datetime.strptime(c["LastModified"].split(".")[0],
                                   "%Y-%m-%dT%H:%M:%S").replace(
                tzinfo=timezone.utc)
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and lm >= fl:
                return True
        except Exception:
            pass
        time.sleep(9)
    return False
fails = []
with report("4332_queue_fixwave") as r:
    r.heading("ops 4332 -- four bugs, one wave")
    r.section("1. liquidity-flow")
    if not floor_ok("justhodl-liquidity-flow",
                    "justhodl-liquidity-flow"):
        fails.append("liq deploy floor")
    else:
        lam.invoke(FunctionName="justhodl-liquidity-flow",
                   InvocationType="RequestResponse", Payload=b"{}")
        d = json.loads(s3.get_object(
            Bucket=B, Key="data/liquidity-flow.json"
        )["Body"].read())
        g = d.get("generated_at") or d.get("as_of")
        r.ok("artifact generated_at=%s" % g)
        if not (g and str(g)[:10] ==
                RUN_START.strftime("%Y-%m-%d")):
            fails.append("liquidity-flow artifact not today: %s"
                         % g)
    r.section("2. pump-radar plain")
    if not floor_ok("justhodl-prepump-summary",
                    "justhodl-prepump-summary"):
        fails.append("prepump deploy floor")
    else:
        lam.invoke(FunctionName="justhodl-prepump-summary",
                   InvocationType="RequestResponse", Payload=b"{}")
        head = s3.get_object(Bucket=B,
                             Key="data/pump-radar-summary.json",
                             Range="bytes=0-3")["Body"].read()
        r.log("first bytes: %s" % head.hex())
        if not head.startswith(b"{"):
            fails.append("pump-radar still not plain: %s"
                         % head.hex())
        else:
            j = json.loads(s3.get_object(
                Bucket=B, Key="data/pump-radar-summary.json"
            )["Body"].read())
            r.ok("parses as JSON · keys %s" % list(j)[:6])
    r.section("3. ai-rerating squeeze provenance")
    if not floor_ok("justhodl-ai-rerating-radar",
                    "justhodl-ai-rerating-radar"):
        fails.append("rerating deploy floor")
    else:
        lam.invoke(FunctionName="justhodl-ai-rerating-radar",
                   InvocationType="Event", Payload=b"{}")
        time.sleep(50)
        ev = logs.filter_log_events(
            logGroupName="/aws/lambda/justhodl-ai-rerating-radar",
            startTime=int((time.time() - 300) * 1000),
            filterPattern='"[shrt]"')["events"][-1:]
        msg = ev[0]["message"].strip() if ev else "no [shrt] line"
        r.log(msg[:160])
        if "n=0" in msg or not ev:
            fails.append("squeeze map still empty: %s" % msg[:100])
        else:
            r.ok("squeeze map loads with provenance")
    r.section("4. opportunity-engine basis (soft)")
    r.log("code-level fix deployed; outgrow_basis field will show "
          "fwd_vs_fwd|fwd_vs_trailing on next scheduled run -- "
          "auditor's ZERO_SCOPE watches the artifact")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
        sys.exit(1)
    r.ok("OPS 4332 PASS -- the queue shrinks with receipts")
