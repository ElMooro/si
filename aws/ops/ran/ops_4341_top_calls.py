"""ops_4341 -- seal per-seat top calls: every council profile carries
top_call{symbol,direction,expected_return_pct,horizon_days,basis} or a
disclosed null; page renders the column."""
import json, subprocess, sys, time, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from ops_report import report
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=400,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
RUN_START = datetime.now(timezone.utc)
fails = []
with report("4341_top_calls") as r:
    r.heading("ops 4341 -- every seat shows its hand")
    try:
        ts = subprocess.run(
            ["git", "log", "-1", "--format=%ct", "--",
             "aws/lambdas/justhodl-alpha-council"],
            capture_output=True, text=True, timeout=30
        ).stdout.strip()
        fl = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except Exception:
        fl = RUN_START
    ok = False
    for _ in range(55):
        try:
            c = lam.get_function_configuration(
                FunctionName="justhodl-alpha-council")
            lm = datetime.strptime(c["LastModified"].split(".")[0],
                                   "%Y-%m-%dT%H:%M:%S").replace(
                tzinfo=timezone.utc)
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and c.get("State") in (None, "Active") \
                    and lm >= fl:
                ok = True
                break
        except Exception:
            pass
        time.sleep(9)
    if not ok:
        fails.append("deploy floor")
    else:
        lam.invoke(FunctionName="justhodl-alpha-council",
                   InvocationType="RequestResponse", Payload=b"{}")
        d = json.loads(s3.get_object(
            Bucket=B, Key="data/alpha-council.json"
        )["Body"].read())
        cl = d.get("council") or []
        with_tc = [c0 for c0 in cl if c0.get("top_call")]
        r.ok("council=%d · seats with open top_call=%d"
             % (len(cl), len(with_tc)))
        for c0 in cl[:12]:
            tc = c0.get("top_call")
            if tc:
                r.log("%-24s -> %s %s exp=%s%% / %sd (%s, "
                      "conf=%s, %sd old)"
                      % (c0["engine"], tc["symbol"],
                         tc["direction"],
                         tc.get("expected_return_pct"),
                         tc.get("horizon_days"),
                         tc.get("expected_basis"),
                         tc.get("confidence"), tc.get("age_d")))
            else:
                r.log("%-24s -> no open call (disclosed)"
                      % c0["engine"])
        if len(with_tc) < max(10, len(cl) - 2):
            fails.append("only %d/%d seats show a call"
                         % (len(with_tc), len(cl)))
        for c0 in with_tc:
            tc = c0["top_call"]
            okc = tc.get("horizon_days") and (
                tc.get("expected_return_pct") is not None
                or tc.get("expected_basis")
                == "insufficient_history"
                or tc.get("state") == "GRADED")
            if not okc:
                fails.append("%s top_call incomplete: %s"
                             % (c0["engine"],
                                json.dumps(tc,
                                           default=str)[:140]))
    body = ""
    for _ in range(13):
        try:
            body = urllib.request.urlopen(urllib.request.Request(
                "https://justhodl.ai/alpha-council.html",
                headers={"User-Agent": "ops/4341"}),
                timeout=25).read().decode("utf-8", "ignore")
            if "top call" in body:
                break
        except Exception:
            pass
        time.sleep(20)
    for mk in ("top call", "GRADED", "realized_return_pct"):
        if mk not in body:
            fails.append("edge missing %s" % mk)
    if "top call" in body:
        r.ok("page column live (%d bytes)" % len(body))
    if fails:
        for f in fails:
            r.fail("  %s" % f)
        sys.exit(1)
    r.ok("OPS 4341 PASS -- expected return and its clock, on "
         "every proven seat")

# retrigger: graded-fallback + relaxed completeness

# retrigger: label-honest fallback (macro calls display as expressed)
