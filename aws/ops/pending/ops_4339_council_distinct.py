"""ops_4339 -- council v1.1 seal: consensus rows must carry DISTINCT
engines only (council_n == unique engines >= 2)."""
import json, subprocess, sys, time
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
with report("4339_council_distinct") as r:
    r.heading("ops 4339 -- one engine, one vote")
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
        cc = d.get("consensus_calls") or []
        r.ok("council=%s · consensus=%s (distinct-engine rule)"
             % (d.get("n_council"), len(cc)))
        for x in cc[:8]:
            names = [e0["engine"] for e0 in x["engines"]]
            r.log("%s %s · n=%s · %s"
                  % (x["symbol"], x["direction"],
                     x["council_n"], names))
            if len(set(names)) != x["council_n"] \
                    or x["council_n"] < 2:
                fails.append("dup/thin consensus on %s"
                             % x["symbol"])
        if not cc:
            r.warn("honest zero: no 2-distinct-engine overlap "
                   "among open signals right now")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
        sys.exit(1)
    r.ok("OPS 4339 PASS -- independence enforced; the council's "
         "word now means what it claims")
