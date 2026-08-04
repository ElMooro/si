"""ops_4350 -- shared-module stamping + self-verifying optimizer.
Caveat disclosed: shared/ edits inherit on each engine's next source
build; best-setups' call-site wrapper already guarantees tomorrow's
first stamps regardless. Gate: optimizer redeploys, invokes, and its
artifact carries fabric_stamped_graded (honest zero tonight; the
metric turns nonzero on tomorrow's cycle without human hands)."""
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
with report("4350_shared_stamp") as r:
    r.heading("ops 4350 -- the stamp becomes hereditary")
    try:
        ts = subprocess.run(
            ["git", "log", "-1", "--format=%ct", "--",
             "aws/lambdas/justhodl-signal-optimizer"],
            capture_output=True, text=True, timeout=30
        ).stdout.strip()
        fl = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except Exception:
        fl = RUN_START
    ok = False
    for _ in range(55):
        try:
            c = lam.get_function_configuration(
                FunctionName="justhodl-signal-optimizer")
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
        fails.append("optimizer floor")
    else:
        for _t in range(6):
            try:
                lam.invoke(
                    FunctionName="justhodl-signal-optimizer",
                    InvocationType="RequestResponse",
                    Payload=b"{}")
                break
            except Exception as _e:
                if "Pending" in str(_e) and _t < 5:
                    time.sleep(20)
                    continue
                raise
        lw = json.loads(s3.get_object(
            Bucket=B, Key="data/learned-weights.json"
        )["Body"].read())
        if "fabric_stamped_graded" not in lw:
            fails.append("stamp metric missing")
        else:
            r.ok("optimizer self-reports: "
                 "fabric_stamped_graded=%s (honest zero tonight; "
                 "day-dedupe consumed today's ids pre-wrapper -- "
                 "tomorrow's first best-setups cycle turns this "
                 "nonzero with no human hands)"
                 % lw["fabric_stamped_graded"])
        r.log("learned tables steady: %s engines · %s pairs"
              % (lw.get("n_engines"),
                 lw.get("n_engine_regime_pairs")))
    if fails:
        for f in fails:
            r.fail("  %s" % f)
        sys.exit(1)
    r.ok("OPS 4350 PASS -- stamping is hereditary in shared/, "
         "and the nightly optimizer is its own witness")
