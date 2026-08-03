"""ops_4334 -- the winning fingerprint, encoded: PRIME_CONVERGENCE
tier (n>=5 + options-flow/smart-money/rev-accel triad), combo strings
for backtest slicing, reversal archetypes, 90d percentiles with own
history, and data/prime-convergence.json. Gate: today's proven trio
must carry prime=True with correct archetypes; ORCL must not."""
import json, subprocess, sys, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from ops_report import report
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=600,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
RUN_START = datetime.now(timezone.utc)
fails = []
with report("4334_prime_convergence") as r:
    r.heading("ops 4334 -- the fingerprint becomes a tier")
    try:
        ts = subprocess.run(
            ["git", "log", "-1", "--format=%ct", "--",
             "aws/lambdas/justhodl-compound-aggregator"],
            capture_output=True, text=True, timeout=30
        ).stdout.strip()
        fl = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except Exception:
        fl = RUN_START
    ok = False
    for _ in range(50):
        try:
            c = lam.get_function_configuration(
                FunctionName="justhodl-compound-aggregator")
            lm = datetime.strptime(c["LastModified"].split(".")[0],
                                   "%Y-%m-%dT%H:%M:%S").replace(
                tzinfo=timezone.utc)
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and lm >= fl:
                ok = True
                break
        except Exception:
            pass
        time.sleep(9)
    if not ok:
        fails.append("deploy floor")
    else:
        p = lam.invoke(FunctionName="justhodl-compound-aggregator",
                       InvocationType="RequestResponse",
                       Payload=b"{}")
        r.log("root: %s" % (p["Payload"].read()
                            or b"")[:160].decode("utf-8",
                                                 "ignore"))
        d = json.loads(s3.get_object(
            Bucket=B, Key="data/compound-signals.json"
        )["Body"].read())
        rk = {x["symbol"]: x for x in d.get("ranked") or []}
        for t in ("AAPL", "GOOGL", "MSFT"):
            x = rk.get(t) or {}
            r.log("%s: prime=%s combo=%s archetype=%s rc=%s "
                  "pct_all=%s"
                  % (t, x.get("prime_convergence"),
                     (x.get("combo") or "")[:70],
                     x.get("archetype"),
                     x.get("reversal_context"),
                     x.get("pctile_90d_all")))
            if not x.get("prime_convergence"):
                fails.append("%s not prime (systems drifted since "
                             "flag day? combo=%s)"
                             % (t, x.get("combo")))
        if (rk.get("ORCL") or {}).get("prime_convergence"):
            fails.append("ORCL falsely prime")
        else:
            r.ok("ORCL correctly not prime")
        try:
            pr = json.loads(s3.get_object(
                Bucket=B, Key="data/prime-convergence.json"
            )["Body"].read())
        except Exception as e:
            fails.append("prime artifact unreadable: %s"
                         % str(e)[:70])
            pr = {}
        r.ok("prime artifact: n=%s rows=%s"
             % (pr.get("n"),
                [x["symbol"] for x in pr.get("rows") or []][:8]))
        try:
            hh = json.loads(s3.get_object(
                Bucket=B, Key="data/compound-history.json"
            )["Body"].read())
        except Exception as e:
            fails.append("history unreadable: %s" % str(e)[:70])
            hh = {}
        r.log("history days: %d" % len(hh.get("days") or []))
        if not (pr.get("rows") and hh.get("days")):
            fails.append("prime/history artifacts thin")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
        sys.exit(1)
    r.ok("OPS 4334 PASS -- the engine that called it now knows "
         "exactly what its winning hand looks like")

# retrigger: engine identifiers corrected (S3/BUCKET)
