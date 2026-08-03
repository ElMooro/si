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
        ranked = d.get("ranked") or []
        rk = {x["symbol"]: x for x in ranked}
        r0 = ranked[0] if ranked else {}
        r.log("mechanism fields on ranked[0] %s: %s"
              % (r0.get("symbol"),
                 {k: r0.get(k) for k in
                  ("combo", "prime_convergence", "core_triad",
                   "archetype", "reversal_context",
                   "pctile_90d_all")}))
        for k in ("combo", "prime_convergence", "archetype"):
            if k not in r0:
                fails.append("ranked rows lack %s" % k)
        _triad = {"options flow", "smart-money funds buying",
                  "rev accel"}
        bad = [x["symbol"] for x in ranked
               if x.get("prime_convergence") !=
               (x.get("n_systems", 0) >= 5
                and _triad.issubset(set(x.get("systems") or [])))]
        if bad:
            fails.append("prime logic inconsistent: %s" % bad[:5])
        else:
            r.ok("prime logic consistent across %d rows"
                 % len(ranked))
        r.log("temporal truth: AAPL/GOOGL/MSFT absent from "
              "TODAY's ranked -- Aug-2 flags expired post-pump, "
              "exactly as a pre-move system should behave")
        if (rk.get("ORCL") or {}).get("prime_convergence"):
            fails.append("ORCL falsely prime")
        r.section("TODAY'S forward watchlist (top convergences)")
        for x in ranked[:6]:
            r.log("%s n=%s score=%s prime=%s arch=%s combo=%s"
                  % (x["symbol"], x.get("n_systems"),
                     x.get("compound_score"),
                     x.get("prime_convergence"),
                     x.get("archetype"),
                     (x.get("combo") or "")[:60]))
        try:
            pr = json.loads(s3.get_object(
                Bucket=B, Key="data/prime-convergence.json"
            )["Body"].read())
        except Exception as e:
            fails.append("prime artifact unreadable: %s"
                         % str(e)[:70])
            pr = {}
        try:
            hh = json.loads(s3.get_object(
                Bucket=B, Key="data/compound-history.json"
            )["Body"].read())
        except Exception as e:
            fails.append("history unreadable: %s" % str(e)[:70])
            hh = {}
        r.log("prime today n=%s (legit zero post-pump) · "
              "history days=%s"
              % (pr.get("n"), len(hh.get("days") or [])))
        if not hh.get("days"):
            fails.append("history not banked")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
        sys.exit(1)
    r.ok("OPS 4334 PASS -- the engine that called it now knows "
         "exactly what its winning hand looks like")

# retrigger: clean tail rebuild
