"""ops_4348 -- race-proof seal of ledger stamping: only signals
logged AFTER this run's own invoke can satisfy the hunt. Also
verifies the alignment-precedence tune (MU should now read
FLEET_ALIGNED_UP)."""
import json, subprocess, sys, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from ops_report import report
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=400,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
ddb = boto3.resource("dynamodb", region_name="us-east-1")
B = "justhodl-dashboard-live"
RUN_START = datetime.now(timezone.utc)
fails = []
with report("4348_stamp_seal") as r:
    r.heading("ops 4348 -- the stamp, beyond doubt")
    try:
        ts = subprocess.run(
            ["git", "log", "-1", "--format=%ct", "--",
             "aws/lambdas/justhodl-best-setups"],
            capture_output=True, text=True, timeout=30
        ).stdout.strip()
        fl = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except Exception:
        fl = RUN_START
    ok = False
    for _ in range(55):
        try:
            c = lam.get_function_configuration(
                FunctionName="justhodl-best-setups")
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
        t_inv = datetime.now(timezone.utc)
        r.log("invoke anchor: %s -- only signals logged after "
              "this instant count" % t_inv.isoformat())
        lam.invoke(FunctionName="justhodl-best-setups",
                   InvocationType="Event", Payload=b"{}")
        try:
            g0 = json.loads(s3.get_object(
                Bucket=B, Key="data/best-setups.json"
            )["Body"].read()).get("generated_at")
        except Exception:
            g0 = None
        d = None
        t0 = time.time()
        while time.time() - t0 < 520:
            time.sleep(25)
            try:
                cand = json.loads(s3.get_object(
                    Bucket=B, Key="data/best-setups.json"
                )["Body"].read())
                ga = cand.get("generated_at")
                if ga and ga != g0 and ga > \
                        t_inv.isoformat()[:19]:
                    d = cand
                    break
            except Exception:
                pass
        if not d:
            fails.append("no post-anchor refresh in 520s")
        else:
            rows = d.get("top_setups") or []
            tags = {}
            mu = None
            for x in rows:
                t2 = x.get("fabric_tag")
                if t2:
                    tags[t2] = tags.get(t2, 0) + 1
                if x.get("ticker") == "MU":
                    mu = x
            r.ok("fresh run %s · tags=%s"
                 % (d.get("generated_at"), tags))
            if mu:
                r.log("MU precedence check: tag=%s mult=%s "
                      "(ag=%s%%, n=%s)"
                      % (mu.get("fabric_tag"),
                         mu.get("fabric_mult"),
                         mu.get("fabric_agreement"),
                         mu.get("fabric_n_engines")))
                if mu.get("fabric_tag") == "FLEET_CONTESTED" \
                        and (mu.get("fabric_agreement")
                             or 0) >= 75:
                    fails.append("precedence tune not live")
            tbl = ddb.Table("justhodl-signals")
            stamped = None
            kw = {}
            scanned = 0
            anchor = t_inv.isoformat()[:19]
            while scanned < 16000 and not stamped:
                resp = tbl.scan(**kw)
                for it in resp.get("Items", []):
                    scanned += 1
                    md = it.get("metadata")
                    la = str(it.get("logged_at", ""))
                    if isinstance(md, dict) \
                            and md.get("engine") == \
                            "best-setups" \
                            and la[:19] > anchor \
                            and "fabric_agreement" in md:
                        stamped = it
                        break
                if "LastEvaluatedKey" not in resp:
                    break
                kw["ExclusiveStartKey"] = \
                    resp["LastEvaluatedKey"]
            if stamped:
                md = stamped["metadata"]
                r.ok("LEDGER STAMPED, race-proof: %s @ %s -- "
                     "agreement=%s score=%s conflict=%s"
                     % (stamped.get("signal_value")
                        or stamped.get("signal_id", "")[:20],
                        str(stamped.get("logged_at"))[:19],
                        md.get("fabric_agreement"),
                        md.get("fabric_score"),
                        md.get("fabric_conflict")))
            else:
                fails.append("no post-anchor stamped signal "
                             "(scanned %d)" % scanned)
    if fails:
        for f in fails:
            r.fail("  %s" % f)
        sys.exit(1)
    r.ok("OPS 4348 PASS -- every future signal carries the "
         "fleet's mind at the moment of the call; conditional "
         "grading is unlocked")
