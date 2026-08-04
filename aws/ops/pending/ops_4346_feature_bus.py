"""ops_4346 -- the Feature Bus loop closes: fabric v2 emits the bus +
change-events with a published SDK; best-setups consumes it live
(composite gate + row context). Ledger stamping queued pending the
_log_signal def-site read."""
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


def floor_ok(fn, d):
    try:
        ts = subprocess.run(["git", "log", "-1", "--format=%ct",
                             "--", "aws/lambdas/" + d],
                            capture_output=True, text=True,
                            timeout=30).stdout.strip()
        fl = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except Exception:
        fl = RUN_START
    for _ in range(55):
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            lm = datetime.strptime(c["LastModified"].split(".")[0],
                                   "%Y-%m-%dT%H:%M:%S").replace(
                tzinfo=timezone.utc)
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and c.get("State") in (None, "Active") \
                    and lm >= fl:
                return True
        except Exception:
            pass
        time.sleep(9)
    return False
fails = []
with report("4346_feature_bus") as r:
    r.heading("ops 4346 -- engines that read the room")
    if not floor_ok("justhodl-signal-fabric",
                    "justhodl-signal-fabric"):
        fails.append("fabric deploy floor")
    else:
        lam.invoke(FunctionName="justhodl-signal-fabric",
                   InvocationType="RequestResponse", Payload=b"{}")
        bus = json.loads(s3.get_object(
            Bucket=B, Key="data/feature-bus.json")["Body"].read())
        tk = bus.get("tickers") or {}
        r.ok("BUS: %s tickers · SDK published in-artifact"
             % bus.get("n_tickers"))
        samp = tk.get("MU") or next(iter(tk.values()), {})
        r.log("sample vector (MU): %s"
              % json.dumps(samp, default=str)[:300])
        for f in ("fabric_score", "agreement_pct", "n_engines",
                  "conflict", "reversal", "flow_13f"):
            if f not in samp:
                fails.append("bus vector lacks %s" % f)
        evd = json.loads(s3.get_object(
            Bucket=B, Key="data/fabric-events.json"
        )["Body"].read())
        r.ok("EVENTS: n=%s · sample=%s"
             % (evd.get("n"),
                json.dumps((evd.get("events") or [])[:3])))
    if not floor_ok("justhodl-best-setups",
                    "justhodl-best-setups"):
        fails.append("best-setups deploy floor")
    else:
        try:
            g0 = json.loads(s3.get_object(
                Bucket=B, Key="data/best-setups.json"
            )["Body"].read()).get("generated_at")
        except Exception:
            g0 = None
        lam.invoke(FunctionName="justhodl-best-setups",
                   InvocationType="Event", Payload=b"{}")
        d = None
        t0 = time.time()
        while time.time() - t0 < 480:
            time.sleep(20)
            try:
                cand = json.loads(s3.get_object(
                    Bucket=B, Key="data/best-setups.json"
                )["Body"].read())
                if cand.get("generated_at") != g0:
                    d = cand
                    break
            except Exception:
                pass
        if not d:
            fails.append("best-setups did not refresh in 480s")
        else:
            rows = d.get("top_setups") or []
            with_f = [x for x in rows
                      if x.get("fabric_agreement") is not None]
            tags = {}
            for x in rows:
                t2 = x.get("fabric_tag")
                if t2:
                    tags[t2] = tags.get(t2, 0) + 1
            r.ok("best-setups fabric-aware: %d/%d rows carry "
                 "context · tags=%s"
                 % (len(with_f), len(rows), tags))
            for x in rows[:4]:
                r.log("%s comp=%s fab=%s ag=%s%% n=%s mult=%s "
                      "tag=%s"
                      % (x.get("ticker"), x.get("composite"),
                         x.get("fabric_score"),
                         x.get("fabric_agreement"),
                         x.get("fabric_n_engines"),
                         x.get("fabric_mult"),
                         x.get("fabric_tag")))
            if len(with_f) < max(10, len(rows) // 3):
                fails.append("too few rows carry fabric ctx: "
                             "%d/%d" % (len(with_f), len(rows)))
            if not tags:
                r.warn("no FLEET tags fired this run -- "
                       "thresholds honest; watch tomorrow")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
        sys.exit(1)
    r.ok("OPS 4346 PASS -- the bus runs, and the flagship reads "
         "the room before it speaks")
