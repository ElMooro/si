"""ops_4347 -- the learning loop: optimizer learns per-engine x regime
lifts from grades; fabric consumes learned weights, propagates through
the peer graph, archives the bus daily; best-setups stamps fabric
context into every ledger entry. Constants start dying tonight."""
import json, subprocess, sys, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from ops_report import report
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=400,
                                 retries={"max_attempts": 1}))
ev = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
ddb = boto3.resource("dynamodb", region_name="us-east-1")
B = "justhodl-dashboard-live"
ACC = "857687956942"
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


def inv(fn):
    for _t in range(6):
        try:
            lam.invoke(FunctionName=fn,
                       InvocationType="RequestResponse",
                       Payload=b"{}")
            return
        except Exception as _e:
            if "Pending" in str(_e) and _t < 5:
                time.sleep(20)
                continue
            raise
fails = []
with report("4347_learning_loop") as r:
    r.heading("ops 4347 -- constants die; the loop learns")
    if not floor_ok("justhodl-signal-optimizer",
                    "justhodl-signal-optimizer"):
        fails.append("optimizer floor")
    else:
        try:
            ev.put_rule(Name="justhodl-signal-optimizer-cadence",
                        ScheduleExpression="cron(50 23 * * ? *)",
                        State="ENABLED")
            ev.put_targets(
                Rule="justhodl-signal-optimizer-cadence",
                Targets=[{"Id": "1",
                          "Arn": "arn:aws:lambda:us-east-1:"
                                 "%s:function:justhodl-signal-"
                                 "optimizer" % ACC}])
            try:
                lam.add_permission(
                    FunctionName="justhodl-signal-optimizer",
                    StatementId="evb-optimizer",
                    Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",
                    SourceArn="arn:aws:events:us-east-1:%s:rule/"
                              "justhodl-signal-optimizer-cadence"
                              % ACC)
            except lam.exceptions.ResourceConflictException:
                pass
        except Exception as e:
            r.warn("cadence: %s" % str(e)[:60])
        inv("justhodl-signal-optimizer")
        lw = json.loads(s3.get_object(
            Bucket=B, Key="data/learned-weights.json"
        )["Body"].read())
        r.ok("LEARNED: %s engines · %s (engine,regime) pairs"
             % (lw.get("n_engines"),
                lw.get("n_engine_regime_pairs")))
        for m0 in (lw.get("top_regime_movers") or [])[:6]:
            r.log("  %s lift=%+.1f (win %s%%, n=%s)"
                  % (m0["key"], m0["lift"], m0["win"], m0["n"]))
        if (lw.get("n_engines") or 0) < 40 \
                or (lw.get("n_engine_regime_pairs") or 0) < 30:
            fails.append("learned tables thin")
    if not floor_ok("justhodl-signal-fabric",
                    "justhodl-signal-fabric"):
        fails.append("fabric floor")
    else:
        inv("justhodl-signal-fabric")
        bus = json.loads(s3.get_object(
            Bucket=B, Key="data/feature-bus.json")["Body"].read())
        tk = bus.get("tickers") or {}
        peers = [s0 for s0, v in tk.items()
                 if v.get("peer_fabric_score") is not None]
        r.ok("BUS v2.1: %s tickers · %d carry peer_fabric_score"
             % (bus.get("n_tickers"), len(peers)))
        fb = json.loads(s3.get_object(
            Bucket=B, Key="data/signal-fabric.json"
        )["Body"].read())
        learned_hits = 0
        for t2 in (fb.get("tickers") or [])[:80]:
            for e2 in t2["engines"]:
                if str(e2.get("weight_basis", "")
                       ).startswith("learned"):
                    learned_hits += 1
        r.ok("fusion now runs on learned weights: %d envelopes "
             "(sample of 80 tickers)" % learned_hits)
        if learned_hits < 20:
            fails.append("learned weights not flowing")
        akey = ("data/archive/feature-bus/%s.json"
                % RUN_START.strftime("%Y%m%d"))
        s3.head_object(Bucket=B, Key=akey)
        r.ok("bus archived: %s" % akey)
        if len(peers) < 50:
            fails.append("peer graph thin: %d" % len(peers))
    if not floor_ok("justhodl-best-setups",
                    "justhodl-best-setups"):
        fails.append("best-setups floor")
    else:
        try:
            g0 = json.loads(s3.get_object(
                Bucket=B, Key="data/best-setups.json"
            )["Body"].read()).get("generated_at")
        except Exception:
            g0 = None
        lam.invoke(FunctionName="justhodl-best-setups",
                   InvocationType="Event", Payload=b"{}")
        t0 = time.time()
        fresh = False
        while time.time() - t0 < 480:
            time.sleep(25)
            try:
                if json.loads(s3.get_object(
                        Bucket=B, Key="data/best-setups.json"
                )["Body"].read()).get("generated_at") != g0:
                    fresh = True
                    break
            except Exception:
                pass
        if not fresh:
            fails.append("best-setups no refresh")
        else:
            tbl = ddb.Table("justhodl-signals")
            stamped = None
            kw = {}
            scanned = 0
            today = RUN_START.strftime("%Y-%m-%d")
            while scanned < 12000 and not stamped:
                resp = tbl.scan(**kw)
                for it in resp.get("Items", []):
                    scanned += 1
                    md = it.get("metadata")
                    if isinstance(md, dict) \
                            and md.get("engine") == \
                            "best-setups" \
                            and str(it.get("logged_at",
                                           "")).startswith(
                                today) \
                            and "fabric_agreement" in md:
                        stamped = it
                        break
                if "LastEvaluatedKey" not in resp:
                    break
                kw["ExclusiveStartKey"] = \
                    resp["LastEvaluatedKey"]
            if stamped:
                md = stamped["metadata"]
                r.ok("LEDGER STAMPED: %s -- fabric_agreement=%s "
                     "score=%s conflict=%s"
                     % (stamped.get("signal_value"),
                        md.get("fabric_agreement"),
                        md.get("fabric_score"),
                        md.get("fabric_conflict")))
            else:
                fails.append("no fabric-stamped best-setups "
                             "signal found today "
                             "(scanned %d)" % scanned)
    if fails:
        for f in fails:
            r.fail("  %s" % f)
        sys.exit(1)
    r.ok("OPS 4347 PASS -- the platform now learns its own "
         "weights; tomorrow's constants are tonight's grades")

# retrigger: emitter-level stamping (signals_emit shared module)

# retrigger: call-site emitter wrapper (layer module untouched)

# retrigger: positional-symbol discovery in emitter wrapper
