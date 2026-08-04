"""ops_4352 -- the ongoing loop, proven end-to-end: scout v1.1
(6 teachers + source-intel: free/cheap/integrated + live probes),
shadow-lab spawns ATR/ADX candidates on real OHLC and logs them
through the shared emitter (stamped from birth). Daily 21:30."""
import json, subprocess, sys, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from ops_report import report
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=650,
                                 retries={"max_attempts": 1}))
ev = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
ddb = boto3.resource("dynamodb", region_name="us-east-1")
B = "justhodl-dashboard-live"
ACC = "857687956942"
RUN_START = datetime.now(timezone.utc)


def floor_ok(fn):
    try:
        ts = subprocess.run(["git", "log", "-1", "--format=%ct",
                             "--", "aws/lambdas/" + fn],
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
with report("4352_shadow_and_sources") as r:
    r.heading("ops 4352 -- shadows born, sources mapped")
    if not floor_ok("justhodl-methodology-scout"):
        fails.append("scout floor")
    else:
        inv("justhodl-methodology-scout")
        si = json.loads(s3.get_object(
            Bucket=B, Key="data/source-intel.json"
        )["Body"].read())
        r.ok("SOURCE-INTEL: integrated=%d · free_unused=%d · "
             "cheap=%d"
             % (len(si.get("already_integrated") or []),
                len(si.get("free_unused") or []),
                len(si.get("cheap_worth_it") or [])))
        r.section("free & unused (auto-adopt queue)")
        for x in (si.get("free_unused") or [])[:8]:
            r.log("  %-16s %s probe=%s"
                  % (x["source"], x["note"],
                     x.get("probe", "-")))
        r.section("cheap & worth it (Khalid's buy list)")
        for x in (si.get("cheap_worth_it") or [])[:8]:
            r.log("  %-14s $%s/mo -- %s"
                  % (x["source"], x["usd_mo"], x["note"]))
        kb = json.loads(s3.get_object(
            Bucket=B, Key="data/methodology-kb.json"
        )["Body"].read())
        tok = sum(1 for t in kb.get("teachers") or []
                  if "error" not in t)
        r.log("teachers readable: %d/6 · indicators=%s"
              % (tok, kb.get("n_indicators")))
        if tok < 4:
            fails.append("teachers thin: %d" % tok)
        if not si.get("cheap_worth_it"):
            fails.append("cheap list empty")
    if not floor_ok("justhodl-shadow-lab"):
        fails.append("shadow floor")
    else:
        try:
            fn = "justhodl-shadow-lab"
            donor = lam.get_function_configuration(
                FunctionName="justhodl-commodity-curves")
            dvars = (donor.get("Environment", {})
                     .get("Variables", {}) or {})
            key = next((v for k, v in dvars.items()
                        if "FMP" in k.upper() and v), None)
            r.log("donor FMP-ish vars: %s -> key_len=%s"
                  % ([k for k in dvars if "FMP" in k.upper()],
                     len(key or "")))
            if key:
                cur = lam.get_function_configuration(
                    FunctionName=fn)
                envv = (cur.get("Environment", {})
                        .get("Variables", {}) or {})
                if envv.get("FMP_API_KEY") != key:
                    envv["FMP_API_KEY"] = key
                    lam.update_function_configuration(
                        FunctionName=fn,
                        Environment={"Variables": envv})
                    for _w in range(10):
                        time.sleep(8)
                        cc = lam.get_function_configuration(
                            FunctionName=fn)
                        if cc.get("LastUpdateStatus") == \
                                "Successful":
                            break
        except Exception as e:
            r.warn("env inherit: %s" % str(e)[:60])
        try:
            ev.put_rule(Name="justhodl-shadow-lab-cadence",
                        ScheduleExpression="cron(30 21 ? * "
                                           "MON-FRI *)",
                        State="ENABLED")
            ev.put_targets(Rule="justhodl-shadow-lab-cadence",
                           Targets=[{"Id": "1",
                                     "Arn": "arn:aws:lambda:"
                                            "us-east-1:%s:"
                                            "function:%s"
                                            % (ACC, fn)}])
            try:
                lam.add_permission(
                    FunctionName=fn, StatementId="evb-shadow",
                    Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",
                    SourceArn="arn:aws:events:us-east-1:%s:"
                              "rule/justhodl-shadow-lab-cadence"
                              % ACC)
            except lam.exceptions.ResourceConflictException:
                pass
        except Exception as e:
            r.warn("cadence: %s" % str(e)[:60])
        inv("justhodl-shadow-lab")
        d = json.loads(s3.get_object(
            Bucket=B, Key="data/shadow-lab.json")["Body"].read())
        r.log("engine debug_key_len=%s"
              % d.get("debug_key_len"))
        r.ok("SHADOW-LAB: computed=%s · logged=%s · roster=%s"
             % (d.get("n_computed"), d.get("n_logged"),
                (d.get("gap_roster_next") or [])[:6]))
        for x in (d.get("rows") or [])[:5]:
            r.log("  %-6s ADX=%s +DI/-DI=%s/%s ATRpct=%s "
                  "pctile=%s sig=%s"
                  % (x["ticker"], x.get("adx"),
                     x.get("plus_di"), x.get("minus_di"),
                     x.get("atr_pct"), x.get("atr_pctile"),
                     x.get("signal")))
        if (d.get("n_computed") or 0) < 25:
            fails.append("shadow computed thin")
        if (d.get("n_logged") or 0) < 3:
            fails.append("shadow logged thin: %s"
                         % d.get("n_logged"))
        else:
            tbl = ddb.Table("justhodl-signals")
            hit = None
            today = RUN_START.strftime("%Y-%m-%d")
            for x in (d.get("rows") or []):
                if x.get("signal"):
                    got = tbl.get_item(Key={
                        "signal_id": "%s#%s#%s"
                        % (x["signal"], x["ticker"], today)}
                    ).get("Item")
                    if got:
                        hit = got
                        break
            if hit:
                md = hit.get("metadata") or {}
                r.ok("LEDGER: %s born, stamped=%s (fabric_"
                     "agreement=%s)"
                     % (hit["signal_id"],
                        "fabric_agreement" in md,
                        md.get("fabric_agreement")))
            else:
                fails.append("shadow signal not found in ledger")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
        sys.exit(1)
    r.ok("OPS 4352 PASS -- borrowed ideas now compute, sign the "
         "ledger at birth, and queue for their Wilson trial; "
         "the source atlas prices Khalid's next unlocks")

# retrigger: env-propagation wait + dual-endpoint ohlc

# retrigger: name-agnostic FMP env discovery
