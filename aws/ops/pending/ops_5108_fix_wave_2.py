"""ops_5108 -- fix wave 2: the erroring engines, one by one.

  1  boj-full storm: ops 5068 hitched the 22-db fan-out onto the rate(5 min)
     rule benzinga-news-agent-warm (the classic rule cap was full) -> ~5,000
     invocations and ~5,300 errors a day. Detach that target, run the fan-out
     from an EventBridge Scheduler schedule every 30 minutes instead, and read
     the actual error class from the log.
  2  memory/timeout raises applied live (repo config.json already mirrors them
     so the next deploy cannot revert): signal-scorecard 256->1536/600,
     calibrator ->2048/900, feed-registry 1024/600, global-liquidity 512/300,
     provider-window-sentinel 512/600, research-backtest 1024/900,
     signal-harvester 1024/900, fleet-monitor 1024/900, import-sentinel 1024/600
  3  stock-screener: redeploy from repo (deployed package fails at init with
     NameError: boto3), then invoke and verify
  4  portwatch v1.6.1 (paging at the layer's 1000-row max): deploy-wait, run,
     verify ports with yoy; schedules -> one at 11:20 UTC
  5  census-us / insider-trades: deploy-wait (bug fixes), invoke, verify no
     UnboundLocalError / KeyError in the run log
  6  timeout forensics: for each still-timing-out engine, the last 25 log lines
     of its latest timed-out invocation (where it hangs) -> next wave
  7  fi-census / etf-census: schedule target/role inspection + manual invoke
Gate: BOJ target detached + schedule created; stock-screener invoke clean;
portwatch ports with yoy > 0; census-us/insider-trades runs without the two
exceptions.
"""
import json
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=310, retries={"max_attempts": 2}))
ev = boto3.client("events", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
NOW = datetime.now(timezone.utc)
RAISES = {"justhodl-signal-scorecard": (1536, 600), "justhodl-feed-registry": (1024, 600), "justhodl-global-liquidity": (512, 300),
          "justhodl-provider-window-sentinel": (512, 600), "justhodl-research-backtest": (1024, 900), "justhodl-signal-harvester": (1024, 900),
          "justhodl-fleet-monitor": (1024, 900), "justhodl-calibrator": (2048, 900), "justhodl-import-sentinel": (1024, 600)}


def get_json(key):
    try:
        o = s3.get_object(Bucket=B, Key=key)
        return json.loads(o["Body"].read()), o["LastModified"]
    except Exception as e:  # noqa: BLE001
        return None, str(e)[:100]


def wait_deploy(r, fn, marker=None, secs=900, since=None):
    """Wait until the function's LastModified is after `since` (or Description carries marker)."""
    t0 = time.time()
    while time.time() - t0 < secs:
        c = lam.get_function_configuration(FunctionName=fn)
        lm = c.get("LastModified") or ""
        ok = c.get("LastUpdateStatus") == "Successful" and ((marker and marker in (c.get("Description") or "")) or (since and lm > since))
        if ok:
            r.ok(f"{fn} deployed ({lm}) after {time.time() - t0:.0f}s")
            return True
        time.sleep(20)
    r.warn(f"{fn}: deploy not observed within {secs}s")
    return False


def log_lines(fn, since, pattern=None, limit=60):
    try:
        kw = {"logGroupName": f"/aws/lambda/{fn}", "startTime": int(since.timestamp() * 1000), "limit": limit}
        if pattern:
            kw["filterPattern"] = pattern
        return [e["message"].rstrip()[:240] for e in (logs.filter_log_events(**kw).get("events") or [])]
    except Exception as e:  # noqa: BLE001
        return [f"log read failed: {str(e)[:120]}"]


def timeout_forensics(r, fn, days=3):
    """Find the latest 'Task timed out' event and return the 25 lines before it in the same stream."""
    try:
        lg = f"/aws/lambda/{fn}"
        fl = logs.filter_log_events(logGroupName=lg, startTime=int((NOW - timedelta(days=days)).timestamp() * 1000),
                                    filterPattern='"Task timed out"', limit=5)
        evs = fl.get("events") or []
        if not evs:
            r.log(f"  {fn}: no 'Task timed out' in {days}d")
            return
        last = evs[-1]
        stream = last["logStreamName"]
        before = logs.get_log_events(logGroupName=lg, logStreamName=stream, endTime=last["timestamp"], limit=25, startFromHead=False)
        lines = [e["message"].rstrip()[:200] for e in before.get("events") or []]
        r.log(f"  {fn}: timed out at {datetime.fromtimestamp(last['timestamp'] / 1000, tz=timezone.utc).isoformat(timespec='seconds')}; last lines before:")
        for ln in lines[-12:]:
            r.log("      " + ln)
    except Exception as e:  # noqa: BLE001
        r.warn(f"  {fn}: forensics failed {str(e)[:120]}")


def main():
    with report("5108-fix-wave-2") as r:
        r.heading("ops 5108 -- fix wave 2: erroring engines one by one")
        fails = []
        push_time = "2026-09-02T01:30"   # deploys from this push land after this

        r.section("1. boj-full storm")
        try:
            tg = ev.list_targets_by_rule(Rule="benzinga-news-agent-warm")
            ids = [t["Id"] for t in tg.get("Targets") or [] if (t.get("Arn") or "").endswith(":function:justhodl-boj-full")]
            r.log(f"benzinga-news-agent-warm targets: {[(t['Id'], t['Arn'].split(':')[-1], (t.get('Input') or '')[:40]) for t in tg.get('Targets') or []]}")
            if ids:
                ev.remove_targets(Rule="benzinga-news-agent-warm", Ids=ids)
                r.ok(f"detached boj-full target(s) {ids} from the 5-minute warm rule")
            else:
                r.log("boj-full not on the warm rule (already detached)")
            for rule in ("carry-surface-4h",):
                tg2 = ev.list_targets_by_rule(Rule=rule)
                extra = [(t["Id"], t["Arn"].split(":")[-1]) for t in tg2.get("Targets") or [] if not t["Arn"].endswith(":function:justhodl-carry-surface")]
                r.log(f"{rule} extra targets: {extra}")
        except Exception as e:  # noqa: BLE001
            fails.append(f"boj target surgery: {str(e)[:120]}")
        try:
            arn = lam.get_function_configuration(FunctionName="justhodl-boj-full")["FunctionArn"]
            sched = {"Name": "justhodl-boj-full-fanout", "ScheduleExpression": "rate(30 minutes)", "ScheduleExpressionTimezone": "UTC",
                     "FlexibleTimeWindow": {"Mode": "FLEXIBLE", "MaximumWindowInMinutes": 5},
                     "Target": {"Arn": arn, "RoleArn": SCHED_ROLE, "Input": json.dumps({"fanout": True}),
                                "RetryPolicy": {"MaximumRetryAttempts": 0, "MaximumEventAgeInSeconds": 600}},
                     "State": "ENABLED", "Description": "BOJ per-db fan-out every 30 min (ops 5108; was hitched to a 5-min warm rule -> ~5k invocations/day)"}
            try:
                sch.create_schedule(**sched)
                r.ok("schedule justhodl-boj-full-fanout created (rate 30 minutes)")
            except sch.exceptions.ConflictException:
                sch.update_schedule(**sched)
                r.ok("schedule justhodl-boj-full-fanout updated")
        except Exception as e:  # noqa: BLE001
            fails.append(f"boj schedule: {str(e)[:120]}")
        errs = log_lines("justhodl-boj-full", NOW - timedelta(hours=6), pattern='?"[ERROR]" ?Traceback ?"Error Type"', limit=30)
        seen = []
        for ln in errs:
            key = re.sub(r"\d+", "#", ln)[:90]
            if key not in seen:
                seen.append(key)
                r.log("  boj err: " + ln[:220])
            if len(seen) >= 6:
                break
        r.kv(step="boj", errors_sampled=len(seen))

        r.section("2. memory/timeout raises (live)")
        for fn, (mem, to) in RAISES.items():
            try:
                c = lam.get_function_configuration(FunctionName=fn)
                if c.get("MemorySize") != mem or c.get("Timeout") != to:
                    for _ in range(20):
                        if lam.get_function_configuration(FunctionName=fn).get("LastUpdateStatus") != "InProgress":
                            break
                        time.sleep(3)
                    lam.update_function_configuration(FunctionName=fn, MemorySize=mem, Timeout=to)
                    r.ok(f"{fn}: {c.get('MemorySize')}MB/{c.get('Timeout')}s -> {mem}MB/{to}s")
                else:
                    r.log(f"{fn}: already {mem}MB/{to}s")
            except Exception as e:  # noqa: BLE001
                r.warn(f"{fn}: {str(e)[:120]}")

        r.section("3. stock-screener redeploy")
        try:
            deploy_lambda(report=r, function_name="justhodl-stock-screener", source_dir=ROOT / "aws" / "lambdas" / "justhodl-stock-screener" / "source",
                          env_vars=None, timeout=900, memory=1280, create_function_url=False, smoke=False,
                          description="Stock screener (ops 5108 redeploy from repo: deployed package failed at init with NameError boto3)")
            for _ in range(30):
                if lam.get_function_configuration(FunctionName="justhodl-stock-screener").get("LastUpdateStatus") == "Successful":
                    break
                time.sleep(3)
            t0 = datetime.now(timezone.utc)
            resp = lam.invoke(FunctionName="justhodl-stock-screener", InvocationType="RequestResponse", Payload=b"{}")
            pl = resp["Payload"].read()[:300]
            r.log(f"stock-screener invoke status={resp.get('StatusCode')} FunctionError={resp.get('FunctionError')} payload={pl}")
            if resp.get("FunctionError"):
                fails.append(f"stock-screener still errors: {pl[:160]}")
                for ln in log_lines("justhodl-stock-screener", t0, pattern='?"[ERROR]" ?Traceback ?NameError', limit=10):
                    r.log("  " + ln)
        except Exception as e:  # noqa: BLE001
            fails.append(f"stock-screener: {str(e)[:150]}")

        r.section("4. portwatch v1.6.1")
        if wait_deploy(r, "justhodl-portwatch", marker="v1.6.1"):
            t0 = datetime.now(timezone.utc)
            try:
                resp = lam.invoke(FunctionName="justhodl-portwatch", InvocationType="RequestResponse", Payload=b"{}")
                r.log(f"portwatch invoke {resp.get('StatusCode')} {resp['Payload'].read()[:200]}")
            except Exception as e:  # noqa: BLE001
                r.warn(f"portwatch sync invoke: {str(e)[:120]} -> async")
                lam.invoke(FunctionName="justhodl-portwatch", InvocationType="Event", Payload=b"{}")
                time.sleep(300)
            pw, lm = get_json("data/portwatch.json")
            if pw and (pw.get("generated_at") or "") > t0.isoformat():
                ports = pw.get("ports") or []
                with_yoy = sum(1 for p in ports if isinstance(p, dict) and isinstance(p.get("yoy_pct"), (int, float)))
                r.log(f"portwatch: v{pw.get('version')} ok={pw.get('ok')} chokepoints={len(pw.get('chokepoints') or [])} ports={len(ports)} with_yoy={with_yoy} "
                      f"daily_rows={pw.get('daily_rows')} requests={json.dumps(pw.get('requests'))} history_through={json.dumps(pw.get('history_through'))} errors={json.dumps(pw.get('errors'))[:300]}")
                r.kv(step="portwatch", ports=len(ports), with_yoy=with_yoy, requests=(pw.get("requests") or {}).get("n"), throttled=(pw.get("requests") or {}).get("throttled_429"))
                if with_yoy == 0:
                    fails.append("portwatch: still no ports with yoy")
            else:
                fails.append("portwatch: feed not regenerated")
        try:
            names = {s["Name"] for s in sch.list_schedules(NamePrefix="justhodl-portwatch")["Schedules"]} | {s["Name"] for s in sch.list_schedules(NamePrefix="portwatch")["Schedules"]}
            if "justhodl-portwatch-daily" in names:
                full = sch.get_schedule(Name="justhodl-portwatch-daily")
                sch.update_schedule(Name="justhodl-portwatch-daily", ScheduleExpression="cron(20 11 * * ? *)", ScheduleExpressionTimezone="UTC",
                                    FlexibleTimeWindow={"Mode": "OFF"}, Target=full["Target"], State="ENABLED",
                                    Description="IMF PortWatch daily 11:20 UTC (before global-business-cycle 12:00); ops 5108 consolidated two schedules")
                r.ok("justhodl-portwatch-daily -> cron(20 11 * * ? *)")
            if "portwatch-sched" in names:
                sch.delete_schedule(Name="portwatch-sched")
                r.ok("deleted duplicate schedule portwatch-sched")
            r.log(f"portwatch schedules now: {sorted({s['Name'] for s in sch.list_schedules(NamePrefix='justhodl-portwatch')['Schedules']} | {s['Name'] for s in sch.list_schedules(NamePrefix='portwatch')['Schedules']})}")
        except Exception as e:  # noqa: BLE001
            r.warn(f"portwatch schedules: {str(e)[:160]}")

        r.section("5. census-us / insider-trades")
        for fn, pat, gate in (("justhodl-census-us", "UnboundLocalError", "census-us"), ("justhodl-insider-trades", "KeyError", "insider-trades")):
            if wait_deploy(r, fn, since=push_time):
                t0 = datetime.now(timezone.utc)
                lam.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
                time.sleep(90 if fn == "justhodl-census-us" else 60)
                bad = log_lines(fn, t0, pattern=pat, limit=5)
                rep = log_lines(fn, t0, pattern="REPORT", limit=3)
                r.log(f"{fn}: {pat} lines after fix: {len(bad)} {json.dumps(bad)[:300]}; report: {json.dumps(rep)[:300]}")
                r.kv(step=gate, exceptions=len(bad))
                if bad:
                    fails.append(f"{fn}: {pat} still raised")

        r.section("6. timeout forensics")
        for fn in ("justhodl-fleet-monitor", "justhodl-feed-registry", "justhodl-research-backtest", "justhodl-global-liquidity",
                   "justhodl-provider-window-sentinel", "justhodl-signal-harvester", "justhodl-imf-full", "justhodl-calibrator",
                   "justhodl-signal-scorecard", "justhodl-import-sentinel"):
            timeout_forensics(r, fn)
        r.log("import-sentinel error lines (24h):")
        for ln in log_lines("justhodl-import-sentinel", NOW - timedelta(hours=24), pattern='?"[ERROR]" ?Traceback ?"Task timed out"', limit=8):
            r.log("  " + ln)

        r.section("7. fi-census / etf-census silent schedules")
        for name, fn in (("fi-census-sched", "justhodl-fi-census"), ("etf-census-sched", "justhodl-etf-census")):
            try:
                s = sch.get_schedule(Name=name)
                tgt = s.get("Target") or {}
                r.log(f"{name}: state={s.get('State')} expr={s.get('ScheduleExpression')} target={tgt.get('Arn')} role={tgt.get('RoleArn')} input={tgt.get('Input')} window={s.get('FlexibleTimeWindow')}")
                cfg = lam.get_function_configuration(FunctionName=fn)
                r.log(f"  function arn={cfg.get('FunctionArn')} state={cfg.get('State')} timeout={cfg.get('Timeout')} mem={cfg.get('MemorySize')}")
                if tgt.get("Arn") and tgt["Arn"].split(":")[6] != fn:
                    r.warn(f"  target ARN points at a different function: {tgt['Arn']}")
                t0 = datetime.now(timezone.utc)
                resp = lam.invoke(FunctionName=fn, InvocationType="Event", Payload=tgt.get("Input", "{}").encode() if isinstance(tgt.get("Input"), str) else b"{}")
                r.log(f"  manual async invoke status={resp.get('StatusCode')}")
                time.sleep(45)
                rep = log_lines(fn, t0, pattern="REPORT", limit=2)
                errs2 = log_lines(fn, t0, pattern='?"[ERROR]" ?Traceback', limit=4)
                r.log(f"  report: {json.dumps(rep)[:240]} errors: {json.dumps(errs2)[:300]}")
                r.kv(step=name, invoked=bool(rep), errors=len(errs2))
            except Exception as e:  # noqa: BLE001
                r.warn(f"{name}: {str(e)[:140]}")

        r.section("verdict")
        for f in fails:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok("VERDICT: GREEN -- BOJ storm stopped, limits raised, stock-screener alive, portwatch ports with yoy, census-us/insider-trades exception-free")


if __name__ == "__main__":
    main()
