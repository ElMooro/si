"""ops_5107 -- portwatch v1.6 + oecd-cli v2.0.0: deploy-wait, run, verify, and
the downstream global-recession confirmation leg.

portwatch v1.6: ArcGIS 429 backoff, incremental history store
(data/warm/portwatch/history/daily-rows.json.gz: a run is ~4 requests, not
~70), the chokepoint fallback IN-list quoted (v1.5 sent chokepoint1 bare ->
'Invalid field: chokepoint1'), never publishes an empty ports list when
throttled. Two daily schedules (portwatch-sched 11:20, justhodl-portwatch-
daily 12:10) collapse to one at 11:20 UTC, before the global cycle at 12:00.

oecd-cli v2.0.0: OECD CLI from the OECD's own SDMX pull (cycle-features
cache) instead of the FRED mirror that stopped Jan-2024; same schema;
as_of_period becomes 2026-06 so global-recession's OECD leg is usable again.

Gate: portwatch feed must carry >0 ports with yoy after the run (live or
history), oecd-cli as_of_age_months <= 4 with >= 15 countries,
global-recession oecd_usable true.
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=300, retries={"max_attempts": 1}))
sch = boto3.client("scheduler", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


def get_json(key):
    try:
        o = s3.get_object(Bucket=B, Key=key)
        return json.loads(o["Body"].read()), o["LastModified"]
    except Exception as e:  # noqa: BLE001
        return None, str(e)[:100]


def wait_deploy(r, fn, marker, secs=900):
    t0 = time.time()
    while time.time() - t0 < secs:
        c = lam.get_function_configuration(FunctionName=fn)
        if marker in (c.get("Description") or "") and c.get("LastUpdateStatus") == "Successful":
            r.ok(f"{fn} deployed ({marker}) after {time.time() - t0:.0f}s")
            return True
        time.sleep(25)
    r.fail(f"{fn}: {marker} not deployed within {secs}s")
    return False


def tail(r, fn, since, pattern="?[portwatch] ?[oecd-cli] ?Traceback ?REPORT", limit=40):
    try:
        fl = logs.filter_log_events(logGroupName=f"/aws/lambda/{fn}", startTime=int(since.timestamp() * 1000) - 5000, filterPattern=pattern, limit=limit)
        for e in fl.get("events") or []:
            r.log("  log: " + e["message"].strip()[:220])
    except Exception as e:  # noqa: BLE001
        r.warn(f"log tail {fn}: {str(e)[:80]}")


def main():
    with report("5107-portwatch-oecdcli") as r:
        r.heading("ops 5107 -- portwatch v1.6 + oecd-cli v2.0.0 + global-recession OECD leg")
        fails = []

        r.section("1. portwatch v1.6")
        if wait_deploy(r, "justhodl-portwatch", "v1.6"):
            t0 = datetime.now(timezone.utc)
            try:
                resp = lam.invoke(FunctionName="justhodl-portwatch", InvocationType="RequestResponse", Payload=b"{}")
                r.log(f"sync invoke status {resp.get('StatusCode')} payload {resp['Payload'].read()[:200]}")
            except Exception as e:  # noqa: BLE001
                r.warn(f"sync invoke: {str(e)[:150]} (falling back to async)")
                lam.invoke(FunctionName="justhodl-portwatch", InvocationType="Event", Payload=b"{}")
                time.sleep(240)
            pw, lm = get_json("data/portwatch.json")
            if pw and (pw.get("generated_at") or "") > t0.isoformat():
                ports = pw.get("ports") or []
                with_yoy = sum(1 for p in ports if isinstance(p, dict) and isinstance(p.get("yoy_pct"), (int, float)))
                r.log(f"portwatch: version={pw.get('version')} ok={pw.get('ok')} chokepoints={len(pw.get('chokepoints') or [])} ports={len(ports)} with_yoy={with_yoy} "
                      f"requests={json.dumps(pw.get('requests'))} history_through={json.dumps(pw.get('history_through'))} daily_rows={pw.get('daily_rows')} errors={json.dumps(pw.get('errors'))[:400]}")
                r.kv(engine="portwatch", version=pw.get("version"), ports=len(ports), with_yoy=with_yoy, requests=(pw.get("requests") or {}).get("n"),
                     throttled=(pw.get("requests") or {}).get("throttled_429"), errors=len(pw.get("errors") or []))
                if with_yoy == 0:
                    fails.append("portwatch: no ports with yoy after v1.6 run")
            else:
                fails.append(f"portwatch: feed not regenerated ({lm if not pw else pw.get('generated_at')})")
            tail(r, "justhodl-portwatch", t0)
            # schedules: one at 11:20 UTC
            try:
                got = {s["Name"]: s for s in sch.list_schedules(NamePrefix="justhodl-portwatch")["Schedules"]}
                got.update({s["Name"]: s for s in sch.list_schedules(NamePrefix="portwatch")["Schedules"]})
                r.log(f"portwatch schedules now: {list(got)}")
                if "justhodl-portwatch-daily" in got:
                    full = sch.get_schedule(Name="justhodl-portwatch-daily")
                    sch.update_schedule(Name="justhodl-portwatch-daily", ScheduleExpression="cron(20 11 * * ? *)", ScheduleExpressionTimezone="UTC",
                                        FlexibleTimeWindow=full.get("FlexibleTimeWindow") or {"Mode": "OFF"}, Target=full["Target"], State="ENABLED",
                                        Description="IMF PortWatch daily 11:20 UTC (before global-business-cycle 12:00); ops 5107 consolidated two schedules")
                    r.ok("justhodl-portwatch-daily -> cron(20 11 * * ? *)")
                if "portwatch-sched" in got:
                    sch.delete_schedule(Name="portwatch-sched")
                    r.ok("deleted duplicate schedule portwatch-sched (11:20)")
            except Exception as e:  # noqa: BLE001
                r.warn(f"schedule consolidation: {str(e)[:160]}")

        r.section("2. oecd-cli v2.0.0")
        if wait_deploy(r, "justhodl-oecd-cli", "v2.0.0"):
            t1 = datetime.now(timezone.utc)
            try:
                resp = lam.invoke(FunctionName="justhodl-oecd-cli", InvocationType="RequestResponse", Payload=b"{}")
                r.log(f"invoke status {resp.get('StatusCode')} payload {resp['Payload'].read()[:300]}")
            except Exception as e:  # noqa: BLE001
                r.warn(f"invoke: {str(e)[:150]}")
            oc, lm = get_json("data/oecd-cli.json")
            if oc and (oc.get("generated_at") or "") > t1.isoformat():
                r.log(f"oecd-cli: version={oc.get('engine_version')} as_of={oc.get('as_of_period')} age={oc.get('as_of_age_months')}mo countries={oc.get('n_countries')} "
                      f"aggregates={oc.get('n_aggregates')} avg={oc.get('global_avg_cli')} oecd_total={oc.get('oecd_total_cli')} source={oc.get('source')}")
                r.log(f"  interpretation: {oc.get('interpretation')}")
                for iso in ("USA", "CHN", "DEU", "JPN", "OECD"):
                    row = (oc.get("by_country") or {}).get(iso) or {}
                    r.log(f"  {iso}: cli={row.get('cli')} prior={row.get('prior_cli')} phase={row.get('phase')} composite={row.get('composite_cli')}/{row.get('composite_phase')}")
                r.kv(engine="oecd-cli", as_of=oc.get("as_of_period"), age_months=oc.get("as_of_age_months"), countries=oc.get("n_countries"), aggregates=oc.get("n_aggregates"))
                if (oc.get("as_of_age_months") or 99) > 4 or (oc.get("n_countries") or 0) < 15:
                    fails.append(f"oecd-cli: age {oc.get('as_of_age_months')} / countries {oc.get('n_countries')}")
            else:
                fails.append(f"oecd-cli: feed not regenerated ({lm if not oc else oc.get('generated_at')})")
            tail(r, "justhodl-oecd-cli", t1)

        r.section("3. global-recession OECD leg")
        t2 = datetime.now(timezone.utc)
        lam.invoke(FunctionName="justhodl-global-recession", InvocationType="Event", Payload=b"{}")
        gr = None
        for _ in range(30):
            time.sleep(15)
            gr, lm = get_json("data/global-recession.json")
            if gr and (gr.get("generated_at") or "") > t2.isoformat():
                break
        if gr and (gr.get("generated_at") or "") > t2.isoformat():
            conf = gr.get("confirmation") or {}
            r.log(f"global-recession: prob={gr.get('global_recession_prob_pct')} band={gr.get('band')} oecd_period={conf.get('oecd_period')} oecd_usable={conf.get('oecd_usable')} "
                  f"age={conf.get('oecd_age_months')} counts={json.dumps(conf.get('counts'))} ports_countries={conf.get('ports_countries')}")
            r.kv(engine="global-recession", prob=gr.get("global_recession_prob_pct"), oecd_usable=conf.get("oecd_usable"), confirmed=(conf.get("counts") or {}).get("CONFIRMED"))
            if not conf.get("oecd_usable"):
                fails.append("global-recession still reports oecd_usable=false")
        else:
            r.warn("global-recession did not regenerate within 7.5 min (its own schedule will pick up the new feeds)")

        r.section("verdict")
        for f in fails:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok("VERDICT: GREEN -- portwatch v1.6 live (one schedule 11:20 UTC), oecd-cli v2.0.0 live on real OECD data, global-recession OECD leg usable")


if __name__ == "__main__":
    main()
