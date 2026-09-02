"""ops_5114 -- closing wave.

  A  repo-monitor errors + HTTP_ERR lines strictly after its 03:28 deploy
  B  OECD walker retry schedules (ops 5110): truncated/failure ledgers now vs
     479/264 at 02:45; stop the schedules if the ledgers stopped shrinking
  C  the small-error tail: distinct error samples (7d) for equity-research,
     cds-proxy, ecb-deep, fortress, outcome-checker, fedliquidityapi, a2a-bus,
     cb-injection, ecb-derived, provider-catalog, risk-gate,
     real-economy-collector, cftc-futures-positioning-agent
  D  global cycle: invoke justhodl-global-business-cycle now that portwatch
     carries ports with yoy -> physical_confirmation counts
  E  fleet-wide: total Errors over the last 6h across every function vs the
     7d baseline from the 5098 audit; top 10 error producers now
No gate beyond D (physical countries_with_ports > 0).
"""
import json
import re
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=120, retries={"max_attempts": 2}))
sch = boto3.client("scheduler", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
NOW = datetime.now(timezone.utc)


def get_json(key):
    try:
        o = s3.get_object(Bucket=B, Key=key)
        return json.loads(o["Body"].read()), o["LastModified"]
    except Exception as e:  # noqa: BLE001
        return None, str(e)[:100]


def metrics(fns, start, end, period):
    out = {}
    for i in range(0, len(fns), 250):
        chunk = fns[i:i + 250]
        q = []
        for j, n in enumerate(chunk):
            for met, tag in (("Invocations", "i"), ("Errors", "e")):
                q.append({"Id": f"{tag}{j}", "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": met, "Dimensions": [{"Name": "FunctionName", "Value": n}]},
                                                             "Period": period, "Stat": "Sum"}, "ReturnData": True})
        nxt = None
        while True:
            kw = {"MetricDataQueries": q, "StartTime": start, "EndTime": end}
            if nxt:
                kw["NextToken"] = nxt
            res = cw.get_metric_data(**kw)
            for m in res.get("MetricDataResults") or []:
                j = int(m["Id"][1:])
                out.setdefault(chunk[j], {})[m["Id"][0]] = sum(m.get("Values") or [0])
            nxt = res.get("NextToken")
            if not nxt:
                break
    return out


def log_lines(fn, since, pattern=None, limit=40):
    try:
        kw = {"logGroupName": f"/aws/lambda/{fn}", "startTime": int(since.timestamp() * 1000), "limit": limit}
        if pattern:
            kw["filterPattern"] = pattern
        return [e["message"].rstrip()[:260] for e in (logs.filter_log_events(**kw).get("events") or [])]
    except Exception as e:  # noqa: BLE001
        return [f"log read failed: {str(e)[:120]}"]


def distinct(lines, n=5):
    seen, out = set(), []
    for ln in lines:
        key = re.sub(r"[0-9a-f]{8}-[0-9a-f-]{27}|\d{4}-\d\d-\d\d[T ][\d:.]+Z?|\d+", "#", ln)[:110]
        if key in seen:
            continue
        seen.add(key)
        out.append(ln)
        if len(out) >= n:
            break
    return out


def main():
    with report("5114-closing-wave") as r:
        r.heading("ops 5114 -- closing wave")
        fails = []

        r.section("A. repo-monitor strictly after its 03:28 deploy")
        since = datetime(2026, 9, 2, 3, 30, tzinfo=timezone.utc)
        m = metrics(["justhodl-repo-monitor"], since, NOW, 3600)["justhodl-repo-monitor"]
        errs = distinct(log_lines("justhodl-repo-monitor", since, pattern="?HTTP_ERR ?SRF_ERR ?Traceback", limit=30))
        r.log(f"invocations={int(m.get('i', 0))} errors={int(m.get('e', 0))} HTTP_ERR lines: {json.dumps(errs)[:600]}")
        r.kv(engine="repo-monitor", invocations=int(m.get("i", 0)), errors=int(m.get("e", 0)), http_err_lines=len(errs))
        rm, _ = get_json("data/repo-monitor.json")
        if rm:
            M = rm.get("metrics") or rm.get("M") or {}
            r.log(f"feed generated_at={rm.get('generated_at')} SOFR_Volume={json.dumps(M.get('SOFR_Volume'))[:160]} FRA_OIS={json.dumps(M.get('FRA_OIS_Proxy'))[:160]} SRF={json.dumps(M.get('SRF_Usage'))[:120]}")

        r.section("B. OECD walker retry progress")
        wst, lm = get_json("data/_state/sdmx-walk-oecd.json")
        n_fail = len((wst or {}).get("failures") or {})
        n_trunc = len((wst or {}).get("truncated") or [])
        n_done = len((wst or {}).get("done") or [])
        r.log(f"ledgers now: failures={n_fail} (was 479) truncated={n_trunc} (was 264) done={n_done} (was 1548) state_updated={lm}")
        wm = metrics(["justhodl-sdmx-walker"], NOW - timedelta(hours=9), NOW, 3600)["justhodl-sdmx-walker"]
        r.log(f"walker invocations/errors last 9h: {int(wm.get('i', 0))}/{int(wm.get('e', 0))}")
        r.kv(engine="sdmx-walker", failures=n_fail, truncated=n_trunc, done=n_done)
        progress = (n_fail < 479) or (n_trunc < 264)
        if not progress:
            for name in ("justhodl-sdmx-walker-oecd-retrunc", "justhodl-sdmx-walker-oecd-refail"):
                try:
                    s = sch.get_schedule(Name=name)
                    sch.update_schedule(Name=name, ScheduleExpression=s["ScheduleExpression"], ScheduleExpressionTimezone="UTC", FlexibleTimeWindow=s.get("FlexibleTimeWindow") or {"Mode": "OFF"},
                                        Target=s["Target"], State="DISABLED", Description="DISABLED by ops 5114: no ledger progress after 9h")
                    r.warn(f"{name} -> DISABLED (no progress)")
                except Exception as e:  # noqa: BLE001
                    r.warn(f"{name}: {str(e)[:100]}")
        else:
            r.ok("retries are shrinking the ledgers; schedules kept")
        if n_fail == 0 and n_trunc == 0:
            for name in ("justhodl-sdmx-walker-oecd-retrunc", "justhodl-sdmx-walker-oecd-refail"):
                try:
                    sch.delete_schedule(Name=name)
                    r.ok(f"{name} deleted (ledgers empty)")
                except Exception as e:  # noqa: BLE001
                    r.warn(f"{name}: {str(e)[:100]}")

        r.section("C. small-error tail (7d samples)")
        for fn in ("justhodl-equity-research", "justhodl-cds-proxy", "justhodl-ecb-deep", "justhodl-fortress", "justhodl-outcome-checker", "fedliquidityapi",
                   "justhodl-a2a-bus", "justhodl-cb-injection", "justhodl-ecb-derived", "justhodl-provider-catalog", "justhodl-risk-gate",
                   "justhodl-real-economy-collector", "cftc-futures-positioning-agent"):
            errs = distinct(log_lines(fn, NOW - timedelta(days=7), pattern='?"[ERROR]" ?Traceback ?"Task timed out" ?"Error Type"', limit=40))
            mm = metrics([fn], NOW - timedelta(hours=24), NOW, 86400)[fn]
            r.log(f"{fn}: 24h {int(mm.get('i', 0))}/{int(mm.get('e', 0))} · {json.dumps(errs)[:700]}")
            r.kv(engine=fn, inv24h=int(mm.get("i", 0)), err24h=int(mm.get("e", 0)), distinct=len(errs))

        r.section("D. global cycle physical layer")
        pw, _ = get_json("data/portwatch.json")
        ports = (pw or {}).get("ports") or []
        wy = [p for p in ports if isinstance(p, dict) and isinstance(p.get("yoy_pct"), (int, float))]
        r.log(f"portwatch: generated_at={(pw or {}).get('generated_at')} ports={len(ports)} with_yoy={len(wy)}")
        t0 = datetime.now(timezone.utc)
        lam.invoke(FunctionName="justhodl-global-business-cycle", InvocationType="Event", Payload=b"{}")
        gbc = None
        for _ in range(24):
            time.sleep(15)
            gbc, _ = get_json("data/global-business-cycle.json")
            if gbc and (gbc.get("generated_at") or "") > t0.isoformat():
                break
        if gbc and (gbc.get("generated_at") or "") > t0.isoformat():
            ph = gbc.get("physical_confirmation") or {}
            comp = gbc.get("composite") or {}
            agg = gbc.get("aggregate") or {}
            r.log(f"GBC v{gbc.get('engine_version')}: physical counts={json.dumps(ph.get('counts'))} countries_with_ports={ph.get('countries_with_ports')} carried={ph.get('carried_from_previous_run')} "
                  f"multi_pillar={comp.get('countries_multi_pillar')} pillars={json.dumps(comp.get('pillar_counts'))} global={agg.get('global_phase')} {agg.get('global_avg_cli')} p6m={(gbc.get('downturn_probability_6m') or {}).get('probability_now')}")
            confirmed = [(iso, (c.get("physical") or {}).get("median_yoy_pct")) for iso, c in (gbc.get("by_country") or {}).items() if (c.get("physical") or {}).get("state") == "CONFIRMED"]
            divergent = [iso for iso, c in (gbc.get("by_country") or {}).items() if (c.get("physical") or {}).get("state") == "DIVERGENT"]
            r.log(f"  CONFIRMED: {confirmed[:12]} · DIVERGENT: {divergent}")
            r.kv(engine="gbc", countries_with_ports=ph.get("countries_with_ports"), confirmed=(ph.get("counts") or {}).get("CONFIRMED"), divergent=(ph.get("counts") or {}).get("DIVERGENT"))
            if not ph.get("countries_with_ports"):
                fails.append("GBC physical layer still 0 countries with ports")
        else:
            fails.append("GBC did not regenerate within 6 min")

        r.section("E. fleet-wide errors: last 6h vs 7d baseline")
        fns = []
        for page in lam.get_paginator("list_functions").paginate():
            fns.extend(f["FunctionName"] for f in page["Functions"])
        now6 = metrics(fns, NOW - timedelta(hours=6), NOW, 21600)
        tot_i = sum(v.get("i", 0) for v in now6.values())
        tot_e = sum(v.get("e", 0) for v in now6.values())
        audit, _ = get_json("data/audit/fleet-data-audit-5098.json")
        base_i = sum(x["inv7d"] for x in (audit or {}).get("engines") or [])
        base_e = sum(x["err7d"] for x in (audit or {}).get("engines") or [])
        r.log(f"7d baseline (ops 5098): {base_i:.0f} invocations / {base_e:.0f} errors = {100 * base_e / max(base_i, 1):.2f}% ; last 6h: {tot_i:.0f} / {tot_e:.0f} = {100 * tot_e / max(tot_i, 1):.2f}% "
              f"(6h scaled to 7d: {tot_e * 28:.0f} errors vs {base_e:.0f})")
        top = sorted(((fn, v.get("e", 0), v.get("i", 0)) for fn, v in now6.items() if v.get("e", 0) > 0), key=lambda x: -x[1])[:12]
        r.log("top error producers (6h): " + "; ".join(f"{fn} {int(e)}/{int(i)}" for fn, e, i in top))
        r.kv(scope="fleet", inv6h=int(tot_i), err6h=int(tot_e), err_pct_6h=round(100 * tot_e / max(tot_i, 1), 2), err_pct_7d_baseline=round(100 * base_e / max(base_i, 1), 2))
        fanout, _ = get_json("config/fanout-manifest.json")
        members = sorted({fn for v in (fanout or {}).get("ticks", {}).values() for fn in v})
        inv_m = sum(1 for fn in members if now6.get(fn, {}).get("i", 0) > 0)
        err_m = [fn for fn in members if now6.get(fn, {}).get("e", 0) > 0]
        r.log(f"fan-out members (6h): {inv_m}/{len(members)} invoked, errored: {err_m}")

        r.section("verdict")
        for f in fails:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok("VERDICT: GREEN")


if __name__ == "__main__":
    main()
