"""ops 6596 - READ-ONLY census of every feed behind chart.html (Khalid, 2026-10-21 08:36 CT: "bring back chart.html
with all the engines inside it").

chart.html is a shell over ~40 jh-chart-*.js modules.  Its page-data contract (config/page-data-contracts.json)
binds 82 outputs of 39 engines to it; the modules also read a few families the contract does not bind (the
watchlist-insights GitHub workflow, the TradingView bar bank, the symdir series bank) and one dead file
(data/fmp-ratios.json - a one-off harvest from 2026-09-15 with no writer).  The fleet was frozen Oct 16-21 00:30 UTC,
so every feed whose writer fires after ~13:40 UTC has not been refreshed since Oct 15.

For every bound output: S3 age + size + the file's own generated stamp; for every writer: its trigger(s), last
invocation, invocations/errors per day for 8 days, concurrency (held?); for the unbound families: newest object.
Verdict per output: FRESH / STALE / DEAD (no writer) / MISSING, and per writer: "ran since the thaw" or not, with
the next scheduled fire.  Nothing is changed - this decides what ops 6598 refreshes on demand.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import re
import sys
import time
from collections import defaultdict

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "scripts"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=120)
THAW = datetime(2026, 10, 21, 0, 30, tzinfo=timezone.utc)
PAGE = "chart.html"
# families the chart modules read that the page contract does not bind to a writer (ops 6596 recon, 2026-10-21)
FAMILIES = [
    ("data/watchlist-insights/stock/", "github:watchlist-insights.yml (cron 35 23 * * *)"),
    ("data/watchlist-insights/etf/", "github:watchlist-insights.yml (cron 35 23 * * *)"),
    ("data/warm/tv-bars/universe/", "justhodl-tv-bars (02:30 UTC)"),
    ("data/warm/tv-bars/", "justhodl-tv-bars (02:30 UTC)"),
    ("data/series/", "justhodl-symdir series bank (opened-on-demand heal + fredfresh)"),
    ("data/etf-flow-hist/", "?"),
    ("data/providers/", "?"),
]
EXTRA_KEYS = [("(none - one-off ops 5576 harvest 2026-09-15)", "data/fmp-ratios.json")]
EXTRA_FNS = ["justhodl-tv-bars", "justhodl-wl-series-api", "justhodl-symdir-fredfresh"]
STAMP_RE = re.compile(r'"(generated_at|generated|as_of|asof|updated_at|updated|timestamp|ts|run_at|date)"\s*:\s*"([^"]{8,40})"')


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def fn_of(arn):
    m = re.search(r":function:([^/:]+)", arn or "")
    return m.group(1) if m else None


def triggers():
    """function -> [expr...] from the committed schedule manifest + fanout ticks (same resolution as the data-flow audit)."""
    sched = json.loads((ROOT / "config/schedule-manifest.json").read_text(encoding="utf-8"))
    fan = json.loads((ROOT / "config/fanout-manifest.json").read_text(encoding="utf-8"))
    fires, tick_time = defaultdict(list), {}
    for sec in ("rules", "schedules"):
        for e in sched.get(sec, []):
            if e.get("state") != "ENABLED":
                continue
            fns = sorted({fn_of(t.get("arn")) for t in e.get("targets") or [] if fn_of(t.get("arn"))})
            if "justhodl-scheduler" in fns:
                for t in e["targets"]:
                    m = re.search(r'"tick":\s*"([^"]+)"', t.get("input") or "")
                    if m:
                        tick_time[m.group(1)] = e["expr"]
                continue
            for f in fns:
                fires[f].append(e["expr"] + ((" tz=" + e["timezone"]) if e.get("timezone") and e["timezone"] != "UTC" else ""))
    for tick, members in (fan.get("ticks") or {}).items():
        if tick in tick_time:
            for f in members:
                fires[f].append("tick:%s@%s" % (tick, tick_time[tick]))
    return fires


DOW = {"SUN": 0, "MON": 1, "TUE": 2, "WED": 3, "THU": 4, "FRI": 5, "SAT": 6}


def next_fire(exprs, now):
    """Earliest fire today (UTC) from cron(M H dom mon dow year) expressions - good enough for daily/weekday shapes."""
    best = None
    for raw in exprs:
        ex = raw.split("@", 1)[1] if raw.startswith("tick:") else raw
        m = re.match(r"cron\((\S+) (\S+) (\S+) (\S+) (\S+) (\S+)\)", ex.split(" tz=")[0])
        if not m:
            if ex.startswith("rate("):
                return "rate"
            continue
        mi, hr, dom, mon, dow, _ = m.groups()
        if not (mi.isdigit() and hr.isdigit()):
            continue
        if dow not in ("?", "*"):
            days = set()
            for part in dow.split(","):
                if "-" in part:
                    a, b = part.split("-")
                    ra, rb = DOW.get(a, -1), DOW.get(b, -1)
                    days |= {d for d in range(ra, rb + 1)}
                else:
                    days.add(DOW.get(part, -1))
            if ((now.weekday() + 1) % 7) not in days:
                continue  # not today
        t = now.replace(hour=int(hr), minute=int(mi), second=0, microsecond=0)
        if best is None or t < best:
            best = t
    return best.strftime("%H:%M") if best else "-"


def bound_hours(exprs):
    if not exprs:
        return None
    hours = []
    for raw in exprs:
        ex = raw.split("@", 1)[1] if raw.startswith("tick:") else raw
        m = re.match(r"cron\((\S+) (\S+) (\S+) (\S+) (\S+) (\S+)\)", ex)
        if not m:
            hours.append(30)
            continue
        dow = m.group(5)
        if dow in ("?", "*"):
            hours.append(30)
        elif dow in ("MON-FRI", "TUE-SAT"):
            hours.append(80)
        else:
            hours.append(8 * 24 + 6)
    return min(hours)


def metrics(cw, fns, now):
    """per function: last-8-days daily invocations/errors + last invocation hour (period 3600)."""
    out = {}
    start = now - timedelta(days=8)
    for i in range(0, len(fns), 50):
        chunk = fns[i:i + 50]
        q = []
        for j, fn in enumerate(chunk):
            for met in ("Invocations", "Errors"):
                q.append({"Id": "m%d%s" % (j, met[0].lower()), "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": met, "Dimensions": [{"Name": "FunctionName", "Value": fn}]},
                                                                       "Period": 3600, "Stat": "Sum"}, "ReturnData": True})
        res, token = [], None
        while True:
            kw = {"MetricDataQueries": q, "StartTime": start, "EndTime": now, "ScanBy": "TimestampAscending"}
            if token:
                kw["NextToken"] = token
            r = cw.get_metric_data(**kw)
            res += r.get("MetricDataResults", [])
            token = r.get("NextToken")
            if not token:
                break
        for j, fn in enumerate(chunk):
            rec = {"inv_days": defaultdict(int), "err_days": defaultdict(int), "last": None, "inv8d": 0, "err8d": 0}
            for mr in res:
                if mr["Id"] == "m%di" % j:
                    for t, v in zip(mr["Timestamps"], mr["Values"]):
                        if v:
                            rec["inv_days"][t.strftime("%m-%d")] += int(v)
                            rec["inv8d"] += int(v)
                            rec["last"] = t if (rec["last"] is None or t > rec["last"]) else rec["last"]
                elif mr["Id"] == "m%de" % j:
                    for t, v in zip(mr["Timestamps"], mr["Values"]):
                        if v:
                            rec["err_days"][t.strftime("%m-%d")] += int(v)
                            rec["err8d"] += int(v)
            out[fn] = rec
    return out


def stamp_of(s3, key, size):
    if size is None or size > 60 * 1024 * 1024 or key.endswith(".gz"):
        return "-"
    try:
        body = s3.get_object(Bucket=LIVE, Key=key, Range="bytes=0-4095")["Body"].read().decode("utf-8", errors="replace")
    except Exception as e:  # noqa: BLE001
        return "err:" + type(e).__name__
    m = STAMP_RE.search(body)
    return (m.group(2)[:25] if m else "-")


def main():
    s3, cw, lam = c("s3"), c("cloudwatch"), c("lambda")
    now = datetime.now(timezone.utc)
    with report("ops_6596_chart_feed_census") as r:
        r.heading("ops 6596 - chart.html feed census (read-only): which engines behind the chart are fresh, stale, dead")
        pc = json.loads((ROOT / "config/page-data-contracts.json").read_text(encoding="utf-8"))
        outs = pc["pages"][PAGE]["outputs"]
        pairs = sorted({(o["engine"], o["key"]) for o in outs}) + EXTRA_KEYS
        fns = sorted({e for e, _ in pairs if e.startswith("justhodl-")} | set(EXTRA_FNS))
        fires = triggers()
        r.log("contract: %d outputs of %d engines bound to %s; +%d extra functions, +%d unbound families; thaw %s; now %s" % (
            len(outs), len({e for e, _ in pairs if e.startswith("justhodl-")}), PAGE, len(EXTRA_FNS), len(FAMILIES), THAW.strftime("%m-%d %H:%M"), now.strftime("%m-%d %H:%M UTC")))
        r.section("1. writers")
        met = metrics(cw, fns, now)
        conf = {}
        for fn in fns:
            try:
                cf = lam.get_function_configuration(FunctionName=fn)
                try:
                    rc = lam.get_function_concurrency(FunctionName=fn).get("ReservedConcurrentExecutions")
                except Exception:  # noqa: BLE001
                    rc = "?"
                conf[fn] = {"timeout": cf.get("Timeout"), "mem": cf.get("MemorySize"), "modified": (cf.get("LastModified") or "")[:10], "rc": rc, "state": cf.get("State")}
            except Exception as e:  # noqa: BLE001
                conf[fn] = {"err": type(e).__name__}
        not_since_thaw, held = [], []
        for fn in fns:
            m, cfv = met.get(fn, {}), conf.get(fn, {})
            last = m.get("last")
            since = (last is not None and last >= THAW)
            ex = fires.get(fn, [])
            if cfv.get("rc") == 0:
                held.append(fn)
            if not since and ex:
                not_since_thaw.append(fn)
            r.log("%-36s trig=%s | last inv %s%s | 8d inv %d err %d | days %s | timeout %ss mem %s rc %s%s" % (
                fn, ";".join(ex)[:70] or "NONE", last.strftime("%m-%d %H:%M") if last else "none in 8d", "" if since else " (NOT since thaw)",
                m.get("inv8d", 0), m.get("err8d", 0), json.dumps(dict(sorted(m.get("inv_days", {}).items())))[:120], cfv.get("timeout"), cfv.get("mem"), cfv.get("rc"),
                " next today %s" % next_fire(ex, now) if ex else ""))
        r.section("2. outputs")
        verdict = defaultdict(int)
        rows = []
        for eng, key in pairs:
            try:
                h = s3.head_object(Bucket=LIVE, Key=key)
                lm = h["LastModified"]
                age_h = (now - lm).total_seconds() / 3600.0
                size = h.get("ContentLength")
                st = stamp_of(s3, key, size)
            except Exception as e:  # noqa: BLE001
                lm, age_h, size, st = None, None, None, type(e).__name__
            ex = fires.get(eng, []) if eng.startswith("justhodl-") else []
            bh = bound_hours(ex)
            if lm is None:
                v = "MISSING"
            elif not ex:
                v = "DEAD-WRITER"
            elif age_h > bh:
                v = "STALE"
            else:
                v = "FRESH"
            verdict[v] += 1
            rows.append((v, eng, key, age_h, size, st, bh))
            r.kv(verdict=v, engine=eng, key=key, age_h=("%.1f" % age_h) if age_h is not None else "-", size=size if size is not None else "-", stamp=st, bound_h=bh if bh else "-")
        for v in ("MISSING", "DEAD-WRITER", "STALE"):
            ks = [x for x in rows if x[0] == v]
            if ks:
                r.log("%s (%d): %s" % (v, len(ks), "; ".join("%s <- %s (%.0fh)" % (k, e, a or 0) for _, e, k, a, _, _, _ in ks)[:3000]))
        r.section("3. unbound families")
        for prefix, who in FAMILIES:
            try:
                page = s3.list_objects_v2(Bucket=LIVE, Prefix=prefix, MaxKeys=1000)
                objs = page.get("Contents", [])
                newest = max((o["LastModified"] for o in objs), default=None)
                oldest = min((o["LastModified"] for o in objs), default=None)
                r.log("%-36s writer %s | first page %d keys%s | newest %s (%.1fh) | oldest %s" % (
                    prefix, who, len(objs), " (more)" if page.get("IsTruncated") else "", newest.strftime("%m-%d %H:%M") if newest else "-",
                    ((now - newest).total_seconds() / 3600.0) if newest else -1, oldest.strftime("%m-%d") if oldest else "-"))
            except Exception as e:  # noqa: BLE001
                r.warn("%s: %s" % (prefix, type(e).__name__))
        r.section("4. fmp-ratios.json detail")
        try:
            body = json.loads(s3.get_object(Bucket=LIVE, Key="data/fmp-ratios.json")["Body"].read())
            tk = body.get("tickers") or {}
            r.log("generated_at %s schema %s cadence %s key_status %s tickers %d: %s" % (body.get("generated_at"), body.get("schema_version"), body.get("cadence"), json.dumps(body.get("key_status"))[:200], len(tk), ",".join(sorted(tk))[:400]))
            one = next(iter(tk.values())) if tk else {}
            r.log("fields per ticker: %s" % sorted(one.keys())[:40])
        except Exception as e:  # noqa: BLE001
            r.warn("fmp-ratios.json: %s" % type(e).__name__)
        r.kv(verdict="SUMMARY", engine="writers not run since thaw: %d" % len(not_since_thaw), key=", ".join(not_since_thaw)[:900], age_h="-", size="-", stamp="held: %s" % (",".join(held) or "none"), bound_h="-")
        r.log("SUMMARY: outputs %s | writers not run since the thaw (%d): %s | held (rc=0): %s" % (json.dumps(dict(verdict)), len(not_since_thaw), ", ".join(not_since_thaw), ", ".join(held) or "none"))
        r.ok("census read")


if __name__ == "__main__":
    main()
