"""ops 6874 - READ-ONLY: the slow-source engines as they run today, and what is stale right now.

Khalid 2026-11-20 22:20 CT: "... do 1 then 2 then 3 thrn have my data update as needed to stay fresh and make sure my
bill stays the same".  Step 3 (slow sources to a weekly cadence) and the freshness pass need facts before a plan:

  A. For every engine that pulls from a slow official source (monthly / quarterly / annual publications, weekly CFTC and
     DOL): its live trigger(s) and expression, runs and run time over the last 7 days, what that costs a month in
     Lambda, and how many files it wrote on each of the last three days (access logs, sampled).
  B. The freshness monitor's own last report: how many feeds it checks, how many are past their limit, which.
  C. The freshness rules: the default limit, the prefixes left out, the per-file limits already set.
  D. The provider banks: phase, last run, and any provider hold (the FMP bandwidth hold).

Nothing is written but this report.
"""
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import unquote
import concurrent.futures as cf
import importlib.util
import json
import random
import re
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "scripts"))
_spec = importlib.util.spec_from_file_location("ops6703", str(Path(__file__).resolve().parent / "ops_6703_banks_steady_cost.py"))
B = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(B)

SLOW = re.compile(r"bls|(^|-)bea(-|$)|census|(^|-)bis(-|$)|imf|worldbank|oecd|eurostat|(^|-)eia(-|$)|openfigi|cftc|(^|-)cot(-|$)|(^|-)eba(-|$)|fdic|(^|-)dol(-|$)|sdmx|13f|"
                  r"(^|-)tic(-|$)|(^|-)boj(-|$)|z1|wgi|gleif", re.I)
SOURCE_CADENCE = [("worldbank", "World Bank: annual and quarterly series, revised a few times a year"), ("wgi", "World Bank governance indicators: annual"),
                  ("eba", "EBA risk dashboard: quarterly"), ("fdic", "FDIC call reports: quarterly"), ("dol", "DOL claims: weekly, Thursday 08:30 ET"),
                  ("cftc", "CFTC commitments of traders: weekly, Friday 15:30 ET"), ("cot", "CFTC commitments of traders: weekly, Friday 15:30 ET"),
                  ("13f", "SEC 13F: quarterly, filed over the 45 days after quarter end"), ("bls", "BLS: monthly releases 08:30 ET"), ("bea", "BEA: monthly and quarterly releases 08:30 ET"),
                  ("census", "Census: monthly releases"), ("imf", "IMF: monthly and quarterly"), ("oecd", "OECD: monthly"), ("eurostat", "Eurostat: monthly, some daily"),
                  ("bis", "BIS: quarterly, some monthly"), ("gleif", "GLEIF: daily file, slow-moving"), ("sdmx", "BIS / Eurostat / OECD dataset walks: monthly and quarterly data"),
                  ("eia", "EIA: weekly petroleum Wednesday, daily prices"), ("tic", "Treasury TIC: monthly"), ("boj", "Bank of Japan: daily and ten-day accounts"), ("openfigi", "OpenFIGI: reference data")]
LAMBDA_GBS = 0.0000166667
BANKS = {"justhodl-fmp-bank": ("data/warm/fmp-bank/_state/state.json", "data/warm/fmp-bank/manifest.json"),
         "justhodl-fmp-walker": ("data/warm/fmp/_state/state.json", "data/warm/fmp/manifest.json"),
         "justhodl-polygon-bank": ("data/warm/polygon-bank/_state/state.json", "data/warm/polygon-bank/manifest.json"),
         "justhodl-provider-bank": ("data/warm/provider-bank/_state/state.json", "data/warm/provider-bank/manifest.json"),
         "justhodl-eia-bank": ("data/warm/eia-bank/_state/state.json", "data/warm/eia-bank/manifest.json"),
         "justhodl-polygon-eod": ("data/warm/polygon-full/eod/_state/state.json", "data/warm/polygon-full/eod/manifest.json"),
         "justhodl-bank-histories": ("data/warm/history-book/_state/state.json", "data/warm/history-book/manifest.json"),
         "justhodl-bank-factors": (None, "data/warm/factor-book/manifest.json")}


def cadence_of(fn):
    low = fn.lower()
    for k, why in SOURCE_CADENCE:
        if re.search(r"(^|-)%s(-|$)" % re.escape(k), low) or (k in ("worldbank", "eurostat", "census", "openfigi", "gleif", "sdmx", "13f", "fdic", "imf", "oecd", "bls", "cftc") and k in low):
            return why
    return "?"


def per_day(expr):
    """How often a schedule expression fires: 'daily', 'weekly (DOW)', or the expression itself."""
    m = re.match(r"cron\((\S+) (\S+) (\S+) (\S+) (\S+) (\S+)\)", expr or "")
    if not m:
        return expr or "-"
    mi, h, dom, mon, dow, _ = m.groups()
    if dow not in ("?", "*"):
        return "weekly %s %s:%s" % (dow, h.zfill(2), mi.zfill(2)) if "," not in dow and "-" not in dow else "%s %s:%s" % (dow, h.zfill(2), mi.zfill(2))
    if dom not in ("?", "*"):
        return "monthly day %s %s:%s" % (dom, h.zfill(2), mi.zfill(2))
    return "daily %s:%s" % (h.zfill(2), mi.zfill(2)) if h.isdigit() and mi.isdigit() else expr


def selftest():
    assert per_day("cron(40 3 * * ? *)") == "daily 03:40" and per_day("cron(0 22 ? * FRI *)") == "weekly FRI 22:00" and per_day("cron(5 3 1 * ? *)") == "monthly day 1 03:05"
    assert per_day("rate(1 day)") == "rate(1 day)" and per_day("cron(0 6 ? * MON-FRI *)") == "MON-FRI 06:00"
    assert SLOW.search("justhodl-eba-risk-dashboard") and SLOW.search("justhodl-13f-book") and SLOW.search("bea-economic-agent") and SLOW.search("justhodl-dol-full")
    assert not SLOW.search("justhodl-beacon") and not SLOW.search("justhodl-polygon-eod") and not SLOW.search("justhodl-boiler")
    assert cadence_of("justhodl-eba-risk-dashboard").startswith("EBA") and cadence_of("justhodl-worldbank-full").startswith("World Bank") and cadence_of("justhodl-dol-full").startswith("DOL")
    assert cadence_of("justhodl-cot-tracker").startswith("CFTC") and cadence_of("bls-labor-agent").startswith("BLS")


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report
    import cadence_apply as ca

    selftest()
    cfg = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=60, max_pool_connections=64)
    s3 = boto3.client("s3", region_name=B.REGION, config=cfg)
    lam = boto3.client("lambda", region_name=B.REGION, config=cfg)
    cw = boto3.client("cloudwatch", region_name=B.REGION, config=cfg)
    events = boto3.client("events", region_name=B.REGION, config=cfg)
    scheduler = boto3.client("scheduler", region_name=B.REGION, config=cfg)
    now = datetime.now(timezone.utc)

    def jget(key):
        try:
            return json.loads(s3.get_object(Bucket=B.LIVE, Key=key)["Body"].read())
        except Exception:  # noqa: BLE001
            return {}

    with report("ops_6874_slow_sources_and_freshness") as r:
        r.heading("ops 6874 - the slow-source engines today, and what is stale right now (read-only)")

        r.section("A. Engines on slow official sources")
        fns = []
        for page in lam.get_paginator("list_functions").paginate():
            fns += [f for f in page["Functions"] if SLOW.search(f["FunctionName"])]
        snap = ca.snapshot_triggers(events, scheduler, log=lambda *_a, **_k: None)
        trig = defaultdict(list)
        for t in snap:
            if t.get("fn"):
                trig[t["fn"]].append(t)
        fan = json.loads((ROOT / "config" / "fanout-manifest.json").read_text(encoding="utf-8"))
        in_tick = defaultdict(list)
        for tick, members in (fan.get("ticks") or {}).items():
            for m in (members if isinstance(members, list) else (members.get("functions") or members.get("members") or [])):
                name = m if isinstance(m, str) else (m.get("function") or m.get("fn") or m.get("name"))
                if name:
                    in_tick[name].append(tick)

        def metric(fn, name):
            res = cw.get_metric_statistics(Namespace="AWS/Lambda", MetricName=name, Dimensions=[{"Name": "FunctionName", "Value": fn}],
                                           StartTime=now - timedelta(days=7), EndTime=now, Period=86400, Statistics=["Sum"])
            return sum(d["Sum"] for d in res.get("Datapoints", []))
        with cf.ThreadPoolExecutor(max_workers=12) as ex:
            inv = dict(zip([f["FunctionName"] for f in fns], ex.map(lambda f: (metric(f["FunctionName"], "Invocations"), metric(f["FunctionName"], "Duration")), fns)))

        days = [(now.date() - timedelta(days=d)).isoformat() for d in (1, 2, 3)]
        lg = s3.get_bucket_logging(Bucket=B.LIVE).get("LoggingEnabled") or {}
        tb, tp = lg.get("TargetBucket"), lg.get("TargetPrefix") or ""
        writes = {d: Counter() for d in days}
        rng = random.Random(6874)
        for day in days:
            base = "%s%s/%s/%s/%s/" % (tp, B.ACCOUNT, B.REGION, B.LIVE, day.replace("-", "/"))
            keys, tok = [], None
            while True:
                kw = {"Bucket": tb, "Prefix": base, "MaxKeys": 1000}
                if tok:
                    kw["ContinuationToken"] = tok
                res = s3.list_objects_v2(**kw)
                keys += [o["Key"] for o in res.get("Contents", [])]
                if not res.get("IsTruncated") or len(keys) >= 1500000:
                    break
                tok = res.get("NextContinuationToken")
            if not keys:
                continue
            sample = rng.sample(keys, min(4000, len(keys)))
            scale = len(keys) / float(len(sample))

            def read(k):
                try:
                    return s3.get_object(Bucket=tb, Key=k)["Body"].read().decode("utf-8", "replace")
                except Exception:  # noqa: BLE001
                    return ""
            with cf.ThreadPoolExecutor(max_workers=48) as ex:
                for text in ex.map(read, sample):
                    for line in text.splitlines():
                        m = B.LOG_RE.match(line)
                        if m and m.group(7) in B.WRITE_OPS and m.group(10).startswith("2"):
                            writes[day][B.writer(m.group(5))] += scale
            r.log("%s: about %d writes to the live bucket by all engines" % (day, sum(writes[day].values())))
        rows = []
        for f in fns:
            fn = f["FunctionName"]
            n, dur_ms = inv.get(fn, (0, 0))
            gbs = dur_ms / 1000.0 * (f.get("MemorySize", 128) / 1024.0)
            month = gbs / 7.0 * 30 * LAMBDA_GBS
            w = [writes[d].get(fn, 0) for d in days]
            wmonth = sum(w) / 3.0 * 30 * 0.01 / 1000
            live = [t for t in trig.get(fn, []) if t.get("state") == "ENABLED"]
            how = "; ".join("%s %s [%s]" % (t["kind"], t["name"], per_day(t["expr"])) for t in live) or ("tick %s" % ",".join(in_tick[fn]) if in_tick.get(fn) else "no enabled trigger")
            rows.append((month + wmonth, fn, n, dur_ms / 1000.0 / n if n else 0, month, w, wmonth, how, len(trig.get(fn, [])) - len(live)))
        rows.sort(reverse=True)
        r.log("%d functions match; runs and run time are the last 7 days, files written are %s" % (len(rows), ", ".join(days)))
        tot_l = tot_w = 0.0
        for total, fn, n, avg, month, w, wmonth, how, off in rows:
            tot_l += month
            tot_w += wmonth
            r.log("%-40s runs %3d x %5.0f s | Lambda $%.2f/mo | files/day %6d %6d %6d ($%.2f/mo) | %s%s | %s" % (
                fn, n, avg, month, w[0], w[1], w[2], wmonth, how, " (+%d disabled)" % off if off else "", cadence_of(fn)))
        r.log("together: Lambda $%.2f a month, file writes $%.2f a month" % (tot_l, tot_w))

        r.section("B. The freshness monitor's last report")
        mon = jget("data/_freshness-monitor.json")
        r.log("report of %s, status %s, version %s" % (mon.get("generated_at"), mon.get("status"), mon.get("version")))
        counts = {k: v for k, v in mon.items() if isinstance(v, (int, float, str)) and k not in ("generated_at", "status", "version")}
        r.log("counts: %s" % json.dumps(counts)[:900])
        stale = mon.get("stale") or mon.get("stale_keys") or mon.get("top_stale") or []
        r.log("past their limit: %d listed" % len(stale))
        for s in stale[:45]:
            if isinstance(s, dict):
                r.log("  %-58s %-8s age %6s h | limit %s h | %s" % (str(s.get("key"))[:58], s.get("status"), s.get("age_h"), s.get("max_age_h"), str(s.get("reason") or s.get("detail") or "")[:60]))
        other = {k: (len(v) if isinstance(v, (list, dict)) else v) for k, v in mon.items() if isinstance(v, (list, dict))}
        r.log("sections: %s" % json.dumps(other)[:600])

        r.section("C. The freshness rules")
        man = jget("data/_freshness-manifest.json")
        r.log("rules: %s" % json.dumps(man.get("rules"))[:400])
        r.log("left out: %d prefixes %s" % (len(man.get("exclude_prefixes") or []), (man.get("exclude_prefixes") or [])[:14]))
        ov = man.get("key_overrides") or {}
        lim = Counter()
        for v in ov.values():
            h = v.get("max_age_h") if isinstance(v, dict) else v
            try:
                h = float(h)
            except (TypeError, ValueError):
                continue
            lim["<= 26 h" if h <= 26 else "27-72 h" if h <= 72 else "73-200 h (weekly)" if h <= 200 else "> 200 h"] += 1
        r.log("per-file limits: %d (%s)" % (len(ov), dict(lim)))
        for k in ("data/eba-risk-dashboard.json", "data/fdic-bankfind.json", "data/worldbank-wgi.json", "data/cftc-deep-view.json"):
            r.log("  %-40s limit %s" % (k, ov.get(k, "(default)")))

        r.section("D. The provider banks")
        for name, (sk, mk) in BANKS.items():
            st = jget(sk) if sk else {}
            mf = jget(mk)
            lr = st.get("last_run") or mf.get("last_run") or {}
            hold = float(st.get("bandwidth_hold_until") or 0)
            r.log("%-26s phase %-9s | updated %s | last run: calls %s, puts %s, unchanged %s, %s s | objects %s, %.1f GB%s" % (
                name, st.get("phase") or mf.get("phase"), str(st.get("updated_at") or mf.get("updated_at"))[:16], lr.get("calls"), lr.get("puts", lr.get("written")), lr.get("unchanged"),
                lr.get("seconds"), mf.get("objects"), (mf.get("bytes") or 0) / 1e9,
                " | HOLD until %s: %s" % (datetime.fromtimestamp(hold, timezone.utc).strftime("%m-%d %H:%M"), str(st.get("bandwidth_note"))[:170]) if hold > now.timestamp() else ""))
            extra = {k: st.get(k) for k in ("bandwidth_bytes_30d", "bandwidth_used", "bandwidth", "key_status", "runs", "holds", "bandwidth_holds") if st.get(k) is not None}
            if extra:
                r.log("      %s" % json.dumps(extra, default=str)[:400])
        r.kv(slow_engines=len(rows), slow_lambda_month=round(tot_l, 2), slow_writes_month=round(tot_w, 2), feeds_past_limit=len(stale), freshness_status=mon.get("status"),
             per_file_limits=len(ov))
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
