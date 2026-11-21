"""ops 6876 - READ-ONLY: the two expensive slow-source walkers, and which stale feeds are really late.

ops 6874 found (1) justhodl-worldbank-full running 239 times in 7 days at 677 s a run - $11.55 a month of Lambda for
World Bank data that changes a few times a year - and justhodl-eia-bank kicked 12 times a day ($4.68 a month); every
other slow-source engine costs cents; (2) the freshness monitor reporting 667 of 1,554 feeds past their limit, with
ages from 27 hours to five months - an alarm that is always on says nothing.  Before anything is changed:

  A. The walkers: every trigger with its expression and the NAMES of its payload fields, runs per hour over 7 days,
     the engine's own state and manifest, the chain resumer's ledger and parked tickets, a week's Lambda cost of each
     bank engine.
  B. The stale list, sorted into what it really is: each feed joined to the engine that writes it (engine-manifest.json)
     and to that engine's live triggers and its runs and errors over 3 days:
        slower by schedule      the engine is scheduled weekly or monthly - the 26-hour limit is the wrong limit
        no engine runs it       no enabled trigger writes this file - a leftover of a retired engine
        runs daily, file old    the engine ran in the last 3 days and the file did not move - the real problems
        nobody claims it        no engine in the manifest lists the key

Nothing is written but this report.
"""
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
import concurrent.futures as cf
import gzip
import importlib.util
import json
import re
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "scripts"))
_spec = importlib.util.spec_from_file_location("ops6874", str(Path(__file__).resolve().parent / "ops_6874_slow_sources_and_freshness.py"))
F = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(F)
LIVE, REGION = "justhodl-dashboard-live", "us-east-1"
EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
SAFE_VALUE_KEYS = ("mode", "action", "phase", "tick", "kind", "drain", "weekly", "agency", "task", "step", "op", "force", "rewalk", "full")
WALKERS = ["justhodl-worldbank-full", "justhodl-eia-bank"]
BANKS = ["justhodl-fmp-bank", "justhodl-fmp-walker", "justhodl-polygon-bank", "justhodl-provider-bank", "justhodl-eia-bank", "justhodl-polygon-eod",
         "justhodl-bank-histories", "justhodl-bank-factors", "justhodl-worldbank-full", "justhodl-chain-resumer"]
LAMBDA_GBS = 0.0000166667


SECRETISH = re.compile(r"(?i)\b(api_?key|apikey|token|secret|password|signature|sig|key)=([^&\s\"']+)")
LONGTOKEN = re.compile(r"\b[A-Za-z0-9_\-]{36,}\b")


def mask(text, n=200):
    """A line fit for a public report: no address, no credential-looking value, no long opaque token."""
    t = EMAIL.sub("[address]", str(text))
    t = SECRETISH.sub(lambda m: m.group(1) + "=[...]", t)
    return LONGTOKEN.sub("[...]", t)[:n]


def scalars(doc, depth=0, prefix=""):
    """The scalar leaves of a state document, two levels deep, masked - enough to read a phase without dumping a store."""
    out = []
    if not isinstance(doc, dict):
        return out
    for k, v in doc.items():
        if isinstance(v, (str, int, float, bool)) or v is None:
            out.append("%s%s=%s" % (prefix, k, mask(v, 70)))
        elif isinstance(v, list):
            out.append("%s%s=[%d]" % (prefix, k, len(v)))
        elif isinstance(v, dict):
            if depth < 1 and len(v) <= 14:
                out += scalars(v, depth + 1, prefix + str(k) + ".")
            else:
                out.append("%s%s={%d}" % (prefix, k, len(v)))
    return out


def payload_shape(raw):
    """Field names of a trigger payload and the values of a few harmless routing fields - never the payload itself."""
    try:
        d = json.loads(raw) if raw else {}
    except Exception:  # noqa: BLE001
        return "(not JSON, %d bytes)" % len(raw or "")
    if not isinstance(d, dict):
        return "(%s)" % type(d).__name__
    return "{%s}" % ", ".join("%s=%s" % (k, mask(d[k], 30)) if k in SAFE_VALUE_KEYS and isinstance(d[k], (str, int, bool)) else str(k) for k in sorted(d)) if d else "{}"


def age_class(h):
    if h is None:
        return "no age"
    return "27 h - 3 days" if h <= 72 else "3 - 9 days" if h <= 216 else "9 - 35 days" if h <= 840 else "over 5 weeks"


def cadence(exprs):
    """daily | weekly | monthly | other, from an engine's enabled trigger expressions (the fastest one decides)."""
    best = None
    rank = {"daily": 0, "weekly": 1, "monthly": 2, "other": 3}
    for e in exprs:
        p = F.per_day(e)
        c = "daily" if p.startswith("daily") or p.startswith("rate(1 day") or p.startswith("rate(24 hour") or re.match(r"(MON-FRI|TUE-SAT|MON-SAT|SUN-THU)", p) or re.match(r"rate\((\d+) (minute|hour)", p) else \
            "weekly" if p.startswith("weekly") or p.startswith("rate(7 day") or re.match(r"[A-Z]{3},[A-Z]{3} ", p) else "monthly" if p.startswith("monthly") else "other"
        if best is None or rank[c] < rank[best]:
            best = c
    return best


def selftest():
    assert payload_shape('{"mode": "resume", "token": "abc", "n": 3}') == "{mode=resume, n, token}" and payload_shape("") == "{}" and payload_shape("[1]") == "(list)"
    assert "token" in payload_shape('{"token": "s3cr3t"}') and "s3cr3t" not in payload_shape('{"token": "s3cr3t"}')
    assert scalars({"phase": "BACKFILL", "blocks": {"a": 1}, "runs": [1, 2], "big": {str(i): i for i in range(20)}, "who": "a.b@c.org"}) == ["phase=BACKFILL", "blocks.a=1", "runs=[2]", "big={20}", "who=[address]"]
    assert mask("GET https://x.org/v3/a?apikey=ABC123&symbol=AAPL by a@b.co") == "GET https://x.org/v3/a?apikey=[...]&symbol=AAPL by [address]"
    assert mask("id 0123456789abcdef0123456789abcdef01234567 ok") == "id [...] ok" and mask("phase BACKFILL lap 3") == "phase BACKFILL lap 3"
    assert age_class(30) == "27 h - 3 days" and age_class(100) == "3 - 9 days" and age_class(500) == "9 - 35 days" and age_class(2000) == "over 5 weeks" and age_class(None) == "no age"
    assert cadence(["cron(40 3 * * ? *)"]) == "daily" and cadence(["cron(0 22 ? * FRI *)"]) == "weekly" and cadence(["cron(5 3 1 * ? *)"]) == "monthly" and cadence([]) is None
    assert cadence(["cron(0 22 ? * FRI *)", "rate(24 hours)"]) == "daily" and cadence(["rate(7 days)"]) == "weekly" and cadence(["cron(0 21 ? * MON-FRI *)"]) == "daily" and cadence(["cron(0 21 ? * FRI,SAT *)"]) == "weekly"


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report
    import cadence_apply as ca

    selftest()
    cfg = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=60, max_pool_connections=64)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    cw = boto3.client("cloudwatch", region_name=REGION, config=cfg)
    logs = boto3.client("logs", region_name=REGION, config=cfg)
    events = boto3.client("events", region_name=REGION, config=cfg)
    scheduler = boto3.client("scheduler", region_name=REGION, config=cfg)
    now = datetime.now(timezone.utc)

    def raw(key):
        try:
            b = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
        except Exception:  # noqa: BLE001
            return None
        if b[:2] == b"\x1f\x8b":
            try:
                b = gzip.decompress(b)
            except Exception:  # noqa: BLE001
                pass
        return b

    def jget(key):
        b = raw(key)
        try:
            return json.loads(b) if b else {}
        except Exception:  # noqa: BLE001
            return {}

    def metric(fn, name, days, period=86400):
        res = cw.get_metric_statistics(Namespace="AWS/Lambda", MetricName=name, Dimensions=[{"Name": "FunctionName", "Value": fn}],
                                       StartTime=now - timedelta(days=days), EndTime=now, Period=period, Statistics=["Sum"])
        return res.get("Datapoints", [])

    with report("ops_6876_walkers_and_real_staleness") as r:
        r.heading("ops 6876 - the slow-source walkers, and which stale feeds are really late (read-only)")
        snap = ca.snapshot_triggers(events, scheduler, log=lambda *_a, **_k: None)
        trig = defaultdict(list)
        for t in snap:
            if t.get("fn"):
                trig[t["fn"]].append(t)
        fan = json.loads((ROOT / "config" / "fanout-manifest.json").read_text(encoding="utf-8"))
        ticked = set()
        for members in (fan.get("ticks") or {}).values():
            for m in (members if isinstance(members, list) else []):
                ticked.add(m if isinstance(m, str) else (m.get("function") or m.get("fn") or m.get("name")))

        r.section("A. The walkers")
        for fn in WALKERS:
            r.log("%s" % fn)
            for t in trig.get(fn, []):
                shape = "?"
                try:
                    if t["kind"] == "sched":
                        x = scheduler.get_schedule(Name=t["name"], GroupName=t.get("group") or "default")
                        shape = payload_shape((x.get("Target") or {}).get("Input") or "")
                    else:
                        tg = events.list_targets_by_rule(Rule=t["name"], EventBusName=t.get("bus") or "default").get("Targets", [])
                        shape = " / ".join(payload_shape(x.get("Input") or "") for x in tg) or "{}"
                except Exception as e:  # noqa: BLE001
                    shape = "(could not read: %s)" % type(e).__name__
                r.log("  trigger %s %s | %s | %s | payload fields %s" % (t["kind"], t["name"], t.get("state"), F.per_day(t.get("expr")), shape))
            pts = metric(fn, "Invocations", 7, 3600)
            by = defaultdict(lambda: [0] * 24)
            for p in pts:
                ts = p["Timestamp"].astimezone(timezone.utc)
                by[ts.date().isoformat()][ts.hour] += int(p["Sum"])
            r.log("  runs per UTC hour (00..23):")
            for d in sorted(by):
                r.log("    %s  %s  = %d" % (d, " ".join("%2d" % n if n else " ." for n in by[d]), sum(by[d])))
            for name in ("_state/state.json", "manifest.json", "_state/inflight.json"):
                base = "data/warm/%s/" % fn.replace("justhodl-", "")
                doc = jget(base + name)
                if doc:
                    r.log("  %s: %s" % (name, "; ".join(scalars(doc))[:1500]))
            try:
                ev = logs.filter_log_events(logGroupName="/aws/lambda/" + fn, startTime=int((now - timedelta(hours=30)).timestamp() * 1000),
                                            filterPattern='?drain ?pass ?phase ?parked ?chain ?weekly ?done ?changed ?STEADY ?BACKFILL ?lap', limit=40).get("events", [])
                for e in ev[-14:]:
                    r.log("    log %s  %s" % (datetime.fromtimestamp(e["timestamp"] / 1000, timezone.utc).strftime("%m-%d %H:%M"), mask(e["message"].strip(), 230)))
            except Exception as e:  # noqa: BLE001
                r.log("  logs not read: %s" % type(e).__name__)
        ledger = jget("data/_state/chain-resumer/log.json")
        entries = ledger if isinstance(ledger, list) else (ledger.get("log") or ledger.get("resumes") or ledger.get("entries") or [])
        per = Counter()
        for e in entries:
            if isinstance(e, dict):
                per[(str(e.get("fn") or e.get("function") or e.get("engine")), str(e.get("at") or e.get("ts") or e.get("time") or "")[:10])] += 1
        r.log("chain resumer ledger: %d entries; resumes per engine and day: %s" % (len(entries), dict(sorted(per.items())[-24:])))
        last = jget("data/_state/chain-resumer/last-run.json")
        r.log("chain resumer last run: %s" % "; ".join(scalars(last))[:500])
        for prefix in ("data/_state/chain-parked/", "data/_state/chain-parked-held/"):
            keys = [o["Key"] for o in s3.list_objects_v2(Bucket=LIVE, Prefix=prefix, MaxKeys=200).get("Contents", [])]
            r.log("%s: %d tickets %s" % (prefix, len(keys), Counter(k.split("/")[3] for k in keys if len(k.split("/")) > 4).most_common(6)))
        r.log("a week of Lambda for the bank engines:")
        confs = {}
        for fn in BANKS:
            try:
                confs[fn] = lam.get_function_configuration(FunctionName=fn)
            except Exception:  # noqa: BLE001
                continue
            inv = sum(p["Sum"] for p in metric(fn, "Invocations", 7))
            dur = sum(p["Sum"] for p in metric(fn, "Duration", 7))
            err = sum(p["Sum"] for p in metric(fn, "Errors", 7))
            month = dur / 1000.0 * confs[fn].get("MemorySize", 128) / 1024.0 / 7 * 30 * LAMBDA_GBS
            r.log("  %-28s runs %4d (errors %d) x %5.0f s, %5d MB -> $%.2f a month | triggers: %s" % (
                fn, inv, err, dur / 1000.0 / inv if inv else 0, confs[fn].get("MemorySize", 0), month,
                "; ".join("%s [%s]" % (t["name"], F.per_day(t["expr"])) for t in trig.get(fn, []) if t.get("state") == "ENABLED") or "-"))

        r.section("B. The stale list, sorted into what it is")
        mon = jget("data/_freshness-monitor.json")
        results = mon.get("results") or []
        r.log("monitor report of %s: %d feeds checked, %d past their limit" % (mon.get("generated_at"), len(results), mon.get("n_stale") or 0))
        em = json.loads((ROOT / "engine-manifest.json").read_text(encoding="utf-8"))
        owner = defaultdict(list)
        for e in em.get("engines", []):
            for k in e.get("keys") or []:
                owner[k].append(e["engine"])
        live_fns = set()
        for page in lam.get_paginator("list_functions").paginate():
            live_fns |= {f["FunctionName"] for f in page["Functions"]}
        stale = [x for x in results if isinstance(x, dict) and x.get("status") not in (None, "FRESH", "OK", "UNKNOWN")]
        need = sorted({o for x in stale for o in owner.get(x.get("key"), []) if o in live_fns})

        def runs3(fn):
            return fn, sum(p["Sum"] for p in metric(fn, "Invocations", 3)), sum(p["Sum"] for p in metric(fn, "Errors", 3))
        with cf.ThreadPoolExecutor(max_workers=12) as ex:
            ran = {fn: (n, e) for fn, n, e in ex.map(runs3, need)}
        groups = defaultdict(list)
        for x in stale:
            key, age = x.get("key"), x.get("age_h")
            owners = owner.get(key, [])
            livers = [o for o in owners if o in live_fns]
            if not owners:
                g = "nobody claims it"
                who = "-"
            elif not livers:
                g, who = "no engine runs it", owners[0] + " (function gone)"
            else:
                o = max(livers, key=lambda f: ran.get(f, (0, 0))[0])
                exprs = [t["expr"] for t in trig.get(o, []) if t.get("state") == "ENABLED"]
                c = cadence(exprs) or ("daily" if o in ticked else None)
                n, errs = ran.get(o, (0, 0))
                who = "%s [%s, %d runs / %d errors in 3 days]" % (o, c or "no enabled trigger", n, errs)
                if c in ("weekly", "monthly", "other") and (age or 0) <= (240 if c == "weekly" else 900):
                    g = "slower by schedule"
                elif n == 0:
                    g = "no engine runs it"
                else:
                    g = "runs daily, file old" if x.get("status") != "INVALID" else "runs, content not readable by the monitor"
            groups[g].append((age or 0, key, x.get("status"), x.get("max_age_h"), who))
        for g in ("runs daily, file old", "runs, content not readable by the monitor", "slower by schedule", "no engine runs it", "nobody claims it"):
            rows = sorted(groups.get(g, []))
            ages = Counter(age_class(a) for a, *_ in rows)
            r.log("%s: %d feeds (%s)" % (g, len(rows), ", ".join("%s %d" % kv for kv in ages.most_common())))
            show = rows[:70] if g == "runs daily, file old" else rows[:14]
            for age, key, status, lim, who in show:
                r.log("    %-56s %-12s age %7.1f h (limit %s) | %s" % (str(key)[:56], status, age, lim, who))
        real = groups.get("runs daily, file old", [])
        by_engine = Counter(w.split(" [")[0] for *_x, w in real)
        r.log("engines behind the really-late feeds: %s" % ", ".join("%s %d" % kv for kv in by_engine.most_common(40)))
        failing = sorted({w.split(" [")[0] for *_x, w in real if re.search(r"/ ([1-9]\d*) errors", w)})
        r.log("of those, engines with errors in the last 3 days: %s" % (failing or "none"))
        r.kv(feeds_checked=len(results), past_limit=len(stale), really_late=len(real), slower_by_schedule=len(groups.get("slower by schedule", [])),
             no_engine_runs_it=len(groups.get("no engine runs it", [])), nobody_claims=len(groups.get("nobody claims it", [])), engines_behind_late_feeds=len(by_engine))
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
