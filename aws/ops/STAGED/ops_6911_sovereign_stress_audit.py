"""ops 6911 - READ-ONLY audit before building: the CISS page, the sovereign and stress pages, their engines and data.

Khalid 2026-11-22 09:36 ET: "now add an icon for sovereign stress: this engine should help: https://justhodl.ai/ciss.html
you may need to enhance that engine even further and also you may need to fix some things that dont work in that
engine ... most of this engine data is pulled from the ecb data source".

Nothing is written.  Measured:
  1. each page in a real browser - errors, failed requests, what it shows and where it shows nothing;
  2. the documents behind them - shape, dates, how many series qualify and why the rest do not;
  3. the engines - schedule, runs and errors of the last 14 days, last log lines that look like trouble;
  4. the chart's button bar as it is live.
"""
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
import gzip
import importlib.util
import json
import re
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
LIVE, BASE = "justhodl-dashboard-live", "https://justhodl.ai"
UA = "Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome"
PAGES = ("/ciss.html", "/sovereign-stress.html", "/systemic-stress.html", "/global-sovereign.html", "/sovereign.html", "/euro-fragmentation.html", "/global-stress.html", "/stress.html")
ENGINES = ("justhodl-ciss-stress", "justhodl-ciss-ai", "justhodl-sovereign-stress", "justhodl-systemic-stress", "justhodl-global-sovereign", "justhodl-sovereign-fiscal", "justhodl-global-stress")
PREFIXES = ("data/ciss", "data/sovereign", "data/systemic", "data/global-sov", "data/global-stress", "data/euro-frag", "data/europe", "data/periphery", "data/datasets/CISS", "data/datasets/CLIFS")
READ = r"""() => { const t = document.body ? document.body.innerText : ''; const flat = t.replace(/\s+/g, ' ');
  const count = (re) => (flat.match(re) || []).length;
  return { title: document.title, chars: flat.length, head: flat.slice(0, 420),
    marks: { unavailable: count(/unavailable/gi), pending: count(/pending/gi), dash: count(/—/g), nan: count(/\bNaN\b/g), undef: count(/\bundefined\b/g), loading: count(/loading|checking source/gi), error: count(/\berror\b|failed/gi), long_decimal: count(/\d\.\d{9,}/g) },
    tables: Array.from(document.querySelectorAll('table')).map(x => x.querySelectorAll('tbody tr').length).slice(0, 8), svgs: document.querySelectorAll('svg').length, canvases: document.querySelectorAll('canvas').length,
    ox: document.documentElement.scrollWidth - document.documentElement.clientWidth }; }"""
CISS = r"""() => { const g = (id) => { const n = document.getElementById(id); return n ? n.innerText.replace(/\s+/g, ' ').trim() : null; };
  const rows = Array.from(document.querySelectorAll('#ciss-table-body tr'));
  const cells = rows.map(r => Array.from(r.children).map(c => c.innerText.replace(/\s+/g, ' ').trim()));
  const quality = {}; cells.forEach(c => { quality[c[3]] = (quality[c[3]] || 0) + 1; });
  return { status: g('ciss-status'), read: g('ciss-read'), narrative: g('ciss-narrative'), reconcile: g('ciss-reconcile'), rows: rows.length, quality: quality, first: cells.slice(0, 6), last: cells.slice(-3),
    noValue: cells.filter(c => /—|Unavailable/i.test(c[1] || '')).length, aux: Array.from(document.querySelectorAll('#ciss-aux article, section[aria-labelledby="aux-title"] article')).map(a => a.innerText.replace(/\s+/g, ' ').slice(0, 170)),
    options: Array.from(document.querySelectorAll('#ciss-category option')).map(o => o.textContent) }; }"""


def fetch(path, timeout=60):
    with urllib.request.urlopen(urllib.request.Request(BASE + path, headers={"User-Agent": UA, "Cache-Control": "no-cache"}), timeout=timeout) as resp:
        return resp.read()


def shape(v, depth=0):
    """A short description of a JSON value."""
    if isinstance(v, dict):
        return "{%d keys: %s}" % (len(v), ", ".join(list(v)[:14])) if depth else {k: shape(x, 1) for k, x in list(v.items())[:40]}
    if isinstance(v, list):
        return "[%d] %s" % (len(v), (shape(v[0], 1) if v else ""))
    s = json.dumps(v)
    return s if len(s) < 70 else s[:67] + "..."


def selftest():
    assert shape({"a": [1, 2], "b": {"c": 1}}) == {"a": "[2] 1", "b": "{1 keys: c}"} and shape("x" * 100, 1).endswith("...")


def main():
    selftest()
    import boto3
    from ops_report import report
    s3 = boto3.client("s3", region_name="us-east-1")
    lam = boto3.client("lambda", region_name="us-east-1")
    cw = boto3.client("cloudwatch", region_name="us-east-1")
    logs = boto3.client("logs", region_name="us-east-1")
    events = boto3.client("events", region_name="us-east-1")
    spec = importlib.util.spec_from_file_location("ops6846", str(sorted(Path(__file__).resolve().parent.glob("ops_6846_*.py"))[0]))
    A = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(A)
    now = datetime.now(timezone.utc)
    stamp = int(time.time())

    def jget(key):
        o = s3.get_object(Bucket=LIVE, Key=key)
        raw = o["Body"].read()
        return json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw), o["LastModified"], len(raw)

    with report("ops_6911_sovereign_stress_audit") as r:
        r.heading("ops 6911 - sovereign stress: the pages, engines and data as they are (read-only), %s" % now.isoformat(timespec="minutes"))

        r.section("1. The pages, in a real browser")
        sync_playwright = A.ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = A.launch(p, r.log)
            for path in PAGES:
                ctx = browser.new_context(viewport={"width": 1366, "height": 900}, user_agent=UA)
                page = ctx.new_page()
                errors, bad, asked = [], [], []
                page.on("pageerror", lambda e, errors=errors: errors.append(str(e)[:160]))
                page.on("console", lambda m, errors=errors: errors.append("console: " + m.text[:140]) if m.type == "error" else None)
                page.on("response", lambda resp, bad=bad, asked=asked: (asked.append(resp.url) if "/data/" in resp.url else None, bad.append("%s %s" % (resp.status, resp.url.replace(BASE, "")[:110])) if resp.status >= 400 else None))
                try:
                    resp = page.goto("%s%s?nocache=%d" % (BASE, path, stamp), wait_until="domcontentloaded", timeout=60000)
                    page.wait_for_timeout(9000)
                    d = page.evaluate(READ)
                    r.log("%s -> HTTP %s | %s | %d chars | tables %s, %d svg, %d canvas | overflow %s px" % (path, resp.status if resp else "-", d["title"][:60], d["chars"], d["tables"], d["svgs"], d["canvases"], d["ox"]))
                    r.log("    marks %s" % json.dumps(d["marks"]))
                    r.log("    data asked: %s" % sorted({u.replace(BASE, "").split("?")[0] for u in asked})[:14])
                    r.log("    errors %s | failed requests %s" % (errors[:4], bad[:6]))
                    r.log("    text: %s" % d["head"])
                    if path == "/ciss.html":
                        c = page.evaluate(CISS)
                        r.log("    status: %s" % c["status"])
                        r.log("    read: %s | reconcile: %s" % (c["read"], c["reconcile"]))
                        r.log("    narrative: %s" % (c["narrative"] or "")[:300])
                        r.log("    table: %d rows, %d without a value | quality %s | families %s" % (c["rows"], c["noValue"], json.dumps(c["quality"]), c["options"]))
                        for row in c["first"] + c["last"]:
                            r.log("      %s" % " | ".join(x[:60] for x in row))
                        for a in c["aux"]:
                            r.log("    aux: %s" % a)
                        try:
                            page.evaluate("() => { const b = document.querySelector('#ciss-table-body tr button, #ciss-table-body tr a, #ciss-table-body tr'); if (b) b.click(); }")
                            page.wait_for_timeout(2500)
                            dlg = page.evaluate("() => { const d = document.getElementById('ciss-detail'); return { open: !!(d && d.open), text: d ? d.innerText.replace(/\\s+/g, ' ').slice(0, 600) : '', svgs: d ? d.querySelectorAll('svg').length : 0 }; }")
                            r.log("    detail dialog: open %s, %d svg | %s" % (dlg["open"], dlg["svgs"], dlg["text"]))
                        except Exception as e:  # noqa: BLE001
                            r.log("    detail dialog: %s" % str(e)[:120])
                except Exception as e:  # noqa: BLE001
                    r.log("%s -> %s %s | errors %s | failed %s" % (path, type(e).__name__, str(e)[:120], errors[:3], bad[:4]))
                ctx.close()
            ctx = browser.new_context(viewport={"width": 1600, "height": 900}, user_agent=UA)
            page = ctx.new_page()
            try:
                page.goto("%s/chart.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded", timeout=60000)
                page.wait_for_selector("#btn-repo", timeout=60000, state="attached")
                bar = page.evaluate("() => Array.from(document.querySelectorAll('#tfbar button.wsico')).map(b => [b.id, b.innerText.replace(/\\s+/g, ' ').trim(), b.title.slice(0, 50)])")
                r.section("4. The chart's desk buttons, in order")
                for b in bar:
                    r.log("  %-16s %-18s %s" % tuple(b))
            except Exception as e:  # noqa: BLE001
                r.log("chart.html: %s" % str(e)[:160])
            ctx.close()
            browser.close()

        r.section("2. The documents")
        seen = []
        for prefix in PREFIXES:
            page_ = s3.list_objects_v2(Bucket=LIVE, Prefix=prefix, MaxKeys=40)
            for o in page_.get("Contents", []):
                if o["Key"].count("/") <= 2 or "datasets/" in o["Key"]:
                    seen.append((o["Key"], o["Size"], o["LastModified"]))
            if page_.get("IsTruncated"):
                r.log("  %s...: more than 40 objects" % prefix)
        for key, size, when in seen[:60]:
            r.log("  %-58s %9d bytes  written %s (%d h ago)" % (key, size, when.strftime("%Y-%m-%d %H:%M"), (now - when).total_seconds() // 3600))
        for key in ("data/ciss-stress.json", "data/ciss-ai.json", "data/sovereign-stress.json", "data/systemic-stress.json", "data/global-sovereign.json", "data/sovereign-fiscal.json", "data/global-stress.json"):
            try:
                doc, when, size = jget(key)
            except Exception as e:  # noqa: BLE001
                r.log("%s: %s" % (key, type(e).__name__))
                continue
            r.log("%s (%d bytes, written %s):" % (key, size, when.strftime("%Y-%m-%d %H:%M")))
            for k, v in shape(doc).items():
                r.log("    %-26s %s" % (k, str(v)[:330]))
            if key == "data/ciss-stress.json":
                series = doc.get("series") or {}
                items = list(series.values()) if isinstance(series, dict) else series
                r.log("  series: %d | keys of one: %s" % (len(items), list(items[0])[:40] if items else None))
                if items:
                    r.log("  one series: %s" % json.dumps({k: v for k, v in items[0].items() if not isinstance(v, (list, dict))})[:900])
                    for field in ("category", "family", "frequency", "area", "country", "measure", "status"):
                        if any(field in x for x in items):
                            r.log("  by %s: %s" % (field, dict(Counter(str(x.get(field)) for x in items).most_common(30))))
                    q = Counter(str((x.get("quality") or {}).get("status") if isinstance(x.get("quality"), dict) else x.get("quality")) for x in items)
                    r.log("  by quality: %s" % dict(q))
                    reasons = Counter()
                    for x in items:
                        ql = x.get("quality") if isinstance(x.get("quality"), dict) else {}
                        for why in (ql.get("reasons") or ql.get("missing") or []):
                            reasons[str(why)[:70]] += 1
                    r.log("  reasons a series does not qualify: %s" % dict(reasons.most_common(12)))
                    lasts = Counter(str(x.get("latest_date") or x.get("as_of")) for x in items)
                    r.log("  latest dates: %s" % dict(sorted(lasts.items(), reverse=True)[:12]))
                    for x in items:
                        ident = str(x.get("key") or x.get("id") or x.get("series_key") or "")
                        if "SOV" in ident:
                            r.log("    %-36s %-10s %-12s %s | %s" % (ident, x.get("latest_date"), str(x.get("latest_decimal") or x.get("latest"))[:12], (x.get("quality") or {}).get("status") if isinstance(x.get("quality"), dict) else x.get("quality"), str(x.get("label") or x.get("title") or "")[:60]))
                for k in ("errors", "auxiliary", "aux", "headline", "reconciliation", "quality", "coverage"):
                    if k in doc:
                        r.log("  %s: %s" % (k, json.dumps(doc[k])[:700]))

        r.section("3. The engines")
        for fn in ENGINES:
            try:
                cfg = lam.get_function_configuration(FunctionName=fn)
            except Exception as e:  # noqa: BLE001
                r.log("%s: %s" % (fn, type(e).__name__))
                continue
            stats = {}
            for metric in ("Invocations", "Errors"):
                pts = cw.get_metric_statistics(Namespace="AWS/Lambda", MetricName=metric, Dimensions=[{"Name": "FunctionName", "Value": fn}], StartTime=now - timedelta(days=14), EndTime=now, Period=86400, Statistics=["Sum"]).get("Datapoints", [])
                stats[metric] = int(sum(x["Sum"] for x in pts))
                if metric == "Invocations":
                    stats["days"] = sorted(x["Timestamp"].strftime("%m-%d") for x in pts if x["Sum"])[-6:]
            rules = []
            try:
                for name in events.list_rule_names_by_target(TargetArn=cfg["FunctionArn"]).get("RuleNames", []):
                    rule = events.describe_rule(Name=name)
                    rules.append("%s %s %s" % (name, rule.get("ScheduleExpression"), rule.get("State")))
            except Exception as e:  # noqa: BLE001
                rules.append(type(e).__name__)
            r.log("%s: updated %s | 14 days: %s | rules %s" % (fn, (cfg.get("LastModified") or "")[:16], json.dumps(stats), rules))
            try:
                st = logs.describe_log_streams(logGroupName="/aws/lambda/" + fn, orderBy="LastEventTime", descending=True, limit=1)["logStreams"]
                if st:
                    ev = logs.get_log_events(logGroupName="/aws/lambda/" + fn, logStreamName=st[0]["logStreamName"], limit=40, startFromHead=False)["events"]
                    lines = [e["message"].strip()[:230] for e in ev if not e["message"].startswith(("START", "END", "INIT_START"))]
                    r.log("    last run %s: %s" % (datetime.fromtimestamp(st[0].get("lastEventTimestamp", 0) / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M"), " || ".join(lines[-5:])[:900]))
            except Exception as e:  # noqa: BLE001
                r.log("    logs: %s" % type(e).__name__)
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
