"""ops 6971 - read-only audit of the twelve engines Khalid chose for a tab on chart.html.

Khalid 2026-11-26: "Add these first and audit their engine before you add them and see if you can expand it to cover
more data and that all data from the engine is displaying on the page". The repository holds each engine's code and
each page's code; it cannot say how the engines really run or what their documents really hold. This op gathers both:

  1. each function as deployed (memory, time limit, runtime, last change, names of its settings - never a value);
  2. fourteen days of runs, errors, time taken, and the newest error line of any engine that failed;
  3. every document the twelve pages read: age, size, every field path with how many times it occurs and one sample,
     and a cut-down copy (lists and large maps cut to 25 entries, security identifiers dropped) for the page lab;
  4. the company-research cache: how many tickers are kept and how old they are.

Nothing is invoked and nothing is written. Safe to run again.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import base64
import gzip
import json
import re
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
NAME = "ops_6971_chart_tabs_engine_audit"
REGION, LIVE = "us-east-1", "justhodl-dashboard-live"

ENGINES = [
    "justhodl-equity-research", "justhodl-insider-trades", "justhodl-insider-cluster-scanner",
    "justhodl-options-analytics", "justhodl-opex-calendar", "justhodl-stock-valuations", "justhodl-corp-bonds",
    "justhodl-plumbing-aggregator", "justhodl-liquidity-agent", "justhodl-liquidity-flow", "justhodl-canary-warroom",
    "justhodl-warroom-weights", "justhodl-breadth-thrust", "justhodl-trend-reversal", "justhodl-catalyst-calendar",
    "justhodl-onchain-ratios",
]
DOCS = [
    "data/insider-trades.json", "data/insider-trades-full.json", "data/insider-clusters.json",
    "data/insider-clusters-names.json", "data/insider-sell-cluster.json", "data/insider-industry-cluster.json",
    "data/options-analytics.json", "data/options-analytics-iv-history.json", "data/opex-calendar.json",
    "data/stock-valuations.json", "data/valuations-decisive-call.json", "data/corp-bonds.json",
    "data/plumbing-stress.json", "data/plumbing-history.json", "liquidity-data.json", "data/liquidity-flow.json",
    "data/canary-warroom.json", "data/warroom-history.json", "data/warroom-replay.json", "data/warroom-weights.json",
    "data/canaries-decisive-call.json", "data/breadth-thrust.json", "data/breadth-history.json",
    "data/trend-reversal.json", "data/trend-reversal-history.json", "data/catalyst-calendar.json",
    "data/onchain-ratios.json", "data/cryptoquant-onchain.json",
]
RESEARCH_PREFIXES = ["equity-research/", "data/equity-research/"]
RESEARCH_SAMPLES = ["AAPL", "NVDA"]
CUT = 25
HIDE = {"cusip", "isin", "k", "underlier_id"}
MAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
SECRET = re.compile(r"(?i)(api[_-]?key|apikey|token|secret|password|signature|authorization)(\"?\s*[=:]\s*\"?)[^&\s\"',}]+")
LONG = re.compile(r"[A-Za-z0-9+/_-]{32,}")


def scrub(text):
    """A log line with anything that could be a credential or an address taken out."""
    text = SECRET.sub(lambda m: m.group(1) + m.group(2) + "***", str(text))
    text = MAIL.sub("***", text)
    return LONG.sub("***", text)


def is_map(d):
    """A dict used as a table (many keys, one kind of value): tickers, dates, series ids."""
    if len(d) < 40:
        return False
    kinds = {type(v).__name__ for v in d.values()}
    return len(kinds) <= 2


def walk(node, path, out):
    """Every field path with its count and one sample. Lists are '[]', table keys are '*'."""
    if isinstance(node, dict):
        table = is_map(node)
        slot = out.setdefault(path or "$", [0, None])
        slot[0] += 1
        if table and slot[1] is None:
            slot[1] = "table of %d" % len(node)
        for k, v in node.items():
            if str(k).lower() in HIDE:
                continue
            walk(v, (path + "." if path else "") + ("*" if table else str(k)), out)
    elif isinstance(node, list):
        slot = out.setdefault(path + "[]", [0, None])
        slot[0] += sum(1 for v in node if not isinstance(v, dict))
        for v in node:
            if isinstance(v, (dict, list)):
                walk(v, path + "[]", out)
            elif slot[1] is None and v is not None:
                slot[1] = MAIL.sub("***", str(v))[:60]
    else:
        slot = out.setdefault(path or "$", [0, None])
        slot[0] += 1
        if slot[1] is None and node is not None and node != "":
            slot[1] = MAIL.sub("***", str(node))[:60]


def cut(node):
    """The same document with long lists and tables cut to CUT entries and identifiers dropped."""
    if isinstance(node, dict):
        items = [(k, v) for k, v in node.items() if str(k).lower() not in HIDE]
        if is_map(node):
            items = items[:CUT]
        return {k: cut(v) for k, v in items}
    if isinstance(node, list):
        return [cut(v) for v in node[:CUT]]
    if isinstance(node, str):
        return MAIL.sub("***", node)
    return node


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    cfg = Config(retries={"max_attempts": 6, "mode": "standard"}, read_timeout=120)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    cw = boto3.client("cloudwatch", region_name=REGION, config=cfg)
    logs = boto3.client("logs", region_name=REGION, config=cfg)
    now = datetime.now(timezone.utc)
    pack = {"at": now.isoformat(timespec="seconds"), "engines": {}, "docs": {}, "research": {}}
    with report(NAME) as r:
        r.heading("6971 - the twelve engines chosen for a chart tab, as they run (read-only), %s" % now.isoformat(timespec="minutes"))

        r.section("1. Each function as deployed, and fourteen days of runs")
        for n in ENGINES:
            e = {}
            try:
                c = lam.get_function_configuration(FunctionName=n)
                e.update(memory=c.get("MemorySize"), timeout=c.get("Timeout"), runtime=c.get("Runtime"), changed=c.get("LastModified", "")[:19],
                         code=c.get("CodeSize"), settings=sorted((c.get("Environment") or {}).get("Variables", {}).keys()),
                         layers=len(c.get("Layers") or []), state=c.get("State"))
            except Exception as exc:  # noqa: BLE001
                e["config_error"] = "%s: %s" % (type(exc).__name__, str(exc)[:160])
            try:
                q = []
                for m, stat, tag in (("Invocations", "Sum", "i"), ("Errors", "Sum", "e"), ("Throttles", "Sum", "t"), ("Duration", "Average", "a"), ("Duration", "Maximum", "x")):
                    q.append({"Id": tag, "ReturnData": True, "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": m, "Dimensions": [{"Name": "FunctionName", "Value": n}]}, "Period": 86400, "Stat": stat}})
                resp = cw.get_metric_data(MetricDataQueries=q, StartTime=now - timedelta(days=14), EndTime=now)
                got = {x["Id"]: dict(zip([t.strftime("%m-%d") for t in x.get("Timestamps", [])], x.get("Values", []))) for x in resp["MetricDataResults"]}
                days = sorted(got.get("i", {}))
                e["days"] = {d: [int(got["i"].get(d, 0)), int(got.get("e", {}).get(d, 0)), round(got.get("a", {}).get(d, 0) / 1000.0, 1), round(got.get("x", {}).get(d, 0) / 1000.0, 1)] for d in days}
                runs = sum(v[0] for v in e["days"].values())
                errs = sum(v[1] for v in e["days"].values())
                secs = sum(v[0] * v[2] for v in e["days"].values())
                e.update(runs=runs, errors=errs, throttles=int(sum(got.get("t", {}).values())), seconds=round(secs, 1),
                         usd_month=round(secs / 14.0 * 30 * (e.get("memory") or 0) / 1024.0 * 0.0000166667, 3))
            except Exception as exc:  # noqa: BLE001
                e["metrics_error"] = "%s: %s" % (type(exc).__name__, str(exc)[:160])
            if e.get("errors"):
                try:
                    ev = logs.filter_log_events(logGroupName="/aws/lambda/" + n, startTime=int((now - timedelta(days=14)).timestamp() * 1000),
                                                filterPattern='?ERROR ?Traceback ?"Task timed out" ?Exception', limit=40)
                    e["error_lines"] = [scrub(x["message"].strip())[:300] for x in ev.get("events", [])][-4:]
                except Exception as exc:  # noqa: BLE001
                    e["log_error"] = "%s: %s" % (type(exc).__name__, str(exc)[:160])
            pack["engines"][n] = e
            r.log("%s | %s MB, %s s, %s, changed %s | 14 d: %s runs, %s errors, %s s in all, about $%s a month | settings: %s" % (
                n.replace("justhodl-", ""), e.get("memory"), e.get("timeout"), e.get("runtime"), e.get("changed"), e.get("runs"), e.get("errors"),
                e.get("seconds"), e.get("usd_month"), ", ".join(e.get("settings") or []) or "none"))
            for line in e.get("error_lines", []):
                r.log("      error line: " + line)

        r.section("2. Every document the twelve pages read")
        for key in DOCS:
            d = {}
            try:
                o = s3.get_object(Bucket=LIVE, Key=key)
                raw = o["Body"].read()
                d.update(bytes=len(raw), written=o["LastModified"].isoformat(timespec="seconds"), hours=round((now - o["LastModified"]).total_seconds() / 3600.0, 1),
                         cache=o.get("CacheControl"))
                if raw[:2] == b"\x1f\x8b":
                    raw = gzip.decompress(raw)
                doc = json.loads(raw)
                paths = {}
                walk(doc, "", paths)
                d["paths"] = dict(sorted(paths.items())[:4000])
                d["cut"] = cut(doc)
                top = list(doc.keys())[:40] if isinstance(doc, dict) else ["list of %d" % len(doc)]
                r.log("%s | %s bytes, written %s h ago, %d field paths | top: %s" % (key, format(d["bytes"], ","), d["hours"], len(paths), ", ".join(map(str, top))))
            except Exception as exc:  # noqa: BLE001
                d["error"] = "%s: %s" % (type(exc).__name__, str(exc)[:160])
                r.log("%s | NOT READ: %s" % (key, d["error"]))
            pack["docs"][key] = d

        r.section("3. The company-research cache")
        for prefix in RESEARCH_PREFIXES:
            try:
                ages, n, newest, sizes = [], 0, None, 0
                for page in s3.get_paginator("list_objects_v2").paginate(Bucket=LIVE, Prefix=prefix, PaginationConfig={"MaxItems": 60000}):
                    for o in page.get("Contents", []):
                        if not o["Key"].endswith(".json"):
                            continue
                        n += 1
                        sizes += o["Size"]
                        h = (now - o["LastModified"]).total_seconds() / 3600.0
                        ages.append(h)
                        newest = h if newest is None else min(newest, h)
                info = {"documents": n, "bytes": sizes, "within_26h": sum(a < 26 for a in ages), "within_7d": sum(a < 168 for a in ages),
                        "older_30d": sum(a >= 720 for a in ages), "newest_hours": None if newest is None else round(newest, 1)}
                pack["research"][prefix] = info
                r.log("%s | %s" % (prefix, json.dumps(info)))
                if not n:
                    continue
                for t in RESEARCH_SAMPLES:
                    try:
                        o = s3.get_object(Bucket=LIVE, Key="%s%s.json" % (prefix, t))
                        raw = o["Body"].read()
                        size = len(raw)
                        if raw[:2] == b"\x1f\x8b":
                            raw = gzip.decompress(raw)
                        doc = json.loads(raw)
                        paths = {}
                        walk(doc, "", paths)
                        pack["docs"]["%s%s.json" % (prefix, t)] = {"bytes": size, "written": o["LastModified"].isoformat(timespec="seconds"),
                                                                 "hours": round((now - o["LastModified"]).total_seconds() / 3600.0, 1),
                                                                 "paths": dict(sorted(paths.items())[:6000]), "cut": cut(doc) if t == "AAPL" else None}
                        r.log("   %s | %s bytes, %d field paths, schema %s, %d top-level blocks" % (t, format(size, ","), len(paths), doc.get("schema") or doc.get("schema_version") or doc.get("version"), len(doc)))
                    except Exception as exc:  # noqa: BLE001
                        r.log("   %s | not read: %s: %s" % (t, type(exc).__name__, str(exc)[:120]))
            except Exception as exc:  # noqa: BLE001
                r.log("%s | not listed: %s: %s" % (prefix, type(exc).__name__, str(exc)[:160]))

        body = gzip.compress(json.dumps(pack, separators=(",", ":"), default=str).encode(), 9)
        b64 = base64.b64encode(body).decode("ascii")
        r.section("attachment")
        r.lines.append("<!-- FIXTURE audit %d bytes" % len(body))
        r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
        r.lines.append("FIXTURE-END -->")
        r.kv(engines=len(pack["engines"]), documents=len(pack["docs"]), attachment_bytes=len(body))
        r.ok("gathered; nothing invoked, nothing written")


if __name__ == "__main__":
    main()
