"""ops 6996 - read-only: the history a test of the canaries needs, cut from the warehouse; and why the weights engine stopped.

Khalid, 2026-11-27 11:06 ET: "... test it against historical data to calibrate it".

Every series the macro grid and the plumbing aggregator turn into a canary is asked of the site's own resolver (the
warehouse), one request at a time. Daily series are kept as the last observation of each ISO week; the S&P 500 is kept
daily (falls are measured on closes). The CISS series come from the CISS engine's own document, the feed histories
from the feeds' own documents. The cut goes into the report as one fixture so the record can be built and tested
offline on real data.

Also read: the monthly schedule of justhodl-warroom-weights (no run since 2026-10-01) and anything holding it.

Nothing is invoked and nothing is written. Safe to run again.
"""
from datetime import datetime, timezone
from pathlib import Path
import ast
import base64
import gzip
import json
import re
import sys
import time
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
NAME = "ops_6996_canaries_history_cut"
REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
RESOLVER = "https://justhodl-data-proxy.raafouis.workers.dev/series?id="
EXTRA = ["VIXCLS", "VXVCLS", "BAMLH0A0HYM2", "BAMLC0A0CM", "USREC", "NASDAQCOM", "SP500", "T10Y3M", "NFCI", "STLFSI4", "DGS2", "DGS10", "ICSA",
         "DFF", "IORB", "DTB3", "SOFR", "TEDRATE", "DTWEXBGS", "TWEXB", "WMTSECL1"]
_MASK = [(re.compile(r"(api_?key=)[^&\s\"']*", re.I), r"\1***"), (re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+"), "***")]


def clean(text):
    text = str(text)
    for pat, rep in _MASK:
        text = pat.sub(rep, text)
    return text


def literal_list(path, name):
    """A module-level list of dict(...) or {...} literals, read without importing the engine."""
    tree = ast.parse(Path(path).read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == name:
            out = []
            for el in node.value.elts:
                if isinstance(el, ast.Call):
                    out.append({k.arg: ast.literal_eval(k.value) for k in el.keywords})
                else:
                    out.append(ast.literal_eval(el))
            return out
    return []


def ymd(text):
    t = str(text)[:10]
    return int(t[:4]) * 10000 + int(t[5:7]) * 100 + int(t[8:10])


def sig6(v):
    return float("%.6g" % v)


def weekly(points):
    """Last observation of each ISO week, oldest first."""
    out, last = [], None
    for d, v in points:
        y, w, _ = datetime(int(d[:4]), int(d[5:7]), int(d[8:10])).isocalendar()
        key = (y, w)
        if key == last:
            out[-1] = (d, v)
        else:
            out.append((d, v))
            last = key
    return out


def freq_of(points):
    if len(points) < 3:
        return "?"
    gaps = sorted((datetime.fromisoformat(b[0]) - datetime.fromisoformat(a[0])).days for a, b in zip(points[-60:], points[-59:]))
    g = gaps[len(gaps) // 2]
    return "d" if g <= 4 else "w" if g <= 10 else "m" if g <= 45 else "q"


def main():
    import boto3
    from ops_report import report

    s3 = boto3.client("s3", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION)
    now = datetime.now(timezone.utc)

    with report(NAME) as r:
        def say(text):
            r.log(clean(text))

        r.heading("6996 - the history cut for the canaries' record, and the weights engine's schedule (read-only), %s" % now.isoformat(timespec="minutes"))

        # ── 1. why the weights engine stopped ───────────────────────────────────────────────────────────────────
        r.section("1. justhodl-warroom-weights: its schedule and anything holding it")
        fn = "justhodl-warroom-weights"
        try:
            sch = boto3.client("scheduler", region_name=REGION)
            try:
                g = sch.get_schedule(Name="justhodl-warroom-weights-monthly", GroupName="default")
                say("scheduler schedule: state %s, %s (%s), target %s, role %s, last modified %s" % (
                    g.get("State"), g.get("ScheduleExpression"), g.get("ScheduleExpressionTimezone"), str((g.get("Target") or {}).get("Arn"))[-60:],
                    str((g.get("Target") or {}).get("RoleArn"))[-50:], g.get("LastModificationDate")))
            except Exception as e:  # noqa: BLE001
                say("scheduler schedule justhodl-warroom-weights-monthly: %s" % str(e)[:200])
            names = []
            for page in sch.get_paginator("list_schedules").paginate(NamePrefix="justhodl-warroom"):
                names += [(x.get("Name"), x.get("State"), x.get("GroupName")) for x in page.get("Schedules") or []]
            say("schedules named justhodl-warroom*: %s" % json.dumps(names))
        except Exception as e:  # noqa: BLE001
            say("scheduler not read: %s" % str(e)[:200])
        try:
            ev = boto3.client("events", region_name=REGION)
            rules = ev.list_rules(NamePrefix="justhodl-warroom").get("Rules") or []
            say("event rules named justhodl-warroom*: %s" % json.dumps([[x.get("Name"), x.get("ScheduleExpression"), x.get("State")] for x in rules]))
        except Exception as e:  # noqa: BLE001
            say("event rules not read: %s" % str(e)[:160])
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            say("function: state %s, last update %s, modified %s, runtime %s, handler %s, env names %s" % (
                c.get("State"), c.get("LastUpdateStatus"), c.get("LastModified"), c.get("Runtime"), c.get("Handler"), sorted((c.get("Environment") or {}).get("Variables") or {})))
            try:
                rc = lam.get_function_concurrency(FunctionName=fn)
                say("reserved concurrency: %s" % rc.get("ReservedConcurrentExecutions"))
            except Exception as e:  # noqa: BLE001
                say("reserved concurrency not read: %s" % str(e)[:120])
            try:
                pol = json.loads(lam.get_policy(FunctionName=fn)["Policy"])
                say("resource policy: %s" % json.dumps([[s.get("Sid"), (s.get("Principal") or {}).get("Service"), str(((s.get("Condition") or {}).get("ArnLike") or {}).get("AWS:SourceArn"))[-70:]]
                                                       for s in pol.get("Statement") or []]))
            except Exception as e:  # noqa: BLE001
                say("resource policy: %s" % str(e)[:160])
        except Exception as e:  # noqa: BLE001
            say("function not read: %s" % str(e)[:200])
        for key in ("spend-governor/state.json", "spend-governor/holds.json", "spend-governor/caps.json"):
            try:
                d = json.loads(s3.get_object(Bucket="justhodl-ops-private-857687956942", Key=key)["Body"].read())
                txt = json.dumps(d)
                i = txt.find("warroom-weights")
                say("governor %s: %s" % (key, ("mentions it: ..." + txt[max(0, i - 160):i + 200]) if i >= 0 else "does not mention the weights engine (%d bytes)" % len(txt)))
            except Exception as e:  # noqa: BLE001
                say("governor %s: %s" % (key, str(e)[:100]))
        for cfg in ("config/retired-engines.json", "config/cadence-floor.json", "config/trigger-limits.json"):
            try:
                txt = (ROOT / cfg).read_text(encoding="utf-8")
                i = txt.find("warroom-weights")
                say("%s: %s" % (cfg, ("..." + txt[max(0, i - 200):i + 260].replace("\n", " ")) if i >= 0 else "does not mention it"))
            except Exception as e:  # noqa: BLE001
                say("%s: %s" % (cfg, str(e)[:100]))

        # ── 2. the cut ──────────────────────────────────────────────────────────────────────────────────────────
        r.section("2. The series, from the warehouse")
        grid = literal_list(ROOT / "aws/lambdas/justhodl-canary-grid/source/lambda_function.py", "SIGNALS")
        plumb = literal_list(ROOT / "aws/lambdas/justhodl-plumbing-aggregator/source/lambda_function.py", "INDICATORS")
        say("definitions read from the engines' own sources: %d grid signals, %d plumbing indicators" % (len(grid), len(plumb)))
        fred_ids, feeds, other = [], [], []
        for sg in grid:
            for sid in (sg["fred"] if isinstance(sg["fred"], list) else [sg["fred"]]):
                if sid.startswith("feed:"):
                    feeds.append(sid[5:])
                elif sid.startswith(("spread:", "ratio:")):
                    fred_ids += sid.split(":", 1)[1].split(":")
                elif sid.startswith("sum:"):
                    fred_ids += re.findall(r"[A-Za-z0-9_]+", sid[4:])
                elif sid.startswith(("ecb:", "ecbspread:")) or "/" in sid:
                    other.append(sid)
                else:
                    fred_ids.append(sid)
        fred_ids += [p["id"] for p in plumb if p.get("source") == "FRED"] + EXTRA
        seen, ids = set(), []
        for x in fred_ids:
            if x not in seen:
                seen.add(x)
                ids.append(x)
        say("to ask: %d FRED series, %d feed histories; not asked (ECB / DBnomics, not behind the resolver): %s" % (len(ids), len(set(feeds)), json.dumps(sorted(set(other)))[:600]))

        cut = {"cut_at": now.isoformat(timespec="seconds"), "sampling": "daily series: last observation of each ISO week; others as published", "series": {}, "feeds": {}, "ciss": {}}
        missing = []
        for sid in ids:
            try:
                req = urllib.request.Request(RESOLVER + urllib.parse.quote("fred:" + sid, safe=""), headers={"User-Agent": "JustHodl-Ops/6996"})
                with urllib.request.urlopen(req, timeout=45) as resp:
                    p = json.loads(resp.read().decode("utf-8", "replace"))
                obs = p.get("obs") if isinstance(p, dict) else None
                pts = []
                for o in obs or []:
                    d, v = (o[0], o[1]) if isinstance(o, (list, tuple)) else (o.get("date"), o.get("value"))
                    try:
                        pts.append((str(d)[:10], float(v)))
                    except (TypeError, ValueError):
                        continue
                pts.sort()
                if len(pts) < 5:
                    missing.append(sid)
                    say("  %-22s no observations in the warehouse" % sid)
                    continue
                f = freq_of(pts)
                keep = weekly(pts) if f == "d" else pts
                cut["series"][sid] = {"f": f, "n_raw": len(pts), "name": str(p.get("name"))[:90], "p": [[ymd(d), sig6(v)] for d, v in keep]}
                say("  %-22s %s %6d obs %s to %s -> %d kept | %s" % (sid, f, len(pts), pts[0][0], pts[-1][0], len(keep), str(p.get("name"))[:60]))
            except Exception as e:  # noqa: BLE001
                missing.append(sid)
                say("  %-22s not answered: %s" % (sid, str(e)[:100]))
            time.sleep(0.25)
        say("FRED series kept %d of %d; missing %s" % (len(cut["series"]), len(ids), json.dumps(missing)))

        r.section("3. The feed histories and the CISS series")
        docs = {}
        for spec in sorted(set(feeds)):
            try:
                engine, path = spec.split(":", 1)
                if engine not in docs:
                    o = s3.get_object(Bucket=LIVE, Key="data/%s.json" % engine)
                    docs[engine] = (json.loads(o["Body"].read()), o["LastModified"])
                cur = docs[engine][0]
                for part in path.split("."):
                    cur = cur.get(part) if isinstance(cur, dict) else None
                pts = []
                for x in cur or []:
                    if isinstance(x, dict) and x.get("v") is not None:
                        pts.append(("%s-28" % str(x.get("p"))[:7], float(x["v"])))
                    elif isinstance(x, (list, tuple)) and len(x) == 2 and x[1] is not None:
                        pts.append((str(x[0])[:10], float(x[1])))
                pts.sort()
                if len(pts) < 5:
                    say("  feed %-46s no history" % spec)
                    continue
                f = freq_of(pts)
                keep = weekly(pts) if f == "d" else pts
                cut["feeds"][spec] = {"f": f, "n_raw": len(pts), "p": [[ymd(d), sig6(v)] for d, v in keep]}
                say("  feed %-46s %s %5d obs %s to %s -> %d kept (document written %s)" % (spec, f, len(pts), pts[0][0], pts[-1][0], len(keep), docs[engine][1].isoformat(timespec="minutes")))
            except Exception as e:  # noqa: BLE001
                say("  feed %-46s not read: %s" % (spec, str(e)[:100]))
        try:
            d = json.loads(s3.get_object(Bucket=LIVE, Key="data/ciss-stress.json")["Body"].read())
            ser = d.get("series") or []
            items = list(ser.values()) if isinstance(ser, dict) else ser
            for i, s in enumerate(items):
                if not isinstance(s, dict):
                    continue
                pts = [(str(a)[:10], float(b)) for a, b in (s.get("chart_points") or []) if b is not None]
                st = s.get("stats") or s
                cut["ciss"][str(i)] = {"label": str(s.get("label") or s.get("name") or s.get("key"))[:120], "key": s.get("key"), "area": s.get("area"),
                                       "category": s.get("category"), "pctile": st.get("pctile"), "latest": st.get("latest"), "f": "w",
                                       "p": [[ymd(a), sig6(b)] for a, b in pts]}
            say("CISS: %d series in the document's order; the first's fields %s; spans %s to %s" % (
                len(cut["ciss"]), sorted(items[0])[:40] if items else None,
                min((v["p"][0][0] for v in cut["ciss"].values() if v["p"]), default=None), max((v["p"][-1][0] for v in cut["ciss"].values() if v["p"]), default=None)))
        except Exception as e:  # noqa: BLE001
            say("CISS not read: %s" % str(e)[:160])

        r.section("4. The S&P 500, daily")
        for key, name in (("data/warm/tv-bars/universe/I__SPX.json.gz", "spx"), ("data/warm/tv-bars/universe/US__SPY.json.gz", "spy")):
            try:
                o = s3.get_object(Bucket=LIVE, Key=key)
                d = json.loads(gzip.decompress(o["Body"].read()))
                bars = d.get("bars") or []
                pts = []
                for b in bars:
                    try:
                        pts.append((datetime.fromtimestamp(b[0], timezone.utc).strftime("%Y-%m-%d"), float(b[4])))
                    except Exception:  # noqa: BLE001
                        continue
                pts.sort()
                cut[name] = {"f": "d", "source": str(d.get("source")), "symbol": d.get("symbol"), "p": [[ymd(a), sig6(b)] for a, b in pts]}
                say("%s: %d daily closes %s to %s (source %s, written %s); last close %s" % (key.split("/")[-1], len(pts), pts[0][0] if pts else None, pts[-1][0] if pts else None,
                                                                                    d.get("source"), o["LastModified"].isoformat(timespec="minutes"), pts[-1][1] if pts else None))
            except Exception as e:  # noqa: BLE001
                say("%s: %s" % (key, str(e)[:120]))

        text = json.dumps(cut, separators=(",", ":"))
        for pat, rep in _MASK:
            text = pat.sub(rep, text)
        raw = text.encode()
        b64 = base64.b64encode(gzip.compress(raw, 9)).decode()
        say("the cut: %d bytes, %d gzipped" % (len(raw), len(b64) * 3 // 4))
        r.log("<!-- FIXTURE canary-history-cut %d bytes gzip+base64" % len(raw))
        for i in range(0, len(b64), 3000):
            r.log(b64[i:i + 3000])
        r.log("FIXTURE-END -->")
        r.ok("cut made; nothing invoked, nothing written")


if __name__ == "__main__":
    main()
