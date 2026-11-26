"""ops 6961 - the money-flow engine: find it, read it, check it (read-only audit).

Khalid 2026-11-25: "I know i have a flow engine that show where the money is flowing in and flowing out of.
Find me that engine first and lets audit it".

The engine is justhodl-global-flow-desk (its own first line: "WHERE THE MONEY IS GOING (Khalid's mandate)"), page
global-flow-desk.html, document data/global-flow-desk.json. Since 2026-09 its handler runs a different compiler
(flow_desk_research, document 2.0.0) and the first one is kept in the file, uncalled.

This op changes nothing and invokes nothing. It reads:

  1. the function, its schedule, its runs and errors of the last three weeks, its last log lines (and its two feeders');
  2. the live document: how fresh, how many funds, what it says, what it no longer says;
  3. every fund's five-day figure again from the fund's own stored history, and every group total from its members;
  4. the same figures as the Inflows & outflows desk shows them (a second writer of the same source);
  5. every source the first engine read: is it still written, how old;
  6. what the first engine last said, and when;
  7. the page in a real browser, and the other flow pages beside it.
"""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext
from pathlib import Path
import base64
import gzip
import importlib.util
import json
import re
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
NAME = "ops_6961_money_flow_engine_audit"
REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
BASE = "https://justhodl.ai"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36 jh-ops-probe"
FN = "justhodl-global-flow-desk"
FEEDERS = {"justhodl-global-flow-desk": "justhodl-global-flow-desk-daily", "justhodl-etf-true-flows": "justhodl-etf-true-flows-daily",
           "justhodl-tic-flows": "justhodl-tic-flows-sched"}
CONTEXT_KEYS = ("etf-flows/daily.json", "data/dark-pool.json", "data/money-flow-state.json", "data/capital-flow-radar.json",
                "data/aaii-sentiment.json", "data/aaii.json", "data/stablecoin-flow.json", "data/sovereign-fiscal.json",
                "data/polygon-fx-regime.json", "data/capex-pulse.json", "data/structural-pre-signals.json", "data/structural-presignals.json")
SIBLINGS = ("data/etf-true-flows.json", "data/tic-flows.json", "data/capital-flow.json", "data/etf-fund-flows.json", "data/cross-asset-flow-state.json",
            "data/sector-flow-state.json", "data/sector-capital-fusion.json", "data/flow-confluence.json", "data/flows-ai-analysis.json",
            "data/flow-anomaly-detector.json", "data/rotation-dashboard.json", "data/global-flows.json", "data/capital-inflows.json",
            "data/crypto-etf-flows.json", "data/liquidity-flow.json", "data/foreign-flows.json", "data/hot-money.json", "data/etf-desk-research.json",
            "data/etf-creations/index.json", "data/history/global-flow-desk.json")
PAGES = ("global-flow-desk.html", "flows.html", "sectors.html", "capital-flow.html", "capital-flow-radar.html", "money-flow.html", "cross-asset-flow.html",
         "sector-flow.html", "flow-confluence.html", "capital-inflows.html", "foreign-flows.html", "global-flows.html", "hot-money.html",
         "rotation-dashboard.html", "stablecoin-flow.html", "inflows-outflows.html")
SECRET_KV = re.compile(r"(?i)\b(api[_-]?key|apikey|access[_-]?key|token|secret|password|passwd|authorization|signature|key)(\s*[=:]\s*|%3D)[^\s&\"',}]+")
BEARER = re.compile(r"(?i)\bbearer\s+\S+")
LONG_TOKEN = re.compile(r"[A-Za-z0-9_\-+/=]{20,}")
HEX32 = re.compile(r"[0-9a-fA-F]{32,}")


def redact(line):
    line = BEARER.sub("Bearer ***", str(line))
    line = SECRET_KV.sub(lambda m: m.group(1) + m.group(2) + "***", line)

    def opaque(tok):
        if HEX32.fullmatch(tok):
            return False                     # content hashes of public documents are not secrets
        d, u, lo = any(c.isdigit() for c in tok), any(c.isupper() for c in tok), any(c.islower() for c in tok)
        return (d and u and lo) or (len(tok) >= 28 and d and (u or lo) and "-" not in tok and "_" not in tok and "/" not in tok)
    return LONG_TOKEN.sub(lambda m: "***" if opaque(m.group(0)) else m.group(0), line)


def money(v):
    if v is None:
        return "n/a"
    v = float(v)
    a = abs(v)
    s = "-" if v < 0 else "+"
    if a >= 1e9:
        return "%s$%.2fB" % (s, a / 1e9)
    if a >= 1e6:
        return "%s$%.1fM" % (s, a / 1e6)
    return "%s$%.0f" % (s, a)


def hours(dt, now):
    return (now - dt).total_seconds() / 3600.0


def clock(text):
    try:
        t = datetime.fromisoformat(str(text).replace("Z", "+00:00"))
        return t if t.tzinfo else t.replace(tzinfo=timezone.utc)
    except Exception:  # noqa: BLE001
        return None


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    assert "abc123" not in redact("GET https://x/y?apikey=abc123&limit=5")
    cfg = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=90, max_pool_connections=16)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    cw = boto3.client("cloudwatch", region_name=REGION, config=cfg)
    logs = boto3.client("logs", region_name=REGION, config=cfg)
    evb = boto3.client("events", region_name=REGION, config=cfg)
    sch = boto3.client("scheduler", region_name=REGION, config=cfg)
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    now = datetime.now(timezone.utc)
    since = now - timedelta(days=21)
    facts, findings, fixtures, shots = {}, [], {}, {}

    def get(key, limit=80 * 1024 * 1024):
        o = s3.get_object(Bucket=LIVE, Key=key)
        if o["ContentLength"] > limit:
            raise ValueError("too large: %d bytes" % o["ContentLength"])
        raw = o["Body"].read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return raw, o["LastModified"]

    def head(key):
        try:
            o = s3.head_object(Bucket=LIVE, Key=key)
            return o["LastModified"], o["ContentLength"]
        except Exception as exc:  # noqa: BLE001
            return None, str(exc)[:60]

    with report(NAME) as r:
        r.heading("ops 6961 - the money-flow engine (justhodl-global-flow-desk): read-only audit, %s" % now.isoformat(timespec="minutes"))

        def finding(text):
            findings.append(text)
            r.warn("FINDING %d: %s" % (len(findings), text))

        # ------------------------------------------------------------------ 1. the function, its schedule, its runs
        r.section("1. The engine and its two feeders on AWS: deployed code, schedule, runs of the last three weeks")
        for fn, rule in FEEDERS.items():
            short = fn.replace("justhodl-", "")
            try:
                c = lam.get_function_configuration(FunctionName=fn)
                r.log("%s: state %s, %s MB / %ss, code last changed %s | %s" % (short, c.get("State"), c.get("MemorySize"), c.get("Timeout"),
                                                                             str(c.get("LastModified"))[:16], (c.get("Description") or "")[:150]))
                if fn == FN:
                    facts["code_last_changed"] = str(c.get("LastModified"))[:10]
            except Exception as exc:  # noqa: BLE001
                r.log("%s: function not readable: %s" % (short, str(exc)[:120]))
            try:
                rc = lam.get_function_concurrency(FunctionName=fn).get("ReservedConcurrentExecutions")
                r.log("  reserved concurrency: %s%s" % (rc, " (0 = held, it cannot run)" if rc == 0 else ""))
                if rc == 0:
                    finding("%s is held at reserved concurrency 0: it cannot run" % fn)
            except Exception as exc:  # noqa: BLE001
                r.log("  reserved concurrency not readable: %s" % str(exc)[:80])
            seen = []
            try:
                d = evb.describe_rule(Name=rule)
                seen.append("EventBridge rule %s %s" % (d.get("State"), d.get("ScheduleExpression")))
            except Exception:  # noqa: BLE001
                pass
            try:
                s_ = sch.get_schedule(Name=rule, GroupName="default")
                seen.append("Scheduler %s %s" % (s_.get("State"), s_.get("ScheduleExpression")))
            except Exception:  # noqa: BLE001
                pass
            try:
                names = []
                for page in evb.get_paginator("list_rule_names_by_target").paginate(TargetArn="arn:aws:lambda:%s:857687956942:function:%s" % (REGION, fn)):
                    names += page.get("RuleNames", [])
                for nm in names:
                    if nm != rule:
                        d = evb.describe_rule(Name=nm)
                        seen.append("also rule %s %s %s" % (nm, d.get("State"), d.get("ScheduleExpression")))
            except Exception:  # noqa: BLE001
                pass
            r.log("  schedule: %s" % ("; ".join(seen) or "NONE FOUND under the name %s" % rule))
            if fn == FN:
                facts["schedule"] = "; ".join(seen)[:90] or "none"
            q = [{"Id": tag, "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": m, "Dimensions": [{"Name": "FunctionName", "Value": fn}]},
                                           "Period": 86400, "Stat": stat}}
                 for tag, m, stat in (("n", "Invocations", "Sum"), ("e", "Errors", "Sum"), ("d", "Duration", "Maximum"))]
            try:
                res = cw.get_metric_data(MetricDataQueries=q, StartTime=since, EndTime=now)
                days = {}
                for item in res["MetricDataResults"]:
                    for t, v in zip(item["Timestamps"], item["Values"]):
                        days.setdefault(t.date().isoformat(), {})[item["Id"]] = v
                act = {k: v for k, v in days.items() if v.get("n")}
                r.log("  runs per UTC day (21 days): %s" % (", ".join("%s n%d e%d %.0fs" % (k[5:], v.get("n", 0), v.get("e", 0), v.get("d", 0) / 1000.0)
                                                                    for k, v in sorted(act.items())) or "NONE"))
                tn, te = sum(v.get("n", 0) for v in days.values()), sum(v.get("e", 0) for v in days.values())
                if fn == FN:
                    facts["runs_21d"] = "%d runs, %d errors, last %s" % (tn, te, max(act) if act else None)
                    if not act:
                        finding("the engine has not run once in 21 days")
            except Exception as exc:  # noqa: BLE001
                r.log("  metrics not readable: %s" % str(exc)[:100])
            if fn != FN:
                continue
            group = "/aws/lambda/" + fn
            try:
                marks = Counter()
                lines = []
                for page in logs.get_paginator("filter_log_events").paginate(logGroupName=group, startTime=int(since.timestamp() * 1000), limit=10000):
                    for e in page.get("events", []):
                        msg = " ".join(e["message"].split())
                        if "[flow-research]" in msg or "Task timed out" in msg or "[ERROR]" in msg or "Traceback" in msg or "errorMessage" in msg:
                            lines.append((e["timestamp"], msg))
                            marks["failed_attempt" if "failed_attempt" in msg else "timeout" if "Task timed out" in msg else "flow-research" if "[flow-research]" in msg else "error"] += 1
                r.log("  log lines of note in 21 days: %s" % (dict(marks) or "none"))
                for ts, msg in lines[-12:]:
                    r.log("    %s %s" % (datetime.fromtimestamp(ts / 1000, timezone.utc).strftime("%m-%d %H:%M"), redact(msg)[:420]))
                facts["log_marks"] = json.dumps(dict(marks))[:80]
                if marks.get("failed_attempt") or marks.get("timeout") or marks.get("error"):
                    finding("the engine's log holds failures in the last 21 days: %s" % dict(marks))
            except Exception as exc:  # noqa: BLE001
                r.log("  logs not readable: %s" % str(exc)[:120])

        # ------------------------------------------------------------------ 2. the live document
        r.section("2. The live document data/global-flow-desk.json")
        raw, modified = get("data/global-flow-desk.json")
        doc = json.loads(raw)
        q = doc.get("quality") or {}
        gen = clock(doc.get("generated_at"))
        r.log("written %s (%.1f h ago), %d bytes | engine %s %s, contract %s" % (modified.isoformat(timespec="minutes"), hours(modified, now), len(raw),
                                                                                 doc.get("engine"), doc.get("version"), doc.get("contract")))
        r.log("generated_at %s | source (ETF creations) generated %s | period %s | reference date %s" % (
            doc.get("generated_at"), doc.get("source_generated_at"), json.dumps(doc.get("period")), doc.get("source_reference_date")))
        r.log("quality: %s" % json.dumps({k: v for k, v in q.items() if k != "unavailable_funds"})[:700])
        r.log("funds with no comparable figure (%d): %s" % (len(q.get("unavailable_funds") or []), " ".join(q.get("unavailable_funds") or [])[:700]))
        r.log("top-level keys: %s" % " ".join(sorted(doc.keys())))
        facts.update(version=doc.get("version"), generated=str(doc.get("generated_at"))[:16], period_end=(doc.get("period") or {}).get("end_date"),
                     funds="%s of %s" % (q.get("aligned_funds"), q.get("configured_funds")), quality=q.get("status"))
        if gen and hours(gen, now) > 30:
            finding("the document is %.0f hours old (generated %s)" % (hours(gen, now), doc.get("generated_at")))
        if q.get("status") != "partial":
            finding("the document's own quality status is %r" % q.get("status"))
        groups, funds = doc.get("groups") or {}, doc.get("funds") or {}
        kinds = Counter(g.get("kind") for g in groups.values())
        r.log("groups: %d %s | funds: %d" % (len(groups), dict(kinds), len(funds)))

        def table(title, rows, n=40):
            r.log(title)
            for g in rows[:n]:
                r.log("    %-46s %12s   %d of %d funds%s" % (g["label"][:46], money(g.get("net_flow_5d_usd")), g.get("coverage_count") or 0, g.get("configured_count") or 0,
                                                            "" if not g.get("unavailable_members") else "   missing: " + " ".join(g["unavailable_members"])[:90]))

        def ranked(kind):
            rows = [g for g in groups.values() if g.get("kind") == kind]
            return sorted(rows, key=lambda g: (g.get("net_flow_5d_usd") is None, -(g.get("net_flow_5d_usd") or 0)))
        end = (doc.get("period") or {}).get("end_date")
        table("WHERE THE MONEY WENT, five fund-reporting days to %s - the engine's own baskets, most in to most out:" % end, ranked("configured_basket"))
        table("the eleven sector funds, most in to most out:", ranked("configured_sector_fund"))
        table("country fund baskets, most in to most out:", ranked("configured_geography"), 40)
        table("iShares' own classes (iShares funds only):", ranked("issuer_classification"), 40)
        per = sorted(((t, (f.get("comparison") or {}).get("net_flow_5d_usd")) for t, f in funds.items()), key=lambda x: (x[1] is None, -(x[1] or 0)))
        have = [x for x in per if x[1] is not None]
        r.log("single funds, largest in: %s" % ", ".join("%s %s" % (t, money(v)) for t, v in have[:12]))
        r.log("single funds, largest out: %s" % ", ".join("%s %s" % (t, money(v)) for t, v in have[::-1][:12]))
        gross_in, gross_out = sum(v for _, v in have if v > 0), sum(v for _, v in have if v < 0)
        r.log("each fund counted once: %d funds, in %s, out %s, net %s" % (len(have), money(gross_in), money(gross_out), money(gross_in + gross_out)))
        facts.update(net_all=money(gross_in + gross_out), money_in=money(gross_in), money_out=money(gross_out))

        r.log("WHAT THE DOCUMENT NO LONGER SAYS:")
        sec = doc.get("sectors") or {}
        ivr, hot, capex = doc.get("inst_vs_retail") or {}, doc.get("hot_money") or {}, doc.get("capex") or {}
        r.log("    sectors.leaders %s, sectors.laggards %s (the ranked list has %d)" % (sec.get("leaders"), sec.get("laggards"), len(sec.get("ranked") or [])))
        r.log("    institutions against retail: %s / %s / %s, status %s" % (ivr.get("institutional"), ivr.get("retail"), ivr.get("divergence"), ivr.get("status")))
        r.log("    hot money by country: %d scored, in %s, out %s, status %s" % (hot.get("n_scored") or 0, hot.get("top_inflows"), hot.get("top_outflows"), hot.get("status")))
        r.log("    capex: status %s | history rows %d | calls %s" % (capex.get("status"), len(doc.get("history") or []), doc.get("call")))
        r.log("    the brief (%s): %s" % (doc.get("ai_brief_src"), str(doc.get("ai_brief"))[:420]))
        if not sec.get("leaders") and not hot.get("n_scored") and ivr.get("institutional") is None:
            finding("the four readings of the first engine are published empty: sector leaders and laggards, institutions against retail, hot money by country, the written brief")
        ctx = doc.get("retained_contexts") or {}
        r.log("the twelve other sources it keeps beside the figures (kept, never read into a figure):")
        for k in CONTEXT_KEYS:
            c = ctx.get(k) or {}
            r.log("    %-40s %-30s reported %s%s" % (k, c.get("source_status"), c.get("reported_generation"), "" if not c.get("failure_class") else " (%s)" % c.get("failure_class")))
        tic = doc.get("tic_context") or {}
        r.log("TIC beside it: %s, observation %s, source generated %s" % (tic.get("source_status"), tic.get("observation_date"), tic.get("source_generated_at")))
        slim = dict(doc)
        fixtures["global-flow-desk"] = gzip.compress(json.dumps(slim, separators=(",", ":")).encode(), 9)

        # ------------------------------------------------------------------ 3. every figure again from the funds' own histories
        r.section("3. Every five-day figure again, from each fund's own stored history; every group total from its members")
        period = doc.get("period") or {}
        with_hist = {t: f["history"] for t, f in funds.items() if isinstance(f.get("history"), dict) and f["history"].get("key")}

        def read_hist(item):
            t, ref = item
            try:
                return t, json.loads(get(ref["key"])[0])
            except Exception as exc:  # noqa: BLE001
                return t, {"error": str(exc)[:80]}
        with ThreadPoolExecutor(max_workers=12) as pool:
            hist = dict(pool.map(read_hist, with_hist.items()))
        broken = {t: h["error"] for t, h in hist.items() if "error" in h}
        r.log("fund histories read: %d of %d%s" % (len(hist) - len(broken), len(with_hist), "" if not broken else " | unreadable: %s" % json.dumps(broken)[:300]))
        ref = hist.get("IVV") or {}
        dates = [x["date"] for x in ref.get("rows", [])]
        mine, wrong, lens = {}, [], []
        if end in dates and dates.index(end) >= 5:
            i = dates.index(end)
            window = dates[i - 4:i + 1]
            r.log("the reference calendar is IVV's own reporting days: the five days are %s, counted from %s (document says from %s)" % (" ".join(window), dates[i - 5], period.get("start_date")))
            if dates[i - 5] != period.get("start_date"):
                finding("the document's period start %s is not the sixth-last IVV day %s" % (period.get("start_date"), dates[i - 5]))
            newest = dates[-1]
            r.log("IVV's newest stored day is %s; the document compares to %s%s" % (newest, end, "" if newest == end else " - the window ends BEFORE the newest day"))
            with localcontext() as c:
                c.prec = 50
                for t, h in hist.items():
                    if "error" in h:
                        continue
                    rows = {x["date"]: x for x in h.get("rows", [])}
                    lens.append(len(rows))
                    vals = [rows.get(d, {}).get("nav_valued_share_change_decimal") for d in window]
                    mine[t] = sum((Decimal(v) for v in vals), Decimal(0)) if all(v is not None for v in vals) else None
                for t, f in funds.items():
                    said = (f.get("comparison") or {}).get("value_decimal")
                    got = mine.get(t)
                    if (said is None) != (got is None) and t in mine:
                        if said is not None or t not in (q.get("unavailable_funds") or []):
                            wrong.append((t, said, None if got is None else str(got)))
                    elif said is not None and got is not None and Decimal(said) != got:
                        wrong.append((t, said, str(got)))
                n_said = sum(1 for f in funds.values() if (f.get("comparison") or {}).get("value_decimal") is not None)
                r.log("funds with a five-day figure in the document: %d | recomputed from their own rows and equal to the last cent: %d | different: %d %s" % (
                    n_said, n_said - len([w for w in wrong if w[1] is not None]), len(wrong), json.dumps(wrong[:6])))
                could = sorted(t for t, v in mine.items() if v is not None and (funds[t].get("comparison") or {}).get("value_decimal") is None)
                r.log("funds whose own history has all five days but the document shows no figure: %d %s" % (len(could), " ".join(could)[:400]))
                gwrong = []
                for gid, g in groups.items():
                    cov = g.get("covered_members") or []
                    tot = sum((mine[t] for t in cov if mine.get(t) is not None), Decimal(0)) if cov else None
                    said = g.get("value_decimal")
                    if (said is None) != (tot is None) or (said is not None and Decimal(said) != tot):
                        gwrong.append((gid, said, None if tot is None else str(tot)))
                r.log("group totals recomputed from their covered members: %d groups, %d different %s" % (len(groups), len(gwrong), json.dumps(gwrong[:5])))
                facts.update(funds_rechecked="%d equal, %d different" % (n_said - len([w for w in wrong if w[1] is not None]), len(wrong)), groups_rechecked="%d of %d equal" % (len(groups) - len(gwrong), len(groups)))
                if wrong or gwrong:
                    finding("recomputed figures differ: %d funds, %d groups" % (len(wrong), len(gwrong)))
                # the row itself: shares change times NAV
                for t in ("SPY", "IVV", "TLT", "GLD", "XLK"):
                    h = hist.get(t) or {}
                    rows = h.get("rows") or []
                    if len(rows) < 3:
                        continue
                    last = rows[-1]
                    if t == "SPY":
                        r.log("a history row's fields: %s" % " ".join(sorted(last.keys())))
                    sk = next((k for k in last if "share" in k and "change" not in k and k.endswith("decimal")), None)
                    nk = next((k for k in last if "nav" in k and "valued" not in k and k.endswith("decimal")), None)
                    checked, off = 0, []
                    if sk and nk:
                        for a, b in zip(rows[-7:-1], rows[-6:]):
                            v = b.get("nav_valued_share_change_decimal")
                            if v is None or a.get(sk) is None or b.get(sk) is None or b.get(nk) is None:
                                continue
                            calc = (Decimal(b[sk]) - Decimal(a[sk])) * Decimal(b[nk])
                            checked += 1
                            if abs(calc - Decimal(v)) > max(Decimal("0.01"), abs(Decimal(v)) * Decimal("0.000001")):
                                off.append((b["date"], v, str(calc)[:24]))
                    r.log("%s: %d rows %s to %s | last row %s shares %s nav %s flow %s | (shares - shares before) x NAV on the last %d rows: %s" % (
                        t, len(rows), rows[0]["date"], last["date"], last["date"], last.get(sk), last.get(nk), money(last.get("nav_valued_share_change_decimal")), checked,
                        "all equal" if checked and not off else "DIFFERENT %s" % json.dumps(off[:3]) if off else "not checkable (fields %s / %s)" % (sk, nk)))
        else:
            finding("the period end %s is not among IVV's stored days (newest %s)" % (end, dates[-1] if dates else None))
        if lens:
            r.log("history depth: %d funds, median %d days, shortest %d, longest %d" % (len(lens), sorted(lens)[len(lens) // 2], min(lens), max(lens)))

        # ------------------------------------------------------------------ 4. the same source's other reader
        r.section("4. The same figures on the Inflows & outflows desk (its per-fund feeds)")
        try:
            idx = json.loads(get("data/etf-creations/index.json")[0])
            listed = idx.get("funds") if isinstance(idx.get("funds"), (list, dict)) else idx.get("tickers") or []
            names = sorted(listed.keys() if isinstance(listed, dict) else [x.get("ticker") if isinstance(x, dict) else x for x in listed])
            r.log("the desk's index: %d funds, generated %s | keys %s" % (len(names), idx.get("generated_at"), " ".join(sorted(idx.keys()))[:160]))

            def feed(t):
                try:
                    return t, json.loads(get("data/etf-creations/%s.json" % t)[0])
                except Exception as exc:  # noqa: BLE001
                    return t, {"error": str(exc)[:60]}
            with ThreadPoolExecutor(max_workers=12) as pool:
                feeds = dict(pool.map(feed, [t for t in names if t in funds]))
            same, differ, later = 0, [], 0
            for t, fd in feeds.items():
                if "error" in fd:
                    continue
                d, s = fd.get("d") or [], (fd.get("series") or {}).get("flow_5d") or []
                said = (funds[t].get("comparison") or {}).get("net_flow_5d_usd")
                if end in d and said is not None:
                    v = s[d.index(end)]
                    if v is not None and abs(float(v) - float(said)) <= max(0.01, abs(float(said)) * 1e-9):
                        same += 1
                    else:
                        differ.append((t, said, v))
                if d and d[-1] > (end or ""):
                    later += 1
            r.log("funds on both: %d | the desk's 5-day figure on %s equals the engine's: %d | differs: %d %s | funds whose desk feed runs past %s: %d" % (
                len(feeds), end, same, len(differ), json.dumps(differ[:5]), end, later))
            facts["desk_feeds"] = "%d equal, %d differ" % (same, len(differ))
            if differ:
                finding("the Inflows & outflows desk shows a different five-day figure for %d funds" % len(differ))
        except Exception as exc:  # noqa: BLE001
            r.log("desk feeds not compared: %s" % str(exc)[:160])

        # ------------------------------------------------------------------ 5. the sources
        r.section("5. Every source the first engine read, and the flow documents beside it: still written? how old?")
        stale = []
        for k in CONTEXT_KEYS + SIBLINGS:
            lm, size = head(k)
            if lm is None:
                r.log("    %-44s ABSENT (%s)" % (k, size))
                if k in CONTEXT_KEYS:
                    stale.append(k + " absent")
                continue
            extra = ""
            if isinstance(size, int) and size < 30 * 1024 * 1024:
                try:
                    dd = json.loads(get(k)[0])
                    if isinstance(dd, dict):
                        extra = "generated %s | version %s | %s" % (str(dd.get("generated_at") or dd.get("as_of") or dd.get("updated_at"))[:19], dd.get("version"),
                                                                    str(dd.get("contract") or dd.get("engine") or "")[:40])
                except Exception as exc:  # noqa: BLE001
                    extra = "not JSON: %s" % str(exc)[:50]
            age = hours(lm, now)
            r.log("    %-44s written %s  %7.1f h ago  %9d B  %s" % (k, lm.strftime("%m-%d %H:%M"), age, size, extra))
            if k in CONTEXT_KEYS and age > 96:
                stale.append("%s %.0f days" % (k, age / 24))
        r.log("first-engine sources absent or older than four days: %s" % (stale or "none"))
        facts["first_engine_sources_stale"] = "%d of %d" % (len(stale), len(CONTEXT_KEYS))

        # ------------------------------------------------------------------ 6. what the first engine last said
        r.section("6. What the first engine (the money map) last said")
        try:
            lp = doc.get("legacy_preservation")
            r.log("the document's note on the first engine's last publication: %s" % json.dumps(lp)[:900])
            hraw, hlm = get("data/history/global-flow-desk.json")
            hh = json.loads(hraw)
            rows = hh if isinstance(hh, list) else hh.get("rows") or hh.get("history") or hh.get("days") or []
            r.log("its own history file: written %s, %d bytes, %s rows | keys %s" % (hlm.isoformat(timespec="minutes"), len(hraw), len(rows) if hasattr(rows, "__len__") else "?",
                                                                               (" ".join(sorted(hh.keys()))[:200] if isinstance(hh, dict) else "list")))
            seq = rows if isinstance(rows, list) else [dict(v, date=k) for k, v in sorted(rows.items())]
            for row in seq[-4:]:
                r.log("    %s" % json.dumps(row)[:700])
            facts["first_engine_last_wrote"] = hlm.strftime("%Y-%m-%d")
        except Exception as exc:  # noqa: BLE001
            r.log("first engine's history not readable: %s" % str(exc)[:160])
        try:
            vs = s3.list_object_versions(Bucket=LIVE, Prefix="data/global-flow-desk.json", MaxKeys=60).get("Versions", [])
            vs = [v for v in vs if v["Key"] == "data/global-flow-desk.json"]
            r.log("kept versions of the document: %d, oldest %s, newest %s, sizes %s" % (len(vs), min(v["LastModified"] for v in vs).strftime("%Y-%m-%d") if vs else None,
                                                                               max(v["LastModified"] for v in vs).strftime("%Y-%m-%d %H:%M") if vs else None,
                                                                               [v["Size"] for v in vs[:8]]))
        except Exception as exc:  # noqa: BLE001
            r.log("versions not readable: %s" % str(exc)[:120])

        # ------------------------------------------------------------------ 7. the page, and the pages beside it
        r.section("7. The page in a real browser, and the other flow pages")
        try:
            spec = importlib.util.spec_from_file_location("ops_6846", str(sorted(Path(__file__).resolve().parent.glob("ops_6846_*.py"))[0]))
            A = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(A)
            sync_playwright = A.ensure_playwright(r.log)
            stamp = int(time.time())
            READ = r"""() => { const tx = (id) => { const e = document.getElementById(id); return e ? e.innerText.replace(/\s+/g, ' ').trim() : null; };
              const body = document.body.innerText;
              return { title: document.title, h1: (document.querySelector('h1') || {}).innerText || null, state: tx('state'), asof: tx('asof'), brief: tx('brief'),
                cards: Array.from(document.querySelectorAll('#cards > *')).map(e => e.innerText.replace(/\s+/g, ' ').trim().slice(0, 150)),
                heads: Array.from(document.querySelectorAll('h2, h3')).map(e => e.innerText.replace(/\s+/g, ' ').trim().slice(0, 70)),
                comparison: Array.from(document.querySelectorAll('#comparison tr')).length, comparison_head: tx('comparison') ? tx('comparison').slice(0, 500) : null,
                gaps: tx('gaps') ? tx('gaps').slice(0, 400) : null, contexts: tx('contexts') ? tx('contexts').slice(0, 300) : null, tic: tx('tic-context') ? tx('tic-context').slice(0, 300) : null,
                selection: tx('selection') ? tx('selection').slice(0, 300) : null, chart: !!document.querySelector('#chart svg, #chart canvas, canvas#chart, svg#chart'),
                text: body.length, rows: document.querySelectorAll('tr').length,
                words: { unavailable: (body.match(/unavailable/gi) || []).length, not_measured: (body.match(/not measured|not inferred|no .{0,20}vote/gi) || []).length,
                         inflow: (body.match(/inflow|flowing in|money in/gi) || []).length, outflow: (body.match(/outflow|flowing out|money out/gi) || []).length,
                         bad: (body.match(/\bNaN\b|\bundefined\b|Infinity(?! [A-Z])/g) || []).length },
                ox: document.documentElement.scrollWidth - document.documentElement.clientWidth, height: document.documentElement.scrollHeight }; }"""
            with sync_playwright() as p:
                browser = A.launch(p, r.log)
                ctx = browser.new_context(viewport={"width": 1500, "height": 950}, user_agent=UA)
                page = ctx.new_page()
                errors, failed = [], []
                page.on("pageerror", lambda e: errors.append(str(e)[:160]))
                page.on("response", lambda resp: failed.append("%d %s" % (resp.status, resp.url.split("?")[0][-70:])) if resp.status >= 400 else None)
                page.goto("%s/global-flow-desk.html?probe=%d" % (BASE, stamp), wait_until="domcontentloaded", timeout=90000)
                try:
                    page.wait_for_function("() => document.querySelectorAll('#cards > *').length > 0 || /unavailable/i.test((document.getElementById('state') || {}).innerText || '')", timeout=60000)
                except Exception as exc:  # noqa: BLE001
                    r.log("the page did not fill its cards in 60 s: %s" % str(exc)[:80])
                page.wait_for_timeout(3500)
                v = page.evaluate(READ)
                r.log("global-flow-desk.html: title %r | h1 %r" % (v["title"], v["h1"]))
                r.log("  state: %s | as of: %s" % (v["state"], v["asof"]))
                r.log("  the brief on the page: %s" % str(v["brief"])[:420])
                r.log("  cards (%d): %s" % (len(v["cards"]), " || ".join(v["cards"][:10])[:1300]))
                r.log("  sections: %s" % " | ".join(v["heads"])[:500])
                r.log("  comparison table: %d rows | %s" % (v["comparison"], str(v["comparison_head"])[:420]))
                r.log("  gaps: %s" % v["gaps"])
                r.log("  chart drawn: %s | selection: %s" % (v["chart"], str(v["selection"])[:200]))
                r.log("  words on the page: %s | text %d characters, %d table rows, page height %d px | page errors %d %s | failed requests %s" % (
                    json.dumps(v["words"]), v["text"], v["rows"], v["height"], len(errors), errors[:2], failed[:5]))
                facts.update(page_cards=len(v["cards"]), page_errors=len(errors), page_words_in_out="%d / %d" % (v["words"]["inflow"], v["words"]["outflow"]))
                if errors:
                    finding("the page throws: %s" % errors[:2])
                if not v["cards"]:
                    finding("the page shows no cards")
                shots["1-page-top"] = page.screenshot(type="jpeg", quality=68)
                try:
                    shots["2-page-whole"] = page.screenshot(type="jpeg", quality=42, full_page=True)
                except Exception as exc:  # noqa: BLE001
                    r.log("whole-page picture not taken: %s" % str(exc)[:80])
                ctx.close()
                ctx = browser.new_context(viewport={"width": 390, "height": 844}, user_agent=UA, is_mobile=True, has_touch=True, device_scale_factor=2)
                page = ctx.new_page()
                page.goto("%s/global-flow-desk.html?probe=%d" % (BASE, stamp), wait_until="domcontentloaded", timeout=90000)
                page.wait_for_timeout(9000)
                m = page.evaluate(READ)
                r.log("  on a 390 px phone: sideways scroll %d px, %d cards" % (m["ox"], len(m["cards"])))
                if m["ox"] > 0:
                    finding("the page scrolls sideways by %d px on a phone" % m["ox"])
                shots["3-phone"] = page.screenshot(type="jpeg", quality=55)
                ctx.close()
                r.log("the other flow pages (each opened once, read after it settled):")
                ctx = browser.new_context(viewport={"width": 1500, "height": 950}, user_agent=UA)
                for name in PAGES[1:]:
                    page = ctx.new_page()
                    perr = []
                    page.on("pageerror", lambda e, perr=perr: perr.append(str(e)[:100]))
                    try:
                        resp = page.goto("%s/%s?probe=%d" % (BASE, name, stamp), wait_until="domcontentloaded", timeout=60000)
                        try:
                            page.wait_for_load_state("networkidle", timeout=15000)
                        except Exception:  # noqa: BLE001
                            pass
                        page.wait_for_timeout(2500)
                        s = page.evaluate(READ)
                        r.log("    %-26s %s | h1 %-46r | text %6d, rows %4d | in/out words %d/%d, unavailable %d, bad %d | errors %d %s" % (
                            name, resp.status if resp else "?", (s["h1"] or "")[:44], s["text"], s["rows"], s["words"]["inflow"], s["words"]["outflow"],
                            s["words"]["unavailable"], s["words"]["bad"], len(perr), perr[:1]))
                    except Exception as exc:  # noqa: BLE001
                        r.log("    %-26s not opened: %s" % (name, str(exc)[:90]))
                    page.close()
                ctx.close()
                browser.close()
        except Exception as exc:  # noqa: BLE001
            r.log("browser part failed: %s: %s" % (type(exc).__name__, str(exc)[:200]))

        r.section("findings")
        for i, f in enumerate(findings, 1):
            r.log("%d. %s" % (i, f))
        r.section("attachments")
        for k, body in fixtures.items():
            b64 = base64.b64encode(body).decode("ascii")
            r.lines.append("<!-- FIXTURE %s %d bytes" % (k, len(body)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("FIXTURE-END -->")
        for k, img in shots.items():
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        r.kv(findings=len(findings), **{k: str(v)[:90] for k, v in facts.items()})
        r.ok("AUDIT COMPLETE: nothing was invoked or written; %d findings" % len(findings))


if __name__ == "__main__":
    main()
