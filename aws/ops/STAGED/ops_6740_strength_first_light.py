"""ops 6740 - justhodl-strength birth + first light, and the 13F book 1.3.0's per-ticker table (Khalid 2026-11-04:
"Strength vs S&P 500 ... most matched fund flows is missing; also improve the strength and add which one has been in
long accumulation with very very tight price spread").

After deploy-lambdas ships commit <this>:
  1. justhodl-strength exists with the config's runtime / memory / timeout; the Scheduler schedule justhodl-strength-daily
     (03:40 UTC) exists and matches config; the 13F book is 1.3.0 (its tests passed in the same deploy);
  2. deployed schedule manifest := committed (additions-only, every added entry live-verified) - as ops 6723 did;
  3. the 13F book once (RequestResponse; a root run under the trigger cap) so data/13f-tickers.json exists, read back:
     n_tickers, folded rows for NVDA / AAPL / XLK / SPY (holders now / prior, votes, stance);
  4. the strength engine once (RequestResponse, up to 300 s), data/strength.json read back: as-of session, universe
     counts, benchmark, breadth, the top 15 leaders / laggards, the coil board (top 30 with tier / base / range /
     score / corroboration), flows coverage for the desk's funds (own 1 / 5 / 21 windows), the 13F join coverage,
     sources tiers, errors; the desk rows embedded as JSON for offline reading.
Re-runnable. Direct lane (STAGED). Writes nothing but the two artifacts the engines own and this report.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import re
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
KEY = "config/schedule-manifest.json"
FN, SCHED, OUT = "justhodl-strength", "justhodl-strength-daily", "data/strength.json"
BOOK, TICKERS = "justhodl-13f-book", "data/13f-tickers.json"
CFG = Config(retries={"max_attempts": 4, "mode": "standard"}, connect_timeout=10, read_timeout=330)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def fn_of(arn):
    m = re.search(r":function:([^/:]+)", arn or "")
    return m.group(1) if m else arn


def entries(m):
    return {(e["kind"], e["name"]): e for sec in ("rules", "schedules") for e in m.get(sec, [])}


def money(v):
    if v is None:
        return "-"
    a, s = abs(float(v)), "-" if v < 0 else "+" if v > 0 else ""
    return "%s$%.2fB" % (s, a / 1e9) if a >= 1e9 else "%s$%.0fM" % (s, a / 1e6) if a >= 1e6 else "%s$%.0fK" % (s, a / 1e3) if a >= 1e3 else "%s$%.0f" % (s, a)


def pct(v, d=1):
    return "-" if v is None else ("%+." + str(d) + "f%%") % v


def chip(r):
    f, fl = r.get("f13"), r.get("flow")
    out = []
    if fl and fl.get("current") and (fl.get("w5") or fl.get("w1")):
        w = fl.get("w5") or fl.get("w1")
        out.append("flow %s (%s AUM, %s obs, eff %s%s)" % (money(w["usd"]), pct(w.get("pct_aum"), 2), 5 if fl.get("w5") else 1, w.get("end"), "" if (fl.get("aligned") or {}).get("5") else ", own dates"))
    elif fl:
        out.append("flow source %s" % ("overdue" if fl.get("current") is False else "unmatched"))
    if f:
        out.append("13F %s mgrs %+d net %s %s" % (f.get("n_holders"), f.get("holder_delta") or 0, money(f.get("net_usd")), f.get("stance")))
    return " | ".join(out) or "-"


def main():
    s3, lam, sch = c("s3"), c("lambda"), c("scheduler")
    with report("ops_6740_strength_first_light") as r:
        r.heading("ops 6740 - justhodl-strength: birth, 13F book 1.3.0 per-ticker table, first light %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        r.section("1. function + schedule")
        cfg = json.loads((ROOT / "aws/lambdas/justhodl-strength/config.json").read_text(encoding="utf-8"))
        try:
            cf = lam.get_function_configuration(FunctionName=FN)
        except Exception as e:  # noqa: BLE001
            r.fail("%s not deployed yet (%s) - wait for deploy-lambdas, then run again" % (FN, type(e).__name__))
            raise SystemExit(1)
        r.log("%s: runtime %s, memory %s, timeout %s, state %s, modified %s, env keys %s" % (
            FN, cf.get("Runtime"), cf.get("MemorySize"), cf.get("Timeout"), cf.get("State"), (cf.get("LastModified") or "")[:19], sorted((cf.get("Environment") or {}).get("Variables", {}).keys())))
        if cf.get("Runtime") != cfg["runtime"] or cf.get("MemorySize") != cfg["memory"] or cf.get("Timeout") != cfg["timeout"]:
            r.fail("live configuration differs from config.json")
            raise SystemExit(1)
        sc = cfg["eventbridge_scheduler"]
        try:
            live = sch.get_schedule(Name=SCHED, GroupName="default")
        except Exception as e:  # noqa: BLE001
            r.fail("schedule %s missing (%s)" % (SCHED, type(e).__name__))
            raise SystemExit(1)
        r.log("schedule %s: %s %s tz %s target %s" % (SCHED, live.get("ScheduleExpression"), live.get("State"), live.get("ScheduleExpressionTimezone"), fn_of((live.get("Target") or {}).get("Arn"))))
        if live.get("ScheduleExpression") != sc["cron"] or fn_of((live.get("Target") or {}).get("Arn")) != FN or live.get("State") != "ENABLED":
            r.fail("live schedule differs from config (%s vs %s)" % (live.get("ScheduleExpression"), sc["cron"]))
            raise SystemExit(1)
        bk = lam.get_function_configuration(FunctionName=BOOK)
        r.log("%s: modified %s, state %s" % (BOOK, (bk.get("LastModified") or "")[:19], bk.get("State")))

        r.section("2. deployed schedule manifest := committed (additions-only)")
        repo_text = (ROOT / KEY).read_text(encoding="utf-8")
        repo = json.loads(repo_text)
        old_text = s3.get_object(Bucket=LIVE, Key=KEY)["Body"].read().decode("utf-8")
        a, b = entries(json.loads(old_text)), entries(repo)
        removed = sorted(k for k in a if k not in b)
        added = sorted(k for k in b if k not in a)
        changed = sorted(k for k in a if k in b and {x: a[k][x] for x in ("expr", "state")} != {x: b[k][x] for x in ("expr", "state")})
        r.log("S3 %d entries, repo %d; added %s removed %d changed %d" % (len(a), len(b), added, len(removed), len(changed)))
        if removed or changed:
            r.fail("not additions-only: removed %s changed %s" % (removed[:5], changed[:5]))
            raise SystemExit(1)
        for kind, name in added:
            e = b[(kind, name)]
            if kind != "scheduler":
                r.fail("unexpected added entry %s %s" % (kind, name))
                raise SystemExit(1)
            lv = sch.get_schedule(Name=name, GroupName=e.get("group") or "default")
            if not (lv.get("ScheduleExpression") == e["expr"] and lv.get("State") == e["state"] and fn_of((lv.get("Target") or {}).get("Arn")) == fn_of(e["targets"][0]["arn"])):
                r.fail("declared %s does not match live" % name)
                raise SystemExit(1)
        if added:
            stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
            s3.put_object(Bucket=PRIVATE, Key="backups/schedule-manifest/%s/schedule-manifest.json" % stamp, Body=old_text.encode("utf-8"), ServerSideEncryption="AES256")
            s3.put_object(Bucket=LIVE, Key=KEY, Body=repo_text.encode("utf-8"), ContentType="application/json", CacheControl="no-cache")
            r.ok("manifest uploaded (+%d), backup backups/schedule-manifest/%s/" % (len(added), stamp))
        else:
            r.ok("manifest already equal")

        r.section("3. the 13F book 1.3.0 once -> data/13f-tickers.json")
        t0 = time.time()
        resp = lam.invoke(FunctionName=BOOK, InvocationType="RequestResponse", Payload=json.dumps({"trigger": "ops_6740_first_light"}).encode())
        payload = json.loads(resp["Payload"].read() or b"{}")
        if resp.get("FunctionError"):
            r.fail("book FunctionError %s: %s" % (resp.get("FunctionError"), json.dumps(payload)[:600]))
            raise SystemExit(1)
        book = json.loads(s3.get_object(Bucket=LIVE, Key="data/13f-book.json")["Body"].read())
        tk = (book.get("book") or {}).get("tickers") or {}
        r.log("%.0fs | handler %s | book version %s | tickers block %s" % (time.time() - t0, json.dumps(payload), book.get("version"), json.dumps(tk)))
        if book.get("version", "0") < "1.3.0" or not tk.get("written"):
            r.fail("the live book is %s / tickers %s - deploy-lambdas has not shipped 1.3.0 yet" % (book.get("version"), tk))
            raise SystemExit(1)
        obj = s3.get_object(Bucket=LIVE, Key=TICKERS)
        tbl = json.loads(obj["Body"].read())
        cols = tbl["columns"]
        r.log("%s: %s | period %s | %d tickers | written %s | columns %s" % (TICKERS, tbl.get("contract"), tbl.get("period"), tbl.get("n_tickers"), obj["LastModified"].isoformat(timespec="seconds"), cols))
        for t in ("NVDA", "AAPL", "MSFT", "XLK", "XLF", "SPY", "GLD", "QQQ", "AMZN", "TSLA", "COST", "NFLX"):
            row = (tbl.get("by_ticker") or {}).get(t)
            r.log("  %-5s %s" % (t, json.dumps(dict(zip(cols, row))) if row else "not held by the cohort"))
        st = {}
        for row in (tbl.get("by_ticker") or {}).values():
            d = dict(zip(cols, row))
            st[d["stance"]] = st.get(d["stance"], 0) + 1
        r.log("stances across tickers: %s" % json.dumps(st))

        r.section("4. first light: justhodl-strength once -> data/strength.json")
        t0 = time.time()
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"trigger": "ops_6740_first_light"}).encode())
        payload = json.loads(resp["Payload"].read() or b"{}")
        if resp.get("FunctionError"):
            r.fail("FunctionError %s: %s" % (resp.get("FunctionError"), json.dumps(payload)[:1200]))
            raise SystemExit(1)
        obj = s3.get_object(Bucket=LIVE, Key=OUT)
        doc = json.loads(obj["Body"].read())
        r.log("%.0fs wall | handler %s | written %s | %d bytes | version %s contract %s | run %ss | errors %s" % (
            time.time() - t0, json.dumps(payload), obj["LastModified"].isoformat(timespec="seconds"), obj["ContentLength"], doc.get("version"), doc.get("contract"), doc.get("run_seconds"), doc.get("n_errors")))
        r.log("as of %s | bank %s | universe %s" % (doc.get("as_of"), json.dumps(doc.get("bank")), json.dumps(doc.get("universe"))[:900]))
        bm = doc.get("benchmark") or {}
        r.log("SPY %s as of %s | h %s | above 50 %s above 200 %s | 52w pos %s off high %s" % (bm.get("px"), bm.get("as_of"), json.dumps(bm.get("h")), bm.get("above_50"), bm.get("above_200"), bm.get("pos_52w"), bm.get("off_high_pct")))
        bd = doc.get("boards") or {}
        r.log("breadth %s" % json.dumps(bd.get("breadth")))
        r.log("z stats %s" % json.dumps(doc.get("z_stats")))
        rows = doc.get("rows") or []
        by = {x["ticker"]: x for x in rows}
        for name in ("leaders", "laggards"):
            r.log("%s:" % name.upper())
            for x in (bd.get(name) or [])[:15]:
                v = x.get("vs") or {}
                r.log("  %-6s %-28s %-8s str %5s comp %7s | 1W %7s 1M %7s 3M %7s 6M %7s | trend %7s cons %5s ir %5s 52w %4s | base %s | %s" % (
                    x["ticker"], (x.get("name") or "")[:28], ",".join(x.get("scopes") or [])[:8], x.get("strength"), pct(x.get("composite_pp"), 2), pct(v.get("w"), 2), pct(v.get("m"), 2), pct(v.get("q"), 2), pct(v.get("h"), 2),
                    pct(x.get("rs_trend_63")), x.get("rs_consistency_63"), x.get("ir_63"), x.get("pos_52w"), ((x.get("coil") or {}).get("tier") or "-"), chip(by.get(x["ticker"]) or {})))
        r.log("COIL BOARD (long accumulation, tight spread):")
        for x in (bd.get("coil") or [])[:30]:
            rr = by.get(x["ticker"]) or {}
            cc = rr.get("coil") or {}
            r.log("  %-6s %-28s %-8s %-10s base %3s range %5s%% wick %5s%% atr %5s%% bb pct %4s dry %5s cmf %6s ud %5s rs-hold %7s in-base %4s%% | str %5s score %5s | %s" % (
                x["ticker"], (x.get("name") or "")[:28], ",".join(x.get("scopes") or [])[:8], cc.get("tier"), cc.get("base_len"), cc.get("base_range_pct"), cc.get("wick_range_pct"), cc.get("atr14_pct"), cc.get("bb_width_pctile_252"),
                cc.get("vol_dry"), cc.get("cmf_base"), cc.get("updown_vol"), pct(cc.get("rs_hold_base")), cc.get("pos_in_base"), rr.get("strength"), cc.get("score"), chip(rr)))
        r.log("RS 52-WEEK HIGHS: %s" % ", ".join(x["ticker"] for x in (bd.get("rs_new_highs") or [])))
        r.section("5. the desk: flows and 13F joins (was 'Unavailable' for most rows)")
        desk = [x for x in rows if "desk" in (x.get("scopes") or [])]
        n_flow = sum(1 for x in desk if x.get("flow") and x["flow"].get("current") and (x["flow"].get("w5") or x["flow"].get("w1")))
        n_f13 = sum(1 for x in desk if x.get("f13"))
        n_none = sum(1 for x in desk if not x.get("flow") and not x.get("f13"))
        r.log("desk rows %d | fund flow current %d | 13F read %d | neither %d | flows block %s | 13f block %s" % (len(desk), n_flow, n_f13, n_none, json.dumps(doc.get("flows")), json.dumps(doc.get("f13"))))
        for x in sorted(desk, key=lambda z: -(z.get("strength") or -1)):
            v = x.get("vs") or {}
            r.log("  %-6s %-16s %-6s px %8s str %5s | 1D %6s 1W %6s 1M %6s 3M %6s 6M %7s 1Y %7s | %-10s %-13s base %-16s | %s" % (
                x["ticker"], (x.get("name") or "")[:16], x.get("sleeve"), x.get("px"), x.get("strength"), pct(v.get("d")), pct(v.get("w")), pct(v.get("m")), pct(v.get("q")), pct(v.get("h")), pct(v.get("y")),
                (x.get("posture") or {}).get("code"), (x.get("pattern") or "-"), "%s %s" % ((x.get("coil") or {}).get("tier") or "-", (x.get("coil") or {}).get("base_len") or ""), chip(x)))
        r.log("sources: %s" % json.dumps(doc.get("sources")))
        r.log("errors: %s" % json.dumps((doc.get("errors") or [])[:20]))
        r.section("6. desk rows (JSON, for offline reading)")
        r.log("```json\n%s\n```" % json.dumps({"generated_at": doc.get("generated_at"), "as_of": doc.get("as_of"), "benchmark": bm, "boards": {k: bd.get(k) for k in ("breadth", "coil")}, "desk": desk}, separators=(",", ":"), default=str))
        r.kv(as_of=str(doc.get("as_of")), rows=str(len(rows)), ranked=str(bd.get("n_ranked")), desk_flow=str(n_flow), desk_13f=str(n_f13), coil=str(len(bd.get("coil") or [])), errors=str(doc.get("n_errors")), book=str(book.get("version")), tickers=str(tbl.get("n_tickers")))
        if n_none > 0:
            r.warn("%d desk rows carry neither a fund flow nor a 13F read: %s" % (n_none, [x["ticker"] for x in desk if not x.get("flow") and not x.get("f13")]))
        r.ok("STRENGTH LIVE: %d rows as of %s; desk flows %d/%d funds current, 13F %d; coil board %d names" % (len(rows), doc.get("as_of"), n_flow, sum(1 for x in desk if x.get("kind") == "fund"), n_f13, len(bd.get("coil") or [])))


if __name__ == "__main__":
    main()
    sys.exit(0)
