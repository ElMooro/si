"""ops 6739 - READ-ONLY: the sources a server-side strength engine would stand on (Khalid 2026-11-04: "Strength vs S&P
500 ... most matched fund flows is missing; also improve the strength and add which one has been in long accumulation
with very very tight price spread").

A. the ETF desk packet (data/etf-desk-research.json): for every ETF in the strength universe, is the fund's flow
   current-eligible now, and which of its windows are matched - the SPY-aligned 1 / 5 / 21 (what strong.html prints
   today, 1 observation only) and the fund's OWN latest 1 / 5 / 21 (same data, the fund's own last reporting date) -
   plus the same counts across all configured funds; the canonical packet it replays (300 funds).
B. the Polygon EOD bank (data/warm/polygon-full/eod/<T>.json.gz): manifest, per-ticker depth and read time for a
   sample across sleeves (dot tickers included), whether complete_only answers right now.
C. the FMP walker: is the S&P 500 constituent list banked (universe expansion), what the manifest holds.
D. the 13F book: version, periods, the latest period snapshot's per-identity table and how many of the 52 desk
   tickers it covers (the smart-money read for stocks, which have no fund flows).
E. the three producers' schedules and last runs (polygon-eod, etf-fund-flows, etf-global-desk).
Nothing is written. No key involved.
"""
from datetime import datetime, timezone
from pathlib import Path
from collections import Counter
import gzip
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
from ops_report import report  # noqa: E402

REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=120)
DESK_ETFS = ["QQQ", "IWM", "DIA", "XLK", "SMH", "XLF", "XLE", "XLU", "XLP", "XLV", "XLI", "XLY", "GLD", "TLT", "HYG", "EEM", "FXI", "EWJ", "KRE", "XBI",
             "SLV", "GDX", "GDXJ", "PPLT", "CPER", "IAU", "SIVR", "IBIT", "ETHA", "FBTC", "BITO", "SPY"]
DESK_STOCKS = ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AVGO", "JPM", "LLY", "XOM", "UNH", "V", "MA", "HD", "COST", "NFLX", "AMD", "ORCL", "JNJ"]
BAR_SAMPLE = ["SPY", "QQQ", "NVDA", "GLD", "IBIT", "SIVR", "PPLT", "BRK.B", "ETHA", "KRE", "COST"]
PRODUCERS = [("justhodl-polygon-eod", "justhodl-polygon-eod-daily"), ("justhodl-etf-fund-flows", "justhodl-etf-fund-flows-daily"), ("justhodl-etf-global-desk", "justhodl-etf-global-desk-daily")]


def get(s3, key):
    obj = s3.get_object(Bucket=LIVE, Key=key)
    raw = obj["Body"].read()
    if key.endswith(".gz"):
        raw = gzip.decompress(raw)
    return json.loads(raw), obj["LastModified"], len(raw)


def win(w):
    if not w:
        return "absent"
    return "%s %s..%s %s%s" % (w.get("status"), w.get("start_date"), w.get("end_date"), w.get("flow_usd_decimal"),
                               (" reasons " + ",".join(w.get("reasons") or [])) if w.get("status") != "matched_reporting_window" else "")


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    logs = boto3.client("logs", region_name=REGION, config=CFG)
    sch = boto3.client("scheduler", region_name=REGION, config=CFG)
    ev = boto3.client("events", region_name=REGION, config=CFG)
    now = datetime.now(timezone.utc)
    with report("ops_6739_strength_sources_probe") as r:
        r.heading("ops 6739 - strength engine sources (read-only) %s" % now.isoformat(timespec="minutes"))

        r.section("A. ETF desk packet: flow windows for the strength universe")
        desk, mod, nbytes = get(s3, "data/etf-desk-research.json")
        q = desk.get("quality") or {}
        ref = desk.get("reference") or {}
        r.log("written %s | generated %s | query_date %s | version %s | %d bytes | reference end %s (%s obs) | quality %s" % (
            mod.isoformat(timespec="seconds"), desk.get("generated_at"), desk.get("query_date"), desk.get("version"), nbytes, ref.get("end_date"), ref.get("observations"),
            json.dumps({k: q[k] for k in ("configured_funds", "flows_current_eligible", "profiles_current_eligible", "holdings_complete", "status") if k in q})))
        funds = desk.get("funds") or {}
        tot = Counter()
        for t, f in funds.items():
            fl = f.get("flows") or {}
            cur = bool((f.get("quality") or {}).get("flow_current_eligible")) and str(fl.get("source_valid_until") or "") > now.isoformat()
            tot["current_now"] += cur
            for n in ("1", "5", "21"):
                aw = (fl.get("aligned_windows") or {}).get(n) or {}
                ow = (fl.get("latest_windows") or {}).get(n) or {}
                tot["aligned_%s_matched" % n] += cur and aw.get("status") == "matched_reporting_window" and aw.get("flow_usd_decimal") is not None
                tot["own_%s_matched" % n] += cur and ow.get("status") == "matched_reporting_window" and ow.get("flow_usd_decimal") is not None
            tot["latest_eff_" + str(fl.get("latest_effective_date"))] += 1
        r.log("all %d configured funds: %s" % (len(funds), json.dumps(dict(sorted(tot.items())))))
        for t in DESK_ETFS:
            f = funds.get(t)
            if not f:
                r.log("  %-5s NOT A CONFIGURED DESK FUND" % t)
                continue
            fl = f.get("flows") or {}
            qq = f.get("quality") or {}
            r.log("  %-5s eligible %s | valid_until %s | latest_effective %s processed %s | status %s" % (
                t, qq.get("flow_current_eligible"), fl.get("source_valid_until"), fl.get("latest_effective_date"), fl.get("latest_processed_date"), (fl.get("quality") or {}).get("status")))
            for n in ("1", "5", "21"):
                r.log("        aligned %-2s %s" % (n, win((fl.get("aligned_windows") or {}).get(n))))
                r.log("        own     %-2s %s" % (n, win((fl.get("latest_windows") or {}).get(n))))
        try:
            canon, cmod, cbytes = get(s3, "data/provider-fund-flow-research.json")
            cf = canon.get("funds") or {}
            st = Counter((f.get("quality") or {}).get("status") for f in cf.values())
            r.log("canonical packet: written %s | generated %s | %d funds | status %s | reference end %s | %d bytes" % (
                cmod.isoformat(timespec="seconds"), canon.get("generated_at"), len(cf), json.dumps(st), (canon.get("reference") or {}).get("end_date"), cbytes))
            extra = [t for t in DESK_ETFS if t in cf and t not in funds]
            r.log("strength ETFs in the canonical 300 but not in the desk: %s" % (extra or "-"))
        except Exception as e:  # noqa: BLE001
            r.warn("canonical packet: %s %s" % (type(e).__name__, str(e)[:120]))

        r.section("B. Polygon EOD bank")
        import polygon_bankfirst as pb
        man = pb._get(pb.MANIFEST) or {}
        r.log("manifest keys: %s" % sorted(man.keys())[:30])
        small = {k: v for k, v in man.items() if not isinstance(v, (dict, list)) or len(json.dumps(v)) < 400}
        r.log("manifest (scalars): %s" % json.dumps(small, default=str)[:1500])
        r.log("last completed session by the clock: %s | in session window now: %s" % (pb._last_completed_session(now), pb._in_session_window(now)))
        for t in BAR_SAMPLE:
            t0 = time.time()
            rows = pb.bars(t, complete_only=False)
            dt = time.time() - t0
            comp = pb.bars(t, sessions=5, complete_only=True)
            r.log("  %-6s %4d sessions %s..%s read %.2fs | complete_only answers: %s | last close %s vol %s" % (
                t, len(rows), rows[0]["date"] if rows else "-", rows[-1]["date"] if rows else "-", dt, bool(comp), rows[-1].get("c") if rows else "-", rows[-1].get("v") if rows else "-"))

        r.section("C. FMP walker: S&P 500 constituents + manifest")
        import fmp_bank as fb
        wm = fb.walker_manifest() or {}
        ds_map = wm.get("datasets") if isinstance(wm.get("datasets"), (dict, list)) else wm
        names = list(ds_map.keys()) if isinstance(ds_map, dict) else [str((d or {}).get("name") or (d or {}).get("dataset") or d)[:60] for d in (ds_map or [])]
        r.log("walker manifest keys %s | %d datasets; price-ish: %s" % (sorted(wm.keys())[:20] if isinstance(wm, dict) else type(wm).__name__, len(names), [k for k in names if "price" in k or "constituent" in k][:12]))
        for ds in ("sp500-constituent", "nasdaq-constituent", "dowjones-constituent"):
            rows = fb.walker(ds)
            syms = [x.get("symbol") for x in rows if isinstance(x, dict)] if isinstance(rows, list) else []
            r.log("  %-20s %s rows | sample %s" % (ds, len(rows) if isinstance(rows, list) else rows, syms[:8]))

        r.section("D. 13F book + latest period snapshot")
        book, bmod, bbytes = get(s3, "data/13f-book.json")
        b = book.get("book") or {}
        periods = ((b.get("history") or {}).get("periods")) or []
        r.log("book %s written %s | required_period %s | periods %s" % (book.get("version"), bmod.isoformat(timespec="seconds"), b.get("required_period"), periods[-5:]))
        latest = periods[-1] if periods else None
        if latest:
            key = (b.get("history") or {}).get("prefix", "data/13f-book/periods/") + (latest if isinstance(latest, str) else latest.get("period")) + ".json"
            try:
                snap, smod, sbytes = get(s3, key)
                by = snap.get("by_identity") or {}
                tick = {}
                for pid, row in by.items():
                    if row and row[0]:
                        tick.setdefault(row[0], []).append(row)
                r.log("snapshot %s: %d identities, %d tickers, %d bytes, columns %s" % (key, len(by), len(tick), sbytes, snap.get("columns")))
                for t in DESK_STOCKS + DESK_ETFS[:12]:
                    rows = tick.get(t)
                    r.log("  %-5s %s" % (t, ["holders %s prior %s value %s" % (x[3], x[5], x[4]) for x in rows] if rows else "not in snapshot"))
            except Exception as e:  # noqa: BLE001
                r.warn("snapshot %s: %s %s" % (key, type(e).__name__, str(e)[:120]))
        st_all = ((b.get("stance") or {}).get("cohort") or {}).get("all") or {}
        r.log("stance cohort/all keys: %s | accumulating %d distributing %d" % (list(st_all.keys())[:8], len(st_all.get("accumulating") or []), len(st_all.get("distributing") or [])))

        r.section("E. producers: schedules + last runs")
        for fn, sname in PRODUCERS:
            try:
                cf = lam.get_function_configuration(FunctionName=fn)
                r.log("%s: %s MB / %s s, modified %s, state %s" % (fn, cf.get("MemorySize"), cf.get("Timeout"), (cf.get("LastModified") or "")[:19], cf.get("State")))
            except Exception as e:  # noqa: BLE001
                r.warn("%s: %s" % (fn, type(e).__name__))
            expr = None
            try:
                d = sch.get_schedule(Name=sname, GroupName="default")
                expr = "scheduler %s %s" % (d.get("ScheduleExpression"), d.get("State"))
            except Exception:  # noqa: BLE001
                try:
                    d = ev.describe_rule(Name=sname)
                    expr = "events %s %s" % (d.get("ScheduleExpression"), d.get("State"))
                except Exception as e:  # noqa: BLE001
                    expr = "no schedule found (%s)" % type(e).__name__
            r.log("   schedule %s: %s" % (sname, expr))
            try:
                st = logs.describe_log_streams(logGroupName="/aws/lambda/" + fn, orderBy="LastEventTime", descending=True, limit=3)
                for s in st.get("logStreams") or []:
                    r.log("   stream last event %s" % datetime.fromtimestamp((s.get("lastEventTimestamp") or 0) / 1000, tz=timezone.utc).isoformat(timespec="minutes"))
            except Exception as e:  # noqa: BLE001
                r.warn("   logs: %s" % type(e).__name__)
        r.kv(desk_funds=str(len(funds)), current_now=str(tot["current_now"]), aligned_1=str(tot["aligned_1_matched"]), aligned_5=str(tot["aligned_5_matched"]),
             own_1=str(tot["own_1_matched"]), own_5=str(tot["own_5_matched"]), own_21=str(tot["own_21_matched"]))
        r.ok("ops 6739 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
