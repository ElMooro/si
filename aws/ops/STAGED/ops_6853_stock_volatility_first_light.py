"""ops 6853 - justhodl-stock-volatility: birth and first light (re-runnable).

Khalid 2026-11-19: "create an engine that would measure volatility. Most and least volatile stocks. Also it would
measure the price spreads for all stocks and rank them based on that. Price spread last day, week, month, quarter".
After deploy-lambdas ships the engine:
  1. the function exists with config.json's runtime / memory / timeout / env; the Scheduler schedule
     justhodl-stock-volatility-daily (04:06 UTC) exists, is ENABLED and targets it;
  2. deployed schedule manifest := committed (additions-only, the added entry live-verified) - the reconciler disables
     what the manifest does not declare;
  3. the engine once (RequestResponse; a first-light invoke through ops, the precedent of every engine birth), then
     data/stock-volatility.json read back: every rank is a permutation in the order of its measure, every board is the
     end of its ranking, what is set aside says why, the groups add up, the spreads are the highs over the lows;
  4. a sample recomputed a second way on the runner, straight from the banked bars (the standard library's statistics
     module, brute-force highs and lows) and, for eight large names, from Yahoo's own daily bars - another provider;
  5. the artifact embedded (gzip + base64) so the desk is verified offline against it before the browser proof.
Writes nothing but the schedule manifest (additions-only, with a backup) and what the engine owns.
"""
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import base64
import gzip
import json
import math
import re
import statistics
import sys
import time
import urllib.request

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-stock-volatility" / "source"))
from ops_report import report  # noqa: E402

sys.modules.setdefault("chain_guard", SimpleNamespace(begin=lambda event, context=None: 0))
import lambda_function as LF  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
KEY = "config/schedule-manifest.json"
FN, SCHED, OUT = "justhodl-stock-volatility", "justhodl-stock-volatility-daily", "data/stock-volatility.json"
PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
CFG = Config(retries={"max_attempts": 4, "mode": "standard"}, connect_timeout=10, read_timeout=900, max_pool_connections=32)
ONCE = Config(retries={"max_attempts": 1, "mode": "standard"}, connect_timeout=10, read_timeout=900)      # an invoke is never repeated by the client
MEASURES = [m for m, _, _ in LF.RANKED]
SAMPLE = ["AAPL", "MSFT", "NVDA", "TSLA", "JPM", "XOM", "PFE", "KO", "WMT", "AMD", "PLTR", "BRK.B", "SPY", "QQQ", "TLT", "GLD", "BIL"]
YAHOO = ["AAPL", "MSFT", "NVDA", "JPM", "XOM", "KO", "WMT", "TSLA"]


def c(name, cfg=CFG):
    return boto3.client(name, region_name=REGION, config=cfg)


def fn_of(arn):
    m = re.search(r":function:([^/:]+)", arn or "")
    return m.group(1) if m else arn


def entries(m):
    return {(e["kind"], e["name"]): e for sec in ("rules", "schedules") for e in m.get(sec, [])}


def second_way(C, H, L):
    out = {}
    for k in (21, 63, 252):
        out["hv%d" % k] = round(statistics.stdev([math.log(C[i] / C[i - 1]) for i in range(len(C) - k, len(C))]) * math.sqrt(252) * 100, 2) if len(C) > k else None
    for key, k in (("d", 1), ("w", 5), ("m", 21), ("q", 63)):
        out["sp_" + key] = round((max(H[-k:]) / min(L[-k:]) - 1) * 100, 2) if len(C) >= k else None
    return out


def yahoo_bars(sym, as_of):
    url = "%s/yf-ohlc?symbol=%s&range=2y&interval=1d&nowarehouse=1" % (PROXY, sym)
    with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 jh-ops"}), timeout=45) as rs:
        doc = json.loads(rs.read().decode("utf-8"))
    out = []
    for b in doc.get("bars") or []:
        t = b.get("time")
        day = datetime.fromtimestamp(t, tz=timezone.utc).date().isoformat() if isinstance(t, (int, float)) else str(t)[:10]
        if day <= as_of and all(isinstance(b.get(k), (int, float)) and b[k] > 0 for k in ("high", "low", "close")):
            out.append((day, b["high"], b["low"], b["close"]))
    return out, doc.get("source")


def main():
    s3, lam, sch = c("s3"), c("lambda"), c("scheduler")
    lam_once = c("lambda", ONCE)
    with report("ops_6853_stock_volatility_first_light") as r:
        r.heading("ops 6853 - justhodl-stock-volatility: birth and first light %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))

        r.section("1. function + schedule")
        cfg = json.loads((ROOT / "aws/lambdas/justhodl-stock-volatility/config.json").read_text(encoding="utf-8"))
        try:
            cf = lam.get_function_configuration(FunctionName=FN)
        except Exception as e:  # noqa: BLE001
            r.fail("%s not deployed yet (%s) - wait for deploy-lambdas, then run again" % (FN, type(e).__name__))
            raise SystemExit(1)
        env = (cf.get("Environment") or {}).get("Variables", {})
        r.log("%s: runtime %s, memory %s, timeout %s, state %s, modified %s, handler %s, env %s" % (
            FN, cf.get("Runtime"), cf.get("MemorySize"), cf.get("Timeout"), cf.get("State"), (cf.get("LastModified") or "")[:19], cf.get("Handler"), json.dumps({k: env[k] for k in sorted(env) if k in cfg["env"]})))
        if cf.get("Runtime") != cfg["runtime"] or cf.get("MemorySize") != cfg["memory"] or cf.get("Timeout") != cfg["timeout"] or cf.get("Handler") != cfg["handler"]:
            r.fail("live configuration differs from config.json")
            raise SystemExit(1)
        diff = {k: (env.get(k), v) for k, v in cfg["env"].items() if env.get(k) != v}
        if diff:
            r.fail("live environment differs from config.json: %s" % json.dumps(diff))
            raise SystemExit(1)
        if cf.get("State") != "Active":
            r.fail("function state is %s, not Active" % cf.get("State"))
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
        r.ok("function and schedule match config.json (one run a day)")

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

        r.section("3. first light: the engine once -> data/stock-volatility.json")
        t0 = time.time()
        resp = lam_once.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"trigger": "ops_6853_first_light"}).encode())
        payload = json.loads(resp["Payload"].read() or b"{}")
        if resp.get("FunctionError"):
            r.fail("FunctionError %s: %s" % (resp.get("FunctionError"), json.dumps(payload)[:1500]))
            raise SystemExit(1)
        obj = s3.get_object(Bucket=LIVE, Key=OUT)
        body = obj["Body"].read()
        out = json.loads(body)
        r.log("%.0fs wall | handler %s | written %s | %d bytes (gzip %d) | version %s contract %s | run %ss | errors %s | trigger %s | content-type %s cache %s" % (
            time.time() - t0, json.dumps(payload), obj["LastModified"].isoformat(timespec="seconds"), len(body), len(gzip.compress(body)), out.get("version"), out.get("contract"), out.get("run_seconds"),
            out.get("n_errors"), json.dumps(out.get("trigger")), obj.get("ContentType"), obj.get("CacheControl")))
        u = out["universe"]
        r.log("universe: %s" % json.dumps({k: v for k, v in u.items() if k != "reclassified"}))
        rows = [dict(zip(out["cols"], x)) for x in out["rows"]]
        by = {x["t"]: x for x in rows}
        bad = []
        if out["cols"] != LF.COLS or any(len(x) != len(LF.COLS) for x in out["rows"]):
            bad.append("the table is not the engine's columns")
        if len(by) != len(rows):
            bad.append("a ticker is listed twice")
        for kind in LF.KINDS:
            sub = [x for x in rows if x["kind"] == kind]
            if len(sub) != u["scanned_" + kind]:
                bad.append("%s count" % kind)
            for m in MEASURES:
                col = LF.RANK_COL[m]
                ranked = sorted([x for x in sub if x[col] is not None], key=lambda x: x[col])
                if [x[col] for x in ranked] != list(range(1, len(ranked) + 1)) or len(ranked) != u["ranked"][kind][m]:
                    bad.append("%s %s ranks are not 1..%d" % (kind, m, len(ranked)))
                if any(ranked[i][m] > ranked[i - 1][m] for i in range(1, len(ranked))):
                    bad.append("%s %s ranks are not in the order of the measure" % (kind, m))
                wrong = [x["t"] for x in sub if (x[col] is None) != (x[m] is None or m in (x["aside"] or "").split(","))]
                if wrong:
                    bad.append("%s %s: rank and set-aside disagree on %s" % (kind, m, wrong[:4]))
        for x in rows:
            why, aside = x["why"], (x["aside"] or "").split(",") if x["aside"] else []
            if (why is None) != (not aside):
                bad.append("%s: why %s but aside %s" % (x["t"], why, aside))
            if why == "pinned" and not (x["kind"] == "stock" and x["sp_m"] is not None and x["sp_m"] <= LF.PIN_SPREAD_M and aside == MEASURES):
                bad.append("%s pinned wrongly" % x["t"])
            if why == "thin" and not ((x["miss"] or 0) > LF.THIN_MISS and aside == MEASURES):
                bad.append("%s thin wrongly" % x["t"])
            if why == "jump" and not (x["jd"] and x["ja"] <= LF.JUMP_ASIDE and aside == MEASURES):
                bad.append("%s jump wrongly" % x["t"])
            if why is None and x["kind"] == "stock" and x["sp_m"] is not None and x["sp_m"] <= LF.PIN_SPREAD_M:
                bad.append("%s should be pinned" % x["t"])
            if why == "print" and not (x["pd"] and x["pa"] <= LF.JUMP_ASIDE and aside == MEASURES and (x["pr"] >= LF.PRINT_FLAG or x["pr"] <= 1 / LF.PRINT_FLAG)):
                bad.append("%s is set aside for a print it does not carry (%s, %s)" % (x["t"], x["pd"], x["pr"]))
            if why not in (None, "jump", "print", "thin", "pinned"):
                bad.append("%s: unknown reason %s" % (x["t"], why))
            if why is None and x["pa"] is not None and x["pa"] <= LF.JUMP_ASIDE:
                bad.append("%s has a print %d sessions back and is ranked" % (x["t"], x["pa"]))
            if why is None and x["ja"] is not None and x["ja"] <= LF.JUMP_ASIDE:
                bad.append("%s has a jump %d sessions back and is ranked" % (x["t"], x["ja"]))
            sp = [x["sp_" + k] for k in "dwmq" if x["sp_" + k] is not None]
            if any(sp[i] < sp[i - 1] - 0.011 for i in range(1, len(sp))):
                bad.append("%s: a wider window is narrower" % x["t"])
            for k in "dwmq":
                hi, lo, s_, pos = x["hi_" + k], x["lo_" + k], x["sp_" + k], x["pos_" + k]
                if s_ is None:
                    continue
                if not (lo > 0 and hi >= lo) or abs((hi / lo - 1) * 100 - s_) > 0.2 + 0.004 * s_ or (pos is not None and not 0 <= pos <= 100):
                    bad.append("%s %s: spread %s vs high %s low %s pos %s" % (x["t"], k, s_, hi, lo, pos))
            for k in ("hv21", "hv63", "hv252", "yz21", "atr"):
                if x[k] is not None and not x[k] >= 0:
                    bad.append("%s %s negative" % (x["t"], k))
        floor = out["board_rule"]["min_mcap"]
        ends = {"most_volatile": ("hv21", False), "least_volatile": ("hv21", True), "widest_d": ("sp_d", False), "narrowest_d": ("sp_d", True), "widest_w": ("sp_w", False), "narrowest_w": ("sp_w", True),
                "widest_m": ("sp_m", False), "narrowest_m": ("sp_m", True), "widest_q": ("sp_q", False), "narrowest_q": ("sp_q", True)}
        for name, (m, low) in ends.items():
            pool = sorted([x for x in rows if x["kind"] == "stock" and (x["mcap"] or 0) >= floor and x[LF.RANK_COL[m]] is not None], key=lambda x: x[LF.RANK_COL[m]], reverse=low)
            if out["boards"][name] != [x["t"] for x in pool[:LF.BOARD_N]] or len(pool) != out["board_rule"]["population"][m]:
                bad.append("board %s is not the end of its ranking" % name)
        for name in ("vol_rising", "vol_falling"):
            got = [by[t]["hv21"] - by[t]["hv21p"] for t in out["boards"][name]]
            if len(got) != LF.BOARD_N or any((got[i] > got[i - 1] + 1e-9) if name == "vol_rising" else (got[i] < got[i - 1] - 1e-9) for i in range(1, len(got))):
                bad.append("board %s is not in order" % name)
        if out["pinned"] != [x["t"] for x in sorted([x for x in rows if x["why"] == "pinned"], key=lambda x: (-(x["mcap"] or 0), x["t"]))] or len(out["pinned"]) != u["pinned"]["stock"]:
            bad.append("the pinned list")
        if sum(b_["n"] for b_ in out["bands"]) != u["ranked"]["stock"]["hv21"]:
            bad.append("bands do not hold every ranked stock")
        if sum(g["n"] for g in out["by_sector"]) != u["scanned_stock"] or sum(g["n"] for g in out["by_size"]) != u["scanned_stock"]:
            bad.append("sector / size tables")
        st_ranked = [x["hv21"] for x in rows if x["kind"] == "stock" and x["rk_hv21"] is not None]
        if abs(statistics.median(st_ranked) - out["market"]["stock"]["med"]["hv21"]) > 0.011:
            bad.append("the market's median hv21 %s is not the rows' median %s" % (out["market"]["stock"]["med"]["hv21"], statistics.median(st_ranked)))
        h = out["history"]
        bank_last = json.loads(s3.get_object(Bucket=LIVE, Key="data/warm/polygon-full/eod/manifest.json")["Body"].read()).get("last_session")
        if len(h) != LF.HISTORY_SESSIONS or h[-1]["date"] != out["as_of"] or any(not (x["p25"] <= x["med"] <= x["p75"]) for x in h):
            bad.append("history")
        if out.get("as_of") != bank_last:
            bad.append("as_of %s is not the bank's last session %s" % (out.get("as_of"), bank_last))
        if u["scanned"] != len(rows) or u["scanned"] != u["scanned_stock"] + u["scanned_etf"] or any(x["t"] in by for x in u["reclassified"]):
            bad.append("universe counts")
        (r.fail if bad else r.ok)("invariants: %s" % ("; ".join(bad[:12]) if bad else "every rank is 1..N in the order of its measure, every board is the end of its ranking, what is set aside says why, the groups add up, every spread is its high over its low, as of the bank's last session"))
        if bad:
            raise SystemExit(1)
        try:      # what the page will fetch: the same key, same origin
            req = urllib.request.Request("https://justhodl.ai/%s?nocache=%d" % (OUT, int(time.time())), headers={"User-Agent": "Mozilla/5.0 jh-ops"})
            with urllib.request.urlopen(req, timeout=60) as rs:
                edge = json.loads(rs.read().decode("utf-8"))
            same = edge.get("generated_at") == out.get("generated_at")
            (r.ok if same else r.warn)("edge: https://justhodl.ai/%s serves contract %s generated %s - %s" % (OUT, edge.get("contract"), edge.get("generated_at"), "this run" if same else "not this run yet (edge cache)"))
        except Exception as e:  # noqa: BLE001
            r.warn("edge: https://justhodl.ai/%s not readable from the runner (%s: %s) - the browser proof reads it" % (OUT, type(e).__name__, str(e)[:120]))

        r.section("4. a sample recomputed a second way")
        miss = []
        tops = [out["boards"][b_][0] for b_ in ("most_volatile", "least_volatile", "widest_d", "widest_q", "narrowest_q", "vol_rising")]
        sample = [t for t in dict.fromkeys(SAMPLE + tops + out["pinned"][:3]) if t in by]
        rec, _ = LF.recorded_splits(s3, u["bank_first_session"], out["as_of"], set(sample))
        book = {LF.canon(b_["symbol"]): b_ for b_ in LF.bank_enrich.rows(fields=LF.BOOK_FIELDS)}
        for t in sample:
            own, _ = LF.T.own_history(LF.read_bars(s3, t), str((book.get(t) or {}).get("ipo_date") or "")[:10] or None)
            bars = LF.clean(LF.T.rebase(own, rec.get(t) or [])[0])
            again = second_way([b_["c"] for b_ in bars], [b_["h"] for b_ in bars], [b_["l"] for b_ in bars])
            diff = {k: (by[t][k], v) for k, v in again.items() if v is not None and by[t][k] is not None and abs(by[t][k] - v) > 0.011}
            if diff:
                miss.append("%s %s" % (t, json.dumps(diff)))
        r.log("recomputed from the banked bars: %s" % ", ".join(sample))
        (r.fail if miss else r.ok)("banked bars, second way: %s" % ("; ".join(miss[:6]) if miss else "%d rows agree to the cent on the three volatilities and the four spreads" % len(sample)))
        far, seen = [], 0
        for t in YAHOO:
            try:
                yb, src = yahoo_bars(t, out["as_of"])
            except Exception as e:  # noqa: BLE001
                r.warn("Yahoo bars for %s not readable from the runner (%s: %s)" % (t, type(e).__name__, str(e)[:80]))
                continue
            if len(yb) < 260 or yb[-1][0] != out["as_of"]:
                r.warn("Yahoo bars for %s end %s (%d bars, source %s): not compared" % (t, yb[-1][0] if yb else None, len(yb), src))
                continue
            seen += 1
            again = second_way([b_[3] for b_ in yb], [b_[1] for b_ in yb], [b_[2] for b_ in yb])
            x = by[t]
            gaps = {k: (x[k], v) for k, v in again.items() if v is not None and x[k] is not None and abs(x[k] - v) > (1.0 if k.startswith("hv") else 0.6)}
            r.log("  %-5s desk hv21 %6.2f hv63 %6.2f hv252 %6.2f day %5.2f week %5.2f month %6.2f quarter %6.2f | Yahoo (%s) %s" % (
                t, x["hv21"], x["hv63"], x["hv252"], x["sp_d"], x["sp_w"], x["sp_m"], x["sp_q"], src, json.dumps(again)))
            if gaps:
                far.append("%s %s" % (t, json.dumps(gaps)))
        if seen:
            (r.fail if far else r.ok)("another provider: %s" % ("; ".join(far[:6]) if far else "%d large names agree with Yahoo's own daily bars within 1 point of volatility and 0.6 of spread" % seen))
        if miss or far:
            raise SystemExit(1)

        r.section("5. the two ends")
        med = out["market"]["stock"]["med"]
        r.log("market: %s" % json.dumps(out["market"]["stock"]))
        key = {"most_volatile": "hv21", "least_volatile": "hv21", "vol_rising": "hv21", "vol_falling": "hv21"}
        for name, tickers in out["boards"].items():
            m = key.get(name) or ("sp_" + name[-1])
            r.log("  %-13s %s" % (name, ", ".join("%s %s" % (t, by[t][m]) for t in tickers[:10])))
        r.log("  pinned (%d): %s" % (len(out["pinned"]), ", ".join(out["pinned"][:40])))
        r.log("  left out as notes / preferreds / warrants (%d): %s" % (len(u["reclassified"]), ", ".join(x["t"] for x in u["reclassified"])))
        r.log("changes: %s" % json.dumps(out.get("changes"))[:600])
        blob = base64.b64encode(gzip.compress(body, 9)).decode("ascii")
        r.lines.append("<!-- FEED-PACK-GZ-B64-BEGIN stock-volatility")
        r.lines.extend(blob[i:i + 16000] for i in range(0, len(blob), 16000))
        r.lines.append("FEED-PACK-GZ-B64-END -->")
        r.kv(as_of=str(out["as_of"]), stocks=str(u["scanned_stock"]), etfs=str(u["scanned_etf"]), median_hv21=str(med["hv21"]), median_day_spread=str(med["sp_d"]), pinned=str(len(out["pinned"])),
             run_seconds=str(out.get("run_seconds")), bytes=str(len(body)))
        r.ok("FIRST LIGHT: %s stocks and %s ETFs measured as of %s in %ss - median stock volatility %s%%, median day spread %s%%" % (
            u["scanned_stock"], u["scanned_etf"], out["as_of"], out.get("run_seconds"), med["hv21"], med["sp_d"]))


if __name__ == "__main__":
    main()
    sys.exit(0)
