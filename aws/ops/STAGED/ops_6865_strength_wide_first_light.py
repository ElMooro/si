"""ops 6865 - justhodl-strength 1.1.0: first light of the whole-market table (re-runnable).

Khalid 2026-11-20: "today everything is dumping hard however i cant see which stocks and etfs are showing strength and
dumping less than sp500".

  1. the deployed function matches config.json (900 s, the new key in its environment) and is settled;
  2. ONE invoke, {"wide_only": true}: the whole-market pass alone, on the session the published desk stands on - the
     desk's own artifact is not rewritten (a re-run of this op finds the table already there and invokes nothing,
     unless RUN_AGAIN=1 is set in the script);
  3. the table read back from S3: contract, session, rows, the last session's read; a sample recomputed a second way
     from the banked bars; the history snapshot;
  4. what a browser gets from https://justhodl.ai/data/strength-wide.json and the forming-bar file (status, type, size);
  5. today's forming bars over the table by the module's own reference of the page's join - the live read the page
     must restate; the table, the forming bars and that read are embedded for the page's tests.
"""
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import base64
import gzip
import json
import sys
import time
import urllib.request

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-strength" / "source"))
from ops_report import report  # noqa: E402

sys.modules.setdefault("chain_guard", SimpleNamespace(begin=lambda event, context=None: 0))
import lambda_function as LF  # noqa: E402
import wide as W  # noqa: E402

FN = "justhodl-strength"
BUCKET = "justhodl-dashboard-live"
RUN_AGAIN = False
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36 JustHodlBot/ops-6865"
CFG = Config(retries={"max_attempts": 4, "mode": "standard"}, connect_timeout=5, read_timeout=60)
ONCE = Config(retries={"max_attempts": 1, "mode": "standard"}, connect_timeout=10, read_timeout=900)      # an invoke is never repeated by the client
SAMPLE = ["AAPL", "MSFT", "NVDA", "JPM", "XOM", "WMT", "KO", "FICO", "BRK.B", "QQQ", "IWM", "TLT", "GLD", "XLU", "XLP", "SMH", "HYG"]


def embed(r, name, obj):
    body = json.dumps(obj, separators=(",", ":"), default=str).encode("utf-8")
    blob = base64.b64encode(gzip.compress(body, 9)).decode("ascii")
    r.lines.append("<!-- FEED-PACK-GZ-B64-BEGIN %s" % name)
    r.lines.extend(blob[i:i + 16000] for i in range(0, len(blob), 16000))
    r.lines.append("FEED-PACK-GZ-B64-END -->")
    r.log("embedded %s: %d bytes json, %d b64" % (name, len(body), len(blob)))


def group_line(r, label, g):
    if not g or not g.get("n"):
        r.log("  %-30s none" % label)
        return
    r.log("  %-30s n %5d  up %5.1f%%  flat %4d  down less %4d  down more %4d  held up %5.1f%%  median %+.2f%%" % (
        label, g["n"], g["up_pct"], g["flat"], g["down_less"], g["down_more"], g["held_pct"], g["median_ret"]))


def show_read(r, rd):
    r.log("regime %s, SPY %+.2f%%, %d names counted; rank carried in as of %s (%s), record %s" % (rd["regime"], rd["spy_ret"], rd["n_counted"], rd["rank_as_of"], rd["rank_col"], rd["record_cols"]))
    for key, label in (("stocks", "all stocks"), ("stocks_1b", "stocks $1B+"), ("stocks_10b", "stocks $10B+"), ("sp500", "S&P 500 members"), ("etfs", "ETFs"), ("etfs_liquid", "ETFs trading $5M+ a day")):
        group_line(r, label, rd["groups"][key])
    for g in rd["by_sector"]:
        group_line(r, "$1B+ " + g["sector"][:24], g)
    for g in rd["by_class"]:
        group_line(r, "ETF " + g["label"][:24], g)
    r.log("by rank carried in ($1B+ stocks, weakest tenth -> strongest): held up %s | median %s" % ([d.get("held_pct") for d in rd["by_rank"]], [d.get("median_ret") for d in rd["by_rank"]]))
    r.log("by the dump-day record carried in ($1B+ stocks): %s" % "; ".join("%s: n %s, held up %s%%, median %s%%" % (g["label"], g["n"], g.get("held_pct"), g.get("median_ret")) for g in rd["by_record"]))
    for name, tickers in rd["boards"].items():
        r.log("  board %-16s %s" % (name, ", ".join(tickers[:25])))


def main():
    now = datetime.now(timezone.utc)
    s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
    lam = boto3.client("lambda", region_name="us-east-1", config=CFG)
    lam_once = boto3.client("lambda", region_name="us-east-1", config=ONCE)
    with report("ops_6865_strength_wide_first_light") as r:
        r.heading("ops 6865 - justhodl-strength 1.1.0: first light of the whole-market table %s" % now.isoformat(timespec="minutes"))

        r.section("1. the deployed function")
        cfg = json.loads((ROOT / "aws/lambdas/justhodl-strength/config.json").read_text(encoding="utf-8"))
        cf = lam.get_function_configuration(FunctionName=FN)
        for _ in range(30):
            if cf.get("State") == "Active" and cf.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(4)
            cf = lam.get_function_configuration(FunctionName=FN)
        env = (cf.get("Environment") or {}).get("Variables", {})
        r.log("%s: runtime %s, memory %s, timeout %s, state %s / %s, modified %s, code sha %s, env %s" % (
            FN, cf.get("Runtime"), cf.get("MemorySize"), cf.get("Timeout"), cf.get("State"), cf.get("LastUpdateStatus"), (cf.get("LastModified") or "")[:19], (cf.get("CodeSha256") or "")[:12],
            json.dumps({k: env.get(k) for k in sorted(cfg["env"])})))
        if cf.get("Timeout") != cfg["timeout"] or cf.get("MemorySize") != cfg["memory"] or env.get("WIDE_KEY") != cfg["env"]["WIDE_KEY"] or cf.get("State") != "Active" or cf.get("LastUpdateStatus") != "Successful":
            r.fail("the live function is not the committed configuration yet (timeout %s vs %s, WIDE_KEY %s) - wait for deploy-lambdas, then run again" % (cf.get("Timeout"), cfg["timeout"], env.get("WIDE_KEY")))
            raise SystemExit(1)
        r.ok("function matches config.json: %ss, %s MB, WIDE_KEY %s" % (cf["Timeout"], cf["MemorySize"], env["WIDE_KEY"]))

        r.section("2. one invoke: the whole-market pass alone")
        desk = json.loads(s3.get_object(Bucket=BUCKET, Key=LF.OUT_KEY)["Body"].read())
        desk_meta = s3.head_object(Bucket=BUCKET, Key=LF.OUT_KEY)
        r.log("the published desk: version %s, as of %s, generated %s (object written %s)" % (desk.get("version"), desk["as_of"], desk.get("generated_at"), desk_meta["LastModified"].isoformat()))
        have = None
        try:
            have = json.loads(s3.get_object(Bucket=BUCKET, Key=LF.WIDE_KEY)["Body"].read())
        except Exception as exc:  # noqa: BLE001
            r.log("no whole-market table yet (%s)" % type(exc).__name__)
        if have and have.get("as_of") == desk["as_of"] and have.get("version") == LF.VERSION and not RUN_AGAIN:
            r.log("the table already stands on %s (version %s, generated %s): nothing invoked" % (have["as_of"], have["version"], have["generated_at"]))
        else:
            t0 = time.time()
            resp = lam_once.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"wide_only": True, "trigger": "ops_6865_first_light"}).encode())
            payload = json.loads(resp["Payload"].read() or b"{}")
            r.log("invoke: status %s, function error %s, %.1fs, answer %s" % (resp.get("StatusCode"), resp.get("FunctionError"), time.time() - t0, json.dumps(payload)[:700]))
            if resp.get("FunctionError") or "error" in (payload.get("wide") or {}):
                r.fail("the whole-market pass did not publish: %s" % json.dumps(payload)[:500])
                raise SystemExit(1)
        after = s3.head_object(Bucket=BUCKET, Key=LF.OUT_KEY)
        (r.ok if after["LastModified"] == desk_meta["LastModified"] else r.fail)("the desk's own artifact was not rewritten by this op (written %s)" % after["LastModified"].isoformat())

        r.section("3. the table, read back")
        obj = s3.get_object(Bucket=BUCKET, Key=LF.WIDE_KEY)
        raw = obj["Body"].read()
        doc = json.loads(raw)
        rows = [dict(zip(doc["cols"], x)) for x in doc["rows"]]
        by = {x["t"]: x for x in rows}
        u = doc["universe"]
        r.log("%s %s %s: as of %s (prior %s), generated %s, %d rows x %d cols, %d bytes, cache %s, %.1fs on Lambda, errors %d" % (
            doc["engine"], doc["version"], doc["contract"], doc["as_of"], doc["prior_session"], doc["generated_at"], len(rows), len(doc["cols"]), len(raw), obj.get("CacheControl"), doc["run_seconds"], doc["n_errors"]))
        r.log("universe: %s" % json.dumps({k: u[k] for k in ("eligible", "eligible_stock", "eligible_etf", "scanned", "scanned_stock", "scanned_etf", "ranked", "set_aside", "short_history", "unbanked", "not_current", "sp500_members")}))
        r.log("benchmark %s" % json.dumps(doc["benchmark"]))
        r.log("dump days %s | rally days %s" % (json.dumps([d["date"] for d in doc["dump_days"]]), json.dumps([d["date"] for d in doc["rally_days"]])))
        r.log("splits ahead: %s" % json.dumps(doc["splits_ahead"])[:900])
        checks = [
            ("contract and version", doc["contract"] == "strength-wide.v1" and doc["version"] == LF.VERSION == "1.1.0"),
            ("stands on the desk's session", doc["as_of"] == desk["as_of"]),
            ("columns as committed", doc["cols"] == W.COLS),
            ("more than 6,500 names", len(rows) > 6500 and u["scanned_stock"] > 3500 and u["scanned_etf"] > 2500),
            ("every row is of the table's session with a prior close", all(x["c"] for x in rows) and sum(1 for x in rows if x["pc"]) > 0.98 * len(rows)),
            ("ranked: most stocks and ETFs", u["ranked"]["stock"] > 3500 and u["ranked"]["etf"] > 2500),
            ("no errors", doc["n_errors"] == 0),
            ("cache five minutes", obj.get("CacheControl") == "max-age=300"),
            ("the rules are the desk's", doc["rules"]["flat_pct"] == LF.FLAT_PCT and doc["rules"]["dump_pct"] == LF.DUMP_PCT and doc["rules"]["rally_pct"] == LF.RALLY_PCT),
            ("geared funds set aside, never ranked", all(x["rs"] is None for x in rows if x["why"]) and all(x["why"] == "geared" for x in rows if x["cls"] == "leveraged")),
        ]
        for label, ok in checks:
            (r.ok if ok else r.fail)(label)
        bench = {b["date"]: b["c"] for b in W.read_bars(s3, "SPY", tail=400, through=doc["as_of"])}
        bad, n_checked = [], 0
        for t in SAMPLE:
            x = by.get(t)
            if not x or x["n"] < 130:
                continue
            bars = [b for b in W.read_bars(s3, t, tail=400, through=doc["as_of"]) if b["date"] in bench]
            c, s = [b["c"] for b in bars], [bench[b["date"]] for b in bars]
            again = {k: round((c[-1] / c[-1 - n] - 1) * 100 - (s[-1] / s[-1 - n] - 1) * 100, 2) for k, n in (("x1", 1), ("x5", 5), ("x21", 21), ("x63", 63), ("x126", 126))}
            diff = {k: (x[k], v) for k, v in again.items() if x[k] is not None and abs(x[k] - v) > 0.011}
            n_checked += 1
            if diff:
                bad.append("%s %s" % (t, json.dumps(diff)))
        (r.fail if bad else r.ok)("second way: %s" % ("; ".join(bad) if bad else "%d sampled names agree to the cent on all five horizons" % n_checked))
        hk = LF.HISTORY_PREFIX + doc["as_of"] + ".wide.json.gz"
        try:
            h = s3.head_object(Bucket=BUCKET, Key=hk)
            r.ok("history snapshot %s (%d bytes, %s)" % (hk, h["ContentLength"], h.get("ContentEncoding")))
        except Exception as exc:  # noqa: BLE001
            r.fail("history snapshot %s missing (%s)" % (hk, type(exc).__name__))
        r.log("the last session as the engine read it:")
        show_read(r, doc["last_session"])

        r.section("4. what a browser gets")
        for path in ("/data/strength-wide.json", "/data/warm/polygon-full/today.json.gz"):
            try:
                t0 = time.time()
                req = urllib.request.Request("https://justhodl.ai" + path, headers={"User-Agent": UA, "Accept-Encoding": "gzip"})
                with urllib.request.urlopen(req, timeout=40) as resp:
                    wire = resp.read()
                    hd = {k: resp.headers.get(k) for k in ("Content-Type", "Content-Encoding", "Cache-Control", "X-Edge-TTL", "X-Edge-Cache", "Last-Modified")}
                body = gzip.decompress(wire) if hd.get("Content-Encoding") == "gzip" else wire
                j = json.loads(body)
                (r.ok if (j.get("contract") == "strength-wide.v1" or j.get("session")) else r.fail)("GET %s -> %d wire bytes, %d decoded, %.2fs, %s, keys %s" % (path, len(wire), len(body), time.time() - t0, json.dumps(hd), ", ".join(sorted(j.keys()))[:120]))
            except Exception as exc:  # noqa: BLE001
                r.fail("GET %s failed: %s: %s" % (path, type(exc).__name__, str(exc)[:160]))

        r.section("5. today's forming bars over the table")
        today = json.loads(gzip.decompress(s3.get_object(Bucket=BUCKET, Key=W.TODAY_KEY)["Body"].read()))
        r.log("forming file: session %s, bars as of %s, %d rows" % (today.get("session"), today.get("as_of"), len(today.get("rows") or {})))
        live = W.live_read(doc, today, class_labels=LF.CLASS_LABEL)
        if live:
            r.log("live: %d names without a print, %d unconfirmed (%s)" % (live["n_no_print"], live["n_unconfirmed"], ", ".join(live["unconfirmed"])))
            show_read(r, live)
        else:
            r.log("no live session: the forming file's session %s is not after the table's %s" % (today.get("session"), doc["as_of"]))
        keep = set(by) | {"SPY"}
        today_small = dict(today, rows={t: v for t, v in (today.get("rows") or {}).items() if t in keep})
        embed(r, "wide", doc)
        embed(r, "today", today_small)
        embed(r, "live-read", live or {})
        g = doc["last_session"]["groups"]["stocks_1b"]
        r.kv(rows=str(len(rows)), as_of=doc["as_of"], lambda_seconds=str(doc["run_seconds"]), bytes=str(len(raw)), last_session_held_1b="%s%%" % g.get("held_pct"),
             live_held_1b="%s%%" % ((live or {}).get("groups", {}).get("stocks_1b", {}).get("held_pct")), live_spy="%s" % (live or {}).get("spy_ret"))
        if getattr(r, "_failed", False):
            raise SystemExit(1)
        r.ok("FIRST LIGHT: data/strength-wide.json holds %d names as of %s (%.0fs on Lambda); the desk's artifact untouched" % (len(rows), doc["as_of"], doc["run_seconds"]))


if __name__ == "__main__":
    main()
    sys.exit(0)
