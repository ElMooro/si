"""ops 6864 - READ-ONLY dry run of the strength engine's whole-market pass over the real warehouse (re-runnable).

Khalid 2026-11-20: "today everything is dumping hard however i cant see which stocks and etfs are showing strength and
dumping less than sp500".  justhodl-strength 1.1.0 adds wide.py: every listed common stock and plain ETF against SPY.
Before it is deployed its own build() is run here on the runner against the real bank: how many names it measures, how
long that takes, what the table holds, how big the feed is.  Then:

  * a sample of rows is recomputed a second way straight from the banked bars;
  * the names the desk already ranks are compared with the desk's own artifact (same excess returns);
  * today's forming bars are laid over the table with the module's own reference of the page's join - the live read.

Nothing is written (every put is refused).  The table and the forming bars are embedded for the page's offline harness.
"""
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import base64
import gzip
import json
import sys
import time

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

BUCKET = "justhodl-dashboard-live"
SAMPLE = ["AAPL", "MSFT", "NVDA", "TSLA", "JPM", "XOM", "WMT", "KO", "UNH", "FICO", "MKC", "CTSH", "SNDK", "BRK.B", "QQQ", "IWM", "TLT", "GLD", "XLU", "XLP", "XLK", "SMH", "HYG", "IBIT"]
SHOW = ["t", "kind", "cls", "mcap", "sp", "pc", "c", "x1", "x5", "x21", "x63", "x126", "rs", "rsp", "dn", "dh", "ds", "dx", "rn", "rb", "rx", "dnx", "upx", "atr", "p52", "ma", "n", "why"]


class ReadOnly:
    """The real S3 client with every write refused: a dry run cannot publish."""

    def __init__(self, s3):
        self._s3 = s3

    def get_object(self, **kw):
        return self._s3.get_object(**kw)

    def head_object(self, **kw):
        return self._s3.head_object(**kw)

    def put_object(self, **kw):
        raise RuntimeError("dry run: put refused for %s" % kw.get("Key"))


def embed(r, name, obj):
    body = json.dumps(obj, separators=(",", ":"), default=str).encode("utf-8")
    blob = base64.b64encode(gzip.compress(body, 9)).decode("ascii")
    r.lines.append("<!-- FEED-PACK-GZ-B64-BEGIN %s" % name)
    r.lines.extend(blob[i:i + 16000] for i in range(0, len(blob), 16000))
    r.lines.append("FEED-PACK-GZ-B64-END -->")
    r.log("embedded %s: %d bytes json, %d gzipped, %d b64" % (name, len(body), len(gzip.compress(body, 6)), len(blob)))


def group_line(r, label, g):
    if not g or not g.get("n"):
        r.log("  %-30s none" % label)
        return
    r.log("  %-30s n %5d  up %5.1f%%  flat %4d  down less %4d  down more %4d  held up %5.1f%%  beat SPY %5.1f%%  median %+.2f%%" % (
        label, g["n"], g["up_pct"], g["flat"], g["down_less"], g["down_more"], g["held_pct"], g["beat_pct"], g["median_ret"]))


def show_read(r, rd, by):
    r.log("regime %s, SPY %+.2f%%, %d names counted; ranks as of %s (%s)" % (rd["regime"], rd["spy_ret"], rd["n_counted"], rd["rank_as_of"], rd["rank_col"]))
    for key, label in (("stocks", "all stocks"), ("stocks_1b", "stocks $1B+"), ("stocks_10b", "stocks $10B+"), ("sp500", "S&P 500 members"), ("etfs", "ETFs"), ("etfs_liquid", "ETFs trading $5M+ a day")):
        group_line(r, label, rd["groups"][key])
    r.log("market value of $1B+ stocks by outcome: %s" % json.dumps(rd["mcap_1b"]))
    for g in rd["by_sector"]:
        group_line(r, "$1B+ " + g["sector"][:24], g)
    for g in rd["by_class"]:
        group_line(r, "ETF " + g["label"][:24], g)
    r.log("by rank carried in ($1B+ stocks, weakest tenth -> strongest): held up %s | median %s | n %s" % (
        [d.get("held_pct") for d in rd["by_rank"]], [d.get("median_ret") for d in rd["by_rank"]], [d.get("n") for d in rd["by_rank"]]))
    ind = rd["industries"]
    r.log("industries (%d with 5+ $1B+ members) holding up: %s" % (ind["n"], "; ".join("%s %s%% of %d (median %+.2f%%)" % (g["industry"], g["held_pct"], g["n"], g["median_ret"]) for g in ind["holding"][:12])))
    r.log("industries breaking: %s" % "; ".join("%s %s%% of %d (median %+.2f%%)" % (g["industry"], g["held_pct"], g["n"], g["median_ret"]) for g in ind["breaking"][:12]))
    for name, tickers in rd["boards"].items():
        r.log("  board %-16s %s" % (name, ", ".join(tickers[:25])))


def main():
    now = datetime.now(timezone.utc)
    s3 = ReadOnly(boto3.client("s3", region_name="us-east-1", config=Config(retries={"max_attempts": 4, "mode": "standard"}, connect_timeout=5, read_timeout=30, max_pool_connections=40)))
    with report("ops_6864_strength_wide_dry_run") as r:
        r.heading("ops 6864 - strength, the whole-market pass: dry run over the warehouse %s (read-only)" % now.isoformat(timespec="minutes"))
        desk = json.loads(s3.get_object(Bucket=BUCKET, Key=LF.OUT_KEY)["Body"].read())
        as_of = desk["as_of"]
        r.log("the published desk: version %s, as of %s, generated %s, %d rows" % (desk.get("version"), as_of, desk.get("generated_at"), len(desk.get("rows") or [])))
        rules = {"flat_pct": LF.FLAT_PCT, "dump_pct": LF.DUMP_PCT, "rally_pct": LF.RALLY_PCT, "weights": dict(LF.WEIGHTS)}
        logs = []
        t0 = time.time()
        out = W.build(s3, now, as_of, LF.VERSION, rules, classify=LF.asset_class_of, class_labels=LF.CLASS_LABEL, log=logs.append, remaining_s=1500.0)
        took = time.time() - t0
        for line in logs:
            r.log(line)
        rows = [dict(zip(out["cols"], x)) for x in out["rows"]]
        by = {x["t"]: x for x in rows}
        body = json.dumps(out, separators=(",", ":"), default=str).encode("utf-8")
        r.log("build() %.1fs on the runner; engine %s %s contract %s; as of %s (prior session %s); %d rows x %d columns; %d bytes, %d gzipped" % (
            took, out["engine"], out["version"], out["contract"], out["as_of"], out["prior_session"], len(rows), len(out["cols"]), len(body), len(gzip.compress(body, 6))))

        r.section("universe")
        r.log(json.dumps(out["universe"])[:2400])
        r.log("benchmark: %s" % json.dumps(out["benchmark"]))
        r.log("dump days in the window: %s" % json.dumps(out["dump_days"]))
        r.log("rally days in the window: %s" % json.dumps(out["rally_days"]))
        r.log("splits announced for the sessions after the table (%d): %s" % (len(out["splits_ahead"]), json.dumps(out["splits_ahead"])[:1500]))
        blank = {c: sum(1 for x in rows if x[c] is None) for c in out["cols"]}
        r.log("blank readings per column among %d rows: %s" % (len(rows), json.dumps({k: v for k, v in blank.items() if v})))
        cls = {}
        for x in rows:
            cls[x["cls"]] = cls.get(x["cls"], 0) + 1
        r.log("asset classes: %s" % json.dumps(dict(sorted(cls.items(), key=lambda kv: -kv[1]))))
        aside = [x for x in rows if x["why"]]
        r.log("set aside (%d): pinned %s | jump %s | thin %d" % (len(aside), ", ".join(x["t"] for x in aside if x["why"] == "pinned")[:700],
                                                               ", ".join("%s %s" % (x["t"], x["jd"]) for x in aside if x["why"] == "jump")[:900], sum(1 for x in aside if x["why"] == "thin")))
        st = [x for x in rows if x["kind"] == "stock" and x["rs"] is not None]
        big = [x for x in st if (x["mcap"] or 0) >= 1e9]
        r.log("strongest $1B+ stocks by rank: %s" % ", ".join("%s %.0f" % (x["t"], x["rs"]) for x in sorted(big, key=lambda x: -x["rs"])[:40]))
        r.log("weakest $1B+ stocks by rank: %s" % ", ".join("%s %.0f" % (x["t"], x["rs"]) for x in sorted(big, key=lambda x: x["rs"])[:30]))
        rep = sorted([x for x in big if (x["dn"] or 0) >= 4 and x["dh"] == x["dn"]], key=lambda x: -(x["dx"] or 0))
        r.log("held up on every dump day of the window, $1B+ (%d): %s" % (len(rep), ", ".join("%s %d/%d %+.2f" % (x["t"], x["dh"], x["dn"], x["dx"]) for x in rep[:60])))
        ext = sorted([x for x in rows if x["x21"] is not None], key=lambda x: -abs(x["x21"]))[:25]
        r.log("largest 21-session excess either way (a check on the hygiene): %s" % ", ".join("%s %+.0f $%.0fM %s" % (x["t"], x["x21"], (x["mcap"] or 0) / 1e6, x["why"] or "") for x in ext))

        r.section("a sample of rows")
        for t in SAMPLE:
            x = by.get(t)
            r.log("  %s" % (json.dumps({k: x[k] for k in SHOW}) if x else "%s not in the table" % t))

        r.section("a second way: excess returns straight from the banked bars")
        bench = {b["date"]: b["c"] for b in W.read_bars(s3, "SPY", tail=400, through=as_of)}
        bad, checked = [], 0
        for t in SAMPLE:
            x = by.get(t)
            if not x or x["n"] < 130:
                continue
            bars = [b for b in W.read_bars(s3, t, tail=400, through=as_of) if b["date"] in bench]
            c = [b["c"] for b in bars]
            s = [bench[b["date"]] for b in bars]
            again = {k: round((c[-1] / c[-1 - n] - 1) * 100 - (s[-1] / s[-1 - n] - 1) * 100, 2) for k, n in (("x1", 1), ("x5", 5), ("x21", 21), ("x63", 63), ("x126", 126))}
            diff = {k: (x[k], v) for k, v in again.items() if x[k] is not None and abs(x[k] - v) > 0.011}
            checked += 1
            if diff:
                bad.append("%s %s" % (t, json.dumps(diff)))
        (r.warn if bad else r.ok)("second way: %s" % ("differences (a split or a cut explains each, or it is a fault): " + "; ".join(bad) if bad else "%d sampled names agree to the cent on all five horizons" % checked))

        r.section("against the desk's own artifact (the names it already ranks)")
        both = [(x, by[x["ticker"]]) for x in desk["rows"] if x["ticker"] in by and x.get("vs")]
        agree = {k: 0 for k in ("x1", "x5", "x21", "x63", "x126")}
        diffs = []
        for d, w in both:
            for k, wk in (("d", "x1"), ("w", "x5"), ("m", "x21"), ("q", "x63"), ("h", "x126")):
                a, b = d["vs"].get(k), w[wk]
                if a is None or b is None:
                    continue
                if abs(a - b) <= 0.011:
                    agree[wk] += 1
                else:
                    diffs.append((abs(a - b), d["ticker"], wk, a, b))
        r.log("%d names in both; equal excess returns per horizon: %s; differing readings %d" % (len(both), json.dumps(agree), len(diffs)))
        r.log("largest differences (desk, table): %s" % "; ".join("%s %s %s vs %s" % (t, k, a, b) for _, t, k, a, b in sorted(diffs, reverse=True)[:25]))
        pairs = [(d["strength"], w["rs"]) for d, w in both if d.get("strength") is not None and w["rs"] is not None and w["kind"] == "stock"]
        if len(pairs) > 20:
            n = len(pairs)
            mx, my = sum(p[0] for p in pairs) / n, sum(p[1] for p in pairs) / n
            cov = sum((a - mx) * (b - my) for a, b in pairs)
            corr = cov / ((sum((a - mx) ** 2 for a, _ in pairs) * sum((b - my) ** 2 for _, b in pairs)) ** 0.5)
            r.log("stocks ranked by both: %d; correlation of the desk's strength with the table's rank %.3f" % (n, corr))

        r.section("the last session read by the engine (%s)" % as_of)
        show_read(r, out["last_session"], by)
        th = ((desk.get("thesis") or {}).get("last_session") or {})
        top = (((th.get("strength") or {}).get("stocks") or {}).get("top")) or {}
        r.log("the desk's own thesis block for the same session: SPY %s, strongest tenth of its stocks held up %s%%, mean excess %s pp" % (th.get("spy_ret_pct"), top.get("held_up_pct"), top.get("mean_excess_pp")))

        r.section("today's forming bars over the table (the page's join, by its reference in wide.py)")
        raw = s3.get_object(Bucket=BUCKET, Key=W.TODAY_KEY)
        today = json.loads(gzip.decompress(raw["Body"].read()))
        r.log("forming file: session %s as_of %s, %d rows, written %s" % (today.get("session"), today.get("as_of"), len(today.get("rows") or {}), raw["LastModified"].isoformat()))
        t1 = time.time()
        live = W.live_read(out, today, class_labels=LF.CLASS_LABEL)
        if not live:
            r.warn("no live session: the forming file's session %s is not after the table's %s" % (today.get("session"), as_of))
        else:
            r.log("live read in %.2fs: bars as of %s; %d names without a print today; unconfirmed moves %d: %s" % (time.time() - t1, live["bars_as_of"], live["n_no_print"], live["n_unconfirmed"], ", ".join(live["unconfirmed"])))
            show_read(r, live, by)
            j = W.live_join(out, today)
            jr = {x["t"]: x for x in j["rows"]}
            r.log("SPY live: %s" % json.dumps({k: (round(v, 3) if isinstance(v, float) else v) for k, v in j["spy"].items()}))
            for t in ("SNDK", "FICO", "MKC", "NVDA", "AAPL", "XLP", "TLT"):
                x = jr.get(t)
                if x:
                    r.log("  %-5s prev %s bar %s ret %s unconfirmed %s split %s live %s" % (t, x.get("prev"), x.get("bar"), None if x["ret"] is None else round(x["ret"], 2), x["unconfirmed"], x["split"],
                                                                                          json.dumps({k: round(v, 2) for k, v in (x["live"] or {}).items() if v is not None})))
        embed(r, "wide", out)
        embed(r, "today", today)
        if live:
            embed(r, "live-read", live)
        g = out["last_session"]["groups"]["stocks_1b"]
        r.kv(measured=str(len(rows)), stocks=str(out["universe"]["scanned_stock"]), etfs=str(out["universe"]["scanned_etf"]), seconds="%.0f" % took, bytes=str(len(body)),
             errors=str(out["n_errors"]), last_session_held_1b="%s%%" % g.get("held_pct"), live_held_1b="%s%%" % ((live or {}).get("groups", {}).get("stocks_1b", {}).get("held_pct")))
        r.ok("DRY RUN: %d names measured in %.0fs, %d bytes; nothing written" % (len(rows), took, len(body)))


if __name__ == "__main__":
    main()
    sys.exit(0)
