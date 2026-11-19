"""ops 6851 - READ-ONLY dry run of the stock volatility engine over the whole warehouse (re-runnable).

Khalid 2026-11-19: "create an engine that would measure volatility. Most and least volatile stocks. Also it would
measure the price spreads for all stocks and rank them based on that. Price spread last day, week, month, quarter".
Before the engine is deployed, its own build() is run here on the runner against the real bank: how many symbols it
measures, how long that takes, what the boards hold, how big the feed is.  A sample of rows is then recomputed a second
way straight from the banked bars (the standard library's statistics module, brute-force highs and lows).  Nothing is
written - build() only reads; the handler's put is not called.  The result is embedded so the desk is designed and
tested on the real table.
"""
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import base64
import gzip
import json
import math
import statistics
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-stock-volatility" / "source"))
from ops_report import report  # noqa: E402

sys.modules.setdefault("chain_guard", SimpleNamespace(begin=lambda event, context=None: 0))   # build() never calls it; the import must resolve off-Lambda
import lambda_function as LF  # noqa: E402

SHOW = ["t", "name", "mcap", "px", "chg", "hv21", "hv63", "hv252", "yz21", "atr", "hvp", "hv21p", "mx", "mxd", "sp_d", "sp_w", "sp_m", "sp_q", "pos_q", "own_d", "own_q", "rk_hv21", "rk_d", "rk_q", "aside", "why"]
SAMPLE = ["AAPL", "MSFT", "NVDA", "TSLA", "JPM", "XOM", "PFE", "KO", "WMT", "AMD", "PLTR", "GME", "BRK.B", "SPY", "QQQ", "TLT", "GLD", "BIL"]


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


def second_way(bars):
    """The same figures by other means: statistics.stdev over log returns, max / min over slices."""
    C, H, L = [b["c"] for b in bars], [b["h"] for b in bars], [b["l"] for b in bars]
    out = {}
    for k in (21, 63, 252):
        out["hv%d" % k] = round(statistics.stdev([math.log(C[i] / C[i - 1]) for i in range(len(C) - k, len(C))]) * math.sqrt(252) * 100, 2) if len(C) > k else None
    for key, k in (("d", 1), ("w", 5), ("m", 21), ("q", 63)):
        out["sp_" + key] = round((max(H[-k:]) / min(L[-k:]) - 1) * 100, 2) if len(C) >= k else None
    return out


def main():
    now = datetime.now(timezone.utc)
    s3 = ReadOnly(boto3.client("s3", region_name="us-east-1", config=Config(retries={"max_attempts": 4, "mode": "standard"}, connect_timeout=5, read_timeout=30, max_pool_connections=40)))
    with report("ops_6851_stock_volatility_dry_run") as r:
        r.heading("ops 6851 - stock volatility, dry run over the warehouse %s (read-only)" % now.isoformat(timespec="minutes"))
        logs = []
        t0 = time.time()
        out = LF.build(s3, now, log=logs.append, remaining_s=1500.0)
        took = time.time() - t0
        for line in logs:
            r.log(line)
        r.log("build() %.1fs on the runner; engine %s %s; as of %s" % (took, out["engine"], out["version"], out["as_of"]))
        rows = [dict(zip(out["cols"], x)) for x in out["rows"]]
        by = {x["t"]: x for x in rows}

        r.section("universe")
        r.log(json.dumps(out["universe"]))
        r.log("board rule: %s" % json.dumps(out["board_rule"]))

        r.section("the market")
        for kind in ("stock", "etf"):
            r.log("%s: %s" % (kind, json.dumps(out["market"][kind])))
        r.log("benchmark SPY: %s" % json.dumps({k: out["market"]["benchmark"].get(k) for k in SHOW if k in out["market"]["benchmark"]}))
        r.log("bands (stocks by hv21): %s" % json.dumps(out["bands"]))
        r.log("by size: %s" % json.dumps([{"size": g["size"], "n": g["n"], "hv21": g["med"]["hv21"], "sp_d": g["med"]["sp_d"], "sp_q": g["med"]["sp_q"], "expanding": g["expanding"], "contracting": g["contracting"]} for g in out["by_size"]]))
        for g in out["by_sector"]:
            r.log("  sector %-24s n %4d  hv21 %6s  hv63 %6s  day %5s  week %5s  month %5s  quarter %6s  expanding %d / contracting %d" % (
                g["sector"][:24], g["n"], g["med"]["hv21"], g["med"]["hv63"], g["med"]["sp_d"], g["med"]["sp_w"], g["med"]["sp_m"], g["med"]["sp_q"], g["expanding"], g["contracting"]))
        h = out["history"]
        r.log("history: %d sessions %s..%s; first %s; last %s" % (len(h), h[0]["date"] if h else None, h[-1]["date"] if h else None, json.dumps(h[0]) if h else None, json.dumps(h[-1]) if h else None))
        r.log("history, every 21st session: %s" % json.dumps([[x["date"], x["n"], x["med"], x["sp_d"]] for x in h[::21]]))

        r.section("boards ($1B+ common stocks)")
        measure_of = {"most_volatile": "hv21", "least_volatile": "hv21", "vol_rising": "hv21", "vol_falling": "hv21"}
        for name, tickers in out["boards"].items():
            m = measure_of.get(name) or ("sp_" + name[-1])
            r.log("%-13s %s" % (name, ", ".join("%s %s" % (t, by[t][m]) for t in tickers[:12])))
        r.log("changes: %s" % json.dumps(out["changes"])[:400])
        r.log("pinned to a deal price (%d stocks): %s" % (len(out["pinned"]), ", ".join("%s $%.1fB month %s%% hv21 %s" % (t, (by[t]["mcap"] or 0) / 1e9, by[t]["sp_m"], by[t]["hv21"]) for t in out["pinned"][:60])))
        rc = out["universe"]["reclassified"]
        r.log("left out as a note, preferred share or warrant the book calls common stock (%d): %s" % (len(rc), "; ".join("%s = %s" % (x["t"], x["why"]) for x in rc)))

        r.section("the ends of the whole table (every stock, no size floor)")
        st = [x for x in rows if x["kind"] == "stock"]
        for label, key, rev in (("most volatile", "rk_hv21", False), ("least volatile", "rk_hv21", True), ("widest day", "rk_d", False), ("widest quarter", "rk_q", False)):
            sub = sorted([x for x in st if x[key] is not None], key=lambda x: x[key], reverse=rev)[:10]
            r.log("%-15s %s" % (label, ", ".join("%s hv21 %s day %s mcap $%.0fM" % (x["t"], x["hv21"], x["sp_d"], (x["mcap"] or 0) / 1e6) for x in sub)))
        aside = [x for x in rows if x["aside"]]
        prints = sorted([x for x in rows if x["pa"] is not None], key=lambda x: x["pa"])
        r.log("flagged print on %d rows (%d inside the last 63 sessions): %s" % (len(prints), len([x for x in prints if x["pa"] <= LF.JUMP_ASIDE]),
                                                                                "; ".join("%s %s x%s %d ago ($%.0fM, quarter %s%%)" % (x["t"], x["pd"], x["pr"], x["pa"], (x["mcap"] or 0) / 1e6, x["sp_q"]) for x in prints[:40])))
        sr = out["universe"]["splits_refused"]
        r.log("inferred splits refused (the session did not open at the new price): %d - %s" % (sr["n"], "; ".join("%s %s x%s" % (x["t"], x["date"], x["ratio"]) for x in sr["sample"])))
        r.log("set aside %d rows: %s; flagged jump on %d rows; examples %s" % (len(aside), json.dumps({w: len([x for x in aside if x["why"] == w]) for w in ("jump", "print", "thin", "pinned")}), len([x for x in rows if x["ja"] is not None]),
                                                                    json.dumps([{k: x[k] for k in ("t", "kind", "mcap", "hv21", "jd", "jr", "ja", "miss", "aside", "why")} for x in sorted([x for x in aside if x["why"] != "pinned"], key=lambda x: -(x["mcap"] or 0))[:10]])))
        blank = {c: sum(1 for x in st if x[c] is None) for c in ("hv21", "hv63", "hv252", "yz21", "atr", "hvp", "hv21p", "sp_d", "sp_w", "sp_m", "sp_q", "own_d", "own_q", "mcap", "sector", "name")}
        r.log("blank readings among %d stocks: %s" % (len(st), json.dumps(blank)))

        r.section("a sample, recomputed a second way from the banked bars")
        bad = []
        for t in SAMPLE:
            x = by.get(t)
            if x is None:
                r.log("  %-6s not in the table" % t)
                continue
            own, _ = LF.T.own_history(LF.read_bars(s3, t), None)
            rec, _ = LF.recorded_splits(s3, out["universe"]["bank_first_session"], out["as_of"], {t})
            bars = LF.clean(LF.T.rebase(own, rec.get(t) or [])[0])
            again = second_way(bars)
            diff = {k: (x[k], v) for k, v in again.items() if v is not None and x[k] is not None and abs(x[k] - v) > 0.011}
            if diff:
                bad.append("%s %s" % (t, json.dumps(diff)))
            r.log("  %s" % json.dumps({k: x[k] for k in SHOW}))
        (r.fail if bad else r.ok)("second way: %s" % ("; ".join(bad[:6]) if bad else "%d sampled rows agree to the cent on hv21 / hv63 / hv252 and the four spreads" % len([t for t in SAMPLE if t in by])))

        r.log("errors %d: %s" % (out["n_errors"], json.dumps(out["errors"][:12])))
        body = json.dumps(out, separators=(",", ":"), default=str).encode("utf-8")
        r.log("feed would be %d bytes (%d gzipped), %d rows x %d columns" % (len(body), len(gzip.compress(body, 6)), len(out["rows"]), len(out["cols"])))
        blob = base64.b64encode(gzip.compress(body, 9)).decode("ascii")
        r.lines.append("<!-- FEED-PACK-GZ-B64-BEGIN stock-volatility")
        r.lines.extend(blob[i:i + 16000] for i in range(0, len(blob), 16000))
        r.lines.append("FEED-PACK-GZ-B64-END -->")
        med = out["market"]["stock"]["med"]
        r.kv(scanned=str(out["universe"]["scanned"]), stocks=str(out["universe"]["scanned_stock"]), etfs=str(out["universe"]["scanned_etf"]), seconds="%.0f" % took,
             median_hv21=str(med["hv21"]), median_day_spread=str(med["sp_d"]), bytes=str(len(body)), errors=str(out["n_errors"]))
        if bad:
            raise SystemExit(1)
        r.ok("DRY RUN: %d symbols measured in %.0fs - median stock hv21 %s%%, day spread %s%%; nothing written" % (out["universe"]["scanned"], took, med["hv21"], med["sp_d"]))


if __name__ == "__main__":
    main()
    sys.exit(0)
