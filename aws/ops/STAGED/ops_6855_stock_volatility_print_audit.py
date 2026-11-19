"""ops 6855 - READ-ONLY audit: are the widest price spreads the dry run found real, or one bad print? (re-runnable)

The second dry run (ops 6851, run 44640732924) ranks MYND's quarter spread at 649,900%: a 63-session high of 0.65 over
a low of 0.0001 on a stock that closed at 0.2495.  HUBC, WHLR, MKDW, VIVK print 9,000% to 38,000%.  Before the engine
is deployed this reads the banked bars behind those rows and says, bar by bar, which of them is a collapse that
happened and which is a single print no trade around it supports.

It then sizes the question over the whole universe: for every symbol, the worst bar of its last 63 sessions measured as
a wick against the bar's own body (high / max(open, close), min(open, close) / low) and against the session before.
Writes nothing.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-stock-volatility" / "source"))
from ops_report import report  # noqa: E402

sys.modules.setdefault("chain_guard", SimpleNamespace(begin=lambda event, context=None: 0))
import lambda_function as LF  # noqa: E402

NAMED = ["MYND", "HUBC", "WHLR", "MKDW", "VIVK", "VWAV", "DBGI", "ZBAO", "FGL", "JDZG", "YFOR", "MEGL", "AKAN", "SKYA"]
WINDOW = 63
CUTS = (1.5, 2.0, 3.0, 5.0, 10.0)


class ReadOnly:
    def __init__(self, s3):
        self._s3 = s3

    def get_object(self, **kw):
        return self._s3.get_object(**kw)

    def head_object(self, **kw):
        return self._s3.head_object(**kw)

    def put_object(self, **kw):
        raise RuntimeError("audit: put refused for %s" % kw.get("Key"))


def wicks(bars, window=WINDOW):
    """Per bar of the last `window`: (date, ago, up, dn, gap_hi, gap_lo). up = high / max(open, close), dn = min(open,
    close) / low: how far the bar's extreme stands from its own body.  gap_hi / gap_lo: the same extreme against the
    closes around it - the session before, its own, the session after (the engine's prints_of)."""
    out, n = [], len(bars)
    for i in range(max(1, n - window), n):
        b = bars[i]
        body_hi, body_lo = max(b["o"], b["c"]), min(b["o"], b["c"])
        near = [bars[i - 1]["c"], b["c"]] + ([bars[i + 1]["c"]] if i + 1 < n else [])     # the engine's prints_of: closes only - the open can be the bad print
        out.append((b["date"], n - 1 - i, b["h"] / body_hi, body_lo / b["l"], b["h"] / max(near), min(near) / b["l"]))
    return out


def main():
    now = datetime.now(timezone.utc)
    s3 = ReadOnly(boto3.client("s3", region_name="us-east-1", config=Config(retries={"max_attempts": 4, "mode": "standard"}, connect_timeout=5, read_timeout=30, max_pool_connections=40)))
    with report("ops_6855_stock_volatility_print_audit") as r:
        r.heading("ops 6855 - are the widest spreads real? %s (read-only)" % now.isoformat(timespec="minutes"))
        book = LF.bank_enrich.rows(fields=LF.BOOK_FIELDS)
        universe, _ = LF.select_universe(book)
        for t in LF.other_issues(book):
            universe.pop(t, None)
        man, _ = LF.optional(s3, LF.polygon_bankfirst.MANIFEST)
        bank_last, bank_first = man["last_session"], man.get("first_session") or LF.polygon_bankfirst.BANK_FIRST_SESSION
        recorded, _ = LF.recorded_splits(s3, bank_first, bank_last, set(universe))
        r.log("universe %d symbols, bank through %s, recorded splits for %d symbols" % (len(universe), bank_last, len(recorded)))

        def series(t):
            raw = LF.read_bars(s3, t)
            if not raw:
                return None, None, None
            own, _ = LF.T.own_history(raw, (universe.get(t) or {}).get("ipo_date"))
            bars, ev = LF.T.rebase(own, recorded.get(t) or [])
            return LF.clean(bars), ev, LF.clean(own)

        # ------------------------------------------------------------------ 1. the named rows, bar by bar
        r.section("1. the rows the dry run ranks widest")
        for t in NAMED:
            try:
                bars, ev, own = series(t)
            except Exception as exc:  # noqa: BLE001
                r.log("%s: %s" % (t, str(exc)[:120]))
                continue
            if not bars or len(bars) < 22:
                r.log("%s: %s bars" % (t, len(bars or [])))
                continue
            tail = bars[-WINDOW:]
            raw_by = {b["date"]: b for b in own}
            hi_b, lo_b = max(tail, key=lambda b: b["h"]), min(tail, key=lambda b: b["l"])
            sp = (hi_b["h"] / lo_b["l"] - 1) * 100
            cl_hi, cl_lo = max(b["c"] for b in tail), min(b["c"] for b in tail)
            body_hi, body_lo = max(max(b["o"], b["c"]) for b in tail), min(min(b["o"], b["c"]) for b in tail)
            r.log("%s: %d bars, last %s close %.4f | quarter spread %.1f%% (high %.4f on %s, low %.4f on %s) | on closes alone %.1f%% | on opens and closes %.1f%%" % (
                t, len(bars), bars[-1]["date"], bars[-1]["c"], sp, hi_b["h"], hi_b["date"], lo_b["l"], lo_b["date"], (cl_hi / cl_lo - 1) * 100, (body_hi / body_lo - 1) * 100))
            r.log("  splits applied: %s" % (", ".join("%s x%s (%s)" % (e.get("date"), e.get("factor"), e.get("source") or e.get("kind") or "?") for e in (ev or [])[-6:]) or "none"))
            for tag, bb in (("high", hi_b), ("low", lo_b)):
                i = next(k for k, b in enumerate(bars) if b["date"] == bb["date"])
                for b in bars[max(0, i - 2):i + 3]:
                    rb = raw_by.get(b["date"]) or {}
                    r.log("  %-4s %s  o %.4f h %.4f l %.4f c %.4f v %12.0f | banked o %s h %s l %s c %s%s" % (
                        tag, b["date"], b["o"], b["h"], b["l"], b["c"], b["v"], rb.get("o"), rb.get("h"), rb.get("l"), rb.get("c"), "   <--" if b["date"] == bb["date"] else ""))
            step = max(1, len(tail) // 9)
            r.log("  closes: %s" % ", ".join("%s %.4f" % (b["date"][5:], b["c"]) for b in tail[::step]))
            w = wicks(bars)
            worst_up, worst_dn = max(w, key=lambda x: x[4]), max(w, key=lambda x: x[5])
            r.log("  farthest high from its neighbourhood: x%.2f on %s (x%.2f over its own body) | farthest low: x%.2f on %s (x%.2f under its own body)" % (
                worst_up[4], worst_up[0], worst_up[2], worst_dn[5], worst_dn[0], worst_dn[3]))

        # ------------------------------------------------------------------ 2. the whole universe
        r.section("2. every symbol: the worst bar of its last 63 sessions")

        def work(t):
            try:
                bars, _, _ = series(t)
                if not bars or len(bars) < LF.MIN_SESSIONS or bars[-1]["date"] < "2026-11-01":
                    return None
                w = wicks(bars)
                up, dn = max(w, key=lambda x: x[4]), max(w, key=lambda x: x[5])
                tail = bars[-WINDOW:]
                sp = (max(b["h"] for b in tail) / min(b["l"] for b in tail) - 1) * 100
                body = (max(max(b["o"], b["c"]) for b in tail) / min(min(b["o"], b["c"]) for b in tail) - 1) * 100
                m = universe[t]
                return {"t": t, "kind": m["kind"], "mcap": m.get("mcap") or 0, "up": up[4], "up_date": up[0], "up_ago": up[1], "up_body": up[2], "dn": dn[5], "dn_date": dn[0], "dn_ago": dn[1], "dn_body": dn[3],
                        "sp": sp, "body": body, "n_up2": sum(1 for x in w if x[4] >= 2.0), "n_dn2": sum(1 for x in w if x[5] >= 2.0)}
            except Exception:  # noqa: BLE001
                return None

        with ThreadPoolExecutor(max_workers=24) as pool:
            rows = [x for x in pool.map(work, sorted(universe)) if x]
        r.log("%d symbols read" % len(rows))
        for kind in ("stock", "etf"):
            sub = [x for x in rows if x["kind"] == kind]
            for cut in CUTS:
                hi = [x for x in sub if x["up"] >= cut]
                lo = [x for x in sub if x["dn"] >= cut]
                big = [x for x in sub if max(x["up"], x["dn"]) >= cut and x["mcap"] >= 1e9]
                r.log("%-5s a high x%.1f over its neighbourhood: %4d symbols | a low x%.1f under: %4d | either, among $1B+: %3d %s" % (
                    kind, cut, len(hi), cut, len(lo), len(big), ", ".join(x["t"] for x in big[:14])))
        for label, key, date, ago, body in (("highs", "up", "up_date", "up_ago", "up_body"), ("lows", "dn", "dn_date", "dn_ago", "dn_body")):
            top = sorted(rows, key=lambda x: -x[key])[:28]
            r.log("the farthest %s: %s" % (label, " | ".join("%s x%.1f %s (%d ago, x%.1f off its body, %s, $%.0fM, quarter %.0f%% vs %.0f%% on bodies, %d such bars)" % (
                x["t"], x[key], x[date], x[ago], x[body], x["kind"], x["mcap"] / 1e6, x["sp"], x["body"], x["n_up2"] if key == "up" else x["n_dn2"]) for x in top)))
        # how much of the quarter spread is wick: spread on highs and lows against spread on opens and closes
        ratio = sorted((((x["sp"] + 100) / (x["body"] + 100), x) for x in rows if x["kind"] == "stock"), key=lambda p: p[0])
        n = len(ratio)
        r.log("stocks: quarter (high / low) over (body high / body low): median x%.3f, 90th x%.3f, 99th x%.3f, 99.9th x%.3f, max x%.1f (%s)" % (
            ratio[n // 2][0], ratio[int(n * .9)][0], ratio[int(n * .99)][0], ratio[int(n * .999)][0], ratio[-1][0], ratio[-1][1]["t"]))
        r.kv(symbols=len(rows), named=len(NAMED))
        r.ok("AUDIT: the banked bars behind the widest spreads are printed; the worst bar of every symbol's quarter is counted. Nothing written")


if __name__ == "__main__":
    main()
