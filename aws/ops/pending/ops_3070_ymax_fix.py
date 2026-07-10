#!/usr/bin/env python3
"""ops 3070 -- verify the period1=0 fix (3069 decoded: range=max forces ~quarterly, 168 bars regardless of interval; 20y/1wk worked). Now range=max routes via epoch params. 3068 asserted blind (no evidence
on fail). Probe AAPL across range/interval combos, log count + first
bar + any Yahoo error verbatim, and PASS on the deepest working combo
(pre-2000 history via any route). Evidence drives the page fix."""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report

AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3070",
      "Cache-Control": "no-cache"}
BASE = ("https://justhodl-data-proxy.raafouis.workers.dev/"
        "yf-ohlc?symbol=%s&range=%s&interval=%s&cb=%d")


def probe(sym, rng, iv):
    try:
        raw = urllib.request.urlopen(urllib.request.Request(
            BASE % (sym, rng, iv, time.time()), headers=UA),
            timeout=45).read().decode("utf-8", "replace")
        d = json.loads(raw)
        bars = d.get("bars") or []
        first = (datetime.fromtimestamp(bars[0]["time"],
                                        tz=timezone.utc)
                 .strftime("%Y-%m-%d") if bars else None)
        return {"rng": rng, "iv": iv, "n": len(bars),
                "first": first,
                "err": (d.get("error") or "")[:120]}
    except Exception as e:
        return {"rng": rng, "iv": iv, "n": -1,
                "err": str(e)[:120]}


def main():
    fails, warns = [], []
    with report("3070_ymax_fix") as rep:
        rep.section("1. AAPL depth matrix")
        # worker deploy wait: poll until max/1wk crosses 1800 bars
        results = []
        for _try in range(18):
            x = probe("AAPL", "max", "1wk")
            if x["n"] > 1800:
                results = [x, probe("AAPL", "max", "1d"),
                           probe("^GSPC", "max", "1mo")]
                break
            time.sleep(20)
        if not results:
            results = [probe("AAPL", "max", "1wk")]
        for j, x in enumerate(results):
            rep.kv(**{("probe_%d_%s_%s" % (j, x["rng"], x["iv"])):
                      json.dumps(x)})
        deep = [x for x in results
                if x["n"] > 1800 and x.get("first")
                and int(x["first"][:4]) < 1990]
        rep.kv(deepest=json.dumps(max(
            deep, key=lambda x: x["n"]) if deep else None))
        if not deep:
            fails.append("no combo reaches pre-2000 -- Yahoo max "
                         "blocked from CF; page needs a different "
                         "deep-history source")
        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS -- deep history reachable; wire page to the "
                "deepest combo")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3070.json").write_text(json.dumps(
        {"ops": 3070, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
