#!/usr/bin/env python3
"""ops 3087 -- /stable/quote payload probe (rr=0 despite singles).
Fetch NVDA + a comma pair runner-side, print type/keys/fields
verbatim: does stable quote carry priceAvg50/priceAvg200/yearHigh,
and what does _http-style parsing see? [skip-deploy]"""
import json
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report

AWS_DIR = Path(__file__).resolve().parents[2]
KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"


def get(url):
    return urllib.request.urlopen(
        urllib.request.Request(url, headers={"User-Agent": "ops"}),
        timeout=25).read().decode("utf-8", "replace")


def main():
    fails, warns = [], []
    with report("3087_quote_probe") as rep:
        raw = get("https://financialmodelingprep.com/stable/quote"
                  "?symbol=NVDA&apikey=" + KEY)
        rep.kv(single_head=raw[:200])
        try:
            d = json.loads(raw)
            row = d[0] if isinstance(d, list) and d else d
            rep.kv(single_type=type(d).__name__,
                   keys=json.dumps(sorted(row.keys()))[:400],
                   price=row.get("price"),
                   priceAvg50=row.get("priceAvg50"),
                   priceAvg200=row.get("priceAvg200"),
                   yearHigh=row.get("yearHigh"),
                   yearLow=row.get("yearLow"))
            for k in ("price", "yearHigh"):
                if row.get(k) is None:
                    fails.append("field %s missing on stable quote"
                                 % k)
            if row.get("priceAvg50") is None:
                warns.append("priceAvg50 absent -- rr stop needs "
                             "another source")
        except Exception as e:
            fails.append("single parse: %s" % str(e)[:80])
        try:
            raw2 = get("https://financialmodelingprep.com/stable/"
                       "quote?symbol=NVDA,TSM&apikey=" + KEY)
            d2 = json.loads(raw2)
            rep.kv(pair_type=type(d2).__name__,
                   pair_len=(len(d2) if isinstance(d2, list)
                             else None),
                   pair_head=raw2[:160])
        except Exception as e:
            rep.kv(pair_error=str(e)[:100])
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3087.json").write_text(json.dumps(
        {"ops": 3087, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
