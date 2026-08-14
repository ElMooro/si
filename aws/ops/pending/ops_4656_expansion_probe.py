"""ops 4656 — expansion evidence probe for stock-buying v1.4:
matrix concept hits (ps/fcf/inventory/etc), 13F flows shape,
dark-pool shape, options/ETF-flow store discovery + samples.
"""
import json

import boto3

from ops_report import report

B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def j(key):
    try:
        return json.loads(s3.get_object(Bucket=B,
                                        Key=key)["Body"].read())
    except Exception as e:
        return {"__err": str(e)[:60]}


def main():
    with report("4656_expansion_probe") as r:
        r.heading("ops 4656 — v1.4 evidence probe")
        mx = j("data/fundamental-census-matrix.json")
        cols = sorted((mx.get("cols") or {}).keys())
        ticks = mx.get("tickers") or []
        ai = ticks.index("AAPL") if "AAPL" in ticks else 0
        import re
        for concept, pat in (
                ("ps", r"(^|_)ps($|_)|price_sales"),
                ("fcf", r"fcf"),
                ("inventory", r"invent"),
                ("revenue-chg", r"revenue.*chg|rev.*accel"),
                ("eps-chg", r"eps.*chg"),
                ("shares", r"share_count|shares_out")):
            hits = [c for c in cols if re.search(pat, c)][:8]
            vals = []
            for h in hits[:4]:
                arr = (mx.get("cols") or {}).get(h) or []
                vals.append("%s=%s" % (h, arr[ai]
                                       if ai < len(arr)
                                       else None))
            r.log("[%s] %s · %s" % (concept, hits,
                                    " ; ".join(vals)))
        r.section("store discovery")
        toks = ("option", "flow", "13f", "dark", "etf",
                "whale", "put", "call", "institution")
        keys = []
        kw = {"Bucket": B, "Prefix": "data/"}
        while True:
            pg = s3.list_objects_v2(**kw)
            for o in pg.get("Contents") or []:
                k = o["Key"].lower()
                if any(t in k for t in toks):
                    keys.append(o["Key"])
            if not pg.get("IsTruncated"):
                break
            kw["ContinuationToken"] = pg["NextContinuationToken"]
        r.log("stores: %s" % keys[:16])
        for k in ("data/13f-flows-by-ticker.json",
                  "data/dark-pool.json",
                  "data/etf-true-flows.json"):
            d = j(k)
            if "__err" in d:
                r.warn("%s: %s" % (k, d["__err"]))
                continue
            r.log("%s top-keys=%s" % (k, list(d.keys())[:8]))
            bt = d.get("by_ticker") or d.get("rows") or {}
            if isinstance(bt, dict) and bt:
                k0 = "AAPL" if "AAPL" in bt else \
                    sorted(bt.keys())[0]
                r.log("  [%s]=%s" % (k0,
                                     json.dumps(bt[k0])[:260]))
            elif isinstance(bt, list) and bt:
                r.log("  row0=%s" % json.dumps(bt[0])[:260])
        for k in keys[:6]:
            if "option" in k.lower() or "put" in k.lower() \
                    or "call" in k.lower():
                d = j(k)
                r.log("%s keys=%s" % (k, list(d.keys())[:8]
                                      if isinstance(d, dict)
                                      else type(d).__name__))
        r.ok("probe complete")


if __name__ == "__main__":
    main()
