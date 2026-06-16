"""justhodl-finviz-groups — official Finviz group aggregates → data/finviz-groups.json
Sector (perf+valuation), industry (perf+valuation, 144), country (40), market-cap buckets (6).
Powers sector/industry/country/cap rotation + sector-level valuation context. Spaced calls."""
import json, time
from datetime import datetime, timezone
import boto3
import finviz as FV

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def _merge(perf, val):
    by = {r["name"]: dict(r) for r in perf}
    for r in val:
        by.setdefault(r["name"], {"name": r["name"]}).update(r)
    return sorted(by.values(), key=lambda x: x.get("perf_m") if x.get("perf_m") is not None else -999, reverse=True)


def lambda_handler(event=None, context=None):
    out = {"generated_at": datetime.now(timezone.utc).isoformat(), "source": "finviz-grp-export"}
    def g(dim, v):
        try:
            r = FV.fetch_group(dim, v); print("  %s v=%d -> %d" % (dim, v, len(r))); return r
        except Exception as e:
            print("  %s v=%d FAIL %s" % (dim, v, str(e)[:60])); return []
        finally:
            time.sleep(4)
    out["sectors"] = _merge(g("sector", 140), g("sector", 120))
    out["industries"] = _merge(g("industry", 140), g("industry", 120))
    out["countries"] = g("country", 140)
    out["mktcaps"] = g("capitalization", 140)
    out["counts"] = {k: len(out[k]) for k in ("sectors", "industries", "countries", "mktcaps")}
    s3.put_object(Bucket=BUCKET, Key="data/finviz-groups.json",
                  Body=json.dumps(out, separators=(",", ":"), default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")
    print("wrote data/finviz-groups.json", out["counts"])
    return {"ok": True, "counts": out["counts"]}
