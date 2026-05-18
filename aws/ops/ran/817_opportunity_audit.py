"""ops/817 — audit before build.

Part A: dump the output schema of every opportunity engine so the new
        cross-engine CONVICTION STACK is built against real structures,
        not assumptions (where the picks list is, the per-pick fields).
Part B: probe FMP for the future merger-arb / short-squeeze / dividend
        builds — confirm what data actually exists before committing.
"""
import json
import os
import urllib.request
from datetime import datetime, timezone

import boto3
from botocore.config import Config

cfg = Config(read_timeout=60, connect_timeout=20, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
BUCKET = "justhodl-dashboard-live"
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
BASE = "https://financialmodelingprep.com/stable"

report = {"ops": 817, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Opportunity-engine schema audit + FMP capability probe"}

# ── Part A: engine output schemas ──
ENGINE_KEYS = [
    "data/bagger-engine.json", "data/capital-return.json",
    "data/coffee-can.json", "data/deep-value.json",
    "data/asymmetric-scorer.json", "data/earnings-pead.json",
    "data/eps-revision-velocity.json", "data/fundamentals.json",
    "data/master-ranker.json", "data/insider-aggregate.json",
    "data/momentum-breakout.json", "data/pead-signals.json",
    "data/revenue-acceleration.json", "screener/opportunity-screener.json",
    "data/beta-laggards.json", "data/metals-miners.json",
]


def shape(v, depth=0):
    """Compact structural description of a JSON value."""
    if isinstance(v, dict):
        return {"_type": "dict", "_keys": sorted(v.keys())[:30]}
    if isinstance(v, list):
        s = {"_type": "list", "_len": len(v)}
        if v and isinstance(v[0], dict):
            s["_item_keys"] = sorted(v[0].keys())[:30]
        elif v:
            s["_item_type"] = type(v[0]).__name__
        return s
    return {"_type": type(v).__name__, "_val": str(v)[:60]}


engines = {}
for key in ENGINE_KEYS:
    e = {}
    try:
        ob = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
        e["ok"] = True
        e["top_level"] = sorted(ob.keys()) if isinstance(ob, dict) else None
        e["generated_at"] = (ob.get("generated_at")
                             or ob.get("generatedAt")
                             or ob.get("timestamp")) if isinstance(
                                 ob, dict) else None
        # describe every list-valued top-level key (these hold the picks)
        lists = {}
        if isinstance(ob, dict):
            for k, v in ob.items():
                if isinstance(v, (list, dict)) and k not in (
                        "generated_at", "schema_version"):
                    lists[k] = shape(v)
                    # one level deeper for dict-of-lists (e.g. bagger tiers)
                    if isinstance(v, dict):
                        sub = {sk: shape(sv) for sk, sv in list(v.items())[:8]
                               if isinstance(sv, (list, dict))}
                        if sub:
                            lists[k]["_nested"] = sub
        e["structures"] = lists
    except Exception as ex:
        e["ok"] = False
        e["err"] = f"{type(ex).__name__}: {str(ex)[:140]}"
    engines[key] = e
report["engines"] = engines


# ── Part B: FMP capability probe ──
def probe(url):
    full = url + (f"&apikey={FMP}" if "?" in url else f"?apikey={FMP}")
    try:
        with urllib.request.urlopen(full, timeout=20) as r:
            d = json.loads(r.read())
        sample = d[0] if isinstance(d, list) and d else d
        return {"ok": True,
                "n": len(d) if isinstance(d, list) else 1,
                "keys": sorted(sample.keys()) if isinstance(sample, dict)
                else None,
                "sample": {k: sample[k] for k in list(sample)[:14]}
                if isinstance(sample, dict) else str(sample)[:200]}
    except Exception as ex:
        return {"ok": False, "err": f"{type(ex).__name__}: {str(ex)[:160]}"}


report["fmp_probes"] = {
    "ma_latest": probe(f"{BASE}/mergers-acquisitions-latest?page=0&limit=8"),
    "ma_search": probe(f"{BASE}/mergers-acquisitions-search?name=Inc"),
    "etf_holdings_XLK": probe(f"{BASE}/etf/holdings?symbol=XLK"),
    "etf_info_XLK": probe(f"{BASE}/etf/info?symbol=XLK"),
    "shares_float_AAPL": probe(f"{BASE}/shares-float?symbol=AAPL"),
    "dividends_AAPL": probe(f"{BASE}/dividends?symbol=AAPL"),
}

# concise verdicts to drive the next builds
ma = report["fmp_probes"]["ma_latest"]
report["build_guidance"] = {
    "merger_arb": ("M&A feed keys: "
                   + str(ma.get("keys")) if ma.get("ok")
                   else "M&A feed unavailable: " + str(ma.get("err"))),
    "etf_catchup": ("ETF holdings keys: "
                    + str(report["fmp_probes"]["etf_holdings_XLK"].get("keys"))
                    if report["fmp_probes"]["etf_holdings_XLK"].get("ok")
                    else "ETF holdings unavailable"),
    "short_squeeze": ("shares-float keys: "
                      + str(report["fmp_probes"]["shares_float_AAPL"].get(
                          "keys"))),
    "dividend_growth": ("dividends keys: "
                        + str(report["fmp_probes"]["dividends_AAPL"].get(
                            "keys"))),
}

ok_engines = sum(1 for e in engines.values() if e.get("ok"))
report["checks"] = {
    "engines_readable": ok_engines >= 10,
    "ma_probe_done": "ok" in report["fmp_probes"]["ma_latest"],
}
report["all_pass"] = all(report["checks"].values())
report["verdict"] = (
    f"AUDIT COMPLETE — {ok_engines}/{len(ENGINE_KEYS)} opportunity engines "
    "readable, schemas captured for the conviction-stack build; FMP "
    "capabilities probed for merger-arb / short-squeeze / dividend builds."
    if report["all_pass"] else "REVIEW — see checks[]")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/817_opportunity_audit.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/817_opportunity_audit.json")
