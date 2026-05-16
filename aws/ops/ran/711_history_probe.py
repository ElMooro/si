"""ops/711 — probe history-file depth + schema to decide crisis-composite
backfill feasibility. Reports count, date range and snapshot keys for each."""
import json, os
import boto3
from datetime import datetime, timezone

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")

HIST_KEYS = {
    "credit-stress": "data/credit-stress-history.json",
    "vol-surface": "data/vol-surface-history.json",
    "market-internals": "data/market-internals-history.json",
    "global-liquidity": "data/global-liquidity-history.json",
    "regime-composite": "data/regime-composite-history.json",
    "crisis-composite": "data/crisis-composite-history.json",
    "capitulation": "data/capitulation-history.json",
    "china-liquidity": "data/china-liquidity-history.json",
    "bank-stress": "data/bank-stress-history.json",
}


def probe(key):
    try:
        d = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        return {"exists": False, "err": str(e)[:120]}
    # find the list of snapshots — common shapes
    snaps = None
    list_field = None
    if isinstance(d, list):
        snaps, list_field = d, "(root list)"
    elif isinstance(d, dict):
        for f in ("snapshots", "history", "series", "points", "data"):
            if isinstance(d.get(f), list):
                snaps, list_field = d[f], f
                break
    out = {"exists": True, "top_keys": sorted(d.keys()) if isinstance(d, dict) else "(list)",
           "list_field": list_field}
    if snaps is None:
        out["snapshots"] = "none found"
        return out
    out["count"] = len(snaps)
    if snaps:
        first, last = snaps[0], snaps[-1]
        out["first_snapshot"] = {k: first[k] for k in list(first.keys())[:8]} if isinstance(first, dict) else first
        out["last_snapshot"] = {k: last[k] for k in list(last.keys())[:8]} if isinstance(last, dict) else last
        # date span
        def ts_of(x):
            if not isinstance(x, dict):
                return None
            for f in ("ts", "date", "timestamp", "generated_at", "as_of"):
                if x.get(f):
                    return x[f]
            return None
        t0, t1 = ts_of(first), ts_of(last)
        out["date_span"] = {"first": t0, "last": t1}
    return out


def main():
    report = {"probed_at": datetime.now(timezone.utc).isoformat(), "histories": {}}
    for name, key in HIST_KEYS.items():
        report["histories"][name] = probe(key)
        print(f"{name}: {report['histories'][name].get('count', report['histories'][name].get('err','?'))}")
    # feasibility verdict
    usable = {n: h for n, h in report["histories"].items()
              if h.get("exists") and isinstance(h.get("count"), int) and h["count"] > 5
              and n not in ("crisis-composite", "capitulation", "china-liquidity", "bank-stress")}
    report["backfill_feasibility"] = {
        "engines_with_usable_history": sorted(usable.keys()),
        "n_usable": len(usable),
        "verdict": ("partial backfill possible" if usable
                    else "no meaningful upstream history — backfill not feasible"),
    }
    report["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/711_history_probe.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print("DONE -> 711_history_probe.json :: " + json.dumps(report["backfill_feasibility"]))


if __name__ == "__main__":
    main()
