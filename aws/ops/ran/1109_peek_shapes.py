"""1109 — peek at engine output shapes to design convergence-radar."""
import json, pathlib
from datetime import datetime, timezone
import boto3
s3 = boto3.client("s3", region_name="us-east-1")

def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "engines": {}}
    keys = [
        "data/buzz-velocity.json", "data/momentum-breakout.json", "data/options-flow.json",
        "data/eps-revision-velocity.json", "data/earnings-tracker.json", "data/political-trades.json",
        "data/sec-filings-intel.json", "data/ark-holdings.json", "data/lobbying-intel.json",
        "data/sector-earnings-diffusion.json", "data/fundamentals.json", "data/ticker-trends.json",
        "data/retail-sentiment.json", "data/news-velocity.json", "data/hiring-velocity.json",
        "data/earnings-pead.json", "data/earnings-cascade.json", "data/dividend-growth.json",
        "data/capital-return.json", "data/etf-flows.json", "data/sympathetic-momentum.json",
        "data/political-stocks.json", "data/earnings-tone-velocity.json", "data/patent-velocity.json",
        "data/earnings-whisper.json",
    ]
    for k in keys:
        info = {}
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=k)
            d = json.loads(obj["Body"].read())
            info["top_keys"] = list(d.keys())[:15]
            # Find ticker lists
            info["ticker_paths"] = []
            for kk, vv in d.items():
                if isinstance(vv, list) and vv and isinstance(vv[0], dict):
                    fk = list(vv[0].keys())
                    if any(t.lower() in [x.lower() for x in fk] for t in ['ticker','symbol']):
                        info["ticker_paths"].append({
                            "path": kk, "type": "list", "len": len(vv),
                            "item_keys": fk[:14],
                            "sample": {k2: str(vv[0].get(k2))[:80] for k2 in fk[:10]}
                        })
                elif isinstance(vv, dict) and len(vv) > 5:
                    first_v = next(iter(vv.values()), None)
                    if isinstance(first_v, dict):
                        info["ticker_paths"].append({
                            "path": kk, "type": "dict", "len": len(vv),
                            "item_keys": list(first_v.keys())[:14],
                            "sample_key": next(iter(vv.keys())),
                        })
        except Exception as e:
            info["err"] = str(e)[:120]
        out["engines"][k] = info
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path("aws/ops/reports/1109_shapes.json").write_text(json.dumps(out, indent=2, default=str))
    print("[1109] DONE")

if __name__ == "__main__":
    main()
