"""1112 — for each engine, find the fields that indicate DIRECTION (bullish/bearish).

We need to know which fields per engine let us classify a ticker's signal as
LONG-pump, SHORT-squeeze, or neutral. This drives the pre-pump directional filter.
"""
import json, pathlib
from datetime import datetime, timezone
import boto3
s3 = boto3.client("s3", region_name="us-east-1")

ENGINES = [
    ("buzz-velocity",         "data/buzz-velocity.json",         "top_30"),
    ("momentum-breakout",     "data/momentum-breakout.json",     "all_qualifying"),
    ("options-flow",          "data/options-flow.json",          "all_qualifying"),
    ("eps-revision-velocity", "data/eps-revision-velocity.json", "all_qualifying"),
    ("earnings-pead",         "data/earnings-pead.json",         "all_qualifying"),
    ("sec-filings-intel",     "data/sec-filings-intel.json",     "all_tickers"),
    ("political-trades",      "data/political-trades.json",      "high_watch_recent_15"),
    ("lobbying-intel",        "data/lobbying-intel.json",        "all_tickers"),
    ("ark-holdings",          "data/ark-holdings.json",          "cross_fund_top"),
    ("fundamentals",          "data/fundamentals.json",          "companies"),
    ("ticker-trends",         "data/ticker-trends.json",         "top_20"),
    ("retail-sentiment",      "data/retail-sentiment.json",      "top_30_by_mentions"),
    ("hiring-velocity",       "data/hiring-velocity.json",       "double_confirmed"),
    ("capital-return",        "data/capital-return.json",        "cannibals"),
    ("earnings-cascade",      "data/earnings-cascade.json",      "strong_cascades"),
    ("dividend-growth",       "data/dividend-growth.json",       "compounders"),
    ("sympathetic-momentum",  "data/sympathetic-momentum.json",  "top_proxies"),
    ("news-velocity",         "data/news-velocity.json",         "by_ticker"),
]

# Sample tickers we want to see directional reads on
SAMPLES = ["AVGO", "PLTR", "AMD", "ARM", "RDDT", "NBIS", "TSLA", "NVDA"]


def find_ticker_in_container(container, t, is_dict_path=False):
    if is_dict_path:
        return container.get(t)
    if isinstance(container, list):
        for item in container:
            if isinstance(item, dict):
                tk = item.get("ticker") or item.get("symbol")
                if tk and str(tk).upper().strip() == t:
                    return item
    return None


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(),
            "engines": {},
            "ticker_samples": {}}

    for name, key, path in ENGINES:
        info = {"path": path}
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
            d = json.loads(obj["Body"].read())
            cont = d.get(path)
            if cont is None:
                info["err"] = f"path '{path}' missing"
            else:
                is_dict = isinstance(cont, dict)
                info["is_dict"] = is_dict
                info["size"] = len(cont) if (isinstance(cont, list) or is_dict) else None
                # Pick first item to inspect ALL fields
                first = None
                if isinstance(cont, list) and cont:
                    first = cont[0]
                elif is_dict and cont:
                    first = next(iter(cont.values()))
                if isinstance(first, dict):
                    info["all_fields"] = sorted(first.keys())
                    # Look for directional-suggesting fields
                    dir_hints = []
                    for f in first.keys():
                        fl = f.lower()
                        if any(kw in fl for kw in ['flag','flags','tier','signal','direction','side',
                                                      'bullish','bearish','sentiment','transaction',
                                                      'beat','miss','positive','negative','severity',
                                                      'valuation','rating','recommendation','status']):
                            dir_hints.append({"field": f, "sample_value": first.get(f)})
                    info["directional_fields"] = dir_hints
        except Exception as e:
            info["err"] = str(e)[:120]
        out["engines"][name] = info

        # Find samples
        if "err" not in info:
            try:
                obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
                d = json.loads(obj["Body"].read())
                cont = d.get(path)
                for t in SAMPLES:
                    item = find_ticker_in_container(cont, t, info.get("is_dict", False))
                    if item:
                        out["ticker_samples"].setdefault(t, {})[name] = {
                            k: v for k, v in item.items() if k != "events"  # skip large fields
                        }
            except Exception:
                pass

    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path("aws/ops/reports/1112_directional.json").write_text(json.dumps(out, indent=2, default=str))
    print("[1112] DONE")

if __name__ == "__main__":
    main()
