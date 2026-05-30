"""ops 1133 — Inspect actual JSON structure of each per-name primary feed so we can
correctly point primary_tickers_field at the real ticker-list key.
"""
import json, os, traceback
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
BUCKET = "justhodl-dashboard-live"

FEEDS = {
    "baggers":          "data/bagger-engine.json",
    "eps-velocity":     "data/eps-revision-velocity.json",
    "insider-clusters": "data/insider-clusters.json",
    "smart-money":      "data/smart-money-clusters.json",
    "activist-13d":     "data/activist-13d.json",
    "deep-value":       "data/deep-value.json",
    "momentum":         "data/momentum-scanner.json",
}

s3 = boto3.client("s3", region_name=REGION)


def describe(obj, depth=0, path=""):
    """Recurse a few levels and find the most-likely ticker-list location."""
    if depth > 3:
        return []
    out = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            sub = f"{path}.{k}" if path else k
            if isinstance(v, list) and v:
                out.append({
                    "path": sub,
                    "len": len(v),
                    "first_item_keys": (sorted(v[0].keys()) if isinstance(v[0], dict) else type(v[0]).__name__),
                    "first_item_sample": (json.dumps(v[0], default=str)[:300]) if isinstance(v[0], dict) else str(v[0])[:100],
                })
            elif isinstance(v, dict):
                out.extend(describe(v, depth + 1, sub))
    return out


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat(), "feeds": {}}
    try:
        for name, key in FEEDS.items():
            row = {"key": key}
            try:
                obj = s3.get_object(Bucket=BUCKET, Key=key)
                data = json.loads(obj["Body"].read())
                row["top_keys"] = sorted(data.keys()) if isinstance(data, dict) else type(data).__name__
                row["lists_found"] = describe(data)
                # Likely ticker list = the longest list with dict items having a "ticker" or "symbol" field
                best = None
                for L in row["lists_found"]:
                    item_keys = L.get("first_item_keys")
                    has_ticker_field = isinstance(item_keys, list) and any(
                        any(t in (k or "").lower() for t in ["ticker", "symbol"]) for k in item_keys)
                    if has_ticker_field:
                        if best is None or L["len"] > best["len"]:
                            best = L
                row["best_ticker_list_path"] = best.get("path") if best else None
                row["best_ticker_list_len"] = best.get("len") if best else None
                row["best_ticker_list_keys"] = best.get("first_item_keys") if best else None
                row["best_ticker_first_sample"] = best.get("first_item_sample") if best else None
            except ClientError as e:
                row["err"] = f"S3: {e.response['Error']['Code']}"
            except Exception as e:
                row["err"] = str(e)[:200]
            rpt["feeds"][name] = row
    except Exception as e:
        rpt["fatal_err"] = str(e)[:300]
        rpt["traceback"] = traceback.format_exc()[-1200:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1133.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)

    # Pretty print
    for name, row in rpt["feeds"].items():
        print(f"\n=== {name} → {row.get('key')} ===")
        if row.get("err"):
            print(f"  ERR: {row['err']}")
            continue
        print(f"  top_keys: {row.get('top_keys')}")
        bp = row.get("best_ticker_list_path")
        if bp:
            print(f"  ★ likely ticker list at: '{bp}'  (len={row.get('best_ticker_list_len')})")
            print(f"    item keys: {row.get('best_ticker_list_keys')}")
            print(f"    sample:    {row.get('best_ticker_first_sample')[:250]}")
        else:
            print(f"  (no obvious ticker list found)")
            for L in (row.get("lists_found") or [])[:5]:
                print(f"    list at '{L['path']}'  len={L['len']}  keys={L.get('first_item_keys')}")


if __name__ == "__main__":
    main()
