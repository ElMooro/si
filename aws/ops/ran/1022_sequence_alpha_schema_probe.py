"""
ops 1022 - Probe live S3 schemas for Sequence Alpha Detector inputs.

Need exact field shape of cluster items + activist setups + PEAD signals
before building the meta-engine. Read-only probe, no Lambda invokes.
"""
import json
import traceback
from datetime import datetime, timezone
from pathlib import Path

import boto3

REPO_ROOT = Path(__file__).resolve().parents[3]
s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def fetch(key):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def probe(d, list_keys_to_sample):
    """Sample fields in list-valued keys we care about."""
    out = {"top_level": list(d.keys())[:30] if isinstance(d, dict) else []}
    for k in list_keys_to_sample:
        v = d.get(k) if isinstance(d, dict) else None
        if isinstance(v, list):
            out[f"{k}__n"] = len(v)
            out[f"{k}__first_2"] = []
            for item in v[:2]:
                if isinstance(item, dict):
                    out[f"{k}__first_2"].append(
                        {kk: (type(vv).__name__ if isinstance(
                            vv, (dict, list)) else vv)
                         for kk, vv in item.items()})
                else:
                    out[f"{k}__first_2"].append({"_type": type(item).__name__,
                                                  "_value": str(item)[:80]})
    return out


def main():
    report = {"started_at": datetime.now(timezone.utc).isoformat()}
    try:
        ic = fetch("data/insider-clusters.json")
        report["insider_clusters"] = probe(ic, ["clusters",
                                                 "all_clusters",
                                                 "strong_signals"])
    except Exception as e:
        report["insider_clusters"] = {"error": str(e)[:300]}

    try:
        ia = fetch("data/insider-aggregate.json")
        report["insider_aggregate"] = probe(ia, ["top_buys", "top_sells",
                                                  "tickers", "by_ticker",
                                                  "clusters",
                                                  "cluster_buys"])
    except Exception as e:
        report["insider_aggregate"] = {"error": str(e)[:300]}

    try:
        act = fetch("data/activist-13d.json")
        report["activist_13d"] = probe(act, ["top_setups", "all_setups",
                                              "fresh_filings"])
    except Exception as e:
        report["activist_13d"] = {"error": str(e)[:300]}

    try:
        pead = fetch("data/pead-signals.json")
        report["pead_signals"] = probe(pead, ["signals", "names",
                                               "positive_drift",
                                               "negative_drift",
                                               "active_setups"])
    except Exception as e:
        report["pead_signals"] = {"error": str(e)[:300]}

    try:
        pead2 = fetch("data/earnings-pead.json")
        report["earnings_pead"] = probe(pead2, ["signals", "names",
                                                  "active", "watchlist"])
    except Exception as e:
        report["earnings_pead"] = {"error": str(e)[:300]}

    report["ended_at"] = datetime.now(timezone.utc).isoformat()
    out_path = REPO_ROOT / "aws" / "ops" / "reports" / "1022.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"[ops 1022] report written")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
