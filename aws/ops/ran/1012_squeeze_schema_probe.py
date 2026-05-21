"""
ops 1012 - Probe exact short-interest + finra-short schemas before
patching squeeze-pretrigger extractors.

Need to know:
- What are the keys inside short-interest.json["by_ticker"][SYM]?
  (engine expects 'si_pct_float', 'days_to_cover' — confirm exact names)
- What are the keys inside short-interest.json["top_crowded_shorts"][i]?
- What are the keys inside finra-short.json["squeeze_candidates"][i]?
  (already partial info from ops 1010, but confirm full shape)

Read-only audit — no Lambda invokes. Outputs precise field maps to
aws/ops/reports/1012.json so the squeeze-pretrigger extractor patch can
target real keys not guessed ones.
"""
import json
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

import boto3

REPO_ROOT = Path(__file__).resolve().parents[3]
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)


def fetch(key):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def probe_dict_sample(d, n=3):
    """Sample n items from a dict and return their {key: type/value}."""
    out = {}
    for i, (k, v) in enumerate(d.items()):
        if i >= n:
            break
        if isinstance(v, dict):
            out[k] = {kk: (type(vv).__name__ if not isinstance(
                vv, (str, int, float, bool, type(None))) else vv)
                       for kk, vv in v.items()}
        else:
            out[k] = (type(v).__name__ if not isinstance(
                v, (str, int, float, bool, type(None))) else v)
    return out


def probe_list_sample(lst, n=2):
    """First n items of a list with full structure."""
    out = []
    for r in lst[:n]:
        if isinstance(r, dict):
            out.append({k: (type(v).__name__ if isinstance(
                v, (dict, list)) else v) for k, v in r.items()})
        else:
            out.append({"type": type(r).__name__, "value": str(r)[:80]})
    return out


def main():
    report = {"started_at": datetime.now(timezone.utc).isoformat()}

    # ---- short-interest.json ----
    try:
        si = fetch("data/short-interest.json")
        report["short_interest"] = {
            "top_level_keys": list(si.keys()),
            "n_by_ticker": (len(si.get("by_ticker") or {})
                             if isinstance(si.get("by_ticker"), dict) else 0),
            "by_ticker_type": type(si.get("by_ticker")).__name__,
            "by_ticker_sample_3": (probe_dict_sample(si["by_ticker"], 3)
                                    if isinstance(si.get("by_ticker"), dict)
                                    else None),
            "n_top_crowded": len(si.get("top_crowded_shorts") or []),
            "top_crowded_first_2": probe_list_sample(
                si.get("top_crowded_shorts") or [], 2),
            "n_top_high_dtc": len(si.get("top_high_dtc") or []),
            "top_high_dtc_first_2": probe_list_sample(
                si.get("top_high_dtc") or [], 2),
        }
    except Exception as e:
        report["short_interest"] = {"error": str(e)[:300]}

    # ---- finra-short.json ----
    try:
        fr = fetch("data/finra-short.json")
        report["finra_short"] = {
            "top_level_keys": list(fr.keys()),
            "tickers_type": type(fr.get("tickers")).__name__,
            "n_tickers": (len(fr.get("tickers")) if hasattr(
                fr.get("tickers"), "__len__") else None),
            "tickers_sample": (
                probe_dict_sample(fr["tickers"], 3)
                if isinstance(fr.get("tickers"), dict)
                else (probe_list_sample(fr.get("tickers") or [], 2)
                      if isinstance(fr.get("tickers"), list) else None)),
            "n_squeeze_candidates": len(fr.get("squeeze_candidates") or []),
            "squeeze_candidates_first_2": probe_list_sample(
                fr.get("squeeze_candidates") or [], 2),
            "n_top_zscore": len(fr.get("top_zscore") or []),
            "top_zscore_first_2": probe_list_sample(
                fr.get("top_zscore") or [], 2),
        }
    except Exception as e:
        report["finra_short"] = {"error": str(e)[:300]}

    # ---- catalyst-calendar.json ----
    try:
        cc = fetch("data/catalyst-calendar.json")
        report["catalyst_calendar"] = {
            "top_level_keys": list(cc.keys()),
            "generated_at": cc.get("generated_at"),
            "n_catalysts": len(cc.get("catalysts") or []),
            "n_upcoming": len(cc.get("upcoming") or []),
            "n_earnings": len(cc.get("earnings") or []),
            "n_fda": len(cc.get("fda") or []),
            "n_index_changes": len(cc.get("index_changes") or []),
            "schema_keys_present": {
                k: bool(cc.get(k)) for k in
                ["catalysts", "upcoming", "earnings", "fda",
                 "events", "by_ticker", "data"]
            },
        }
    except Exception as e:
        report["catalyst_calendar"] = {"error": str(e)[:300]}

    report["ended_at"] = datetime.now(timezone.utc).isoformat()

    out_path = REPO_ROOT / "aws" / "ops" / "reports" / "1012.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"[ops 1012] report written {out_path.relative_to(REPO_ROOT)}")
    if "error" not in report.get("short_interest", {}):
        print(f"  short-interest by_ticker: "
              f"{report['short_interest'].get('n_by_ticker')} items")
    if "error" not in report.get("finra_short", {}):
        print(f"  finra squeeze_candidates: "
              f"{report['finra_short'].get('n_squeeze_candidates')} items")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
