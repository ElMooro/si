#!/usr/bin/env python3
"""539 — Inspect dealer-gex sidecar to see what 0DTE data exists for BUILD 13 design."""
import json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/539_0dte_audit.json"
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/dealer-gex.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["sidecar_size_kb"] = round(len(body)/1024, 1)
        out["sidecar_modified"] = obj["LastModified"].isoformat()[:19]
        out["top_keys"] = list(p.keys())[:25]
        out["generated_at"] = p.get("generated_at")
        out["version"] = p.get("version")

        # Look at per-symbol structure to see what 0DTE info exists
        per_sym = p.get("per_symbol") or p.get("by_symbol") or {}
        if isinstance(per_sym, dict):
            sample_sym = "SPY" if "SPY" in per_sym else (list(per_sym.keys())[0] if per_sym else None)
            if sample_sym:
                sd = per_sym[sample_sym]
                if isinstance(sd, dict):
                    out[f"sample_{sample_sym}_keys"] = list(sd.keys())[:30]
                    # Drill into 0DTE-related fields
                    for k in ("zero_dte", "0dte", "concentration_0dte", "today_expiry",
                              "next_expiry", "all_expiries", "expiries", "by_expiry",
                              "gamma_by_expiry"):
                        if k in sd:
                            v = sd[k]
                            out[f"sample_{sample_sym}_{k}_preview"] = (
                                json.dumps(v, default=str)[:1000]
                                if isinstance(v, (dict, list)) else str(v)[:500]
                            )
                    # Sample data
                    sample_keys_to_dump = ["composite_regime", "regime", "gex_billion_usd", "max_pain",
                                            "zero_dte_concentration_pct", "0dte_pct", "gamma_flip_strike"]
                    for k in sample_keys_to_dump:
                        if k in sd: out[f"sample_{sample_sym}_{k}"] = sd[k]
        out["composite_keys"] = [k for k in p.keys() if "composite" in k.lower() or "regime" in k.lower()]
    except Exception as e:
        out["err"] = str(e)[:300]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
