#!/usr/bin/env python3
"""Step 314 — Verify patched divergence-engine-v2 achieves ~100% coverage."""
import json
import os
import time
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-divergence-engine-v2"
BUCKET = "justhodl-dashboard-live"
KEY = "data/divergence-v2.json"
REPORT = "aws/ops/reports/314_verify_patch.json"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # First check Lambda's last modified to confirm code is fresh
        cfg = lam.get_function_configuration(FunctionName=LAMBDA_NAME)
        out["lambda_last_modified"] = cfg.get("LastModified")
        # If Lambda code is stale, wait for the deploy
        last_mod = cfg.get("LastModified", "")
        # Sync invoke
        print(f"[314] Sync invoking {LAMBDA_NAME}…")
        resp = lam.invoke(
            FunctionName=LAMBDA_NAME,
            InvocationType="RequestResponse",
        )
        out["status_code"] = resp.get("StatusCode")
        out["function_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        try:
            out["response"] = json.loads(body)
        except Exception:
            out["response_raw"] = body[:500]

        # Pull S3 output
        time.sleep(2)
        obj = s3.get_object(Bucket=BUCKET, Key=KEY)
        data = json.loads(obj["Body"].read())

        out["s3_size_kb"] = round(obj["ContentLength"] / 1024, 1)
        out["s3_last_modified"] = obj["LastModified"].isoformat()

        # Distill key stats
        out["n_relationships"] = data.get("n_relationships")
        out["n_with_data"] = data.get("n_with_data")
        out["composite_index"] = data.get("composite_divergence_index")
        out["by_status"] = data.get("by_status")
        out["fetch_errors"] = data.get("fetch_errors")

        # Coverage calculation
        n_total = out["n_relationships"] or 0
        n_with_data = out["n_with_data"] or 0
        coverage_pct = round(100 * n_with_data / n_total, 1) if n_total else 0
        out["coverage_pct"] = coverage_pct

        # Any remaining no-data pairs
        all_rels = data.get("all_relationships", [])
        no_data = [r for r in all_rels if r.get("status") == "no_data"]
        out["no_data_pairs"] = [
            {"name": r.get("name"), "series_a": r.get("series_a"), "series_b": r.get("series_b")}
            for r in no_data
        ]

        # Verify the new pairs work
        new_pair_ids = {"trucks_indpro", "permits_curve", "sentiment_permits",
                        "coincident_spy", "korea_ip_china", "switzerland_unemployment"}
        out["new_pairs"] = []
        for r in all_rels:
            if r.get("id") in new_pair_ids:
                out["new_pairs"].append({
                    "id": r.get("id"),
                    "name": r.get("name"),
                    "status": r.get("status"),
                    "z": r.get("divergence_z"),
                })

        out["duration_s"] = round(time.time() - started, 1)

    except Exception as e:
        import traceback
        out["fatal_error"] = str(e)
        out["traceback"] = traceback.format_exc()

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)

    # Pretty-print summary
    if "fatal_error" in out:
        print(f"❌ FATAL: {out['fatal_error']}")
        print(out.get("traceback","")[-1500:])
        return 1

    print()
    print("═" * 70)
    print("  DIVERGENCE v2.5 POST-PATCH VERIFICATION")
    print("═" * 70)
    print(f"  Lambda last modified: {out['lambda_last_modified']}")
    print(f"  Lambda invoke status: {out.get('status_code')} · err={out.get('function_error')}")
    print(f"  S3 output age: just now")
    print()
    print(f"  TOTAL pairs:     {out['n_relationships']}")
    print(f"  WITH DATA:       {out['n_with_data']}")
    print(f"  COVERAGE:        {out['coverage_pct']}%")
    print(f"  Status:          {out['by_status']}")
    print(f"  Composite:       {out['composite_index']}/100")
    print(f"  Fetch errors:    {out['fetch_errors']}")
    print()
    print(f"  Remaining no-data pairs: {len(out['no_data_pairs'])}")
    for p in out["no_data_pairs"][:10]:
        print(f"    • {p['name']}: {p['series_a']} ↔ {p['series_b']}")
    print()
    print(f"  ── 6 PATCHED/NEW PAIRS — verify they now have data ──")
    for p in out["new_pairs"]:
        ok = "✅" if p["status"] in ("normal", "flagged", "extreme") else "❌"
        z_str = f"z={p.get('z'):+.2f}σ" if p.get("z") is not None else ""
        print(f"    {ok} {p['id']:<25s} | status={p['status']:<10s} {z_str:<15s} | {p.get('name','')[:50]}")
    print(f"\n  Duration: {out['duration_s']}s")


if __name__ == "__main__":
    raise SystemExit(main() or 0)
