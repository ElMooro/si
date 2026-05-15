#!/usr/bin/env python3
"""572 — Bulk audit for remaining roadmap items. Find what genuinely
doesn't exist yet vs what's already built."""
import io, json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/572_remaining_audit.json"
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # All Lambda function names
    try:
        paginator = lam.get_paginator("list_functions")
        all_names = []
        for page in paginator.paginate():
            for f in page.get("Functions", []):
                all_names.append(f["FunctionName"])
        out["n_lambdas_total"] = len(all_names)
    except Exception as e:
        out["lambda_err"] = str(e)[:200]
        all_names = []

    # Map keywords to roadmap items
    roadmap_keywords = {
        "4_adaptive_khalid":      ["adaptive", "khalid-adaptive", "khalid_adaptive",
                                     "khalid-index-v2", "khalid_v2", "weighted-khalid"],
        "5_stress_test":          ["stress-test", "stress_test", "scenario", "monte-carlo",
                                     "monte_carlo", "what-if", "stress-scenario"],
        "6_political_trades":     ["political", "senator", "house-trades", "stock-act",
                                     "pelosi", "congress", "capitol-trades"],
        "7_reversal_radar":       ["reversal-radar", "reversal_radar", "top-bottom",
                                     "top_bottom", "top-detector", "bottom-detector",
                                     "reversal-detector", "exhaustion"],
        "8_treasury_grade":       ["treasury-grade", "treasury-grading", "auction-grade",
                                     "auction-grading", "auction-score"],
        "9_repo_lending":         ["repo-collateral", "securities-lending",
                                     "tri-party", "margin-debt", "nyse-margin",
                                     "collateral-mix"],
        "10_correlation_break":   ["correlation-break", "correlation_break",
                                     "corr-break", "regime-correlation"],
    }

    matches = {}
    for item, kws in roadmap_keywords.items():
        item_matches = []
        for name in all_names:
            for kw in kws:
                if kw in name.lower():
                    item_matches.append(name)
                    break
        matches[item] = item_matches
    out["matches_by_item"] = matches

    # Also check S3 keys for these topics
    s3_prefixes = {
        "5_stress_test": ["stress/", "scenarios/", "data/stress-",
                           "data/scenarios-"],
        "6_political_trades": ["political/", "senators/", "data/political-",
                                 "data/senators-", "data/congress-"],
        "7_reversal_radar": ["reversal/", "data/reversal-", "data/top-",
                                "data/bottom-"],
        "8_treasury_grade": ["data/treasury-grade", "data/auction-grade",
                                 "auctions/"],
        "9_repo_lending": ["data/repo-collateral", "data/securities-lending",
                              "data/margin-debt", "data/nyse-margin"],
        "10_correlation_break": ["data/correlation-break", "data/correlation"],
        "4_adaptive_khalid": ["data/khalid-adaptive", "data/khalid-v2",
                                 "data/adaptive-khalid"],
    }
    s3_results = {}
    for item, pfxs in s3_prefixes.items():
        found = []
        for pfx in pfxs:
            try:
                resp = s3.list_objects_v2(
                    Bucket="justhodl-dashboard-live", Prefix=pfx, MaxKeys=5,
                )
                for o in resp.get("Contents", []):
                    found.append({
                        "key": o["Key"],
                        "size_kb": round(o["Size"]/1024,1),
                        "modified": o["LastModified"].isoformat()[:19],
                    })
            except Exception: pass
        s3_results[item] = found
    out["s3_matches"] = s3_results

    # Check khalid-index source for "adaptive" patterns
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/report.json")
        body = obj["Body"].read().decode("utf-8")[:50000]  # first 50KB to scan
        out["khalid_index_in_report"] = {
            "has_khalid_adaptive": "khalid_index_adaptive" in body or "khalid_adaptive" in body,
            "has_calibrated_weights": "calibrated_weights" in body,
            "khalid_keys_found": [k for k in ["khalid_index", "khalid_score", "khalid_band",
                                                "khalid_index_adaptive"] if k in body],
        }
    except Exception as e:
        out["khalid_check_err"] = str(e)[:120]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
