#!/usr/bin/env python3
"""591 — Comprehensive audit of all 15 Bloomberg/Refinitiv features before
building. Check existing Lambda freshness + sidecar quality."""
import io, json, os
from datetime import datetime, timezone, timedelta
import boto3

REPORT = "aws/ops/reports/591_bloomberg_15_audit.json"
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # All Lambda names
    paginator = lam.get_paginator("list_functions")
    all_names = []
    for page in paginator.paginate():
        for f in page.get("Functions", []):
            all_names.append(f["FunctionName"])

    # 15 features → keyword matchers
    FEATURES = {
        "1_consensus":     ["consensus", "analyst", "estimate", "ibes", "starmine",
                              "revision", "upgrade", "downgrade"],
        "2_pead":          ["pead", "post-earnings", "post_earnings", "drift",
                              "earnings-surprise"],
        "3_vol_surface":   ["vol-surface", "vol_surface", "volatility-surface",
                              "implied-vol", "skew-surface", "term-structure"],
        "4_macro_surprise": ["macro-surprise", "macro_surprise", "econ-surprise",
                                "cesi"],
        "5_cds":           ["cds", "credit-default", "sovereign-cds",
                              "corporate-cds"],
        "6_etf_flows":     ["etf-flow", "etf_flow", "nav-premium",
                              "fund-flow"],
        "7_market_internals": ["market-internals", "breadth", "advance-decline",
                                   "ad-line", "mcclellan", "tick-trin",
                                   "internals"],
        "8_tic_flows":     ["tic-flow", "tic_flow", "tic-data", "treasury-flows",
                              "primary-dealer", "foreign-holdings"],
        "9_bond_trace":    ["bond-trace", "trace", "corp-bond-pricing",
                              "trace-feed"],
        "10_esi":          ["economic-surprise-index", "esi-composite", "esi"],
        "11_yield_curve_3d": ["yield-curve", "yield_curve", "treasury-curve",
                                  "tsy-curve"],
        "12_sector_heatmap": ["sector-heatmap", "sector_heatmap", "heatmap",
                                  "sector-grid"],
        "13_seasonality":  ["seasonality", "season-pattern", "cycle-analysis",
                              "presidential-cycle"],
        "14_sell_side":    ["sell-side", "sellside", "analyst-targets",
                              "consensus-targets"],
        "15_liquidity_profile": ["liquidity-profile", "bid-ask", "depth-score",
                                      "liquidity-score"],
    }

    matches = {}
    for feat, kws in FEATURES.items():
        m = []
        for n in all_names:
            nl = n.lower()
            if any(kw in nl for kw in kws):
                m.append(n)
        matches[feat] = m

    out["lambda_matches"] = matches

    # For each match, get freshness + invocation count
    detail = {}
    for feat, names in matches.items():
        if not names: continue
        detail[feat] = {}
        for n in names:
            try:
                cfg = lam.get_function_configuration(FunctionName=n)
                detail[feat][n] = {
                    "memory": cfg.get("MemorySize"),
                    "timeout": cfg.get("Timeout"),
                    "last_modified": cfg.get("LastModified"),
                    "state": cfg.get("State"),
                }
                # EB rules
                try:
                    rules = events.list_rule_names_by_target(
                        TargetArn=cfg["FunctionArn"])
                    rule_info = []
                    for rn in rules.get("RuleNames", [])[:2]:
                        ri = events.describe_rule(Name=rn)
                        rule_info.append(f"{rn}={ri.get('ScheduleExpression')}({ri.get('State')})")
                    detail[feat][n]["eb"] = rule_info
                except Exception: pass
            except Exception as e:
                detail[feat][n] = {"err": str(e)[:80]}
    out["lambda_detail"] = detail

    # Check sidecars for each
    sidecar_candidates = {
        "1_consensus":     ["data/consensus.json", "data/analyst-consensus.json",
                              "data/analyst-estimates.json"],
        "2_pead":          ["data/pead.json", "data/post-earnings-drift.json",
                              "data/earnings-pead.json", "data/pead-detector.json"],
        "3_vol_surface":   ["data/vol-surface.json", "data/implied-vol-surface.json",
                              "data/vol-skew.json"],
        "4_macro_surprise": ["data/macro-surprise.json", "data/cesi.json"],
        "5_cds":           ["data/cds.json", "data/cds-spreads.json",
                              "data/sovereign-cds.json"],
        "6_etf_flows":     ["data/etf-flows.json", "data/etf-creations.json",
                              "data/nav-premium.json"],
        "7_market_internals": ["data/market-internals.json", "data/breadth.json",
                                   "data/advance-decline.json"],
        "8_tic_flows":     ["data/tic-flows.json", "data/primary-dealer.json",
                              "data/foreign-holdings.json"],
        "9_bond_trace":    ["data/bond-trace.json", "data/trace-prints.json"],
        "10_esi":          ["data/esi.json", "data/economic-surprise.json"],
        "11_yield_curve_3d": ["data/yield-curve.json", "data/yield-curve-3d.json"],
        "12_sector_heatmap": ["data/sector-heatmap.json", "data/sectors.json"],
        "13_seasonality":  ["data/seasonality.json", "data/cycle-analysis.json"],
        "14_sell_side":    ["data/sell-side.json", "data/analyst-targets.json"],
        "15_liquidity_profile": ["data/liquidity-profile.json", "data/liquidity.json"],
    }
    sidecars = {}
    for feat, keys in sidecar_candidates.items():
        found = []
        for k in keys:
            try:
                obj = s3.head_object(Bucket="justhodl-dashboard-live", Key=k)
                found.append({
                    "key": k,
                    "size_kb": round(obj["ContentLength"]/1024, 1),
                    "modified": obj["LastModified"].isoformat()[:19],
                    "age_h": round((datetime.now(timezone.utc)-obj["LastModified"]).total_seconds()/3600, 1),
                })
            except Exception: pass
        sidecars[feat] = found
    out["sidecars"] = sidecars

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
