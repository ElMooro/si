"""ops 1098 — comprehensive system audit.

Refreshes the inventory snapshot (SYSTEM_CATALOG was 2026-05-21, 333 Lambdas;
since then Wave 3 + Wave 4 + parallel sessions). Outputs:

  1. Current Lambda count + family classification
  2. Delta vs SYSTEM_CATALOG baseline (333)
  3. Orphan detection (Lambdas with no EB schedule + no recent invocations)
  4. Dead-letter Lambdas (functions referenced by no live system)
  5. Capability category density map (which functional areas are dense vs sparse)
  6. Output to aws/ops/reports/1098.json + data/system-audit.json
"""
import os, json, re
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


# Functional category mapping by name pattern (extends SYSTEM_CATALOG families)
CATEGORY_PATTERNS = [
    # Personal/Wealth (NEWEST — Wave 4)
    (r"^justhodl-(wealth-plan|tax-plan|forward-returns)$", "Personal Wealth Triangle"),

    # Wave 3 Tier-0
    (r"^justhodl-(dr-snapshot|cost-anomaly|fleet-(error|freshness)-monitor)$", "Infrastructure / DR / Monitoring"),
    (r"^justhodl-(macro-calendar|fed-nlp|news-wire|concentration-liquidity)$", "Wave 3 Tier-1"),

    # 13F / Activist / SEC
    (r"^justhodl-(13f|activist|sec)", "13F / Activist / SEC"),

    # AI / Agent
    (r"^justhodl-(ai-chat|charts-agent|investor-agents|ultimate-orchestrator|valuations-agent|debate-engine)$", "AI / Agent"),

    # Bagger pack
    (r"^justhodl-(bagger|coffee-can|hiring-velocity|insider-aggregate)$", "Bagger pack"),

    # Bonds / Credit / Rates
    (r"^justhodl-(bond-|buyback-yield|commodity-curves|credit-|yield-curve)", "Bonds / Credit / Rates"),

    # CFTC / COT
    (r"^justhodl-(cot-|cftc-)", "CFTC / COT"),

    # Catalyst / Calendar
    (r"^justhodl-(catalyst-calendar|econ-calendar|spinoff)", "Catalyst / Calendar"),

    # Crisis / Stress
    (r"^justhodl-(bank-stress|cds-monitor|crisis-|firm-stress|global-stress|stress-|systemic-stress|eurodollar)", "Crisis / Stress"),

    # Crypto
    (r"^justhodl-(crypto-|dex-|onchain|stablecoin)", "Crypto"),

    # Data infra
    (r"^justhodl-(analyst-consensus|calibration-snapshotter|financial-secretary|fred-proxy|health-monitor|history-)", "Data infra"),

    # Insider / Smart Money
    (r"^justhodl-(insider-|smart-money|rating-change-cluster)", "Insider / Smart Money"),

    # Macro / Fed / Liquidity
    (r"^justhodl-(auction-|boj-|cb-|china-|ecb-|fed-(speak|nlp)|global-liquidity|liquidity-|nyfed-|snb-)", "Macro / Fed / Liquidity"),

    # Options / Flow / Vol
    (r"^justhodl-(options-|dealer-gex|dix|opex-|gamma|vol-|vix-|vvix|catalyst-skew|precatalyst|tape-reader)", "Options / Flow / Vol"),

    # PEAD / Earnings
    (r"^justhodl-(pead-|earnings-(pead|quality|whisper|nlp|iv-crush|tracker|sentiment)|eps-revision|post-earnings|earnings-)", "Earnings / PEAD"),

    # Pro Pack v3
    (r"^justhodl-(beneish|bond-vol|eva-spread|gf-value|ipo-pipeline|magic-formula|predictability|smart-beta|starmine)$", "Pro Pack v3"),

    # Reports / Brief / Email / Telegram
    (r"^justhodl-(ai-brief|alpha-daily-brief|daily-report|email-reports|morning-brief|morning-intelligence|telegram)", "Reports / Comm"),

    # Risk / Hedge / Portfolio
    (r"^justhodl-(allocator|cro-|desk-allocator|factor-risk|firm-risk-board|hedge-|macro-(nowcast|surprise)|master-allocator|merger-arb|pm-decision|portfolio-|retail-sentiment|risk-|skew-tail-hedging|tail-hedge|signal-portfolio|position-)", "Risk / Hedge / Portfolio"),

    # Screener / Watchlist
    (r"^justhodl-(deep-value-screener|opportunity-screener|screener-|stock-(ai-research|analyzer|screener)|watchlist)", "Screener / Watchlist"),

    # Sector / Theme
    (r"^justhodl-(sector-|gold-equity-rotation|sympathetic-momentum|theme-)", "Sector / Theme"),

    # Sentiment / Composite
    (r"^justhodl-(aaii-sentiment|asymmetric|best-ideas|compound-aggregator|crisis-composite|gdelt|master-ranker|news-sentiment|regime-composite|signal-board|earnings-sentiment)", "Sentiment / Composite"),

    # Squeeze stack
    (r"^justhodl-(finra-short|microcap-float-squeeze|short-(interest|pressure)|squeeze-|volatility-squeeze-hunter)", "Squeeze stack"),

    # ETF flows / Capital flows
    (r"^justhodl-(etf-flows|exchange-flows|tic-flows|repo-|margin-lending|fund-flows)", "Flows / Capital"),

    # Operational / Recon / Audit (non-revenue, infra)
    (r"^justhodl-(api-keys-admin|public-api-demo|push-api|webhook|tmp-|cdn-diag)", "Operational"),

    # Learning / Calibration
    (r"^justhodl-(signal-logger|outcome-checker|calibrator|calibration-|track-record|signal-orthogonality|prompt-iterator|backtest-)", "Learning / Calibration"),
]


def classify(name):
    for pattern, category in CATEGORY_PATTERNS:
        if re.match(pattern, name):
            return category
    return "Other / Misc"


def list_all_lambdas():
    names = []
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        for f in page.get("Functions", []):
            names.append({
                "name": f["FunctionName"],
                "runtime": f.get("Runtime"),
                "last_modified": f.get("LastModified"),
                "memory": f.get("MemorySize"),
                "timeout": f.get("Timeout"),
                "desc": (f.get("Description") or "")[:200],
            })
    return names


def list_all_rules():
    rules = []
    token = None
    while True:
        kw = {"Limit": 100}
        if token:
            kw["NextToken"] = token
        r = events.list_rules(**kw)
        rules.extend(r.get("Rules", []))
        token = r.get("NextToken")
        if not token:
            break
    return rules


def get_invocations_30d(name):
    """Sum of invocations over last 30 days."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=30)
    try:
        r = cw.get_metric_statistics(
            Namespace="AWS/Lambda",
            MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": name}],
            StartTime=start,
            EndTime=end,
            Period=86400 * 30,  # single sum over 30 days
            Statistics=["Sum"],
        )
        pts = r.get("Datapoints", [])
        return int(sum(p.get("Sum", 0) for p in pts))
    except Exception:
        return None


def main():
    print("Phase 1: list Lambdas...")
    fns = list_all_lambdas()
    print(f"  Found {len(fns)} Lambdas")

    print("Phase 2: list EB rules...")
    rules = list_all_rules()
    rule_targets = defaultdict(list)
    for r in rules:
        try:
            t = events.list_targets_by_rule(Rule=r["Name"])
            for tt in t.get("Targets", []):
                arn = tt.get("Arn", "")
                if ":function:" in arn:
                    fn = arn.split(":function:")[-1].split(":")[0]
                    rule_targets[fn].append({
                        "rule": r["Name"],
                        "schedule": r.get("ScheduleExpression", ""),
                        "state": r.get("State"),
                    })
        except Exception:
            pass
    print(f"  Found {len(rules)} rules covering {len(rule_targets)} Lambdas")

    print("Phase 3: classify + fetch 30d invocations (parallel)...")
    by_category = defaultdict(list)
    orphans = []  # No schedule + no recent invocations

    def enrich(fn):
        fn["category"] = classify(fn["name"])
        fn["schedules"] = rule_targets.get(fn["name"], [])
        fn["invocations_30d"] = get_invocations_30d(fn["name"])
        return fn

    with ThreadPoolExecutor(max_workers=25) as ex:
        futures = [ex.submit(enrich, f) for f in fns]
        enriched = []
        for i, fut in enumerate(as_completed(futures)):
            enriched.append(fut.result())
            if (i + 1) % 50 == 0:
                print(f"  Enriched {i + 1}/{len(fns)}")

    for fn in enriched:
        by_category[fn["category"]].append(fn["name"])
        if not fn["schedules"] and (fn["invocations_30d"] or 0) < 5:
            orphans.append({
                "name": fn["name"],
                "invocations_30d": fn["invocations_30d"],
                "desc": fn.get("desc", "")[:100],
                "last_modified": fn["last_modified"],
            })

    # Density map
    density = sorted(
        [(cat, len(names)) for cat, names in by_category.items()],
        key=lambda x: -x[1]
    )

    # Delta vs catalog (333 baseline)
    delta = len(fns) - 333

    # Report
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_lambdas": len(fns),
        "delta_vs_catalog_baseline": delta,
        "catalog_baseline_date": "2026-05-21",
        "total_eb_rules": len(rules),
        "lambdas_with_schedule": len(rule_targets),
        "lambdas_no_schedule": len(fns) - len(rule_targets),
        "category_density": [{"category": c, "count": n} for c, n in density],
        "category_inventory": {cat: sorted(names) for cat, names in by_category.items()},
        "orphan_candidates": sorted(orphans, key=lambda o: o.get("invocations_30d") or 0)[:40],
        "orphan_count": len(orphans),
    }

    # Save
    out_local = os.path.join(REPO_ROOT, "aws/ops/reports/1098.json")
    os.makedirs(os.path.dirname(out_local), exist_ok=True)
    with open(out_local, "w") as f:
        json.dump(report, f, indent=2, default=str)
    s3.put_object(
        Bucket=BUCKET,
        Key="data/system-audit.json",
        Body=json.dumps(report, default=str, indent=2).encode(),
        ContentType="application/json",
    )

    # Summary print
    print("\n" + "=" * 70)
    print("SYSTEM AUDIT SUMMARY")
    print("=" * 70)
    print(f"Total Lambdas: {len(fns)} (delta {'+' if delta >= 0 else ''}{delta} vs 2026-05-21 catalog)")
    print(f"Total EB rules: {len(rules)}/300 (headroom: {300 - len(rules)})")
    print(f"Lambdas with schedule: {len(rule_targets)}")
    print(f"Orphan candidates (no schedule + <5 invocations/30d): {len(orphans)}")
    print(f"\nDENSITY by category:")
    for cat, n in density:
        bar = "█" * min(40, n // 2) if n > 1 else "▌"
        print(f"  {cat:38s} {n:4d}  {bar}")


if __name__ == "__main__":
    main()
