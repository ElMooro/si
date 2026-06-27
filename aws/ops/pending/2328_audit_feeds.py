import boto3, json, time
from datetime import datetime, timezone
s3=boto3.client("s3","us-east-1")
# candidate engines a Cycle & Liquidity Clock should consider (data filename guesses)
CANDIDATES={
 "FED/RATES":["fedwatch-rate-probability","fed-speak","fed-nlp","fed-pivot-factor-router","bond-regime-detector","move-index","bond-vol","treasury-proxy","repo-monitor","funding-plumbing","fed-collateral","nyfed-dealer-survey"],
 "POSITIONING/SENTIMENT":["aaii-sentiment","retail-sentiment","cot-extremes-scanner","cot-tracker","cftc-deep-view","news-sentiment","gdelt-sentiment","pump-positioning"],
 "BREADTH/INTERNALS":["breadth-divergence","breadth-thrust","credit-equity-divergence","sector-earnings-diffusion","concentration-liquidity","gold-equity-rotation"],
 "VOL":["vol-regime","vvix-vov-regime","vix-backwardation-trigger","vix9d-vix-inversion","vol-radar","carry-surface","skew-tail-hedging"],
 "GLOBAL/LIQ":["china-liquidity","liquidity-inflection","liquidity-pulse","liquidity-capacity","global-tide","global-macro","cross-asset-regime","tic-flows","capital-inflows","fx-intelligence","yen-carry","stablecoin-flow"],
 "GROWTH/VALUATION":["labor-leading","activity-nowcast","consumer-pulse","manufacturing-global","bank-stress","stock-valuations","valuations-agent","commodity-curves","seasonality","regime-anomaly"],
}
USED=set("credit-stress crisis-canaries crisis-composite dollar-radar eps-revision-velocity eurodollar-plumbing fomc-reaction global-business-cycle global-liquidity global-stress historical-analogs liquidity-credit-engine liquidity-flow macro-nowcast macro-surprise regime-composite regime-map regime-playbook regime risk-regime sector-rotation settlement-fails sovereign-fiscal systemic-stress treasury-noise us-cycle vix-curve yield-curve".split())
now=datetime.now(timezone.utc)
def info(name):
    k=f"data/{name}.json"
    try:
        h=s3.head_object(Bucket="justhodl-dashboard-live",Key=k)
        age=(now-h["LastModified"]).total_seconds()/86400
        return age
    except Exception:
        return None
for cat,names in CANDIDATES.items():
    print(f"\n=== {cat} ===")
    for n in names:
        age=info(n)
        tag = "MISSING" if age is None else (f"{age:.1f}d" + (" ⚠STALE" if age>4 else " ✓FRESH"))
        u = " [already used]" if n in USED else ""
        print(f"  {n:30} {tag}{u}")
print("\nDONE 2328")
