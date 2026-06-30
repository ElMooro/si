"""ops 2547 — full inventory of the data/ feed layer, bucketed by risk relevance,
flagging what's NOT yet on risk-regime.html. Then probe the most promising
unused ones for shape so we can surface them."""
import boto3, json
s3 = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"

# all data/*.json keys (top-level data/ only, not nested dirs)
keys = []
tok = None
while True:
    kw = dict(Bucket=B, Prefix="data/", Delimiter="/", MaxKeys=1000)
    if tok: kw["ContinuationToken"] = tok
    r = s3.list_objects_v2(**kw)
    for o in r.get("Contents", []):
        k = o["Key"]
        if k.endswith(".json"):
            keys.append(k.replace("data/", "").replace(".json", ""))
    tok = r.get("NextContinuationToken")
    if not tok: break
keys = sorted(set(keys))
print("total data/*.json feeds:", len(keys))

ON_PAGE = {"risk-regime","polygon-fx-regime","cross-asset-flow-state","cross-asset-regime","dollar-radar",
           "gold-equity-rotation","capital-inflows","tic-flows","eurodollar-stress","eurodollar-plumbing",
           "vol-regime","crisis-composite","global-stress","cycle-clock","dark-pool","regime-map",
           "settlement-fails","sovereign-fiscal"}

CATS = {
    "CREDIT / ICE BofA": ["credit","ice","bofa","oas","hy","ig","spread","bond-ind","corporate","junk","yield"],
    "FAILS / SETTLEMENT": ["fail","ftd","ftr","deliver","settle"],
    "DOLLAR SHORTAGE / FUNDING": ["dollar","eurodollar","funding","swap","basis","sofr","repo","liquid","snider","custody","plumbing"],
    "TIC / FOREIGN / CARIBBEAN": ["tic","foreign","custody","caribbean","bahama","holding","sovereign"],
    "FX / PEGS / CURRENCY": ["fx","peg","currency","dxy","hkd","yuan","yen","cny","em-fx","deval","depeg"],
    "RATES / CURVE / AUCTION / MOVE": ["yield-curve","curve","auction","move","rate","treasury","duration","term"],
    "VOL / STRESS / CRISIS": ["vol","vix","stress","crisis","defcon","fear","tail","skew","gamma"],
    "CDS / SOVEREIGN RISK": ["cds","sovereign-cds","default","counterparty"],
    "LIQUIDITY / FED / MACRO REGIME": ["liquidity","fed","nfci","financial-cond","macro","regime","cycle","sahm","recession","quad"],
    "FLOWS / POSITIONING / SMART MONEY": ["flow","positioning","cot","dark","institutional","13f","leveraged","breadth","rotation"],
}
def cat(k):
    out=[]
    for c,subs in CATS.items():
        if any(s in k for s in subs): out.append(c)
    return out or ["(other)"]

bycat={c:[] for c in list(CATS)+["(other)"]}
for k in keys:
    for c in cat(k):
        bycat[c].append(k)

print("\n=== risk-relevant feeds by category (★=already on page) ===")
for c in CATS:
    rel=[("★" if k in ON_PAGE else " ")+k for k in bycat[c]]
    if rel:
        print(f"\n## {c} ({len(rel)})")
        for r in rel: print("  "+r)
print("\nDONE 2547")
