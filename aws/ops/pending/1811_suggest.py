import json, boto3
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
PAIRS="""13f.html 13f-positions.json
activist-13d.html activist-13d.json
alerts.html alert-sentinel.json
alpha-compass.html alpha-compass.json
analogs.html historical-analogs.json
apex.html apex-fusion.json
auctions.html auction-crisis.json
bottleneck.html bottleneck-boom.json
brain.html brain.json
buyback-scanner.html buyback-scanner.json
canaries.html crisis-canaries.json
carry-surface.html carry-surface.json
catalyst-calendar.html catalyst-calendar.json
compass.html forward-returns.json
confluence.html confluence-meta.json
crisis.html crisis-plumbing.json
crypto-opportunities.html crypto-opportunities.json
crypto-risk.html crypto-cycle-risk.json
dealer-survey.html dealer-survey.json
deep-value-overlap.html deep-value-overlap.json
dislocations.html dislocations.json
divergence-v2.html divergence-v2.json
earnings-whisper.html earnings-whisper.json
episode-compass.html episode-compass.json
eps-velocity.html eps-revision-velocity.json
event-study.html event-study.json
forensic.html forensic-screen.json
funding-plumbing.html funding-plumbing.json
global-macro.html global-macro.json
global-tide.html global-tide.json
heatmap.html stock-valuations.json
ignition.html ignition.json
implied-prob.html implied-prob.json
insider-buys.html insider-buys-enriched.json
insider-clusters.html insider-clusters.json
insider.html insider-radar.json
insiders.html insider-trades.json
journal.html journal-graded.json
lce.html liquidity-credit-engine.json
ma-reversion.html ma-reversion.json
market-map.html market-map.json
master-rank.html master-ranker.json
momentum.html momentum-scanner.json
opex-calendar.html opex-calendar.json
options-scanner.html options-flow.json
pairs-scanner.html pairs-scanner.json
pead-signals.html pead-signals.json
regime.html regime.json
risk.html basket-var.json
rotation-radar.html rotation-radar.json
russell-recon.html russell-recon-frontrun.json
rv-iv-scanner.html rv-iv-scanner.json
scorecard.html signal-backtest.json
sector-tilt.html sector-tilt.json
sectors.html sector-rotation.json
signal-halflife.html signal-halflife.json
sizing.html sizing.json
smart-money.html smart-money-clusters.json
stablecoin-flow.html stablecoin-flow.json
supply-inflection.html supply-inflection.json
tape-reader.html tape-reader.json
theme-tiers.html theme-tiers.json
us-cycle.html us-cycle.json
valuations.html stock-valuations.json
vix-capitulation.html vix-backwardation-trigger.json
vol-regime.html vol-regime.json
vol-target-unwind.html vol-target-unwind.json
volatility.html vol-surface.json"""
LBL=["symbol","ticker","pair","t","name","label","category","signal_type","sector","key","id","cohort","theme"]
VAL_PRI=["compound_score","flow_score","score","overall_accuracy","accuracy","z_score","zscore","delta","fwd_3m_pct","dist_hi_pct","carry_pct","upside_pct","cycle_score","value","weight"]
def num(v):
    try:
        if isinstance(v,bool):return None
        return float(v)
    except: return None
def dig(o,p):
    for k in p.split("."):
        o=o.get(k) if isinstance(o,dict) else None
        if o is None:return None
    return o
def series_paths(o):
    out=[]
    def walk(x,pre,d):
        if isinstance(x,dict):
            for k,v in x.items():
                if isinstance(v,list) and v:
                    e=v[0]
                    if isinstance(e,list) and len(e)>=2 and num(e[1]) is not None and isinstance(e[0],str): out.append(pre+k)
                    elif isinstance(e,dict) and any(dd in e for dd in ("date","asofdate","t","period","x")) and any(num(e[c]) is not None for c in e): out.append(pre+k)
                elif isinstance(v,dict) and d<1: walk(v,pre+k+".",d+1)
    walk(o,"",0); return out
def best_array(o):
    best=[None,0]
    def walk(x,pre,d):
        if isinstance(x,dict):
            for k,v in x.items():
                if isinstance(v,list) and v and isinstance(v[0],dict) and len(v)>best[1]: best[0]=pre+k;best[1]=len(v)
                elif isinstance(v,dict) and d<1: walk(v,pre+k+".",d+1)
    walk(o,"",0); return best[0]
for ln in PAIRS.splitlines():
    pg,feed=ln.split()
    try: o=json.loads(s3.get_object(Bucket=B,Key="data/"+feed)["Body"].read())
    except Exception as e: print(f"{pg}|{feed}|ERR|{e.__class__.__name__}"); continue
    sp=series_paths(o)
    if sp: print(f"{pg}|{feed}|line|{sp[0]}"); continue
    ap=best_array(o)
    if not ap: print(f"{pg}|{feed}|none|"); continue
    arr=dig(o,ap); e=arr[0]
    lab=next((k for k in LBL if k in e),None) or next((k for k in e if isinstance(e[k],str)),list(e.keys())[0])
    val=next((k for k in VAL_PRI if k in e and num(e[k]) is not None),None) or next((k for k in e if k!=lab and num(e[k]) is not None),None)
    print(f"{pg}|{feed}|bars|{ap}:{lab}:{val}|n={len(arr)}")
