import json, boto3
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
def num(v):
    try:
        if isinstance(v,bool):return None
        return float(v)
    except: return None
def dig(o,p):
    if not p:return o
    for k in p.split("."):
        o=o.get(k) if isinstance(o,dict) else None
        if o is None:return None
    return o
# (feed, path) — path "" means describe top-level
ARR=[("13f-positions.json","consensus_holds"),("momentum-scanner.json","rankings.composite_top_50"),
     ("vol-regime.json","tickers"),("dislocations.json","top_dislocations"),
     ("buyback-scanner.json","top_opportunities"),("earnings-whisper.json","top_setups"),
     ("implied-prob.json","earnings_implied"),("sizing.json","engine_table"),
     ("stock-valuations.json","sp_table"),("insider-radar.json","latest_buys"),
     ("historical-analogs.json","analogs"),("catalyst-calendar.json","events"),
     ("market-map.json","tiles")]
STRUCT=["confluence-meta.json","regime.json","crisis-plumbing.json","crisis-canaries.json",
        "basket-var.json","vol-surface.json","rotation-radar.json","global-tide.json",
        "forward-returns.json","signal-backtest.json","auction-crisis.json","alpha-compass.json",
        "episode-compass.json","event-study.json","alert-sentinel.json","journal-graded.json",
        "dealer-survey.json","funding-plumbing.json"]
print("### ARRAY NUMERIC FIELDS (for bars value selection) ###")
for feed,path in ARR:
    try: o=json.loads(s3.get_object(Bucket=B,Key="data/"+feed)["Body"].read())
    except Exception as e: print(f"{feed} {path}: ERR {e.__class__.__name__}"); continue
    a=dig(o,path)
    if not isinstance(a,list) or not a: print(f"{feed} {path}: NOT-ARRAY ({type(a).__name__})"); continue
    e=a[0]
    if not isinstance(e,dict): print(f"{feed} {path}: list of {type(e).__name__}"); continue
    nums=[k for k in e if num(e[k]) is not None]
    datey=[k for k in e if k.lower() in ("date","asofdate","t","period","x","time")]
    print(f"{feed} {path}: n={len(a)} dateKeys={datey} numFields={ {k:round(num(e[k]),3) for k in nums[:8]} }")
print("\n### STRUCTURE PROBE (bespoke pages) ###")
for feed in STRUCT:
    try: o=json.loads(s3.get_object(Bucket=B,Key="data/"+feed)["Body"].read())
    except Exception as e: print(f"{feed}: ERR {e.__class__.__name__}"); continue
    if isinstance(o,dict):
        info=[]
        for k,v in o.items():
            if isinstance(v,list) and v:
                e=v[0]
                if isinstance(e,dict): info.append(f"{k}[{len(v)}]{{{','.join(list(e.keys())[:4])}}}")
                elif isinstance(e,list): info.append(f"{k}[{len(v)}][[{type(e[0]).__name__}..]]")
                else: info.append(f"{k}[{len(v)}]")
            elif isinstance(v,dict):
                nk=[kk for kk in v if num(v[kk]) is not None]
                info.append(f"{k}{{{','.join(list(v.keys())[:4])}}}"+(f" nums={nk[:3]}" if nk else ""))
        print(f"{feed}: "+" | ".join(info[:10]))
