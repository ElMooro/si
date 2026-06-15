import json, boto3
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
TEST=["MU","VST","DELL","CEG","NRG","AVGO","ARM","HPE","LDOS","PWR","JBL","FLEX","BWXT","HEI","TDG","EMR","ROK","NDSN"]
def load(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:80]}

# 13F aggregate_by_ticker
d=load("data/13f-positions.json"); agg=d.get("aggregate_by_ticker",{})
print("13F aggregate_by_ticker: type",type(agg).__name__,"n",len(agg) if hasattr(agg,'__len__') else '?')
if isinstance(agg,dict):
    cov=[t for t in TEST if t in agg]
    print("  coverage:",cov)
    if cov: print("  sample",cov[0],":",json.dumps(agg[cov[0]])[:260])
    elif agg: k0=next(iter(agg)); print("  sample",k0,":",json.dumps(agg[k0])[:260])

# estimate revisions fwd_rev_growth
d=load("data/estimate-revisions-latest.json"); fr=d.get("fwd_rev_growth",{})
print("\nfwd_rev_growth: type",type(fr).__name__,"n",len(fr) if hasattr(fr,'__len__') else '?')
if isinstance(fr,dict):
    cov=[t for t in TEST if t in fr]
    print("  coverage:",cov)
    if cov: print("  samples:",{t:fr[t] for t in cov[:5]})
    elif fr: print("  first:",dict(list(fr.items())[:3]))

# rotation chains inner structure
d=load("data/rotation-chains.json"); ch=d.get("chains",{})
print("\nrotation chains: n",len(ch))
if isinstance(ch,dict):
    k0=next(iter(ch)); print("  chain key sample:",k0); print("  chain struct:",json.dumps(ch[k0])[:400])
    # find which chains contain bottleneck tickers
    found={}
    for cname,cval in ch.items():
        blob=json.dumps(cval)
        for t in TEST:
            if f'"{t}"' in blob: found.setdefault(t,[]).append(cname)
    print("  bottleneck tickers found in chains:",{k:v[:2] for k,v in list(found.items())[:10]})

# insider enriched summary
d=load("data/insider-buys-enriched.json"); sm=d.get("summary",{})
print("\ninsider-buys-enriched summary type:",type(sm).__name__)
print("  ",json.dumps(sm)[:300])
