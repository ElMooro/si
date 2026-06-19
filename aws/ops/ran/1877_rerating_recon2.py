import boto3, json, datetime, urllib.request
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"; FMP="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
now=datetime.datetime.now(datetime.timezone.utc)
def g(key):
    try: return json.loads(s3.get_object(Bucket=B,Key=key)["Body"].read())
    except Exception as e: return {"_err":str(e)[:40]}
# 1) estimate-revisions deep
er=g("data/estimate-revisions.json")
print("estimate-revisions: universe=%s with_revisions=%s breadth=%s"%(er.get("universe") if not isinstance(er.get("universe"),list) else len(er.get("universe")), er.get("with_revisions"), json.dumps(er.get("breadth"),default=str)[:120]))
mu=er.get("movers_up")
if isinstance(mu,list) and mu: print("  movers_up n=%d fields=%s\n   e0=%s"%(len(mu),list(mu[0].keys())[:14],json.dumps(mu[0],default=str)[:240]))
# 2) discover valuation/fundamentals data keys
print("\n-- valuation/fundamentals/growth data keys present --")
for pg in s3.get_paginator("list_objects_v2").paginate(Bucket=B,Prefix="data/"):
    for o in pg.get("Contents",[]):
        k=o["Key"]
        if any(t in k.lower() for t in ["valuation","graham","fundamental","dcf","ncav","peg","value","estimate","x-ray","xray"]) and k.endswith(".json"):
            age=(now-o["LastModified"]).total_seconds()/3600
            print("   %-44s %.0fh"%(k,age))
# 3) FMP /stable probes for forward growth + valuation multiples
def fmp(path):
    try:
        raw=urllib.request.urlopen(urllib.request.Request("https://financialmodelingprep.com/stable/%s%sapikey=%s"%(path,"&" if "?" in path else "?",FMP),headers={"User-Agent":"jh"}),timeout=15).read()
        d=json.loads(raw)
        if isinstance(d,list): return "LIST[%d] e0keys=%s e0=%s"%(len(d),list(d[0].keys())[:16] if d else [],json.dumps(d[0],default=str)[:300] if d else "")
        if isinstance(d,dict): return "DICT keys=%s"%list(d.keys())[:16]
        return str(d)[:120]
    except Exception as e:
        try: return "ERR %s %s"%(getattr(e,'code','?'),e.read().decode()[:80])
        except Exception: return "ERR %s"%str(e)[:80]
print("\n-- FMP /stable probes (MU) for growth + valuation --")
for p in ["analyst-estimates?symbol=MU&period=annual&limit=3","analyst-estimates?symbol=MU&limit=3",
          "ratios-ttm?symbol=MU","key-metrics-ttm?symbol=MU","ratios?symbol=MU&limit=1",
          "financial-growth?symbol=MU&limit=1","key-metrics?symbol=MU&limit=1"]:
    print("  /stable/%s\n     %s"%(p.split('?')[0], fmp(p)))
