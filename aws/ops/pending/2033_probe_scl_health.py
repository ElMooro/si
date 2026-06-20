"""ops 2033: is supply-chain-linkage alive or dead? Test exact FMP endpoint + live output + dep-graph source."""
import os, json, urllib.request, urllib.error, boto3
FMP=os.environ.get("FMP_KEY","wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def get(u):
    try:
        with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh/1"}),timeout=25) as r:
            return r.getcode(), r.read().decode()
    except urllib.error.HTTPError as e:
        try:return e.code,e.read().decode()[:200]
        except:return e.code,""
    except Exception as e: return None,str(e)[:120]
print("="*60);print("1) EXACT FMP endpoint the engine uses");print("="*60)
for ep in ["supply-chain-by-symbol?symbol=NVDA","supply-chain?symbol=NVDA","stock-supply-chain?symbol=NVDA"]:
    c,b=get(f"https://financialmodelingprep.com/stable/{ep}&apikey={FMP}")
    print(f"  /stable/{ep.split('?')[0]}: HTTP {c} {b[:120]}")
# also v3/v4 in case
c,b=get(f"https://financialmodelingprep.com/api/v4/supply-chain?symbol=NVDA&apikey={FMP}"); print("  v4/supply-chain:",c,b[:80])
print("\n"+"="*60);print("2) live data/supply-chain-linkage.json — populated or empty?");print("="*60)
try:
    d=json.loads(s3.get_object(Bucket=B,Key="data/supply-chain-linkage.json")["Body"].read())
    print("  generated_at:",d.get("generated_at") or d.get("as_of"))
    names=d.get("companies") or d.get("nodes") or d.get("linkages") or d.get("data") or d
    if isinstance(d,dict):
        print("  top keys:",list(d.keys())[:12])
        # find any populated suppliers/customers
        for k in ("companies","nodes","linkages"):
            v=d.get(k)
            if isinstance(v,(list,dict)) and v:
                samp=(v[0] if isinstance(v,list) else list(v.items())[0])
                print(f"  {k}: {len(v)} → sample {json.dumps(samp)[:240]}")
except Exception as e: print("  read err:",str(e)[:140])
print("\n"+"="*60);print("3) dep-graph.html — what data does it read?");print("="*60)
try:
    h=open("dep-graph.html").read()
    import re
    print("  fetches:",re.findall(r"data/[a-z0-9-]+\.json",h)[:8])
    print("  has d3/force graph:", any(x in h for x in ("d3.","forceSimulation","force-graph","cytoscape")))
except Exception as e: print("  no dep-graph:",str(e)[:80])
print("DONE 2033")
