"""Probe actual /stable/ response field names for the statements investor-lenses needs."""
import json,os,urllib.request,boto3
lam=boto3.client("lambda",region_name="us-east-1")
env=lam.get_function_configuration(FunctionName="justhodl-investor-lenses").get("Environment",{}).get("Variables",{})
k=env.get("FMP_KEY")
B="https://financialmodelingprep.com/stable"
def g(u):
    req=urllib.request.Request(u,headers={"User-Agent":"jh/1"})
    return json.loads(urllib.request.urlopen(req,timeout=15).read().decode())
out={}
for name,u in [("quote",f"{B}/quote?symbol=AAPL&apikey={k}"),
               ("income",f"{B}/income-statement?symbol=AAPL&period=annual&limit=1&apikey={k}"),
               ("balance",f"{B}/balance-sheet-statement?symbol=AAPL&period=annual&limit=1&apikey={k}"),
               ("cashflow",f"{B}/cash-flow-statement?symbol=AAPL&period=annual&limit=1&apikey={k}"),
               ("key-metrics",f"{B}/key-metrics?symbol=AAPL&period=annual&limit=1&apikey={k}"),
               ("financial-growth",f"{B}/financial-growth?symbol=AAPL&period=annual&limit=1&apikey={k}")]:
    try:
        d=g(u); rec=d[0] if isinstance(d,list) and d else d
        out[name]=sorted(rec.keys()) if isinstance(rec,dict) else str(type(d))
    except Exception as e:
        out[name]=f"ERR {type(e).__name__}: {str(e)[:80]}"
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(out,open("aws/ops/reports/probe_fmp_stable.json","w"),indent=2)
print(json.dumps(out,indent=2))
