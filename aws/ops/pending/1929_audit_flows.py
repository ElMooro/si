import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def rd(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:60]}
d=rd("etf-flows/daily.json"); m=d.get("metrics",[]) or []
print("etf-flows/daily.json: %d ETFs"%len(m))
if m:
    print("  sample metric keys:",list(m[0].keys()))
    tickers=sorted([x.get("ticker") for x in m if x.get("ticker")])
    print("  ALL tickers:",tickers)
    # sector complex coverage check
    for complex_name, etfs in {"Semis":["SMH","SOXX","SOXL","SOXS","XSD","PSI"],"Tech":["XLK","QQQ","VGT","TQQQ","SQQQ"],
                               "Biotech":["XBI","IBB","LABU","LABD"],"Energy":["XLE","XOP","ERX","ERY"],
                               "Financials":["XLF","KRE","FAS","FAZ"]}.items():
        have=[e for e in etfs if e in tickers]; miss=[e for e in etfs if e not in tickers]
        print("  %s: have %s | MISSING %s"%(complex_name, have, miss))
for k in ["etf-flows/composite.json","etf-flows/rotation.json","etf-flows/per-ticker-context.json","data/capital-flow.json"]:
    d=rd(k); print("\n%s keys: %s"%(k, list(d.keys())[:18] if isinstance(d,dict) else type(d)))
    if k.endswith("per-ticker-context.json") and isinstance(d,dict):
        kk=list(d.keys())[:2]
        for x in kk: print("   %s: %s"%(x, json.dumps(d[x])[:200]))
# is there a flow history store?
import botocore
for key in ["etf-flows/history.json","data/etf-flow-history.json","etf-flows/flow-history.json"]:
    r=rd(key); print("history %s: %s"%(key, "EXISTS" if "_err" not in r else "none"))
