import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
t=time.time()
try:
    r=lam.invoke(FunctionName="justhodl-etf-fund-flows",InvocationType="RequestResponse")
    print("invoke (%.0fs):"%(time.time()-t), r["Payload"].read().decode()[:160])
except Exception as e: print("invoke err",str(e)[:120])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="etf-flows/daily.json")["Body"].read())
m={x.get("ticker"):x for x in d.get("metrics",[]) if x.get("ticker")}
ok=[t for t,v in m.items() if not v.get("error") and v.get("flow_5d_usd") is not None]
print("\ntotal ETFs in universe output: %d | with flow data: %d"%(len(m),len(ok)))
# leveraged bull/bear positioning pairs
print("\nLEVERAGED POSITIONING (5d $ flow, bull vs bear) — what investors are betting on:")
pairs=[("SPXL","SPXS","S&P500"),("TQQQ","SQQQ","Nasdaq"),("SOXL","SOXS","Semis"),("TNA","TZA","SmallCap"),
       ("FAS","FAZ","Financials"),("ERX","ERY","Energy"),("LABU","LABD","Biotech"),("TECL","TECS","Tech"),
       ("YINN","YANG","China"),("NUGT","DUST","GoldMiners"),("WEBL","WEBS","Internet")]
for bull,bear,name in pairs:
    b=m.get(bull,{}); s=m.get(bear,{})
    bf=b.get("flow_5d_usd"); sf=s.get("flow_5d_usd")
    if bf is None and sf is None: 
        print("  %-11s no data (%s/%s)"%(name,bull,bear)); continue
    net=(bf or 0)-(sf or 0)
    tilt="NET BULLISH" if net>0 else "NET BEARISH"
    print("  %-11s %s: $%sM bull(%s) vs $%sM bear(%s) -> net $%sM %s"%(
        name,bull+"/"+bear, round((bf or 0)/1e6,1),bull, round((sf or 0)/1e6,1),bear, round(net/1e6,1), tilt))
# single-stock leveraged
print("\nSINGLE-STOCK LEVERAGED (retail conviction):")
for t2 in ["NVDL","TSLL","CONL","MSTU","NVDS","TSLQ"]:
    v=m.get(t2,{}); print("  %-5s flow_5d=$%sM z=%s"%(t2, round((v.get('flow_5d_usd') or 0)/1e6,1), v.get('flow_zscore_90d')) if v and not v.get('error') else "  %-5s no data"%t2)
# new sub-sectors
print("\nNEW SUB-SECTOR coverage:", [t2 for t2 in ["VGT","IGV","XOP","OIH","GDX","GDXJ","URA","ITB","JETS","ITA","HACK","ARKB"] if t2 in ok])
