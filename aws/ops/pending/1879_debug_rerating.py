import boto3, json, urllib.request, urllib.parse
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"; FMP="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
def fmp(path):
    try:
        return json.loads(urllib.request.urlopen(urllib.request.Request("https://financialmodelingprep.com/stable/%s%sapikey=%s"%(path,"&" if "?" in path else "?",FMP),headers={"User-Agent":"jh"}),timeout=15).read())
    except Exception as e: return {"_err":str(e)[:60]}
st=json.loads(s3.get_object(Bucket=B,Key="data/ai-infra-stack.json")["Body"].read())
names=[]
for layer in st.get("stack",[]):
    for n in layer.get("names",[]):
        names.append(n)
print("ai-infra-stack: layers=%d total_names=%d"%(len(st.get("stack",[])),len(names)))
if names:
    n0=names[0]; print("name[0] keys=%s"%list(n0.keys()))
    print("  sample:",json.dumps({k:n0.get(k) for k in ["symbol","market_cap","ret_1m_pct","ret_3m_pct","cap_bucket","flow_signals"]},default=str)[:200])
withmc=sum(1 for n in names if n.get("market_cap")); print("names with market_cap: %d/%d"%(withmc,len(names)))
# test fundamentals logic on real names
import datetime
today=datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
for sym in ["NVDA","MU","CRDO","STX"]:
    inc=fmp("income-statement?symbol=%s&limit=2"%sym)
    est=fmp("analyst-estimates?symbol=%s&limit=10"%sym)
    inc_ok=isinstance(inc,list) and inc
    latest=inc[0].get("revenue") if inc_ok else None
    fut=[]
    if isinstance(est,list):
        fut=sorted([e for e in est if (e.get("date") or "")>today and e.get("revenueAvg")],key=lambda e:e["date"])
    fg=None
    if fut and latest:
        if len(fut)>=2: fg=(fut[1]["revenueAvg"]/latest)**0.5-1
        else: fg=fut[0]["revenueAvg"]/latest-1
    print("  %-5s inc=%s latest_rev=%s est_type=%s n_future=%s fwd_growth=%s"%(
        sym, "list[%d]"%len(inc) if inc_ok else type(inc).__name__, latest,
        type(est).__name__ if not isinstance(est,dict) else ("DICT %s"%list(est.keys())[:3]),
        len(fut), round(fg*100,1) if fg is not None else None))
