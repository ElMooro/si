import urllib.request, json
from datetime import date, timedelta
KEY="zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
end=date.today(); start=end-timedelta(days=40)
def ff(tk):
    u=(f"https://api.polygon.io/etf-global/v1/fund-flows?composite_ticker={tk}"
       f"&processed_date.gte={start.isoformat()}&processed_date.lte={end.isoformat()}"
       f"&order=desc&sort=processed_date&limit=40&apiKey={KEY}")
    try:
        j=json.loads(urllib.request.urlopen(u,timeout=30).read())
        res=j.get("results") or []
        if not res: return f"NO DATA (status={j.get('status')})"
        flows=[r.get("fund_flow") for r in res if r.get("fund_flow") is not None]
        latest=res[0]
        return f"rows={len(res)} latest_date={latest.get('processed_date')} 5d_sum={sum(flows[:5]) if flows else None} nav={latest.get('nav')} sh_out={latest.get('shares_outstanding')}"
    except Exception as e: return f"ERR {str(e)[:50]}"
for tk in ["EIDO","EWA","EWT","INDA","FXI","EWJ","EWZ","EWW","EWI","EWN","AAXJ","INDY"]:
    print(f"  {tk:<6} {ff(tk)}")
# check etf-true-flows freshness + coverage of these
import boto3
s3=boto3.client("s3","us-east-1")
try:
    tf=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/etf-true-flows.json")["Body"].read())
    be=tf.get("by_etf") or {}
    print("\netf-true-flows generated:",tf.get("generated_at"),"n_etf:",len(be))
    for tk in ["INDA","EWT","EWI","EWN"]:
        print(f"   {tk} in true-flows:", json.dumps(be.get(tk,{}))[:120])
except Exception as e: print("etf-true-flows:",str(e)[:60])
print("DONE 2178")
