"""ops 2855 — inspect SingStat NODX tables + verify peru_copper canary passes with staleness fix."""
import os, json, time, urllib.request, boto3
from datetime import datetime, timezone
R={"ops":2855,"ts":datetime.now(timezone.utc).isoformat()}
def _get(url,t=45):
    req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0","Accept":"application/json"})
    with urllib.request.urlopen(req,timeout=t) as r: return json.loads(r.read().decode("utf-8","ignore"))
for tid in ["M810001","M450981"]:
    try:
        d=_get("https://tablebuilder.singstat.gov.sg/api/table/tabledata/%s?limit=5"%tid)
        data=d.get("Data") or {}
        rows=data.get("row") or []
        R.setdefault("singstat",{})[tid]={"title":(data.get("title") or d.get("title") or "")[:90],
            "n_rows":len(rows),
            "rows":[{"series":(rw.get("rowText") or "")[:46],
                     "last_cols":[(c.get("key"),c.get("value")) for c in (rw.get("columns") or [])[-3:]]} for rw in rows[:5]]}
    except Exception as e: R.setdefault("singstat",{})[tid]={"err":str(e)[:90]}
# verify peru_copper canary
lam=boto3.client("lambda",region_name="us-east-1"); s3=boto3.client("s3",region_name="us-east-1")
try: lam.invoke(FunctionName="justhodl-canary-grid",InvocationType="RequestResponse")["Payload"].read()
except Exception as e: R["canary_inv"]=str(e)[:60]
time.sleep(3)
cg=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/canary-grid.json")["Body"].read())
peru=[s for s in (cg.get("signals") or []) if s.get("key")=="peru_copper"]
R["canary_peru"]=({"available":peru[0].get("available"),"value":peru[0].get("value"),"age_days":peru[0].get("age_days"),"stress":peru[0].get("stress")} if peru else "missing")
R["grid_n"]={"avail":cg.get("n_available"),"total":cg.get("n_total")}
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:2800])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2855_sg_probe.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2855 COMPLETE")
