"""1996 — isolate catalyst 0-earnings: replicate filter vs live feed, force
re-deploy catalyst-calendar via boto3, re-invoke, verify."""
import json, time, io, zipfile, os, boto3
from datetime import date, datetime, timedelta
s3=boto3.client("s3","us-east-1"); lam=boto3.client("lambda","us-east-1")
B="justhodl-dashboard-live"
et=json.loads(s3.get_object(Bucket=B,Key="data/earnings-tracker.json")["Body"].read())
today=date.today(); cutoff=today+timedelta(days=60)
print("today",today,"cutoff",cutoff)

# replicate NEW logic
fc=et.get("forward_calendar") or []; passed=0; drops={}
for u in fc:
    ed=(u.get("date") or "")[:10]
    if not ed: drops["no_date"]=drops.get("no_date",0)+1; continue
    try: d=datetime.strptime(ed,"%Y-%m-%d").date()
    except ValueError: drops["bad_date"]=drops.get("bad_date",0)+1; continue
    if d<today or d>cutoff: drops["out_of_window"]=drops.get("out_of_window",0)+1; continue
    passed+=1
print(f"NEW-logic forward_calendar would pass: {passed}/{len(fc)}  drops={drops}")
up=et.get("upcoming_14d") or []; pu=0
for u in up:
    ed=(u.get("earnings_date","") or "")[:10]
    try: d=datetime.strptime(ed,"%Y-%m-%d").date()
    except: continue
    if today<=d<=cutoff: pu+=1
print(f"OLD-logic upcoming_14d would pass: {pu}/{len(up)}  dates={[ (u.get('earnings_date') or '')[:10] for u in up]}")

# check deployed code actually contains forward_calendar
import urllib.request
loc=lam.get_function(FunctionName="justhodl-catalyst-calendar")
url=loc["Code"]["Location"]
z=urllib.request.urlopen(url,timeout=30).read()
zf=zipfile.ZipFile(io.BytesIO(z))
src=zf.read([n for n in zf.namelist() if n.endswith("lambda_function.py")][0]).decode()
print("DEPLOYED code has forward_calendar:", "forward_calendar" in src,
      "| Benzinga(via Massive):", "Benzinga (via Massive)" in src,
      "| code bytes:",len(src))

# force re-deploy from repo source dir
SRCDIR="aws/lambdas/justhodl-catalyst-calendar/source"
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z2:
    for root,_,files in os.walk(SRCDIR):
        for f in files:
            if f.endswith(".py"):
                p=os.path.join(root,f); z2.write(p,os.path.relpath(p,SRCDIR))
buf.seek(0)
lam.update_function_code(FunctionName="justhodl-catalyst-calendar",ZipFile=buf.read())
# wait active
for _ in range(24):
    c=lam.get_function(FunctionName="justhodl-catalyst-calendar")["Configuration"]
    if c.get("LastUpdateStatus")!="InProgress" and c.get("State")=="Active": break
    time.sleep(5)
print("re-deployed; LastModified:",lam.get_function(FunctionName='justhodl-catalyst-calendar')['Configuration']['LastModified'])

# re-invoke + verify
r=lam.invoke(FunctionName="justhodl-catalyst-calendar",InvocationType="RequestResponse")
pl=json.loads(r["Payload"].read())
print("invoke status:",pl.get("statusCode"))
try:
    body=json.loads(pl.get("body","{}")); print("  handler body by_type:",body.get("by_type"))
except: print("  raw payload:",str(pl)[:300])
time.sleep(2)
cc=json.loads(s3.get_object(Bucket=B,Key="data/catalyst-calendar.json")["Body"].read())
ev=cc.get("events",[]); ern=[e for e in ev if e.get("type")=="EARNINGS"]
bz=[e for e in ern if "Benzinga" in (e.get("source") or "")]
print(f"AFTER redeploy catalyst EARNINGS: {len(ern)} (Benzinga={len(bz)}) by_source={cc.get('by_source')}")
for e in sorted(bz,key=lambda x:-(x.get('importance') or 0))[:6]:
    print(f"   {e.get('date')} {e.get('ticker'):<6} imp={e.get('importance')} {e.get('impact'):<6} {e.get('session') or '-'}")
print("DONE 1996")
