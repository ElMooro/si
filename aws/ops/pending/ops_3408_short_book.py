"""ops 3408 — short & avoid book E2E (audit item #5).
Gates: G1 deploy settled · G2 schedule 21:50 · G3 invoke -> rows>=5, each
>=2 lenses + pair set, ddb 'short-book' rows today >=3 · G4 page live."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from boto3.dynamodb.conditions import Attr
from ops_report import report
LAM=boto3.client("lambda","us-east-1",config=Config(read_timeout=280,retries={"max_attempts":2}))
S3C=boto3.client("s3","us-east-1"); SCH=boto3.client("scheduler","us-east-1")
DDB=boto3.resource("dynamodb","us-east-1")
UA={"User-Agent":"Mozilla/5.0 (ops-3408)"}; FN="justhodl-short-book"
def inv(fn,it="RequestResponse",tries=6):
    for k in range(tries):
        try: return LAM.invoke(FunctionName=fn,InvocationType=it,Payload=b"{}")
        except Exception as e:
            if "TooManyRequests" in str(e) or "Rate" in str(e): time.sleep(15*(k+1)); continue
            raise
    raise RuntimeError("throttled")
with report("3408_short_book") as rep:
    rep.heading("ops 3408 — short & avoid book")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:340]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:290]; print(line); rep.log(line)
        if not ok: fails.append(n)
    ok1=False; dl=time.time()+360
    while time.time()<dl:
        try:
            c=LAM.get_function_configuration(FunctionName=FN)
            if c.get("LastUpdateStatus")=="Successful":
                info=LAM.get_function(FunctionName=FN)
                with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"],headers=UA),timeout=60) as r:
                    if 'VERSION = "1.0.0"' in zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode():
                        ok1=True; break
        except Exception: pass
        time.sleep(12)
    gate("G1_deployed", ok1, "settled + marker")
    created=None
    try:
        arn=LAM.get_function_configuration(FunctionName=FN)["FunctionArn"]
        try: SCH.get_schedule(Name="justhodl-short-book-daily"); created="exists"
        except Exception:
            SCH.create_schedule(Name="justhodl-short-book-daily",ScheduleExpression="cron(50 21 ? * * *)",
                FlexibleTimeWindow={"Mode":"OFF"},
                Target={"Arn":arn,"RoleArn":"arn:aws:iam::857687956942:role/justhodl-scheduler-role","Input":"{}"})
            created="created"
    except Exception as e: created=f"FAILED {str(e)[:100]}"
    gate("G2_schedule", created in ("exists","created"), created)
    r=inv(FN); print("invoke:",r.get("StatusCode"),r.get("FunctionError"))
    feed=None; dl=time.time()+90
    while time.time()<dl:
        try:
            feed=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/short-book.json")["Body"].read()); break
        except Exception: time.sleep(8)
    bk=(feed or {}).get("book") or []
    ok3=len(bk)>=5 and all(len(x.get("lenses") or [])>=2 and x.get("pair_etf") for x in bk)
    today=datetime.now(timezone.utc).date().isoformat()
    n=0
    try:
        resp=DDB.Table("justhodl-signals").scan(FilterExpression=Attr("signal_type").eq("short-book"))
        n=sum(1 for it in resp.get("Items",[]) if str(it.get("logged_at",""))[:10]==today)
    except Exception as e: print("[scan]",str(e)[:80])
    gate("G3_book_graded", ok3 and n>=3,
         f"book={len(bk)} logged_today={n} top={[ (x['ticker'],x['score'],x['pair_etf']) for x in bk[:5]]}")
    out["book_top"]=bk[:8]
    need=["Short &amp; Avoid Book","short-book.json","Bear lenses"]
    ok4,missing=False,need; dl=time.time()+240
    while time.time()<dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(f"https://justhodl.ai/short-book.html?t={int(time.time())}",headers=UA),timeout=25) as r2:
                b=r2.read().decode("utf-8","replace")
            missing=[m for m in need if m not in b]
            if not missing: ok4=True; break
        except Exception: pass
        time.sleep(12)
    gate("G4_page_live", ok4, f"missing={missing}")
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3408.json").write_text(json.dumps(out,indent=2,default=str)); sys.exit(0)
