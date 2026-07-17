"""ops 3412-3413 — cross-engine joins wave 1: regime stamp at emission
(signals_emit, fleet-wide via shared redeploy) + composer v1.2 (orthogonality
cluster caps 25%, JSI/GSSI gross throttle, earnings_in_window tags, conflict
scan vs short-book with weight halving). Local-repro ALL-ASSERTS pre-push.

Gates: G1 composer v1.2 settled · G2 invoke -> feed.regime + caps/conflict/
earnings fields live on real book · G3 best-setups (redeployed via shared
change) re-run -> today's best-setup-stack rows carry metadata.regime."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from boto3.dynamodb.conditions import Attr
from ops_report import report
LAM=boto3.client("lambda","us-east-1",config=Config(read_timeout=340,retries={"max_attempts":2}))
S3C=boto3.client("s3","us-east-1"); DDB=boto3.resource("dynamodb","us-east-1")
UA={"User-Agent":"Mozilla/5.0 (ops-3413)"}
def inv(fn,it="RequestResponse",tries=6):
    for k in range(tries):
        try: return LAM.invoke(FunctionName=fn,InvocationType=it,Payload=b"{}")
        except Exception as e:
            if "TooManyRequests" in str(e) or "Rate" in str(e): time.sleep(15*(k+1)); continue
            raise
    raise RuntimeError("throttled")
def settled(fn,marker,tmax=360):
    dl=time.time()+tmax
    while time.time()<dl:
        try:
            if LAM.get_function_configuration(FunctionName=fn).get("LastUpdateStatus")=="Successful":
                info=LAM.get_function(FunctionName=fn)
                with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"],headers=UA),timeout=60) as r:
                    if marker in zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8","replace"):
                        return True
        except Exception: pass
        time.sleep(12)
    return False
def zshared(fn, member="signals_emit.py"):
    try:
        info=LAM.get_function(FunctionName=fn)
        with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"],headers=UA),timeout=60) as r:
            return zipfile.ZipFile(io.BytesIO(r.read())).read(member).decode("utf-8","replace")
    except Exception:
        return ""
with report("3413_joins") as rep:
    rep.heading("ops 3413 — joins wave 1")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:340]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:290]; print(line); rep.log(line)
        if not ok: fails.append(n)
    gate("G1_composer_12", settled("justhodl-proven-portfolio",'VERSION = "1.2.0"'), "v1.2 settled")
    t0=datetime.now(timezone.utc).isoformat()
    inv("justhodl-proven-portfolio")
    feed=None; dl=time.time()+120
    while time.time()<dl:
        try:
            j=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/proven-portfolio.json")["Body"].read())
            if j.get("version")=="1.2.0" and (j.get("generated_at") or "")>t0: feed=j; break
        except Exception: pass
        time.sleep(10)
    rg=(feed or {}).get("regime") or {}
    bk=(feed or {}).get("book") or []
    ok2=bool(feed) and "gross_scale" in rg and all("earnings_in_window" in p for p in bk[:10])
    gate("G2_composer_joins", ok2,
         f"regime={json.dumps(rg)[:120]} conflicts={feed and feed.get('n_conflicts')} "
         f"earn_window={feed and feed.get('n_earnings_window')} capped={sum(1 for p in bk if p.get('cluster_capped'))}")
    out["regime"]=rg; out["n_conflicts"]=(feed or {}).get("n_conflicts")
    ok3s = settled("justhodl-best-setups","best-setup-stack") and "_regime_snapshot" in zshared("justhodl-best-setups")
    t1=datetime.now(timezone.utc).isoformat()
    inv("justhodl-best-setups")
    stamped, dl = 0, time.time()+480
    today=datetime.now(timezone.utc).date().isoformat()
    while time.time()<dl and stamped<3:
        try:
            lek=None; stamped=0
            while True:
                kw={"FilterExpression":Attr("signal_type").eq("best-setup-stack")}
                if lek: kw["ExclusiveStartKey"]=lek
                r=DDB.Table("justhodl-signals").scan(**kw)
                for it in r.get("Items",[]):
                    if str(it.get("logged_at",""))[:10]==today and (it.get("metadata") or {}).get("regime"):
                        stamped+=1
                lek=r.get("LastEvaluatedKey")
                if not lek: break
        except Exception as e: print("[scan]",str(e)[:70])
        if stamped<3: time.sleep(25)
    gate("G3_regime_stamped", ok3s and stamped>=3,
         f"shared_bundled={ok3s} stamped_stack_rows_today={stamped}")
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3413.json").write_text(json.dumps(out,indent=2,default=str)); sys.exit(0)
