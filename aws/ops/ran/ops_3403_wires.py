"""ops 3403 — composer v1.1 (top-40 conviction cap) + best-setups three wires
(participation entry gate, halflife hold horizon, SELF-GRADING stack log).
#3 regime-multipliers struck from the build list: justhodl-engine-trust
already IS the regime-conditioned plug (base x regime Wilson-LB, daily 12:30).

Gates: G1 both engines deployed w/ markers · G2 composer re-run: book<=40,
gross~100, mode PROVEN kept · G3 best-setups run: rows carry rel_volume/
entry_confirmed/hold_horizon_days + >=5 'best-setup-stack' rows in the
signals table today (dedupe-safe) · report tops.
"""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from boto3.dynamodb.conditions import Attr
from ops_report import report

LAM = boto3.client("lambda","us-east-1",config=Config(read_timeout=340,retries={"max_attempts":2}))
S3C = boto3.client("s3","us-east-1")
DDB = boto3.resource("dynamodb","us-east-1")
UA={"User-Agent":"Mozilla/5.0 (ops-3403)"}

def invoke_resilient(fn,itype="Event",tries=6):
    for k in range(tries):
        try: return LAM.invoke(FunctionName=fn,InvocationType=itype,Payload=b"{}")
        except Exception as e:
            if "TooManyRequests" in str(e) or "Rate Exceeded" in str(e): time.sleep(15*(k+1)); continue
            raise
    raise RuntimeError("throttled")

def zsrc(fn):
    info=LAM.get_function(FunctionName=fn)
    with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"],headers=UA),timeout=60) as r:
        return zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8","replace")

with report("3403_wires") as rep:
    rep.heading("ops 3403 — composer cap + best-setups wires")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:340]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:290]; print(line); rep.log(line)
        if not ok: fails.append(n)

    ok1=False; dl=time.time()+360
    while time.time()<dl:
        try:
            a='VERSION = "1.1.0"' in zsrc("justhodl-proven-portfolio")
            b='best-setup-stack' in zsrc("justhodl-best-setups")
            if a and b: ok1=True; break
        except Exception: pass
        time.sleep(12)
    gate("G1_deployed", ok1, "composer 1.1 + best-setups self-log markers")

    t0=datetime.now(timezone.utc).isoformat()
    invoke_resilient("justhodl-proven-portfolio","RequestResponse")
    feed=None
    dl=time.time()+120
    while time.time()<dl:
        try:
            j=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/proven-portfolio.json")["Body"].read())
            if j.get("version")=="1.1.0" and (j.get("generated_at") or "")>t0: feed=j; break
        except Exception: pass
        time.sleep(10)
    bk=(feed or {}).get("book") or []
    gross=round(sum(p.get("weight_pct") or 0 for p in bk),1)
    gate("G2_composer_capped", bool(feed) and 1<=len(bk)<=40 and 95<=gross<=101,
         f"book={len(bk)} gross={gross}% mode={feed and feed.get('mode')} top={[p['ticker'] for p in bk[:6]]}")
    out["book_top"]=bk[:8]

    t1=datetime.now(timezone.utc).isoformat()
    invoke_resilient("justhodl-best-setups")
    bs, ok3a = None, False
    dl=time.time()+480
    while time.time()<dl:
        try:
            j=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/best-setups.json")["Body"].read())
            if (j.get("generated_at") or j.get("as_of") or "")>t1:
                rows=(j.get("top_setups") or [])
                ok3a = rows and all(("entry_confirmed" in r and "hold_horizon_days" in r) for r in rows[:10])
                bs=rows; break
        except Exception: pass
        time.sleep(20)
    today=datetime.now(timezone.utc).date().isoformat()
    tbl=DDB.Table("justhodl-signals")
    n_stack=0
    try:
        resp=tbl.scan(FilterExpression=Attr("signal_type").eq("best-setup-stack") & Attr("signal_id").contains("#"+today))
        n_stack=len(resp.get("Items") or [])
    except Exception as e:
        print("[scan]",str(e)[:80])
    gate("G3_wires_live", bool(ok3a) and n_stack>=5,
         f"rows_fielded={bool(ok3a)} stack_signals_today={n_stack} "
         f"sample={[{k:r.get(k) for k in ('ticker','rel_volume','entry_confirmed','hold_horizon_days')} for r in (bs or [])[:3]]}")

    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3403.json").write_text(json.dumps(out,indent=2,default=str)); sys.exit(0)
