"""ops 3415 — joins wave 2: squeeze guard (short-book v1.1), GEX walls +
sector chips + playbook context (best-setups), JSI-pctile robust walker
(composer v1.2.1), feed-registry engine + sentinel newly-stale alerts.
Gates on DATA (doctrine: never log-greps)."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
LAM=boto3.client("lambda","us-east-1",config=Config(read_timeout=340,retries={"max_attempts":2}))
S3C=boto3.client("s3","us-east-1"); SCH=boto3.client("scheduler","us-east-1")
UA={"User-Agent":"Mozilla/5.0 (ops-3415)"}
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
def feed(key):
    try: return json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key=key)["Body"].read())
    except Exception: return {}
with report("3416_wave2_regate") as rep:
    rep.heading("ops 3415 — joins wave 2")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:340]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:290]; print(line); rep.log(line)
        if not ok: fails.append(n)

    gate("G0_all_settled",
         settled("justhodl-short-book",'VERSION = "1.1.0"') and
         settled("justhodl-proven-portfolio",'VERSION = "1.2.2"') and
         settled("justhodl-best-setups","gamma_levels") and
         settled("justhodl-feed-registry",'VERSION = "1.1.0"') and
         settled("justhodl-alert-sentinel","stale_feeds"), "5 engines settled")

    try:
        arn=LAM.get_function_configuration(FunctionName="justhodl-feed-registry")["FunctionArn"]
        try: SCH.get_schedule(Name="justhodl-feed-registry-daily"); cr="exists"
        except Exception:
            SCH.create_schedule(Name="justhodl-feed-registry-daily",ScheduleExpression="cron(30 7 ? * * *)",
                FlexibleTimeWindow={"Mode":"OFF"},
                Target={"Arn":arn,"RoleArn":"arn:aws:iam::857687956942:role/justhodl-scheduler-role","Input":"{}"})
            cr="created"
    except Exception as e: cr=f"FAILED {str(e)[:90]}"
    gate("G1_registry_schedule", cr in ("exists","created"), cr)

    t0=datetime.now(timezone.utc).isoformat()
    inv("justhodl-feed-registry"); time.sleep(4)
    fr=feed("data/feed-registry.json")
    gate("G2_registry", (fr.get("generated_at") or "")>t0 and 150<=fr.get("n_feeds",0)<=3000,
         f"feeds={fr.get('n_feeds')} stale={fr.get('n_stale')} worst={[x['key'] for x in (fr.get('stale') or [])[:4]]}")
    out["stale_top"]=(fr.get("stale") or [])[:8]

    inv("justhodl-short-book")
    sb=None; dl=time.time()+90
    while time.time()<dl:
        sb=feed("data/short-book.json")
        if sb.get("version")=="1.1.0" and (sb.get("generated_at") or "")>t0: break
        time.sleep(8)
    gate("G3_squeeze_guard", bool(sb) and sb.get("version")=="1.1.0" and "squeeze_excluded" in sb,
         f"book={sb.get('n_book')} excluded={sb.get('squeeze_excluded')} risk_tagged={sum(1 for r in (sb.get('book') or []) if r.get('squeeze_risk'))}")

    inv("justhodl-proven-portfolio")
    pp=None; dl=time.time()+120
    while time.time()<dl:
        pp=feed("data/proven-portfolio.json")
        if pp.get("version")=="1.2.2" and (pp.get("generated_at") or "")>t0: break
        time.sleep(10)
    rg=(pp or {}).get("regime") or {}
    gate("G4_jsi_src", rg.get("jsi_src") not in (None,"default"),
         f"jsi_pctile={rg.get('jsi_pctile')} src={rg.get('jsi_src')} scale={rg.get('gross_scale')}")

    t1=datetime.now(timezone.utc).isoformat()
    inv("justhodl-best-setups")
    bs=None; dl=time.time()+480
    while time.time()<dl:
        bs=feed("data/best-setups.json")
        if (bs.get("generated_at") or bs.get("as_of") or "")>t1: break
        time.sleep(20)
    rows=(bs or {}).get("top_setups") or []
    n_sec=sum(1 for r in rows[:25] if r.get("sector_context"))
    n_wall=sum(1 for r in rows[:25] if r.get("gamma_levels"))
    pbc=(bs or {}).get("playbook_context") or []
    gate("G5_setups_context", n_sec>=8 and len(pbc)>=1 and any(pbc[0].values() if pbc else []),
         f"sector_ctx={n_sec}/25 walls={n_wall}/25 playbook_rules={len(pbc)} sample_pb={json.dumps(pbc[:1])[:120]}")
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3416.json").write_text(json.dumps(out,indent=2,default=str)); sys.exit(0)
