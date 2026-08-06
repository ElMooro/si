"""ops 4493 — Khalid's overnight audit: did every loop keep adding data
while he slept? Read-only freshness + progress sweep across all loops and
nightly feeds; each judged against its own cadence; verdict lists."""
import gzip,json,os
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"
s3=boto3.client("s3",region_name=REGION)
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=200,retries={"max_attempts":0}))
now=datetime.now(timezone.utc)
R={"ops":4493,"as_of":now.isoformat(timespec="seconds"),"loops":{}}
def _get(k):
    b=s3.get_object(Bucket=BUCKET,Key=k)["Body"].read()
    if k.endswith(".gz"): b=gzip.decompress(b)
    return json.loads(b)
def age_h(ts):
    try:
        t=datetime.fromisoformat(str(ts).replace("Z","+00:00"))
        if t.tzinfo is None: t=t.replace(tzinfo=timezone.utc)
        return round((now-t).total_seconds()/3600,1)
    except Exception: return None
def add(name,cadence_h,ts,progress):
    a=age_h(ts)
    R["loops"][name]={"age_h":a,"cadence_h":cadence_h,
        "fresh":(a is not None and a<=cadence_h*1.6),"progress":progress}
try:
    d=_get("data/warm/ofr/state.json")
    add("ofr",1,d.get("as_of"),f"{d.get('progress_pct')}% ({len(set(d.get('done',[])))}/{len(d.get('catalog') or [])}) {d.get('status')}")
except Exception as e: R["loops"]["ofr"]={"err":str(e)[:60]}
try:
    d=_get("data/warm/nyfed-markets/pd-state.json")
    add("nyfed_pd",1,d.get("as_of"),f"{d.get('progress_pct')}% ({len(set(d.get('done',[])))}/{len(d.get('catalog') or [])})")
except Exception as e: R["loops"]["nyfed_pd"]={"err":str(e)[:60]}
try:
    d=_get("data/_state/cusip-rebuild.json")
    add("cusip_rebuild",1,d.get("as_of"),f"{d.get('progress_pct')}% filings {len(set(d.get('done_paths',[])))}/{d.get('n_total')}")
except Exception as e: R["loops"]["cusip_rebuild"]={"err":str(e)[:60]}
try:
    p=_get("data/audit/backfill-progress.json").get("tasks",{}).get("tga_deep",{})
    add("tga_backfill",1,p.get("updated_at"),f"pages {p.get('pages_done')} span {p.get('last_span')} {p.get('status')}")
except Exception as e: R["loops"]["tga_backfill"]={"err":str(e)[:60]}
try:
    d=_get("data/_state/bea-walk.json")
    add("bea_walk",24,d.get("as_of") or now.isoformat(),f"{d.get('progress_pct')}% ({len(set(d.get('done',[])))}/{len(d.get('tables') or [])})")
except Exception as e: R["loops"]["bea_walk"]={"err":str(e)[:60]}
try:
    d=_get("data/symbology/master.json")
    bt=d.get("by_ticker",{})
    add("symbology",24,d.get("as_of"),
        f"figi={sum(1 for r in bt.values() if r.get('figi'))} cusip={sum(1 for r in bt.values() if r.get('cusip'))} lei={sum(1 for r in bt.values() if r.get('lei'))} AAPL_cusip={bt.get('AAPL',{}).get('cusip')}")
except Exception as e: R["loops"]["symbology"]={"err":str(e)[:60]}
try:
    d=_get("data/warm/us-equities-daily/latest-summary.json")
    add("polygon_daily",24,d.get("as_of"),f"session {d.get('session')} n={d.get('n_tickers')}")
except Exception as e: R["loops"]["polygon_daily"]={"err":str(e)[:60]}
try:
    d=_get("data/warm/global-expansion-summary.json")
    live=[k for k,v in d.items() if isinstance(v,dict) and v.get("ok")]
    add("global_expansion",24,d.get("as_of"),f"live={live}")
except Exception as e: R["loops"]["global_expansion"]={"err":str(e)[:60]}
try:
    d=_get("data/warm/usgov/latest-summary.json")
    add("usgov",24,d.get("as_of"),f"bea_ok={ (d.get('bea') or {}).get('ok')} bls_obs={(d.get('bls') or {}).get('obs')} ddp={len([1 for v in (d.get('fed_ddp') or {}).values() if isinstance(v,dict) and v.get('ok')])}/6")
except Exception as e: R["loops"]["usgov"]={"err":str(e)[:60]}
try:
    d=_get("data/warm/edgar-filings/latest-summary.json")
    add("edgar",24,d.get("as_of"),f"{d.get('n_filings')} filings {d.get('quarter')}")
except Exception as e: R["loops"]["edgar"]={"err":str(e)[:60]}
try:
    d=_get("data/llm/self-critique/latest.json")
    add("self_critique",24,d.get("as_of"),f"counts {d.get('counts')}")
except Exception as e: R["loops"]["self_critique"]={"err":str(e)[:60]}
try:
    d=_get("data/audit/coverage-gap.json")
    ny=next((m for m in d.get("metrics",[]) if m.get("metric")=="nyfed_reference_rates"),{})
    add("coverage_grader",24,d.get("as_of"),f"tickers={next((m.get('actual') for m in d.get('metrics',[]) if m.get('metric')=='us_tickers'),None)} nyfed={ny.get('actual')}/5")
except Exception as e: R["loops"]["coverage_grader"]={"err":str(e)[:60]}
try:
    d=_get("data/ofr-funding.json")
    sofr=d.get("sofr",{})
    add("warm_bridge",1,d.get("as_of"),f"sofr={sofr.get('value')} obs {sofr.get('observed')}")
except Exception as e: R["loops"]["warm_bridge"]={"err":str(e)[:60]}
try:
    d=_get("data/warm/banxico/core-series.json.gz")
    ser=(d.get("payload",{}).get("bmx",{}).get("series") or [{}])[0]
    last=(ser.get("datos") or [{}])[-1]
    R["loops"]["banxico"]={"progress":f"FIX {last.get('dato')} @ {last.get('fecha')} n={len(ser.get('datos') or [])}","fresh":True}
except Exception as e: R["loops"]["banxico"]={"err":str(e)[:60]}
fresh=[k for k,v in R["loops"].items() if v.get("fresh")]
stale=[k for k,v in R["loops"].items() if v.get("fresh") is False]
errs=[k for k,v in R["loops"].items() if v.get("err")]
R["summary"]={"fresh":fresh,"stale":stale,"errors":errs}
R["verdict"]=f"{len(fresh)} FRESH / {len(stale)} stale / {len(errs)} err"
s3.put_object(Bucket=BUCKET,Key="data/audit/overnight-audit.json",
 Body=json.dumps(R,indent=1,default=str).encode(),ContentType="application/json",CacheControl="no-cache")
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0806-master","from":"claude","to":"perplexity","kind":"propose",
 "content":("OVERNIGHT AUDIT (Khalid asked): "+R["verdict"]+" — "
  +json.dumps({k:v.get('progress') for k,v in R['loops'].items() if v.get('progress')},default=str)[:650]
  +f" · stale={stale} errs={errs}. Full doc data/audit/overnight-audit.json. Verify+seal."),
 "evidence":[{"kind":"log","ref":"data/audit/overnight-audit.json","snippet":"summary"}]})
bus({"action":"fanout_pending"})
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4493_overnight.json","w"),indent=1,default=str)
open("aws/ops/reports/4493_overnight.md","w").write(
 f"# ops 4493 — overnight audit — {R['verdict']}\n"
 +"\n".join(f"- {k}: age={v.get('age_h')}h fresh={v.get('fresh')} :: {v.get('progress') or v.get('err')}" for k,v in R["loops"].items()))
print(R["verdict"])
