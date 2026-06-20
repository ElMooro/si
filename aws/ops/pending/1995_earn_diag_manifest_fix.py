"""1995 — diagnose why earnings-tracker forward_calendar/upcoming feed catalyst
with 0 events; fix RAG manifest entries (proper title in name, keyword keys)."""
import json, time, boto3
s3=boto3.client("s3","us-east-1"); lam=boto3.client("lambda","us-east-1")
B="justhodl-dashboard-live"

print("="*64); print("EARNINGS-TRACKER FEED DIAGNOSIS"); print("="*64)
try:
    et=json.loads(s3.get_object(Bucket=B,Key="data/earnings-tracker.json")["Body"].read())
    print("  as_of:",et.get("as_of") or et.get("generated_at") or et.get("timestamp"))
    print("  top-level keys:",sorted(et.keys()))
    fc=et.get("forward_calendar") or []
    print(f"  forward_calendar: {len(fc)}  n_forward_calendar={et.get('n_forward_calendar')}")
    if fc: print("    sample:",{k:fc[0].get(k) for k in ('ticker','date','session','importance','estimated_eps')})
    for k in ("upcoming_14d","upcoming","calendar","upcoming_earnings"):
        v=et.get(k)
        if isinstance(v,list): print(f"  {k}: {len(v)}"+("  sample keys: "+str(list(v[0].keys())) if v else ""))
except Exception as e:
    print("  read err:",e); et={}

# Check last run of earnings-tracker lambda
try:
    cfg=lam.get_function(FunctionName="justhodl-earnings-tracker")["Configuration"]
    print("  lambda LastModified:",cfg.get("LastModified"),"State:",cfg.get("State"))
except Exception as e: print("  lambda cfg err:",e)

# If forward_calendar empty, re-invoke earnings-tracker to refresh, then re-check
if not (et.get("forward_calendar")):
    print("  forward_calendar empty -> re-invoking earnings-tracker…")
    try:
        r=lam.invoke(FunctionName="justhodl-earnings-tracker",InvocationType="RequestResponse")
        pl=json.loads(r["Payload"].read()); print("    invoke status:",pl.get("statusCode"))
        body=json.loads(pl.get("body","{}")) if isinstance(pl.get("body"),str) else pl
        print("    body keys:",list(body.keys())[:12])
        time.sleep(2)
        et=json.loads(s3.get_object(Bucket=B,Key="data/earnings-tracker.json")["Body"].read())
        fc=et.get("forward_calendar") or []
        print(f"    AFTER re-invoke forward_calendar={len(fc)}")
        if fc: print("      sample:",{k:fc[0].get(k) for k in ('ticker','date','session','importance')})
    except Exception as e:
        print("    re-invoke err:",e)

# Re-run catalyst-calendar after refresh
print("\n  re-invoking catalyst-calendar after earnings refresh…")
try:
    lam.invoke(FunctionName="justhodl-catalyst-calendar",InvocationType="RequestResponse")
    time.sleep(2)
    cc=json.loads(s3.get_object(Bucket=B,Key="data/catalyst-calendar.json")["Body"].read())
    ev=cc.get("events",[]); ern=[e for e in ev if e.get("type")=="EARNINGS"]
    bz=[e for e in ern if "Benzinga" in (e.get("source") or "")]
    print(f"  catalyst EARNINGS now: {len(ern)} (Benzinga={len(bz)})  by_source={cc.get('by_source')}")
    for e in sorted(bz,key=lambda x:-(x.get('importance') or 0))[:5]:
        print(f"    {e.get('date')} {e.get('ticker'):<6} imp={e.get('importance')} {e.get('impact')} {e.get('session') or '-'}")
except Exception as e: print("  catalyst err:",e)

print("\n"+"="*64); print("FIX RAG MANIFEST ENTRIES"); print("="*64)
META={
 "justhodl-analyst-actions":{"title":"Analyst Actions","keys":["analyst","ratings","upgrade","downgrade","guidance","price target","benzinga","rerating"]},
 "justhodl-estimate-revisions":{"title":"Estimate Revisions","keys":["estimate","revision","forward eps","consensus","analyst","growth","dispersion","momentum"]},
 "justhodl-flow-lookthrough":{"title":"Flow Look-Through","keys":["etf","flow","constituent","weight","accumulation","distribution","shares held","rotation","index"]},
 "justhodl-boom-radar":{"title":"Boom Radar","keys":["boom","catalyst","convergence","breakout","squeeze","momentum","multi-signal","explosive"]},
}
try:
    man=json.loads(s3.get_object(Bucket=B,Key="data/engine-manifest.json")["Body"].read())
    cont = man if isinstance(man,dict) and not isinstance(man.get("engines"),(list,dict)) else (man.get("engines") if isinstance(man,dict) else man)
    def fix(entry,eid):
        m=META[eid]
        if "name" in entry: entry["name"]=m["title"]
        if "engine" in entry: entry["engine"]=eid
        if "keys" in entry: entry["keys"]=m["keys"]
        if "on_board" in entry: entry["on_board"]=True
        return entry
    fixed=[]
    if isinstance(cont,dict):
        for eid in META:
            if eid in cont and isinstance(cont[eid],dict): cont[eid]=fix(cont[eid],eid); fixed.append(eid)
    elif isinstance(cont,list):
        for x in cont:
            eid=x.get("engine") or x.get("name")
            if eid in META: fix(x,eid); fixed.append(eid)
    s3.put_object(Bucket=B,Key="data/engine-manifest.json",Body=json.dumps(man,indent=2).encode(),
                  ContentType="application/json",CacheControl="max-age=300")
    print("  fixed entries:",fixed)
    samp=cont["justhodl-boom-radar"] if isinstance(cont,dict) and "justhodl-boom-radar" in cont else None
    if samp: print("  boom-radar entry:",{k:(v[:3] if isinstance(v,list) else v) for k,v in samp.items()})
except Exception as e:
    print("  manifest fix err:",e)
print("DONE 1995")
