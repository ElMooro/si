"""1994 — register 4 new engines in S3 engine-manifest.json (RAG ask-desk),
verify catalyst-calendar now emits Benzinga-authoritative earnings, verify
dossier page carries the new per-ticker card."""
import json, time, urllib.request, boto3
s3=boto3.client("s3","us-east-1"); lam=boto3.client("lambda","us-east-1")
B="justhodl-dashboard-live"

print("="*64); print("ENGINE-MANIFEST MERGE"); print("="*64)
try:
    cur=json.loads(s3.get_object(Bucket=B,Key="data/engine-manifest.json")["Body"].read())
    print("manifest top-level type:",type(cur).__name__)
except Exception as e:
    cur=None; print("manifest read failed:",e)

NEW=[
 {"name":"justhodl-analyst-actions","title":"Analyst Actions","page":"analyst-actions.html",
  "feed":"data/analyst-actions.json","category":"Equity Alpha",
  "desc":"Benzinga analyst ratings, guidance raises/cuts, and price-target changes rolled into a per-ticker net analyst score (guidance>upgrade>PT, importance-weighted)."},
 {"name":"justhodl-estimate-revisions","title":"Estimate Revisions","page":"estimate-revisions.html",
  "feed":"data/estimate-revisions.json","category":"Equity Alpha",
  "desc":"Forward-EPS estimate strength + revision momentum. FMP depth (forward growth, analyst coverage, dispersion) seeded day-1, Benzinga current consensus as timely point, S3 snapshots accrue true revision deltas."},
 {"name":"justhodl-flow-lookthrough","title":"Flow Look-Through","page":"flow-lookthrough.html",
  "feed":"data/flow-lookthrough.json","category":"Equity Alpha",
  "desc":"Single-name net ETF flow pressure from ETF-Global constituent weights + actual shares_held deltas (real ETF buying/selling), with index add/delete reconstitution events."},
 {"name":"justhodl-boom-radar","title":"Boom Radar","page":"boom-radar.html",
  "feed":"data/boom-radar.json","category":"Equity Alpha",
  "desc":"Catalyst-convergence boom detector. Fuses earnings beats, analyst upgrades/guidance, estimate strength, ETF flow accumulation, squeeze regime and breakouts; ranks names by how many independent dimensions agree."},
]

def entry_like(template, e):
    """Build an entry matching template's key schema, best-effort from e."""
    if not isinstance(template,dict): return e
    out={}
    for k in template.keys():
        kl=k.lower()
        if kl in ("name","id","engine","key","slug"): out[k]=e["name"]
        elif kl in ("title","label","display","display_name"): out[k]=e["title"]
        elif kl in ("desc","description","summary","about","what"): out[k]=e["desc"]
        elif kl in ("feed","data","data_file","datafile","source","json","path"): out[k]=e["feed"]
        elif kl in ("page","url","href","link","dashboard"): out[k]="/"+e["page"]
        elif kl in ("category","group","section","tier"): out[k]=e["category"]
        else: out[k]=template.get(k) if not isinstance(template.get(k),(list,dict)) else ""
    # ensure essentials exist even if template lacked them
    out.setdefault("name",e["name"]); out.setdefault("desc",e["desc"])
    return out

added=[]
if isinstance(cur,list):
    existing={json.dumps(x).lower() for x in cur}
    names={(x.get("name") or x.get("id") or x.get("engine") or "").lower() for x in cur if isinstance(x,dict)}
    tmpl=next((x for x in cur if isinstance(x,dict)),None)
    for e in NEW:
        if e["name"].lower() in names: print("  exists:",e["name"]); continue
        cur.append(entry_like(tmpl,e)); added.append(e["name"])
    new_manifest=cur
elif isinstance(cur,dict):
    # could be {engines:[...]} or {name:entry}
    if isinstance(cur.get("engines"),list):
        arr=cur["engines"]; names={(x.get("name") or x.get("id") or "").lower() for x in arr if isinstance(x,dict)}
        tmpl=next((x for x in arr if isinstance(x,dict)),None)
        for e in NEW:
            if e["name"].lower() in names: print("  exists:",e["name"]); continue
            arr.append(entry_like(tmpl,e)); added.append(e["name"])
        new_manifest=cur
    else:
        tmpl=next((v for v in cur.values() if isinstance(v,dict)),None)
        for e in NEW:
            if e["name"] in cur: print("  exists:",e["name"]); continue
            cur[e["name"]]=entry_like(tmpl,e); added.append(e["name"])
        new_manifest=cur
else:
    new_manifest={"engines":NEW}; added=[e["name"] for e in NEW]; print("  manifest absent -> created {engines:[...]}")

if added:
    s3.put_object(Bucket=B,Key="data/engine-manifest.json",
        Body=json.dumps(new_manifest,indent=2).encode(),ContentType="application/json",CacheControl="max-age=300")
    print("  ADDED:",added)
else:
    print("  nothing to add")
# show a sample new entry
samp=NEW[0]["name"]
print("  sample entry schema keys:",
      list((next((x for x in (new_manifest if isinstance(new_manifest,list) else new_manifest.get('engines',[])) if isinstance(x,dict) and (x.get('name')==samp)),{})).keys()) or "n/a")

print("\n"+"="*64); print("VERIFY catalyst-calendar (Benzinga earnings)"); print("="*64)
try:
    r=lam.invoke(FunctionName="justhodl-catalyst-calendar",InvocationType="RequestResponse")
    pl=json.loads(r["Payload"].read()); print("  invoke:",pl.get("statusCode"),json.loads(pl.get("body","{}")).get("by_type",{}))
    time.sleep(2)
    cc=json.loads(s3.get_object(Bucket=B,Key="data/catalyst-calendar.json")["Body"].read())
    ev=cc.get("events",[])
    bz=[e for e in ev if e.get("type")=="EARNINGS" and "Benzinga" in (e.get("source") or "")]
    fmp=[e for e in ev if e.get("type")=="EARNINGS" and e.get("source")=="FMP"]
    print(f"  EARNINGS: {len([e for e in ev if e.get('type')=='EARNINGS'])} total | Benzinga={len(bz)} FMP-supplement={len(fmp)}")
    for e in sorted(bz,key=lambda x:-(x.get('importance') or 0))[:5]:
        print(f"    {e.get('date')} {e.get('ticker'):<6} imp={e.get('importance')} {e.get('impact'):<6} {e.get('session') or '-'} cons={e.get('consensus')}")
    print("  by_source:",cc.get("by_source"))
except Exception as e:
    print("  catalyst verify ERROR:",e)

print("\n"+"="*64); print("VERIFY dossier page live (Pages may lag)"); print("="*64)
try:
    html=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/dossier.html?t="+str(int(time.time())),headers={"User-Agent":"jh"}),timeout=20).read().decode()
    print("  BOOM CONVERGENCE card present:", "BOOM CONVERGENCE" in html)
    print("  analyst-actions feed referenced:", "analyst-actions.json" in html)
except Exception as e:
    print("  dossier fetch:",e)
print("DONE 1994")
