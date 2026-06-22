import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def g(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:50]}
def probe(k):
    d=g(k)
    if "_err" in d: print(f"\n[{k}] {d['_err']}"); return None
    print(f"\n[{k}] keys: {list(d.keys())[:22]}")
    return d
# scorecard / engine-alpha — where is resilience's forward record?
ea=probe("data/engine-alpha.json")
if ea:
    for ak in ("alpha_proven_signals","engines","signals","by_engine","results"):
        v=ea.get(ak)
        if isinstance(v,list) and v:
            print(f"   .{ak} n={len(v)} item0keys={list(v[0].keys()) if isinstance(v[0],dict) else type(v[0]).__name__}")
            res=[x for x in v if isinstance(x,dict) and 'resil' in str(x.get('engine','')+x.get('signal','')+x.get('name','')).lower()]
            print(f"   resilience rows: {res[:2]}")
        elif isinstance(v,dict) and v:
            print(f"   .{ak} dict keys sample: {list(v.keys())[:8]}")
            print(f"   resilience key present: {[kk for kk in v if 'resil' in kk.lower()][:3]}")
sc=probe("data/signal-scorecard.json")
if sc:
    for ak in ("engines","by_engine","scorecard","rows","results","signals"):
        v=sc.get(ak)
        if isinstance(v,list) and v:
            print(f"   .{ak} n={len(v)} item0keys={list(v[0].keys()) if isinstance(v[0],dict) else ''}")
            res=[x for x in v if isinstance(x,dict) and 'resil' in str(x).lower()][:2]
            print(f"   resilience rows: {res}")
            break
# does harvester have eng:resilience logged? check signal-harvester output or DDB
for k in ["data/signal-board.json","data/best-setups.json"]:
    d=g(k)
    if "_err" not in d:
        s=json.dumps(d)
        print(f"\n[{k}] mentions 'resilience': {'resilience' in s.lower()}")
print("DONE 2101")
