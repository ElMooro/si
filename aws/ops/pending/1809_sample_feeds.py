import json, boto3
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
feeds=["regime.json","cross-asset-regime.json","crisis-canaries.json","breadth-thrust.json",
       "carry-surface.json","allocator.json","report.json","calibration-snapshot.json",
       "upside-radar.json","rotation-radar.json","capital-flow.json","us-cycle.json",
       "dislocations.json","deep-value.json","compound-signals.json"]
def shape(v,d=0):
    if isinstance(v,dict):
        ks=list(v.keys())
        return "{"+",".join(ks[:8])+("…" if len(ks)>8 else "")+"}"
    if isinstance(v,list):
        return f"[{len(v)}]"+ (shape(v[0],d+1) if v and d<1 else "")
    return type(v).__name__
def find_series(o,path=""):
    # detect arrays of [date,number] or [{date/x,..}] (chartable history)
    found=[]
    if isinstance(o,dict):
        for k,v in o.items():
            if isinstance(v,list) and v:
                e=v[0]
                if isinstance(e,list) and len(e)>=2 and isinstance(e[0],str) and isinstance(e[1],(int,float)):
                    found.append(f"{path}{k} = [[date,num]]×{len(v)}")
                elif isinstance(e,dict) and any(t in e for t in ("date","Date","t","time","asofdate","period")):
                    found.append(f"{path}{k} = [{{date,…}}]×{len(v)} keys={list(e.keys())[:5]}")
            if isinstance(v,(dict,)) and path=="":
                found+=find_series(v,path=k+".")
    return found
for fd in feeds:
    try:
        o=json.loads(s3.get_object(Bucket=B,Key="data/"+fd)["Body"].read())
        ser=find_series(o)
        print(f"\n{fd}: {shape(o)}")
        if ser:
            for s in ser[:4]: print("   SERIES:",s)
        else:
            # show cross-section arrays
            if isinstance(o,dict):
                for k,v in o.items():
                    if isinstance(v,list) and v and isinstance(v[0],dict):
                        print(f"   ROWS: {k} ×{len(v)} keys={list(v[0].keys())[:6]}")
    except Exception as e: print(f"\n{fd}: ERR {e.__class__.__name__}")
