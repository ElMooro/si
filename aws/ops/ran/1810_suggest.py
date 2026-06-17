import json, boto3
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
LBL=["symbol","ticker","pair","t","name","label","category","signal_type","sector","key","id","cohort","theme"]
VAL_PRI=["compound_score","flow_score","score","overall_accuracy","accuracy","z_score","zscore","delta","fwd_3m_pct","dist_hi_pct","carry_pct","net_flow_5d_usd","upside_pct","value","weight"]
def num(v):
    try:
        if isinstance(v,bool):return None
        return float(v)
    except: return None
def dig(o,p):
    for k in p.split("."):
        o=o.get(k) if isinstance(o,dict) else None
        if o is None:return None
    return o
def series_paths(o):
    out=[]
    def walk(x,pre):
        if isinstance(x,dict):
            for k,v in x.items():
                if isinstance(v,list) and v:
                    e=v[0]
                    if isinstance(e,list) and len(e)>=2 and num(e[1]) is not None and isinstance(e[0],str): out.append(pre+k)
                    elif isinstance(e,dict) and any(d in e for d in ("date","asofdate","t","period","x")) and any(num(e[c]) is not None for c in e if c not in ("date","asofdate","t","period","x")): out.append(pre+k)
                elif isinstance(v,dict) and pre.count(".")<1: walk(v,pre+k+".")
    walk(o,"")
    return out
def best_array(o):
    best=None;bn=0
    def walk(x,pre):
        nonlocal best,bn
        if isinstance(x,dict):
            for k,v in x.items():
                if isinstance(v,list) and v and isinstance(v[0],dict):
                    if len(v)>bn: bn=len(v);best=pre+k
                elif isinstance(v,dict) and pre.count(".")<1: walk(v,pre+k+".")
    walk(o,"")
    return best
for ln in open("/tmp/pf.txt"):
    pg,feed=ln.split(); 
    try:
        o=json.loads(s3.get_object(Bucket=B,Key="data/"+feed)["Body"].read())
    except Exception as e:
        print(f"{pg}|{feed}|ERR|{e.__class__.__name__}"); continue
    sp=series_paths(o)
    if sp:
        print(f"{pg}|{feed}|line|{sp[0]}"); continue
    arr_p=best_array(o)
    if not arr_p: print(f"{pg}|{feed}|none|"); continue
    arr=dig(o,arr_p); e=arr[0]
    lab=next((k for k in LBL if k in e), None) or next((k for k in e if isinstance(e[k],str)), list(e.keys())[0])
    val=next((k for k in VAL_PRI if k in e and num(e[k]) is not None),None)
    if not val: val=next((k for k in e if k!=lab and num(e[k]) is not None and not str(k).lower().endswith(("_usd",))),None) or next((k for k in e if k!=lab and num(e[k]) is not None),None)
    print(f"{pg}|{feed}|bars|{arr_p}:{lab}:{val}|n={len(arr)}")
