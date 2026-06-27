import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
KEY="data/bottleneck-boom-research.json"
def doc():
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=KEY)["Body"].read())
    except Exception as e: return {"_err":str(e)[:60]}
before=doc()
bt = before.get("generated_at") or before.get("as_of") or str(before)[:40]
print("before gen marker:", bt if isinstance(bt,str) else "n/a")
r=lam.invoke(FunctionName="justhodl-bottleneck-research",InvocationType="Event",Payload=b"{}")
print("invoked research engine; polling ~4min for fwd_val...")
d=None
for i in range(20):
    time.sleep(13); cur=doc()
    marker = cur.get("generated_at") or str(cur)[:40]
    # detect fwd_val presence on any candidate
    cands = cur.get("research") or cur.get("by_ticker") or {k:v for k,v in cur.items() if isinstance(v,dict) and v.get("name")}
    has = any(isinstance(v,dict) and v.get("fwd_val") for v in cands.values()) if isinstance(cands,dict) else False
    if has: d=cur; print(f"  t+{(i+1)*13}s fwd_val PRESENT"); break
    print(f"  t+{(i+1)*13}s not yet (cands={len(cands) if isinstance(cands,dict) else '?'})")
if d:
    cands = d.get("research") or d.get("by_ticker") or {k:v for k,v in d.items() if isinstance(v,dict) and v.get("name")}
    for tk in ["VST","LDOS","CEG","MU"]:
        v=cands.get(tk)
        if v and v.get("fwd_val"):
            fv=v["fwd_val"]
            print(f"\n{tk} ({v.get('name','')[:30]}): price=${v.get('price')} pe={v.get('pe')} ind_pe={v.get('industry_pe')}")
            print(f"  growth {fv.get('growth_1y_pct')}% ({fv.get('growth_source')}) backlog {fv.get('backlog_yoy_pct')}%")
            print(f"  fwd P/E {fv.get('fwd_pe')} ({fv.get('fwd_pe_vs_ind_pct')}% vs ind) | fwd P/S {fv.get('fwd_ps')}")
            print(f"  proj P/E {fv.get('proj_pe')} ({fv.get('proj_pe_vs_ind_pct')}% vs ind) | proj P/S {fv.get('proj_ps')}")
            print(f"  targets: bull ${fv.get('tp_bull')} (+{fv.get('tp_bull_upside_pct')}%) base ${fv.get('tp_base')} (+{fv.get('tp_base_upside_pct')}%) bear ${fv.get('tp_bear')}")
            break
else:
    print("fwd_val NOT present in window — engine may still be running theses")
print("DONE 2288")
