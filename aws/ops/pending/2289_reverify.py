import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
KEY="data/bottleneck-boom-research.json"
def doc():
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=KEY)["Body"].read())
    except Exception as e: return {"_err":str(e)[:50]}
t0=time.time()
lam.invoke(FunctionName="justhodl-bottleneck-research",InvocationType="Event",Payload=b"{}")
print("regen research; polling for updated fwd_val (bull>base ordering)...")
d=None
for i in range(20):
    time.sleep(13); cur=doc()
    cands = cur.get("research") or {k:v for k,v in cur.items() if isinstance(v,dict) and v.get("name")}
    sample=next((v for v in cands.values() if isinstance(v,dict) and v.get("fwd_val") and v["fwd_val"].get("tp_bull")), None) if isinstance(cands,dict) else None
    if sample and sample["fwd_val"]["tp_bull"] > (sample["fwd_val"].get("tp_base") or -1):
        d=cur; print(f"  t+{(i+1)*13}s updated + ordering OK"); break
    print(f"  t+{(i+1)*13}s waiting...")
if d:
    cands = d.get("research") or {k:v for k,v in d.items() if isinstance(v,dict) and v.get("name")}
    for tk in ["LDOS","VST","CEG","MU","DELL"]:
        v=cands.get(tk); fv=(v or {}).get("fwd_val")
        if fv:
            print(f"{tk}: ${v.get('price')} pe{v.get('pe') and round(v['pe'],1)}/ind{v.get('industry_pe')} | g{fv.get('growth_1y_pct')}%({fv.get('growth_source')[:18]}) | fwdPE {fv.get('fwd_pe')}({fv.get('fwd_pe_vs_ind_pct')}%) projPE {fv.get('proj_pe')}({fv.get('proj_pe_vs_ind_pct')}%) | bull ${fv.get('tp_bull')}+{fv.get('tp_bull_upside_pct')}% base ${fv.get('tp_base')}+{fv.get('tp_base_upside_pct')}% bear ${fv.get('tp_bear')}+{fv.get('tp_bear_upside_pct')}%")
print("DONE 2289")
