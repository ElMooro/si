import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def doc():
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom-research.json")["Body"].read())
    except Exception as e: return {"_e":str(e)[:40]}
lam.invoke(FunctionName="justhodl-bottleneck-research",InvocationType="Event",Payload=b"{}")
print("regen; polling for industry_ps fields...")
d=None
for i in range(20):
    time.sleep(13); cur=doc(); bt=(cur.get("by_ticker") or {})
    if any(isinstance(v,dict) and (v.get("fwd_val") or {}).get("industry_ps") for v in bt.values()):
        d=cur; print(f"  t+{(i+1)*13}s industry_ps PRESENT"); break
    print(f"  t+{(i+1)*13}s waiting...")
if d:
    bt=d.get("by_ticker") or {}
    for tk in ["NVDA","MU","LDOS","DELL","CEG"]:
        v=bt.get(tk); fv=(v or {}).get("fwd_val") or {}
        if fv:
            print(f"\n{tk}: P/E cur {fv.get('cur_pe')}({fv.get('cur_pe_vs_ind_pct')}%) fwd {fv.get('fwd_pe')}({fv.get('fwd_pe_vs_ind_pct')}%) proj {fv.get('proj_pe')}({fv.get('proj_pe_vs_ind_pct')}%) [ind {fv.get('industry_pe')}]")
            print(f"     P/S cur {fv.get('cur_ps')}({fv.get('cur_ps_vs_ind_pct')}%) fwd {fv.get('fwd_ps')}({fv.get('fwd_ps_vs_ind_pct')}%) proj {fv.get('proj_ps')}({fv.get('proj_ps_vs_ind_pct')}%) [ind P/S {fv.get('industry_ps')}]")
print("DONE 2293")
