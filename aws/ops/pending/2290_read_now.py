import boto3, json, urllib.request
s3=boto3.client("s3","us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom-research.json")["Body"].read())
print("research gen:", d.get("generated_at"))
cands = d.get("research") or {k:v for k,v in d.items() if isinstance(v,dict) and v.get("name")}
n_fv=sum(1 for v in cands.values() if isinstance(v,dict) and v.get("fwd_val"))
print(f"candidates={len(cands)} with fwd_val={n_fv}")
for tk in ["LDOS","VST","CEG","MU","DELL","NRG"]:
    v=cands.get(tk); fv=(v or {}).get("fwd_val")
    if fv:
        order = "OK" if (fv.get('tp_bull') or 0) >= (fv.get('tp_base') or 0) >= (fv.get('tp_bear') or 0) else "CHECK"
        print(f"{tk}: ${v.get('price')} g{fv.get('growth_1y_pct')}%({fv.get('growth_source')[:20]}) fwdPE {fv.get('fwd_pe')}({fv.get('fwd_pe_vs_ind_pct')}%vs ind) projPE {fv.get('proj_pe')}({fv.get('proj_pe_vs_ind_pct')}%) | bull${fv.get('tp_bull')}/+{fv.get('tp_bull_upside_pct')}% base${fv.get('tp_base')}/+{fv.get('tp_base_upside_pct')}% bear${fv.get('tp_bear')}/+{fv.get('tp_bear_upside_pct')}% [{order}]")
# page live?
def get(u):
    with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh","Cache-Control":"no-cache"}),timeout=20) as r: return r.read().decode("utf-8","replace")
html=get("https://justhodl.ai/bottleneck-boom.html")
print("\nPAGE: fwd_val render present:", "Forward &amp; projected valuation" in html, "| reads fwd_val:", "v.fwd_val" in html)
print("DONE 2290")
