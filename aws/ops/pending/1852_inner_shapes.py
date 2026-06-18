import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def g(k):
    return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
def dump(label,obj):
    print("\n### %s"%label); print(json.dumps(obj, indent=1, default=str)[:900])

tr=g("data/theme-rotation.json")
dump("theme-rotation all_themes[0]", tr["all_themes"][0])
dump("theme-rotation summary.top_10_momentum[0]", (tr.get("summary") or {}).get("top_10_momentum",[{}])[0])
bd=tr.get("breadth_details") or {}
k0=list(bd.keys())[0] if bd else None
if k0:
    print("\n### breadth_details['%s'] keys:"%k0, list(bd[k0].keys()))
    cp=bd[k0].get("constituents_perf") or bd[k0].get("constituents") or []
    dump("breadth_details['%s'] constituent[0]"%k0, cp[0] if cp else "EMPTY")
bl=g("data/beta-laggards.json")
dump("beta-laggards groups[0] (minus laggards list)", {kk:vv for kk,vv in bl["groups"][0].items() if kk!="laggards"})
dump("beta-laggards groups[0].laggards[0]", (bl["groups"][0].get("laggards") or [{}])[0])
dump("beta-laggards top_candidates[0]", bl["top_candidates"][0])
sm=g("data/sympathetic-momentum.json")
dump("sympathetic top_setups[0]", sm["top_setups"][0])
sc=g("data/supply-chain-linkage.json")
e0=sc["entries"][0]
print("\n### supply-chain entries[0] keys:", list(e0.keys()))
dump("supply-chain entries[0].suppliers", e0.get("suppliers"))
dump("supply-chain entries[0].customers", e0.get("customers"))
of=g("data/options-flow.json")
dump("options-flow all_qualifying[0]", of["all_qualifying"][0])
st=g("data/stealth-accumulation.json")
dump("stealth top_smart_money_only[0]", (st.get("top_smart_money_only") or [{}])[0])
dump("stealth top_options_flow_only[0]", (st.get("top_options_flow_only") or ["EMPTY"])[0])
un=g("data/universe.json")
dump("universe cap_buckets", un.get("cap_buckets"))
dump("universe stocks[0]", un["stocks"][0])
