import boto3, json, zipfile, io, glob, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
FN="justhodl-theme-second-wave"
src=open(glob.glob("**/justhodl-theme-second-wave/source/lambda_function.py",recursive=True)[0]).read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue())
for _ in range(30):
    st=lam.get_function_configuration(FunctionName=FN)
    if st.get("LastUpdateStatus")!="InProgress": break
    time.sleep(2)
lam.update_function_configuration(FunctionName=FN, Timeout=300, MemorySize=512)
for _ in range(30):
    st=lam.get_function_configuration(FunctionName=FN)
    if st.get("LastUpdateStatus")!="InProgress" and st.get("State")=="Active": break
    time.sleep(2)
print("deployed v2 (mem512/to300)")
r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse")
print("INVOKE:", r["Payload"].read().decode()[:300])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/theme-second-wave.json")["Body"].read())
sm=d["summary"]
print("COUNTS:",{k:sm.get(k) for k in ("n_hot_themes","n_infrastructure","n_laggards","n_smallcap_laggards","n_big_orders","n_smallcap_big_orders","n_top_picks")},"fetched=",d["freshness"]["n_universe_returns_fetched"])
# cap distribution across all surfaced names
from collections import Counter
cap=Counter()
for t in d["hot_themes"]:
    for grp in ("laggards","infrastructure","big_orders"):
        for x in t[grp]: cap[x["cap_bucket"] or "?"]+=1
print("CAP DISTRIBUTION (all surfaced):", dict(cap))
print("\nTOP PICKS (small-cap tilted):")
for p in sm["top_picks"][:12]:
    print("  %-6s %-7s buckets=%s r1m=%s%% sigs=%s conv=%s"%(p["symbol"],p["cap_bucket"],p["buckets"],p.get("ret_1m_pct"),p["signals"][:3],p["conviction"]))
print("\nSAMPLE small-cap laggards & big-orders:")
for t in d["hot_themes"][:4]:
    sl=[x for x in t["laggards"] if x["is_small_cap"]][:2]
    sb=[x for x in t["big_orders"] if x["is_small_cap"]][:2]
    if sl or sb:
        print(" THEME %s (%s):"%(t["etf"],t["name"]))
        for x in sl: print("    LAG-small %-6s %-7s gap=%spp r1m=%s%%"%(x["symbol"],x["cap_bucket"],x["gap_vs_theme_pp"],x["ret_1m_pct"]))
        for x in sb: print("    BIG-small %-6s %-7s sigs=%s"%(x["symbol"],x["cap_bucket"],x["signals"]))
