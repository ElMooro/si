import boto3, json, io, zipfile, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
FN="justhodl-estimate-revisions"
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    for src,arc in [(f"aws/lambdas/{FN}/source/lambda_function.py","lambda_function.py"),("aws/shared/benzinga.py","benzinga.py")]:
        zi=zipfile.ZipInfo(arc); zi.external_attr=0o644<<16; z.writestr(zi,open(src,"rb").read())
for _ in range(24):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); break
    except lam.exceptions.ResourceConflictException: time.sleep(5)
for _ in range(30):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("State")=="Active" and c.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
# ensure FMP_KEY env present (merge, don't clobber)
cfg=lam.get_function_configuration(FunctionName=FN)
env=cfg.get("Environment",{}).get("Variables",{}) or {}
if env.get("FMP_KEY")!="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb":
    env["FMP_KEY"]="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
    lam.update_function_configuration(FunctionName=FN,Environment={"Variables":env})
    for _ in range(20):
        c=lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus")!="InProgress": break
        time.sleep(3)
    print("set FMP_KEY env")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("invoke:",r.get("StatusCode"),r.get("FunctionError"),"|",r["Payload"].read()[:300])
time.sleep(2)
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/estimate-revisions.json")["Body"].read())
print("\nv",j["version"],"status:",j["status"],"n_tracked:",j["n_tracked"],"n_fmp_enriched:",j["n_fmp_enriched"])
print("\n★ ESTIMATE-STRENGTH LEADERS (immediate, FMP fwd-growth fused):")
for s in j.get("estimate_strength_leaders",[])[:10]:
    print(f"  {s['ticker']:<6} strength={s['estimate_strength']} fwdEPSgrowth={s.get('fwd_eps_growth_pct')}% nA={s.get('n_analysts')} d2e={s.get('days_to_earnings')} imp{s.get('importance')}")
print("\n★ TOP PICKS (strong+near-earnings → harvester):",[(p['ticker'],p['score']) for p in j.get('top_picks',[])[:10]])
print("DONE 1987")
