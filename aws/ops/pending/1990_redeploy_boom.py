import boto3, json, io, zipfile, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
FN="justhodl-boom-radar"
b=io.BytesIO()
with zipfile.ZipFile(b,"w",zipfile.ZIP_DEFLATED) as z:
    zi=zipfile.ZipInfo("lambda_function.py"); zi.external_attr=0o644<<16
    z.writestr(zi,open(f"aws/lambdas/{FN}/source/lambda_function.py","rb").read())
for _ in range(24):
    try: lam.update_function_code(FunctionName=FN,ZipFile=b.getvalue()); break
    except lam.exceptions.ResourceConflictException: time.sleep(5)
for _ in range(30):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("State")=="Active" and c.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("invoke:",r.get("StatusCode"),r.get("FunctionError"))
print(r["Payload"].read()[:600])
time.sleep(2)
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/boom-radar.json")["Body"].read())
print("\ndims:",j.get("dimensions_loaded"),"| squeeze_regime:",j.get("squeeze_regime"))
print(f"scanned={j['n_scanned']} 2way={j['n_2way']} 3way={j['n_3way']} 4way+={j['n_4way_plus']}")
print("\n★ HIGH-CONVICTION (>=3 independent signals):")
for c in j.get("high_conviction",[])[:14]:
    print(f"  {c['ticker']:<6} conv={c['convergence']} score={c['boom_score']} [{', '.join(c['dimensions'])}]")
    for rs in c['reasons'][:5]: print(f"        - {rs}")
print("\n★ Strong 2-way sample:")
for c in [x for x in j.get('boom_candidates',[]) if x['convergence']==2][:8]:
    print(f"  {c['ticker']:<6} score={c['boom_score']} [{', '.join(c['dimensions'])}]")
print("\n★ TOP PICKS → harvester:",[(p['ticker'],p['convergence'],p['score']) for p in j.get('top_picks',[])])
print("DONE 1990")
