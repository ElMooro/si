import boto3, datetime
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); ev=boto3.client("events","us-east-1")
now=datetime.datetime.now(datetime.timezone.utc); B="justhodl-dashboard-live"
KW=["option","gamma","flow","skew","etf","rotation","correlation","momentum","dark","gex","unusual","vol"]
# 1) lambdas matching keywords
print("=== EXISTING LAMBDAS (options/flow/etf/vol/rotation) ===")
fns=[]; pag=lam.get_paginator("list_functions")
for pg in pag.paginate():
    for f in pg["Functions"]:
        n=f["FunctionName"].lower()
        if any(k in n for k in KW): fns.append((f["FunctionName"],f["LastModified"]))
for n,lm in sorted(fns):
    # is it scheduled?
    sched=""
    try:
        rules=ev.list_rule_names_by_target(TargetArn=lam.get_function(FunctionName=n)["Configuration"]["FunctionArn"]).get("RuleNames",[])
        sched=("sched:"+",".join(rules)) if rules else "NO SCHEDULE"
    except Exception: sched="?"
    print("  %-42s mod=%s  %s"%(n,lm[:10],sched))
print("total matched lambdas:",len(fns))
# 2) S3 data outputs matching -> freshness
print("\n=== RELATED S3 DATA OUTPUTS (freshness) ===")
keys=[]; tok=None
while True:
    kw={"Bucket":B,"Prefix":"data/","MaxKeys":1000}
    if tok: kw["ContinuationToken"]=tok
    r=s3.list_objects_v2(**kw)
    for o in r.get("Contents",[]):
        k=o["Key"].lower()
        if any(w in k for w in ["option","gamma","flow","skew","etf","rotation","correl","gex","unusual","dark","vol"]):
            keys.append((o["Key"],o["LastModified"],o["Size"]))
    tok=r.get("NextContinuationToken")
    if not tok: break
for k,lm,sz in sorted(keys):
    age=(now-lm).total_seconds()/86400
    flag="FRESH" if age<3 else ("STALE %dd"%age if age<400 else "DEAD %dd"%age)
    print("  %-44s %6.1fKB  %s"%(k,sz/1024,flag))
print("total matched data files:",len(keys))
