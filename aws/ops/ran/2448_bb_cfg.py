import boto3
lam=boto3.client("lambda","us-east-1")
c=lam.get_function_configuration(FunctionName="justhodl-bottleneck-boom")
print("runtime:",c.get("Runtime"),"| timeout:",c.get("Timeout"),"| memory:",c.get("MemorySize"))
print("handler:",c.get("Handler"),"| role:",c.get("Role"))
env=(c.get("Environment") or {}).get("Variables") or {}
print("env keys:",sorted(env.keys()))
# also check related engine output keys exist in S3
s3=boto3.client("s3","us-east-1")
for k in ["data/supply-inflection-scanner.json","data/supply-chain-graph.json","data/global-business-cycle.json","data/liquidity-capacity.json","data/metals-miners.json"]:
    try: s3.head_object(Bucket="justhodl-dashboard-live",Key=k); print("EXISTS",k)
    except Exception: print("MISSING",k)
print("DONE 2448")
