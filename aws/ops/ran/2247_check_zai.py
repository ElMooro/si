import boto3, json
lam=boto3.client("lambda","us-east-1")
e=lam.get_function_configuration(FunctionName="justhodl-equity-research").get("Environment",{}).get("Variables",{})
print("equity-research env keys:", sorted(e.keys()))
print("has ANTHROPIC:", any('ANTHROPIC' in k for k in e), "| has ZAI/Z_AI/GLM:", [k for k in e if any(x in k.upper() for x in ('ZAI','Z_AI','GLM','BIGMODEL'))])
# where do working GLM engines get the key? compare with one
for fn in ["justhodl-premortem-engine","justhodl-crypto-intel","justhodl-altseason"]:
    try:
        ee=lam.get_function_configuration(FunctionName=fn).get("Environment",{}).get("Variables",{})
        zk=[k for k in ee if any(x in k.upper() for x in ('ZAI','Z_AI','GLM','BIGMODEL'))]
        print(f"  {fn}: zai-keys={zk}")
    except Exception as ex: print(f"  {fn}: {str(ex)[:40]}")
print("DONE 2247")
