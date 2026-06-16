import boto3
lam=boto3.client("lambda",region_name="us-east-1")
cands=["justhodl-research-critique","justhodl-morning-intelligence","justhodl-ai-chat","justhodl-page-ai-commentary","justhodl-ai-brief-router","justhodl-bottleneck-research","justhodl-debate-engine","justhodl-nobrainer-rationale"]
for fn in cands:
    try:
        env=lam.get_function_configuration(FunctionName=fn).get("Environment",{}).get("Variables",{})
        keys=[k for k in env if "ANTHROPIC" in k or "ZAI" in k or k=="ZAI_BASE_URL"]
        print(f"  {fn:34} has: {keys if keys else 'NONE'}")
    except Exception as e: print(f"  {fn:34} {type(e).__name__}")
