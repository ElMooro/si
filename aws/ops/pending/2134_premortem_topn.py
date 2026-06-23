import boto3
lam=boto3.client("lambda","us-east-1")
cur=lam.get_function_configuration(FunctionName="justhodl-premortem-engine").get("Environment",{}).get("Variables",{})
cur["TOP_N"]="6"
lam.update_function_configuration(FunctionName="justhodl-premortem-engine",Environment={"Variables":cur})
print("premortem TOP_N env -> 6 (fewer GLM calls/run => reliably completes within timeout, fewer 429s)")
print("DONE 2134")
