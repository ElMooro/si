import boto3, io, zipfile, os, json, time
lam=boto3.client("lambda","us-east-1")
fn="justhodl-hyperliquid-perps"
src="aws/lambdas/justhodl-hyperliquid-perps/source/lambda_function.py"
# build zip (engine uses only stdlib+boto3, no shared deps)
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(src,"lambda_function.py")
    # include shared *.py for parity/safety
    sh="aws/shared"
    if os.path.isdir(sh):
        for f in os.listdir(sh):
            if f.endswith(".py"): z.write(os.path.join(sh,f),f)
zb=buf.getvalue()
print("zip bytes:",len(zb))
role="arn:aws:iam::857687956942:role/lambda-execution-role"
try:
    r=lam.create_function(FunctionName=fn,Runtime="python3.12",Role=role,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":zb},Timeout=60,MemorySize=256,
        Description="Hyperliquid perp intelligence — cross-venue OI/funding/premium, leverage regime, liq-pressure proxy.",
        TracingConfig={"Mode":"Active"},
        DeadLetterConfig={"TargetArn":"arn:aws:sqs:us-east-1:857687956942:justhodl-dlq-default"})
    print("CREATED:",r["FunctionArn"],"state:",r.get("State"))
except Exception as e:
    print("create with DLQ+XRay FAILED:",type(e).__name__,str(e)[:160])
    # retry minimal (no DLQ/tracing) to isolate
    try:
        r=lam.create_function(FunctionName=fn,Runtime="python3.12",Role=role,Handler="lambda_function.lambda_handler",
            Code={"ZipFile":zb},Timeout=60,MemorySize=256,Description="Hyperliquid perp intelligence")
        print("CREATED (minimal):",r["FunctionArn"],"state:",r.get("State"))
    except Exception as e2:
        print("minimal create ALSO FAILED:",type(e2).__name__,str(e2)[:200])
print("DONE 2435")
