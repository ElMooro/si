# ops 1649 — create ask-desk fn + Function URL (CORS) + desk key config + live RAG tests
import json, zipfile, io, os, time, secrets
import boto3
from botocore.config import Config
cfg = Config(read_timeout=880, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1649}
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    for root, _, fs in os.walk("aws/lambdas/justhodl-ask-desk/source"):
        for f in fs:
            fp = os.path.join(root, f)
            z.write(fp, os.path.relpath(fp, "aws/lambdas/justhodl-ask-desk/source"))
zb = buf.getvalue()
role = lam.get_function_configuration(FunctionName="justhodl-historical-analogs")["Role"]
ai_env = (lam.get_function_configuration(FunctionName="justhodl-ai-chat").get("Environment") or {}).get("Variables") or {}
dk = secrets.token_hex(12)
env = {"ANTHROPIC_API_KEY": ai_env.get("ANTHROPIC_API_KEY", ""), "DESK_KEY": dk}
FN = "justhodl-ask-desk"
try:
    lam.create_function(FunctionName=FN, Runtime="python3.12", Role=role,
                         Handler="lambda_function.lambda_handler", Code={"ZipFile": zb},
                         Timeout=180, MemorySize=512, Environment={"Variables": env})
    out["fn"] = "created"
except Exception as e:
    if "exist" in str(e) or "ResourceConflict" in str(e):
        for _ in range(6):
            try:
                lam.update_function_code(FunctionName=FN, ZipFile=zb); break
            except Exception as e2:
                if "ResourceConflict" in str(e2): time.sleep(8)
                else: raise
        cur = (lam.get_function_configuration(FunctionName=FN).get("Environment") or {}).get("Variables") or {}
        dk = cur.get("DESK_KEY") or dk
        out["fn"] = "updated"
    else:
        raise
for _ in range(50):
    c = lam.get_function_configuration(FunctionName=FN)
    if c.get("State") != "Pending" and c.get("LastUpdateStatus") != "InProgress":
        break
    time.sleep(3)
try:
    u = lam.create_function_url_config(FunctionName=FN, AuthType="NONE",
        Cors={"AllowOrigins": ["*"], "AllowMethods": ["POST", "OPTIONS"],
               "AllowHeaders": ["content-type", "x-desk-key"], "MaxAge": 3600})
    url = u["FunctionUrl"]
except Exception as e:
    if "exists" in str(e) or "ResourceConflict" in str(e):
        url = lam.get_function_url_config(FunctionName=FN)["FunctionUrl"]
    else:
        raise
try:
    lam.add_permission(FunctionName=FN, StatementId="furl-public",
                        Action="lambda:InvokeFunctionUrl", Principal="*",
                        FunctionUrlAuthType="NONE")
except Exception as e:
    if "ResourceConflict" not in str(e):
        out["perm_warn"] = str(e)[:80]
out["url"] = url
s3.put_object(Bucket=B, Key="data/askdesk-config.json",
              Body=json.dumps({"url": url, "k": dk}).encode(),
              ContentType="application/json", CacheControl="public, max-age=300")
# live RAG tests via direct invoke
tests = ["What is the current crisis composite and which canaries are red?",
          "Which underlooked stock ranks #1 and what stance did its research debate reach?"]
out["tests"] = []
for q in tests:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                    Payload=json.dumps({"test_question": q}).encode())
    body = json.loads(json.loads(r["Payload"].read())["body"])
    out["tests"].append({"q": q, "sources": body.get("sources_used"),
                          "router_why": body.get("router_why"),
                          "duration_s": body.get("duration_s"),
                          "answer_head": (body.get("answer") or "")[:420],
                          "cited": "[data/" in (body.get("answer") or "")})
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1649_ask_desk.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"fn": out["fn"], "url_ok": bool(url),
                   "t1_cited": out["tests"][0]["cited"]}, default=str))
