# ops 1650 — debate v2.0.2 via ASYNC invoke + S3 poll (GH kills sync waits >350s);
#            ask-desk URL with 6-char-max CORS members; tests; config feed
import json, zipfile, io, os, time, secrets
from datetime import datetime, timezone
import boto3
from botocore.config import Config
cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1650}
def zipdir(d):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, fs in os.walk(d):
            for f in fs:
                fp = os.path.join(root, f)
                z.write(fp, os.path.relpath(fp, d))
    return buf.getvalue()
def upd(fn, zb):
    for _ in range(6):
        try:
            lam.update_function_code(FunctionName=fn, ZipFile=zb); break
        except Exception as e:
            if "ResourceConflict" in str(e): time.sleep(8)
            else: raise
    for _ in range(50):
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") != "InProgress" and c.get("State") != "Pending":
            return
        time.sleep(3)
# A) debate repair run: deploy already-pushed v2.0.2, ASYNC invoke, poll output feed
upd("justhodl-research-papers", zipdir("aws/lambdas/justhodl-research-papers/source"))
t_mark = datetime.now(timezone.utc).isoformat()
lam.invoke(FunctionName="justhodl-research-papers", InvocationType="Event", Payload=b"{}")
res = None
for i in range(14):                      # up to ~7 min of short polls
    time.sleep(30)
    try:
        idx = json.loads(s3.get_object(Bucket=B, Key="data/research-papers.json")["Body"].read())
        if (idx.get("generated_at") or "") > t_mark:
            res = idx
            break
    except Exception:
        pass
if res:
    out["debate"] = {"written": res.get("written_this_run"), "diag": res.get("diagnostics"),
                      "index_head": [{k: p_.get(k) for k in ("t", "conviction", "stance")}
                                       for p_ in (res.get("papers") or [])[:5]]}
    ws = res.get("written_this_run") or []
    if ws:
        pp = json.loads(s3.get_object(Bucket=B, Key=f"data/research/{ws[0]}.json")["Body"].read())
        pa = pp.get("paper") or {}
        out["debate"]["sample"] = {"t": ws[0], "conv": pa.get("conviction_1_10"),
                                     "stance": pa.get("position_stance"),
                                     "models": (pa.get("debate") or {}).get("models")}
else:
    out["debate"] = {"timeout": "no fresh index within poll window"}
# B) ask-desk: create/update fn + URL (CORS members <=6 chars) + config + tests
FN = "justhodl-ask-desk"
zb = zipdir("aws/lambdas/justhodl-ask-desk/source")
role = lam.get_function_configuration(FunctionName="justhodl-historical-analogs")["Role"]
ai_env = (lam.get_function_configuration(FunctionName="justhodl-ai-chat").get("Environment") or {}).get("Variables") or {}
dk = secrets.token_hex(12)
try:
    lam.create_function(FunctionName=FN, Runtime="python3.12", Role=role,
                         Handler="lambda_function.lambda_handler", Code={"ZipFile": zb},
                         Timeout=180, MemorySize=512,
                         Environment={"Variables": {"ANTHROPIC_API_KEY": ai_env.get("ANTHROPIC_API_KEY", ""),
                                                       "DESK_KEY": dk}})
    out["fn"] = "created"
except Exception as e:
    if "exist" in str(e) or "ResourceConflict" in str(e):
        upd(FN, zb)
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
        Cors={"AllowOrigins": ["*"], "AllowMethods": ["POST"],
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
open("aws/ops/reports/1650_async_fix.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"debate": (out["debate"].get("written") if isinstance(out.get("debate"), dict) else None),
                   "url_ok": bool(url), "t1_cited": out["tests"][0]["cited"]}, default=str))
