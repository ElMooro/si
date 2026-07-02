"""ops 2739 — brief contract v4: POSITIVE validation (endgame vs leak shapes).

Five leak flavors later (scaffold, markdown, openers, numbered headers,
citation paragraphs), negative filtering is a losing game. v4 accepts LLM text
only if it reads as finished analyst prose: starts with a capital letter,
contains no double-quote characters and no square brackets; anything else ->
deterministic fallback. Redeploy gfd/footprint/analog, rerun, assert v4.
Report: aws/ops/reports/2739_brief_v4.json.
"""
import os, io, json, re, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=290, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2739, "ts": datetime.now(timezone.utc).isoformat()}
def zip_fn(fn):
    src = "aws/lambdas/%s/source" % fn
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src):
            if "__pycache__" in root: continue
            for f in files:
                z.write(os.path.join(root, f), os.path.relpath(os.path.join(root, f), src))
        for f in sorted(os.listdir("aws/shared")):
            if f.endswith(".py"): z.write(os.path.join("aws/shared", f), f)
    return buf.getvalue()
def wait_ok(fn, budget=240):
    t0 = time.time()
    while time.time() - t0 < budget:
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None, "Successful"): return
        time.sleep(5)
def retry(call, what, tries=6):
    for i in range(tries):
        try: return call()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"): time.sleep(18)
            else: raise
    raise RuntimeError(what)

print("settling 30s…"); time.sleep(30)
for fn, key, bkey in (("justhodl-global-flow-desk", "data/global-flow-desk.json", "ai_brief"),
                      ("justhodl-institutional-footprint", "data/institutional-footprint.json", "ai_dossier"),
                      ("justhodl-positioning-analog", "data/positioning-analog.json", "ai_outlook")):
    retry(lambda f=fn: (wait_ok(f), lam.update_function_code(FunctionName=f, ZipFile=zip_fn(f)))[-1], fn)
    wait_ok(fn)
    r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse")
    assert not r.get("FunctionError"), (fn, r["Payload"].read()[:200])
    d = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    br = d.get(bkey) or ""
    R[fn] = {"src": d.get(bkey + "_src"), "len": len(br), "head": br[:170]}
    print("  %s [%s|%d] %s" % (fn.replace("justhodl-", ""), R[fn]["src"], len(br), br[:130]))
    assert 90 <= len(br) <= 760 and br.rstrip().endswith((".", "!", "?"))
    assert br[:1].isalpha() and br[:1].isupper(), "non-prose start: %r" % br[:40]
    assert chr(34) not in br and "[" not in br and "]" not in br, "quote/bracket leak: %r" % br[:80]
    assert "Analyze the Request" not in br and "**" not in br and not re.match(r"^\s*\d+\.", br)
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2739_brief_v4.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2739 COMPLETE — positive gate holds the line")
