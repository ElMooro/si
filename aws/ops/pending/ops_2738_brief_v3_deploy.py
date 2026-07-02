"""ops 2738 — brief contract v3 DEPLOY (2737 was a hollow green: git-show
lineage copy ran after the source script's auto-removal -> 0-byte script).

Deploys the canonical _clean_brief v3 (scaffold phrases + markdown +
deliberation openers + leading numbered/colon/short header lines, sentence-
complete <=700c, deterministic fallback) to gfd, footprint, and analog;
reruns all three; asserts every brief opener-free and header-free.
Report: aws/ops/reports/2738_brief_v3.json.
"""
import os, io, json, re, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=290, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2738, "ts": datetime.now(timezone.utc).isoformat()}
BAD = ("Wait", "Hmm", "Actually", "Okay", "OK", "So", "Let", "First", "Alright", "Now", "Note")
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
    first = br.split(" ", 1)[0].rstrip(",.")
    firstline = br.split("\n")[0].rstrip()
    R[fn] = {"src": d.get(bkey + "_src"), "len": len(br), "first_word": first, "head": br[:170]}
    print("  %s [%s|%d] %s" % (fn.replace("justhodl-", ""), R[fn]["src"], len(br), br[:130]))
    assert 90 <= len(br) <= 760 and br.rstrip().endswith((".", "!", "?")), "length/completion"
    assert first not in BAD, "opener leak: %r" % br[:60]
    assert not re.match(r"^\s*\d+\.", br), "numbered-header leak: %r" % br[:60]
    assert not firstline.endswith(":"), "colon-header leak: %r" % firstline[:60]
    assert "Analyze the Request" not in br and "**" not in br, "scaffold leak"
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2738_brief_v3.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2738 COMPLETE — three desks, one clean voice")
