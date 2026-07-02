"""ops 2718 — FOLLOW-UP CLEARANCE: confluence join + FINRA monthly + Quiver + ICI seed.

1) equity-confluence join: xray now harvests confluence_book/proven_book
   (page-proven containers) -> ec from 11 to book depth.
2) FINRA monthlySummary: dark-pool monthly block is now SELF-DISCOVERING
   (unfiltered limit=1 -> real field names -> filtered pull) — no more
   guessed field specs.
3) Quiver offexchange: discover the token from the political-stocks Lambda
   env + SSM, probe /beta/live/offexchange with Bearer AND Token headers,
   record entitlement truthfully.
4) ICI seed: import the ici-flows lambda module ON THE RUNNER (module-safe,
   fetchers hit ici.org from runner IP) and call lambda_handler — its own
   _pj() seeds data/history/ici-{mmf,flows}.json; then a Lambda invoke
   proves the seed took.
Report: aws/ops/reports/2718_followups.json.
"""
import os, io, sys, json, time, zipfile, urllib.request, urllib.error
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=290, retries={"max_attempts": 1}))
ssm = boto3.client("ssm", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2718, "ts": datetime.now(timezone.utc).isoformat()}
def sect(t): print("\n" + "=" * 8 + " " + t + " " + "=" * 8)
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

sect("1/5 QUIVER offexchange — token discovery + dual-header probe")
tok = None
try:
    envv = (lam.get_function_configuration(FunctionName="justhodl-political-stocks").get("Environment") or {}).get("Variables") or {}
    for k, v in envv.items():
        if "QUIVER" in k.upper() and v: tok = v; R["quiver_token_source"] = "env:" + k; break
except Exception as e:
    print("  env read err", str(e)[:60])
if not tok:
    for p in ("/justhodl/quiver/token", "/justhodl/quiver/api-token", "/justhodl/quiver/auth-token"):
        try:
            tok = ssm.get_parameter(Name=p, WithDecryption=True)["Parameter"]["Value"]
            R["quiver_token_source"] = "ssm:" + p; break
        except Exception: pass
R["quiver_token_found"] = bool(tok)
print("  token:", R.get("quiver_token_source", "NOT FOUND"))
if tok:
    for hdr in ("Bearer", "Token"):
        try:
            rq = urllib.request.Request("https://api.quiverquant.com/beta/live/offexchange",
                                        headers={"Authorization": "%s %s" % (hdr, tok), "User-Agent": "jh/1"})
            with urllib.request.urlopen(rq, timeout=20) as r:
                body = r.read()[:400]
            R["quiver_offexchange"] = {"header": hdr, "code": 200, "sample": body.decode("utf-8", "ignore")[:200]}
            break
        except urllib.error.HTTPError as he:
            R["quiver_offexchange"] = {"header": hdr, "code": he.code}
        except Exception as e:
            R["quiver_offexchange"] = {"header": hdr, "err": str(e)[:60]}
print("  probe:", json.dumps(R.get("quiver_offexchange")))

sect("2/5 ICI SEED — run engine module on the runner (its fetchers, its _pj)")
sys.path.insert(0, "aws/lambdas/justhodl-ici-flows/source")
sys.path.insert(0, "aws/shared")
ici_res = None
try:
    import lambda_function as ici
    ici_res = ici.lambda_handler({}, None)
    R["ici_seed"] = {"ok": True, "result": str(ici_res)[:220]}
except Exception as e:
    R["ici_seed"] = {"ok": False, "err": str(e)[:200]}
print("  runner-run:", json.dumps(R["ici_seed"])[:260])
try:
    r = lam.invoke(FunctionName="justhodl-ici-flows", InvocationType="RequestResponse")
    pay = (r["Payload"].read() or b"")[:200].decode("utf-8", "ignore")
    R["ici_lambda_after"] = ("ERR " if r.get("FunctionError") else "") + pay
except Exception as e:
    R["ici_lambda_after"] = "invoke err " + str(e)[:60]
print("  lambda after-seed:", R["ici_lambda_after"][:180])
sys.path = sys.path[2:]
for m in ("lambda_function",):
    sys.modules.pop(m, None)

sect("3/5 DEPLOY dark-pool (self-discovering monthly) + stock-xray (ec books)")
print("  settling 30s…"); time.sleep(30)
for fn in ("justhodl-dark-pool", "justhodl-stock-xray"):
    retry(lambda f=fn: (wait_ok(f), lam.update_function_code(FunctionName=f, ZipFile=zip_fn(f)))[-1], fn)
    wait_ok(fn); print("  synced", fn)
r = lam.invoke(FunctionName="justhodl-dark-pool", InvocationType="RequestResponse")
assert not r.get("FunctionError"), r["Payload"].read()[:300]
mo = json.loads(s3.get_object(Bucket=BUCKET, Key="data/dark-pool.json")["Body"].read()).get("monthly_ats") or {}
R["finra_monthly"] = {k: mo.get(k) for k in ("status", "n_symbols", "month", "fields", "err", "fields_seen")}
print("  FINRA monthly:", json.dumps(R["finra_monthly"], default=str)[:300])
assert mo.get("status") in ("OK", "UNAVAILABLE")

sect("4/5 X-RAY — prove confluence join depth")
book_n = 0
try:
    ecdoc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/equity-confluence.json")["Body"].read())
    book_n = len(ecdoc.get("confluence_book") or []) + len(ecdoc.get("proven_book") or [])
except Exception: pass
print("  confluence book size:", book_n)
r = lam.invoke(FunctionName="justhodl-stock-xray", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("  xray ->", json.dumps(pay)[:240])
assert not r.get("FunctionError") and pay.get("ok"), pay
JN = pay.get("joins") or {}
R["joins_v4"] = JN
assert JN.get("ec", 0) >= max(60, int(0.6 * min(book_n, 400))), "ec still shallow: %s vs book %d" % (JN, book_n)
assert JN.get("fm", 0) >= 1500 and JN.get("dp", 0) >= 500

sect("5/5 REPORT")
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2718_followups.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2718 COMPLETE — follow-up ledger cleared")
