# ops 1527 — deploy ecb-derived v2.1 (wages block) + verify
import json, os, time, zipfile, io, boto3
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
FN = "justhodl-ecb-derived"
out = {"ops": 1527, "fn": FN}


def retry_conflict(fn, tries=10, wait=8):
    for i in range(tries):
        try:
            return fn()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
                time.sleep(wait); continue
            raise
    raise RuntimeError("retries exhausted")


def settle():
    for _ in range(40):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") in ("Successful", None) and c.get("State") in ("Active", None):
            return c
        time.sleep(3)
    return c


buf = io.BytesIO(); src = "aws/lambdas/justhodl-ecb-derived/source"
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
    for r, _, fs in os.walk(src):
        for f in fs:
            if "__pycache__" in r or f.endswith(".pyc"):
                continue
            zf.write(os.path.join(r, f), arcname=os.path.relpath(os.path.join(r, f), src))
retry_conflict(lambda: lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue()))
c = settle()
out["config"] = {"timeout": c["Timeout"], "memory": c["MemorySize"]}

r = retry_conflict(lambda: lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}"))
out["function_error"] = r.get("FunctionError", "NONE")
out["invoke_response"] = r["Payload"].read().decode()[:200]
time.sleep(3)
try:
    o = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/ecb-derived.json")
    d = json.loads(o["Body"].read())
    out["version"] = d.get("version")
    out["duration_s"] = d.get("duration_s")
    out["n_flashing"] = d.get("n_flashing")
    out["flashing"] = d.get("flashing")
    w = d.get("wages") or {}
    out["wages_block"] = ("ERR: " + w["err"]) if w.get("err") else ("OK" if w.get("negotiated_yoy_official") is not None else "EMPTY")
    out["wages"] = {k: w.get(k) for k in ("negotiated_yoy_official", "official_as_of", "negotiated_1y_ago",
                                          "tracker_fwd_yoy", "tracker_fwd_to", "tracker_ex_oneoffs_yoy",
                                          "tracker_coverage_pct", "read")}
    out["all_blocks"] = {b: ("ERR" if (d.get(b) or {}).get("err") else "OK" if isinstance(d.get(b), dict) else "MISSING")
                         for b in ("rates_curve", "inflation_expectations", "inflation", "wages", "target2", "credit", "fx")}
    out["n_indicators"] = len(d.get("indicators") or {})
except Exception as e:
    out["s3_err"] = str(e)[:120]

open("aws/ops/reports/1527_wages.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({k: out[k] for k in ("version", "wages_block", "n_flashing", "all_blocks") if k in out}, default=str))
