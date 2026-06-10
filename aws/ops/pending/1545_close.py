# ops 1545 — deploy S3-case fix + credit splice, final end-to-end verify of deep analogs + deep alert backtests
import json, os, time, zipfile, io, boto3
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3c = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1545, "errors": []}


def retry_conflict(fn, tries=10, wait=8):
    for i in range(tries):
        try:
            return fn()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
                time.sleep(wait); continue
            raise
    raise RuntimeError("retries exhausted")


def deploy(fn, src):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for r, _, fs in os.walk(src):
            for f in fs:
                if "__pycache__" not in r and not f.endswith(".pyc"):
                    zf.write(os.path.join(r, f), arcname=os.path.relpath(os.path.join(r, f), src))
    retry_conflict(lambda: lam.update_function_code(FunctionName=fn, ZipFile=buf.getvalue()))
    for _ in range(40):
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") in ("Successful", None):
            return
        time.sleep(3)

deploy("justhodl-historical-analogs", "aws/lambdas/justhodl-historical-analogs/source")
deploy("justhodl-alert-backtester", "aws/lambdas/justhodl-alert-backtester/source")

r = retry_conflict(lambda: lam.invoke(FunctionName="justhodl-historical-analogs", InvocationType="RequestResponse", Payload=b"{}"))
out["analogs_fn_error"] = r.get("FunctionError", "NONE")
time.sleep(2)
a = json.loads(s3c.get_object(Bucket=B, Key="data/historical-analogs.json")["Body"].read())
yrs = sorted({x["date"][:4] for x in a.get("analogs", [])})
out["analogs"] = {"version": a.get("version"), "n_pool": a.get("n_historical_dates_evaluated"),
                  "analog_years": yrs, "duration_s": a.get("duration_s"), "call": a.get("directional_call"),
                  "top8": [(x["date"], x["distance"], x.get("forward_21d_pct"), x.get("forward_63d_pct")) for x in a.get("analogs", [])[:8]],
                  "fwd_21d": (a.get("forward_distribution") or {}).get("21d"),
                  "fwd_63d": (a.get("forward_distribution") or {}).get("63d"),
                  "today_spx": json.loads(a["today"]).get("spx_close") if isinstance(a.get("today"), str) else (a.get("today") or {}).get("spx_close")}

r2 = retry_conflict(lambda: lam.invoke(FunctionName="justhodl-alert-backtester", InvocationType="RequestResponse", Payload=b"{}"))
out["bt_fn_error"] = r2.get("FunctionError", "NONE")
time.sleep(2)
bt = json.loads(s3c.get_object(Bucket=B, Key="data/alert-backtests.json")["Body"].read())
out["bt_span"] = bt.get("spy_span") or bt.get("spx_span") or bt.get("price_span")
out["bt_rows"] = [{"id": x["id"], "n": x.get("n_fires"), "last": x.get("last_fired"),
                   "n21": ((x.get("forward_spy") or {}).get("21d") or {}).get("n"),
                   "med21": ((x.get("forward_spy") or {}).get("21d") or {}).get("median_pct"),
                   "med63": ((x.get("forward_spy") or {}).get("63d") or {}).get("median_pct"),
                   "neg63": ((x.get("forward_spy") or {}).get("63d") or {}).get("pct_negative")} for x in (bt.get("rules") or [])]
open("aws/ops/reports/1545_close.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({"pool": out["analogs"]["n_pool"], "years": yrs, "spx": out["analogs"]["today_spx"],
                  "bt_span": out["bt_span"]}, default=str))
