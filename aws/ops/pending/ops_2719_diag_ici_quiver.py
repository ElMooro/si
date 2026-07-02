"""ops 2719 — dark-pool monthly latest-month fix + ICI/Quiver truth diagnosis.

Monthly ATS block now sorts newest-first and post-filters to the latest month
(2718 aggregated all months since 2018). ICI: fetch the stats pages raw from
the runner and classify BLOCKED vs PARSE-CHANGED (print status/title/hrefs).
Quiver: last-resort inline-source scan already done repo-side; record verdict.
Report: aws/ops/reports/2719_diag.json.
"""
import os, io, json, time, zipfile, urllib.request, urllib.error, re
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=290, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2719, "ts": datetime.now(timezone.utc).isoformat()}
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

sect("1/3 ICI reachability diagnosis (runner IP)")
for url in ("https://www.ici.org/research/stats", "https://www.ici.org/research/stats/mmf"):
    try:
        rq = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
        with urllib.request.urlopen(rq, timeout=25) as r:
            body = r.read().decode("utf-8", "ignore")
        title = re.search(r"<title>([^<]{0,80})", body)
        hrefs = re.findall(r'href="([^"]*(?:xls|combined|mmf|flows)[^"]*)"', body, re.I)[:8]
        R.setdefault("ici", {})[url] = {"code": 200, "len": len(body),
                                        "title": title.group(1) if title else None, "hrefs": hrefs}
    except urllib.error.HTTPError as he:
        R.setdefault("ici", {})[url] = {"code": he.code}
    except Exception as e:
        R.setdefault("ici", {})[url] = {"err": str(e)[:80]}
    print("  ", url, "->", json.dumps(R["ici"][url], default=str)[:220])

sect("1b/3 ICI re-seed after root fix (runner)")
import sys
sys.path.insert(0, "aws/lambdas/justhodl-ici-flows/source"); sys.path.insert(0, "aws/shared")
try:
    import lambda_function as ici
    res = ici.lambda_handler({}, None)
    R["ici_seed_v2"] = {"ok": True, "result": str(res)[:220]}
except Exception as e:
    R["ici_seed_v2"] = {"ok": False, "err": str(e)[:180]}
print("  ici v2:", json.dumps(R["ici_seed_v2"])[:260])
sys.path = sys.path[2:]; sys.modules.pop("lambda_function", None)
# decisive parse map: every data-file href on the live mmf release page
try:
    rq = urllib.request.Request("https://www.ici.org/research/stats/mmf",
                                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
    with urllib.request.urlopen(rq, timeout=25) as r:
        _b = r.read().decode("utf-8", "ignore")
    _h = [h for h in re.findall(r'href="([^"]+)"', _b) if any(x in h.lower() for x in (".xls", ".csv", "/files/", "download"))]
    R["ici_mmf_data_hrefs"] = _h[:20]
except Exception as e:
    R["ici_mmf_data_hrefs"] = ["ERR " + str(e)[:60]]
print("  mmf data hrefs:", json.dumps(R["ici_mmf_data_hrefs"])[:420])

sect("2/3 DEPLOY dark-pool monthly fix + verify latest-month")
print("  settling 30s…"); time.sleep(30)
for _fn in ("justhodl-dark-pool", "justhodl-ici-flows"):
    retry(lambda f=_fn: (wait_ok(f), lam.update_function_code(FunctionName=f, ZipFile=zip_fn(f)))[-1], _fn)
    wait_ok(_fn)
wait_ok("justhodl-dark-pool")
r = lam.invoke(FunctionName="justhodl-dark-pool", InvocationType="RequestResponse")
assert not r.get("FunctionError"), r["Payload"].read()[:300]
mo = json.loads(s3.get_object(Bucket=BUCKET, Key="data/dark-pool.json")["Body"].read()).get("monthly_ats") or {}
R["finra_monthly_v2"] = {k: mo.get(k) for k in ("status", "n_symbols", "month")}
R["finra_top_blocks"] = (mo.get("top_block_share") or [])[:8]
print("  monthly v2:", json.dumps(R["finra_monthly_v2"], default=str))
print("  top blocks:", json.dumps(R["finra_top_blocks"], default=str)[:220])
print("  monthly err:", mo.get("err"))
assert mo.get("status") == "OK", "monthly failed: %s" % mo
assert str(mo.get("month") or "") >= "2026-01", "stale month: %s" % mo.get("month")

sect("3/3 REPORT")
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2719_diag.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2719 COMPLETE")

# rev2

# rev3
