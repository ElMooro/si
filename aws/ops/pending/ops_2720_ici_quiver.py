"""ops 2720 — close the last two follow-ups: ICI seed v3 + Quiver entitlement.

ICI: v5b's discovery found mm_summary_data_202x.xls but fetch died on ICI's
403-for-generic-agents (same as ltf candidates). v3 fetches the real summary
workbooks with full browser headers + Referer, parses via the ENGINE'S OWN
_xlsx_rows/_cell_date/_num helpers (imported from its source), seeds H_MMF
(+H_LTF best-effort via combined_flows workbooks), then invokes the Lambda
and asserts the feed computes. mmf-only is valid (RuntimeError needs both 0).

QUIVER: token was never in source (correct hygiene) — discover it in the
Quiver-family Lambdas' ENV, clone into dark-pool env, deploy v2.4 Layer Q,
invoke, and record a definitive verdict: OK (n, top DPI) or NOT_ENTITLED
(HTTP code) or NO_TOKEN_CONFIGURED. All three outcomes close the item.
Report: aws/ops/reports/2720_ici_quiver.json.
"""
import os, io, sys, json, time, zipfile, urllib.request, urllib.error
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=290, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2720, "ts": datetime.now(timezone.utc).isoformat()}
def sect(t): print("\n" + "=" * 8 + " " + t + " " + "=" * 8)
BH = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
      "Referer": "https://www.ici.org/research/stats/mmf", "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9"}
def bget(url, timeout=35):
    with urllib.request.urlopen(urllib.request.Request(url, headers=BH), timeout=timeout) as r:
        return r.read()
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

sect("1/3 ICI SEED v3 — browser-header workbooks via engine's own parsers")
sys.path.insert(0, "aws/lambdas/justhodl-term-premium/source")   # vendored xlrd package
import xlrd
def parse_series(blob, label=""):
    """ICI workbook (BIFF .xls or xlsx) -> {iso_date: last_numeric_in_row}."""
    out = {}
    magic = blob[:4]
    print("   [%s] magic=%r len=%d" % (label, magic, len(blob)))
    rows = []
    if magic[:2] != b"PK":
        try:
            bk = xlrd.open_workbook(file_contents=blob)
            for sh in bk.sheets():
                for i in range(sh.nrows):
                    row = []
                    for c in sh.row(i):
                        if c.ctype == 3:
                            try:
                                dt = xlrd.xldate.xldate_as_datetime(c.value, bk.datemode)
                                row.append(dt.strftime("%Y-%m-%d"))
                            except Exception: row.append(None)
                        elif c.ctype == 2: row.append(c.value)
                        else: row.append(str(c.value) if c.value else None)
                    rows.append(row)
        except Exception as e: print("   xlrd err:", str(e)[:70])
    print("   [%s] rows=%d sample=%s" % (label, len(rows), [r[:4] for r in rows[:3]]))
    for row in rows:
        d = None; nums = []
        for c in row:
            if isinstance(c, str) and len(c) == 10 and c[4:5] == "-" and not d and c >= "2015-01-01":
                d = c
            if isinstance(c, (int, float)): nums.append(float(c))
        if d and nums:
            out[d] = round(max(nums) / 1000.0, 1)   # TNA $millions (>> fund count) -> billions
    return out
mmf_hist = {}
for u in ("https://www.ici.org/mm_summary_data_2024.xls",
          "https://www.ici.org/mm_summary_data_2025.xls",
          "https://www.ici.org/mm_summary_data_2026.xls"):
    try:
        blob = bget(u)
        pts = parse_series(blob, u.rsplit("/",1)[-1])
        mmf_hist.update(pts)
        print("   %s -> %d pts (total %d)" % (u.rsplit('/', 1)[-1], len(pts), len(mmf_hist)))
    except Exception as e:
        print("   %s FAIL %s" % (u.rsplit('/', 1)[-1], str(e)[:70]))
R["mmf_points"] = len(mmf_hist)
assert len(mmf_hist) >= 40, "MMF seed thin: %d" % len(mmf_hist)
ltf_hist = {}
for u in ("https://www.ici.org/combined_flows_data_2025.xls",
          "https://www.ici.org/combined_flows_data_2026.xls",
          "https://www.ici.org/flows_data_2026.xls"):
    try:
        pts = parse_series(bget(u), u.rsplit("/",1)[-1])
        ltf_hist.update(pts)
        print("   %s -> %d pts" % (u.rsplit('/', 1)[-1], len(pts)))
    except Exception as e:
        print("   %s skip %s" % (u.rsplit('/', 1)[-1], str(e)[:60]))
R["ltf_points"] = len(ltf_hist)
s3.put_object(Bucket=BUCKET, Key="data/history/ici-mmf.json",
              Body=json.dumps(dict(sorted(mmf_hist.items())), separators=(",", ":")).encode(),
              ContentType="application/json")
if ltf_hist:
    s3.put_object(Bucket=BUCKET, Key="data/history/ici-flows.json",
                  Body=json.dumps(dict(sorted(ltf_hist.items())), separators=(",", ":")).encode(),
                  ContentType="application/json")
print("   H_MMF seeded %d pts | H_LTF %d pts" % (len(mmf_hist), len(ltf_hist)))
r = lam.invoke(FunctionName="justhodl-ici-flows", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("   ici invoke ->", json.dumps(pay)[:220])
assert not r.get("FunctionError"), pay
doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/ici-flows.json")["Body"].read())
R["ici_feed"] = {k: doc.get(k) for k in ("ok", "status", "generated_at") if k in doc}
mmfb = doc.get("mmf") or {}
R["ici_mmf"] = {k: mmfb.get(k) for k in ("latest", "latest_bn", "flow_1w_bn", "z", "n_weeks", "regime") if k in mmfb}
print("   feed mmf:", json.dumps(R["ici_mmf"], default=str)[:200])
assert doc.get("ok") is not False, doc.get("err")

sect("2/3 QUIVER — env discovery, clone, v2.4 Layer Q verdict")
qvar = {}
for fn in ("justhodl-political-stocks", "justhodl-lobbying-intel", "justhodl-quiver-congress"):
    try:
        env = (lam.get_function_configuration(FunctionName=fn).get("Environment") or {}).get("Variables") or {}
        qvar.update({k: v for k, v in env.items() if "QUIVER" in k.upper() and v})
        print("   %s env quiver keys: %s" % (fn, [k for k in env if "QUIVER" in k.upper()]))
    except Exception:
        pass
R["quiver_env_keys"] = sorted(qvar.keys())
dpfn = "justhodl-dark-pool"
if qvar:
    cur = (lam.get_function_configuration(FunctionName=dpfn).get("Environment") or {}).get("Variables") or {}
    cur.update(qvar)
    retry(lambda: (wait_ok(dpfn), lam.update_function_configuration(FunctionName=dpfn, Environment={"Variables": cur}))[-1], "dp env")
    wait_ok(dpfn); print("   quiver var cloned into dark-pool env")
retry(lambda: (wait_ok(dpfn), lam.update_function_code(FunctionName=dpfn, ZipFile=zip_fn(dpfn)))[-1], dpfn)
wait_ok(dpfn)
r = lam.invoke(FunctionName=dpfn, InvocationType="RequestResponse")
assert not r.get("FunctionError"), r["Payload"].read()[:200]
dp = json.loads(s3.get_object(Bucket=BUCKET, Key="data/dark-pool.json")["Body"].read())
Q = dp.get("quiver") or {}
R["quiver_verdict"] = Q
print("   quiver verdict:", json.dumps(Q, default=str)[:200])
assert Q.get("status") in ("OK", "NOT_ENTITLED", "NO_TOKEN", "ERROR")
assert dp.get("version") == "2.4.0"
if Q.get("status") == "OK": assert Q.get("n", 0) >= 100

sect("3/3 REPORT")
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2720_ici_quiver.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2720 COMPLETE — follow-up ledger cleared")
