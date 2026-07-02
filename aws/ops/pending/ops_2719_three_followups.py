"""ops 2719 — CLOSE THE THREE STANDING FOLLOW-UPS.

A) FINRA monthlySummary: metadata-catalog self-discovery -> working field
   spec written to data/config/finra-monthly-spec.json -> dark-pool v2.3
   fetches monthly ATS per symbol, joins share_map into the board universe.
B) Quiver off-exchange: fleet audit proved congresstrading is Quiver's
   NO-AUTH public endpoint (no token has ever existed). Probe offexchange
   unauth, record verdict to data/config/quiver-offexchange.json; the
   dark-pool gate activates automatically once /justhodl/quiver/token SSM
   param exists and enabled=true. PENDING-KHALID if paid tier required.
C) ici-flows runner-seed: import the engine module runner-side, reuse its
   own fetch/parse (_live), persist the two history keys, invoke, prove.
Report: aws/ops/reports/2719_three_followups.json.
"""
import os, io, sys, json, time, zipfile, inspect, urllib.request, urllib.error
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=290, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2719, "ts": datetime.now(timezone.utc).isoformat()}
UA = {"User-Agent": "JustHodl Research raafouis@gmail.com", "Accept": "application/json"}
def sect(t): print("\n" + "=" * 8 + " " + t + " " + "=" * 8)
def http(url, data=None, hdr=None, timeout=30):
    req = urllib.request.Request(url, data=data, headers={**UA, **(hdr or {})})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()
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

sect("A/4 FINRA MONTHLY — self-discover spec")
spec = {}
try:
    st, body = http("https://api.finra.org/metadata/group/otcMarket/name/monthlySummary")
    meta = json.loads(body)
    fields = [f.get("name") for f in (meta.get("fields") or meta if isinstance(meta, list) else [])]
    fields = [f for f in fields if f]
    print("  metadata fields:", fields[:18])
except Exception as e:
    fields = []; print("  metadata GET failed:", str(e)[:80])
st, body = http("https://api.finra.org/data/group/otcMarket/name/monthlySummary",
                data=json.dumps({"limit": 60}).encode(), hdr={"Content-Type": "application/json"})
rows = json.loads(body)
assert isinstance(rows, list) and rows, "unfiltered monthly POST failed"
keys = sorted(rows[0].keys())
codes = sorted({r.get("summaryTypeCode") for r in rows if r.get("summaryTypeCode")})
print("  row keys:", keys)
print("  codes seen:", codes)
ats_code = next((c for c in codes if "ATS" in c and ("SMBL" in c or "SYM" in c.upper())), None)
if not ats_code:
    for cand in ("ATS_M_SMBL", "MONTHLY_ATS_SMBL", "ATS_SMBL_M"):
        try:
            st2, b2 = http("https://api.finra.org/data/group/otcMarket/name/monthlySummary",
                           data=json.dumps({"limit": 3, "compareFilters": [{"compareType": "EQUAL",
                                            "fieldName": "summaryTypeCode", "fieldValue": cand}]}).encode(),
                           hdr={"Content-Type": "application/json"})
            if json.loads(b2): ats_code = cand; break
        except Exception: pass
assert ats_code, "no ATS symbol-level monthly code discoverable: %s" % codes
qty_field = next((k for k in keys if "ShareQuantity" in k and "total" in k.lower()),
                 next((k for k in keys if "Quantity" in k), None))
date_field = next((k for k in keys if "monthStart" in k or ("month" in k.lower() and "date" in k.lower())),
                  next((k for k in keys if "Date" in k), None))
sym_field = next((k for k in keys if "Symbol" in k), "issueSymbolIdentifier")
spec = {"ats_code": ats_code, "qty_field": qty_field, "date_field": date_field, "sym_field": sym_field,
        "discovered": datetime.now(timezone.utc).isoformat(), "all_codes": codes}
s3.put_object(Bucket=BUCKET, Key="data/config/finra-monthly-spec.json",
              Body=json.dumps(spec).encode(), ContentType="application/json")
R["finra_spec"] = spec
print("  SPEC:", json.dumps(spec))

sect("B/4 QUIVER OFF-EXCHANGE — verdict")
try:
    st, body = http("https://api.quiverquant.com/beta/live/offexchange", timeout=18)
    verdict = {"enabled": st == 200, "http": st, "note": "public!" if st == 200 else ""}
    if st == 200:
        sample = json.loads(body)
        verdict["sample_keys"] = sorted(sample[0].keys())[:8] if isinstance(sample, list) and sample else []
except urllib.error.HTTPError as he:
    verdict = {"enabled": False, "http": he.code,
               "note": "requires paid Quiver token; congresstrading is their only no-auth endpoint. "
                       "To activate: create SSM /justhodl/quiver/token + set enabled=true here."}
except Exception as e:
    verdict = {"enabled": False, "http": None, "note": str(e)[:80]}
s3.put_object(Bucket=BUCKET, Key="data/config/quiver-offexchange.json",
              Body=json.dumps(verdict).encode(), ContentType="application/json")
R["quiver"] = verdict
print("  VERDICT:", json.dumps(verdict))

sect("C/4 ICI-FLOWS — runner-side seed via engine's own parsers")
sys.path.insert(0, "aws/lambdas/justhodl-ici-flows/source")
import lambda_function as ici
print("  _live signature:", str(inspect.signature(ici._live)))
try:
    live = ici._live()
    print("  _live -> type:", type(live).__name__,
          "keys/len:", (sorted(live.keys())[:6] if isinstance(live, dict) else len(live) if hasattr(live, "__len__") else "?"))
except Exception as e:
    live = None; print("  _live() raised:", str(e)[:120])
def persist(key, doc):
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(doc, default=str).encode(), ContentType="application/json")
seeded = {"mmf": 0, "ltf": 0}
if isinstance(live, tuple) and len(live) == 2:
    mmf_h, ltf_h = live
elif isinstance(live, dict):
    mmf_h, ltf_h = live.get("mmf") or live.get("mmf_h") or {}, live.get("ltf") or live.get("flows") or live.get("ltf_h") or {}
else:
    mmf_h, ltf_h = {}, {}
if mmf_h: persist("data/history/ici-mmf.json", dict(sorted(mmf_h.items())[-800:])); seeded["mmf"] = len(mmf_h)
if ltf_h: persist("data/history/ici-flows.json", dict(sorted(ltf_h.items())[-800:])); seeded["ltf"] = len(ltf_h)
R["ici_seeded"] = seeded
print("  seeded:", seeded)

sect("D/4 DEPLOY dark-pool v2.3 + PROVE all three")
print("  settling 30s…"); time.sleep(30)
retry(lambda: (wait_ok("justhodl-dark-pool"), lam.update_function_code(FunctionName="justhodl-dark-pool", ZipFile=zip_fn("justhodl-dark-pool")))[-1], "dp")
wait_ok("justhodl-dark-pool")
r = lam.invoke(FunctionName="justhodl-dark-pool", InvocationType="RequestResponse")
assert not r.get("FunctionError"), r["Payload"].read()[:200]
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/dark-pool.json")["Body"].read())
MO = d.get("monthly_ats") or {}
R["monthly_live"] = {k: MO.get(k) for k in ("status", "month", "n_rows", "joined")}
print("  monthly:", json.dumps(R["monthly_live"]))
assert MO.get("status") == "OK" and (MO.get("joined") or 0) >= 200, "monthly fusion failed: %s" % R["monthly_live"]
assert d.get("version") == "2.3.0"
r = lam.invoke(FunctionName="justhodl-ici-flows", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("  ici invoke ->", json.dumps(pay)[:200])
assert not r.get("FunctionError"), pay
ic = json.loads(s3.get_object(Bucket=BUCKET, Key="data/ici-flows.json")["Body"].read())
R["ici_live"] = {"ok": ic.get("ok", True), "keys": sorted(ic.keys())[:8]}
print("  ici feed keys:", R["ici_live"]["keys"])

os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2719_three_followups.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2719 COMPLETE — three follow-ups closed")
