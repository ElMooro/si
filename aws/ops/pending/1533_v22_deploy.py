# ops 1533 — deploy ecb-derived v2.2 + acceptance checks + HICP staleness root-cause + v1 hist ids
import json, os, time, zipfile, io, ssl, urllib.request, boto3
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
FN = "justhodl-ecb-derived"
out = {"ops": 1533}
_ctx = ssl.create_default_context()
HEADERS = {"User-Agent": "Mozilla/5.0 (JustHodl research)", "Accept-Encoding": "gzip"}


def retry_conflict(fn, tries=10, wait=8):
    for i in range(tries):
        try:
            return fn()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
                time.sleep(wait); continue
            raise
    raise RuntimeError("retries exhausted")


buf = io.BytesIO(); src = "aws/lambdas/justhodl-ecb-derived/source"
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
    for r, _, fs in os.walk(src):
        for f in fs:
            if "__pycache__" not in r and not f.endswith(".pyc"):
                zf.write(os.path.join(r, f), arcname=os.path.relpath(os.path.join(r, f), src))
retry_conflict(lambda: lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue()))
for _ in range(40):
    c = lam.get_function_configuration(FunctionName=FN)
    if c.get("LastUpdateStatus") in ("Successful", None):
        break
    time.sleep(3)
r = retry_conflict(lambda: lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}"))
out["function_error"] = r.get("FunctionError", "NONE")
time.sleep(3)

d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/ecb-derived.json")["Body"].read())
out["version"] = d.get("version")
out["duration_s"] = d.get("duration_s")
out["fragmentation"] = d.get("fragmentation")
out["next_gc"] = d.get("next_gc")
out["m3"] = {k: (d.get("credit") or {}).get(k) for k in ("m3_yoy", "m3_6m_chg_pp", "m3_as_of")}
out["pct_ranks_found"] = {b: list((d.get(b) or {}).get("pct_ranks", {}).keys())
                          for b in ("inflation", "wages", "credit", "fx", "target2", "rates_curve", "inflation_expectations")
                          if isinstance(d.get(b), dict) and (d.get(b) or {}).get("pct_ranks")}
out["esi_pct_rank"] = ((d.get("indicators") or {}).get("eurodollar_stress_index") or {}).get("pct_rank")
out["rates_curve_as_of"] = (d.get("rates_curve") or {}).get("as_of")

# acceptance: top-level nulls
out["acceptance_top_nulls"] = [k for k, v in d.items() if v is None]

# esi accumulator file
try:
    e = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/ecb-hist/esi.json")["Body"].read())
    out["esi_hist"] = {"n": e.get("n_points"), "latest": e.get("points", [])[-1] if e.get("points") else None}
except Exception as ex:
    out["esi_hist"] = str(ex)[:80]

# v1 hist ids present
keys = s3.list_objects_v2(Bucket="justhodl-dashboard-live", Prefix="data/ecb-hist/")
out["hist_ids"] = sorted(o["Key"].split("/")[-1].replace(".json", "") for o in keys.get("Contents", []))

# ── HICP root cause ──
def ecb_csv(key, q):
    raw = urllib.request.urlopen(urllib.request.Request(
        "https://data-api.ecb.europa.eu/service/data/" + key + q, headers=HEADERS), timeout=45, context=_ctx).read()
    if raw[:2] == b"\x1f\x8b":
        import gzip; raw = gzip.decompress(raw)
    lines = raw.decode("utf-8", "replace").strip().split("\n")
    hdr = lines[0].split(","); ti = hdr.index("TIME_PERIOD"); vi = hdr.index("OBS_VALUE")
    return [(l.split(",")[ti], l.split(",")[vi]) for l in lines[1:] if len(l.split(",")) > max(ti, vi)]

try:
    fresh = ecb_csv("ICP/M.U2.N.000000.4.ANR", "?format=csvdata&lastNObservations=4")
    out["hicp_probe_ecb_lastN"] = fresh[-4:]
except Exception as e:
    out["hicp_probe_ecb_lastN"] = str(e)[:90]
try:
    raw = urllib.request.urlopen(urllib.request.Request(
        "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/prc_hicp_manr?format=JSON&lang=EN&geo=EA&coicop=CP00&lastTimePeriod=4",
        headers=HEADERS), timeout=45, context=_ctx).read()
    j = json.loads(raw)
    idx = j.get("dimension", {}).get("time", {}).get("category", {}).get("index", {})
    vals = j.get("value", {})
    out["hicp_probe_eurostat"] = sorted((t, vals.get(str(i))) for t, i in idx.items())
except Exception as e:
    out["hicp_probe_eurostat"] = str(e)[:120]

open("aws/ops/reports/1533_v22.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({"version": out["version"], "frag": bool(out.get("fragmentation", {}).get("spreads_bp")),
                  "m3": out["m3"]["m3_yoy"], "gc": (out.get("next_gc") or {}).get("date"),
                  "hicp_ecb": out["hicp_probe_ecb_lastN"][-1:] if isinstance(out["hicp_probe_ecb_lastN"], list) else "ERR",
                  "hicp_eu": out["hicp_probe_eurostat"][-1:] if isinstance(out["hicp_probe_eurostat"], list) else "ERR"}, default=str))
