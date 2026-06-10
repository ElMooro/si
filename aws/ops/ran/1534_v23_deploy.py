# ops 1534 — deploy ecb-derived v2.3 (Eurostat EA21 HICP merge) + backfill excess_liquidity/balance_sheet/dfr hist
import json, os, time, zipfile, io, ssl, urllib.request, re, boto3
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
ECB = "https://data-api.ecb.europa.eu/service/data/"
HEADERS = {"User-Agent": "Mozilla/5.0 (JustHodl research)", "Accept-Encoding": "gzip"}
_ctx = ssl.create_default_context()
out = {"ops": 1534, "written": [], "failed": []}


def retry_conflict(fn, tries=10, wait=8):
    for i in range(tries):
        try:
            return fn()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
                time.sleep(wait); continue
            raise
    raise RuntimeError("retries exhausted")


def ecb_csv(key, start=None):
    q = "?format=csvdata" + (f"&startPeriod={start}" if start else "")
    try:
        raw = urllib.request.urlopen(urllib.request.Request(ECB + key + q, headers=HEADERS), timeout=60, context=_ctx).read()
        if raw[:2] == b"\x1f\x8b":
            import gzip; raw = gzip.decompress(raw)
        lines = raw.decode("utf-8", "replace").strip().split("\n")
        hdr = lines[0].split(","); ti = hdr.index("TIME_PERIOD"); vi = hdr.index("OBS_VALUE")
        pts = []
        for ln in lines[1:]:
            c = ln.split(",")
            if len(c) > max(ti, vi) and c[ti].strip() and c[vi].strip():
                try: pts.append([c[ti].strip(), float(c[vi].strip())])
                except ValueError: pass
        pts.sort(); return pts
    except Exception as e:
        out["failed"].append(f"{key}: {str(e)[:70]}"); return []


def write_hist(sid, label, freq, pts, unit=""):
    if not pts:
        out["failed"].append(f"{sid}: empty"); return None
    doc = {"id": sid, "label": label, "freq": freq, "unit": unit, "source": "ECB SDMX",
           "first_date": pts[0][0], "latest_date": pts[-1][0], "n_points": len(pts), "points": pts}
    s3.put_object(Bucket=B, Key=f"data/ecb-hist/{sid}.json", Body=json.dumps(doc).encode(),
                  ContentType="application/json", CacheControl="public, max-age=21600")
    out["written"].append({"id": sid, "n": len(pts), "range": f"{pts[0][0]}→{pts[-1][0]}"})
    return {"id": sid, "label": label, "freq": freq, "first_date": pts[0][0], "latest_date": pts[-1][0], "n_points": len(pts)}

entries = []
ex = ecb_csv("ILM/D.U2.C.EXLIQ.U2.EUR", start="2000-01")
if ex: ex = [[d, round(v / 1000, 1)] for d, v in ex]  # €mn → €bn
e = write_hist("excess_liquidity", "Excess Liquidity (€bn, daily)", "daily", ex, "€bn")
if e: entries.append(e)
bs = ecb_csv("ILM/W.U2.C.A030000.U2.Z06", start="1999-01")
if bs: bs = [[d, round(v / 1000, 1)] for d, v in bs]
e = write_hist("balance_sheet", "Eurosystem Total Assets (€bn, weekly)", "weekly", bs, "€bn")
if e: entries.append(e)
e = write_hist("dfr", "Deposit Facility Rate (%)", "daily", ecb_csv("FM/D.U2.EUR.4F.KR.DFR.LEV", start="1999-01"), "%")
if e: entries.append(e)

man = json.loads(s3.get_object(Bucket=B, Key="data/ecb-hist/_manifest.json")["Body"].read())
have = {x["id"] for x in man["series"]}
for e in entries:
    if e["id"] not in have:
        man["series"].append(e)
man["series"].sort(key=lambda x: x["id"])
s3.put_object(Bucket=B, Key="data/ecb-hist/_manifest.json", Body=json.dumps(man).encode(),
              ContentType="application/json", CacheControl="public, max-age=3600")
out["manifest_n"] = len(man["series"])

# deploy v2.3 + invoke + verify HICP freshness
buf = io.BytesIO(); src = "aws/lambdas/justhodl-ecb-derived/source"
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
    for r, _, fs in os.walk(src):
        for f in fs:
            if "__pycache__" not in r and not f.endswith(".pyc"):
                zf.write(os.path.join(r, f), arcname=os.path.relpath(os.path.join(r, f), src))
retry_conflict(lambda: lam.update_function_code(FunctionName="justhodl-ecb-derived", ZipFile=buf.getvalue()))
for _ in range(40):
    c = lam.get_function_configuration(FunctionName="justhodl-ecb-derived")
    if c.get("LastUpdateStatus") in ("Successful", None): break
    time.sleep(3)
r = retry_conflict(lambda: lam.invoke(FunctionName="justhodl-ecb-derived", InvocationType="RequestResponse", Payload=b"{}"))
out["function_error"] = r.get("FunctionError", "NONE")
time.sleep(3)
d = json.loads(s3.get_object(Bucket=B, Key="data/ecb-derived.json")["Body"].read())
I = d.get("inflation") or {}
out["verify"] = {"version": d.get("version"), "hicp_as_of": I.get("as_of"), "latest_source": I.get("latest_source"),
                 "headline": I.get("headline_yoy"), "core": I.get("core_yoy"), "services": I.get("services_yoy"),
                 "vs_target": I.get("vs_target_pp"), "n_flashing": d.get("n_flashing"), "flashing": d.get("flashing"),
                 "duration_s": d.get("duration_s")}
open("aws/ops/reports/1534_v23.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({"written": [w["id"] for w in out["written"]], "manifest_n": out["manifest_n"], "verify": out["verify"]}, default=str))
