# ops 1526 — resolve negotiated-wages + wage-tracker SDMX keys; fix ecb-derived config to 240/512
import json, time, zipfile, io, boto3
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=240, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
out = {"ops": 1526}

# ── config fix: assert 240/512 on justhodl-ecb-derived ──
FN = "justhodl-ecb-derived"
for attempt in range(8):
    try:
        c = lam.get_function_configuration(FunctionName=FN)
        if c["Timeout"] >= 240 and c["MemorySize"] >= 512:
            break
        lam.update_function_configuration(FunctionName=FN, Timeout=240, MemorySize=512)
        time.sleep(6)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceConflictException":
            time.sleep(8); continue
        out["config_err"] = str(e)[:100]; break
c = lam.get_function_configuration(FunctionName=FN)
out["config_final"] = {"timeout": c["Timeout"], "memory": c["MemorySize"]}

# ── tmp Lambda: proper Dataflow-id extraction + direct key probes ──
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"; TFN = "tmp-wageprobe"
code = r'''
import json, urllib.request, urllib.error, ssl, re
def lambda_handler(e, c):
    ctx = ssl.create_default_context(); ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
    H = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124 Safari/537.36",
         "Accept": "text/csv;q=0.9, */*;q=0.5"}
    out = {}
    # 1) correct id<->name pairing: anchor regex on the Dataflow element itself
    try:
        r = urllib.request.urlopen(urllib.request.Request(
            "https://data-api.ecb.europa.eu/service/dataflow/ECB", headers=H), timeout=60, context=ctx)
        raw = r.read().decode("utf-8", "replace")
        flows = re.findall(r'Dataflow\s[^>]*id="([A-Z0-9_]{2,12})".*?Name[^>]*>([^<]{3,140})<', raw, re.S)
        out["n_flows"] = len(flows)
        rx = re.compile(r"wage|negotiat|inflation.{0,30}(swap|linked)|ILS", re.I)
        out["wage_flows"] = [{"id": i, "name": n.strip()[:90]} for i, n in flows if rx.search(n)][:10]
    except Exception as ex:
        out["catalog_err"] = str(ex)[:80]

    def get(path, q="?format=csvdata&lastNObservations=2"):
        try:
            r = urllib.request.urlopen(urllib.request.Request(
                "https://data-api.ecb.europa.eu/service/data/" + path + q, headers=H), timeout=40, context=ctx)
            return r.status, r.read().decode("utf-8", "replace").strip().split("\n")
        except urllib.error.HTTPError as ex:
            return ex.code, []
        except Exception as ex:
            return str(ex)[:40], []

    def keyrows(lines):
        if len(lines) < 2: return []
        hdr = lines[0].split(",")
        try: ki, ti, vi = hdr.index("KEY"), hdr.index("TIME_PERIOD"), hdr.index("OBS_VALUE")
        except ValueError: return []
        rows = []
        for l in lines[1:]:
            p = l.split(",")
            if len(p) > max(ki, ti, vi): rows.append((p[ki], p[ti], p[vi]))
        return rows

    # 2) probe each candidate flow wholesale (small flows -> full key dump)
    for fid in ["WTS", "ILM", "NIW", "EWT", "STS"]:
        st, lines = get(fid, "?format=csvdata&lastNObservations=1")
        rows = keyrows(lines)
        out[fid] = {"status": st, "n_keys": len(rows), "sample": rows[:12]}
        if fid == "STS" and rows:
            out[fid]["wage_keys"] = [r for r in rows if "INW" in r[0] or "WAG" in r[0]][:8]
    return {"statusCode": 200, "body": json.dumps(out)}
'''
b2 = io.BytesIO()
with zipfile.ZipFile(b2, "w") as z:
    z.writestr("lambda_function.py", code)
try:
    try:
        lam.delete_function(FunctionName=TFN); time.sleep(2)
    except ClientError:
        pass
    lam.create_function(FunctionName=TFN, Runtime="python3.12", Handler="lambda_function.lambda_handler",
                        Role=ROLE, Code={"ZipFile": b2.getvalue()}, Timeout=180, MemorySize=256)
    for _ in range(20):
        time.sleep(2)
        if lam.get_function_configuration(FunctionName=TFN).get("State") == "Active":
            break
    rr = lam.invoke(FunctionName=TFN, InvocationType="RequestResponse", Payload=b"{}")
    body = json.loads(rr["Payload"].read())
    out["probe"] = json.loads(body.get("body", "{}")) if isinstance(body, dict) else body
except Exception as e:
    out["probe_err"] = str(e)[:150]
finally:
    try:
        lam.delete_function(FunctionName=TFN)
    except Exception:
        pass

open("aws/ops/reports/1526_wage.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({"config": out.get("config_final"), "wage_flows": (out.get("probe") or {}).get("wage_flows")}, default=str))
