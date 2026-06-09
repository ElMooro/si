# ops 1525 — deploy justhodl-ecb-derived v2.0 (Top-10 gap fill) + verify + dataflow grep
import json, os, time, zipfile, io, boto3
from botocore.config import Config
from botocore.exceptions import ClientError

cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
FN = "justhodl-ecb-derived"
out = {"ops": 1525, "fn": FN}


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


# ── 1. config bump: ~25 HTTP calls now → Timeout≥240, Memory≥512 ──
c = lam.get_function_configuration(FunctionName=FN)
out["config_before"] = {"timeout": c["Timeout"], "memory": c["MemorySize"]}
if c["Timeout"] < 240 or c["MemorySize"] < 512:
    retry_conflict(lambda: lam.update_function_configuration(
        FunctionName=FN, Timeout=max(240, c["Timeout"]), MemorySize=max(512, c["MemorySize"])))
    c = settle()
out["config_after"] = {"timeout": c["Timeout"], "memory": c["MemorySize"]}

# ── 2. zip-update code (deploy-lambdas.yml may race → conflict retry) ──
buf = io.BytesIO(); src = "aws/lambdas/justhodl-ecb-derived/source"
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
    for r, _, fs in os.walk(src):
        for f in fs:
            if "__pycache__" in r or f.endswith(".pyc"):
                continue
            zf.write(os.path.join(r, f), arcname=os.path.relpath(os.path.join(r, f), src))
retry_conflict(lambda: lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue()))
settle()
out["code_updated"] = True

# ── 3. invoke + 4. verify S3 payload has all v2 blocks ──
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
    blocks = {}
    for b in ("rates_curve", "inflation_expectations", "inflation", "target2", "credit", "fx"):
        v = d.get(b)
        if not isinstance(v, dict):
            blocks[b] = "MISSING"
        elif v.get("err"):
            blocks[b] = f"ERR: {v['err']}"
        else:
            blocks[b] = "OK"
    out["v2_blocks"] = blocks
    rc = d.get("rates_curve", {})
    out["spot_check"] = {
        "estr_pct": rc.get("estr_pct"), "dfr_pct": rc.get("dfr_pct"),
        "estr_dfr_spread_bp": rc.get("estr_dfr_spread_bp"),
        "euribor_ois_proxy_bp": rc.get("euribor_ois_proxy_bp"),
        "implied_next_12m_bp": rc.get("implied_next_12m_bp"),
        "path_read": rc.get("path_read"),
        "spf_lt": (d.get("inflation_expectations") or {}).get("spf_longterm_pct"),
        "hicp_headline": (d.get("inflation") or {}).get("headline_yoy"),
        "hicp_services": (d.get("inflation") or {}).get("services_yoy"),
        "t2_countries": len((d.get("target2") or {}).get("countries") or []),
        "de_minus_it_bn": (d.get("target2") or {}).get("de_minus_it_bn"),
        "nfc_loans_yoy": (d.get("credit") or {}).get("nfc_loans_yoy"),
        "eurusd": (d.get("fx") or {}).get("eurusd"),
        "n_indicators": len(d.get("indicators") or {}),
    }
except Exception as e:
    out["s3_err"] = str(e)[:120]

# ── 5. dataflow catalog raw-text grep via tmp Lambda (ILS / negotiated wages follow-up) ──
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"; TFN = "tmp-ecbflows2"
code = r'''
import json, urllib.request, ssl, re
def lambda_handler(e, c):
    ctx = ssl.create_default_context(); ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
    H = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124 Safari/537.36"}
    out = {}
    try:
        r = urllib.request.urlopen(urllib.request.Request(
            "https://data-api.ecb.europa.eu/service/dataflow/ECB?format=structurespecificdata", headers=H), timeout=60, context=ctx)
        raw = r.read().decode("utf-8", "replace")
    except Exception:
        try:
            r = urllib.request.urlopen(urllib.request.Request(
                "https://data-api.ecb.europa.eu/service/dataflow/ECB", headers=H), timeout=60, context=ctx)
            raw = r.read().decode("utf-8", "replace")
        except Exception as ex:
            return {"statusCode": 200, "body": json.dumps({"err": str(ex)[:100]})}
    out["bytes"] = len(raw)
    # raw-text: capture id="XYZ" near matched Name text, namespace-agnostic
    flows = re.findall(r'id="([A-Z0-9_]{2,12})"[^>]*>.{0,400}?<[^>]*Name[^>]*>([^<]{3,140})<', raw, re.S)
    out["n_flows"] = len(flows)
    def hits(pat):
        rx = re.compile(pat, re.I)
        return [{"id": i, "name": n.strip()[:90]} for i, n in flows if rx.search(n)][:12]
    out["inflation_swap"] = hits(r"inflation.{0,30}(swap|linked|expect)|swap.{0,20}inflation|ILS")
    out["wages"] = hits(r"negotiated|wage")
    out["survey"] = hits(r"survey")
    out["fm_like"] = hits(r"financial market|market data")
    if out["n_flows"] == 0:
        ids = sorted(set(re.findall(r'Dataflow[^>]*id="([A-Z0-9_]{2,12})"', raw)))
        out["raw_ids"] = ids[:60]; out["n_raw_ids"] = len(ids)
        for kw in ("nflation", "wage", "egotiat", "swap"):
            i = raw.lower().find(kw.lower())
            out[f"ctx_{kw}"] = raw[max(0, i-150):i+150].replace("\n", " ") if i >= 0 else None
    return {"statusCode": 200, "body": json.dumps(out)}
'''
b2 = io.BytesIO()
with zipfile.ZipFile(b2, "w") as z:
    z.writestr("lambda_function.py", code)
try:
    try:
        lam.delete_function(FunctionName=TFN)
        time.sleep(2)
    except ClientError:
        pass
    lam.create_function(FunctionName=TFN, Runtime="python3.12", Handler="lambda_function.lambda_handler",
                        Role=ROLE, Code={"ZipFile": b2.getvalue()}, Timeout=120, MemorySize=256)
    for _ in range(20):
        time.sleep(2)
        if lam.get_function_configuration(FunctionName=TFN).get("State") == "Active":
            break
    rr = lam.invoke(FunctionName=TFN, InvocationType="RequestResponse", Payload=b"{}")
    body = json.loads(rr["Payload"].read())
    out["dataflow_grep"] = json.loads(body.get("body", "{}")) if isinstance(body, dict) else body
except Exception as e:
    out["dataflow_grep"] = {"err": str(e)[:120]}
finally:
    try:
        lam.delete_function(FunctionName=TFN)
    except Exception:
        pass

open("aws/ops/reports/1525_v2.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({k: out[k] for k in ("config_after", "function_error", "version", "n_flashing", "v2_blocks") if k in out}, default=str))
