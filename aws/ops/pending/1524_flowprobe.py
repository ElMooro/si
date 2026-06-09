import json, time, zipfile, io, boto3
from botocore.config import Config
lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=170, retries={"max_attempts": 1}))
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"; FN = "tmp-ecbflow"
code = r'''
import json, urllib.request, urllib.error, ssl, re
def lambda_handler(e, c):
    ctx = ssl.create_default_context(); ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
    H = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124 Safari/537.36"}
    out = {}
    # 1) full dataflow catalog — grep names for inflation/swap/wage/expectation
    try:
        u = "https://data-api.ecb.europa.eu/service/dataflow/ECB?format=sdmx-2.1"
        xml = urllib.request.urlopen(urllib.request.Request(u, headers=H), timeout=40, context=ctx).read().decode("utf-8", "replace")
        flows = re.findall(r'Dataflow id="([A-Z0-9_]+)"[^>]*>.*?<com:Name[^>]*>([^<]+)</com:Name>', xml, re.DOTALL)
        if not flows:
            flows = re.findall(r'id="([A-Z0-9_]+)"[^>]*agencyID="ECB".*?Name[^>]*>([^<]+)<', xml, re.DOTALL)
        out["n_flows"] = len(flows)
        def grep(words):
            return [(fid, nm[:70]) for fid, nm in flows if any(w in nm.lower() for w in words)][:12]
        out["inflation_flows"] = grep(["inflation"])
        out["swap_flows"] = grep(["swap"])
        out["wage_flows"] = grep(["wage", "labour cost", "negotiated"])
        out["expectation_flows"] = grep(["expectation"])
        out["market_flows"] = grep(["financial market", "money market"])
    except Exception as ex:
        out["flows_err"] = str(ex)[:100]

    H2 = dict(H); H2["Accept"] = "text/csv;q=0.9, */*;q=0.5"
    def get_csv(path, q="?format=csvdata&lastNObservations=1"):
        try:
            r = urllib.request.urlopen(urllib.request.Request(
                "https://data-api.ecb.europa.eu/service/data/" + path + q, headers=H2), timeout=35, context=ctx)
            return r.status, r.read().decode("utf-8", "replace").strip().split("\n")
        except urllib.error.HTTPError as ex:
            return ex.code, []
        except Exception as ex:
            return str(ex)[:50], []

    # 2) FM monthly EUR discovery (5y5y often monthly under provider BB)
    st, lines = get_csv("FM/M.U2.EUR....")
    ks = [l.split(",")[0] for l in lines[1:] if l]
    out["fm_monthly"] = {"status": st, "n": len(ks),
                         "m_5y5y": [k for k in ks if "5Y5Y" in k][:6],
                         "m_il": [k for k in ks if "IL" in k.split(".")[-2] or "INFL" in k][:8],
                         "sample": ks[:14]}

    # 3) probe candidate flows found by name (top inflation/swap hits) with tiny discovery
    cand = []
    for grp in ("inflation_flows", "swap_flows", "wage_flows"):
        for fid, _ in out.get(grp, [])[:3]:
            if fid not in cand:
                cand.append(fid)
    out["cand_discoveries"] = {}
    for fid in cand[:5]:
        st, lines = get_csv(fid)
        ks = [l.split(",")[0] for l in lines[1:] if l]
        out["cand_discoveries"][fid] = {"status": st, "n": len(ks),
                                        "m_5y5y": [k for k in ks if "5Y5Y" in k][:6],
                                        "sample": ks[:10]}
    return out
'''
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
    zf.writestr("lambda_function.py", code)
out = {}
try:
    try:
        lam.get_function_configuration(FunctionName=FN)
        lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN, Runtime="python3.12", Role=ROLE,
                            Handler="lambda_function.lambda_handler",
                            Code={"ZipFile": buf.getvalue()}, Timeout=150, MemorySize=320)
    for _ in range(25):
        time.sleep(2)
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") in ("Successful", None):
            break
    out = json.loads(lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e:
    out = {"err": str(e)[:150]}
open("aws/ops/reports/1524_flow.json", "w").write(json.dumps(out, indent=2, default=str))
print("done")
