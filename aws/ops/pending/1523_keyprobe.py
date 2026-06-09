import json, time, zipfile, io, boto3
from botocore.config import Config
lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=170, retries={"max_attempts": 1}))
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"; FN = "tmp-ecbkeys"
code = r'''
import json, urllib.request, urllib.error, ssl
def lambda_handler(e, c):
    ctx = ssl.create_default_context(); ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
    H = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124 Safari/537.36",
         "Accept": "text/csv;q=0.9, */*;q=0.5"}
    B = "https://data-api.ecb.europa.eu/service/data/"
    out = {}

    def get(path, q="?format=csvdata&lastNObservations=1"):
        try:
            r = urllib.request.urlopen(urllib.request.Request(B + path + q, headers=H), timeout=30, context=ctx)
            return r.status, r.read().decode("utf-8", "replace").strip().split("\n")
        except urllib.error.HTTPError as ex:
            return ex.code, []
        except Exception as ex:
            return str(ex)[:50], []

    def keys_of(lines):
        return [l.split(",")[0] for l in lines[1:] if l]

    def latest_of(lines):
        # KEY ... TIME_PERIOD, OBS_VALUE — find indices from header
        if len(lines) < 2: return None
        hdr = lines[0].split(",")
        try: ti, vi = hdr.index("TIME_PERIOD"), hdr.index("OBS_VALUE")
        except ValueError: return None
        c = lines[-1].split(",")
        return (c[ti], c[vi]) if len(c) > max(ti, vi) else None

    # 1) FM daily EUR discovery — find 5Y5Y ILS / OIS / EURIBOR series
    st, lines = get("FM/D.U2.EUR....")
    ks = keys_of(lines)
    out["fm_daily"] = {"status": st, "n": len(ks),
        "m_5y5y": [k for k in ks if "5Y5Y" in k][:8],
        "m_il":   [k for k in ks if ".IL" in k or "ILS" in k or "INFL" in k][:10],
        "m_ois":  [k for k in ks if "OIS" in k or "EONIA" in k][:10],
        "m_euribor": [k for k in ks if "EURIBOR" in k][:10],
        "m_bb": [k for k in ks if ".BB." in k][:12],
        "sample": ks[:10]}

    # 1b) FM monthly Euribor fallback
    st, lines = get("FM/M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA", "?format=csvdata&lastNObservations=3")
    out["euribor3m_monthly"] = {"status": st, "latest": latest_of(lines)}

    # 2) EST flow discovery — eSTR + compounded average keys
    st, lines = get("EST")
    ks = keys_of(lines)
    out["est"] = {"status": st, "n": len(ks), "keys": ks[:25]}

    # 3) HICP YoY special aggregates discovery
    st, lines = get("ICP/M.U2.N..4.ANR")
    ks = keys_of(lines)
    want = ["000000", "XEF000", "SERV", "NRG", "FOOD", "IGXE", "TOT_X", "GOODS", "GD"]
    out["icp"] = {"status": st, "n": len(ks),
                  "matched": {w: [k for k in ks if "." + w in k or w + "." in k][:3] for w in want}}

    # 4) BSI loan growth — exact candidates
    for name, key in [("nfc_growth", "BSI/M.U2.Y.U.A20T.A.I.U2.2240.Z01.A"),
                      ("hh_growth",  "BSI/M.U2.Y.U.A20T.A.I.U2.2250.Z01.A"),
                      ("nfc_level",  "BSI/M.U2.Y.U.A20T.A.1.U2.2240.Z01.E"),
                      ("hh_level",   "BSI/M.U2.Y.U.A20T.A.1.U2.2250.Z01.E")]:
        st, lines = get(key, "?format=csvdata&lastNObservations=3")
        out["bsi_" + name] = {"status": st, "latest": latest_of(lines)}

    # 5) NEER discovery
    st, lines = get("EXR/D..EUR.EN00.A")
    out["neer"] = {"status": st, "keys": keys_of(lines)[:8]}

    # 6) YC AAA 1y spot (for forward-implied path)
    st, lines = get("YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_1Y", "?format=csvdata&lastNObservations=3")
    out["yc_1y"] = {"status": st, "latest": latest_of(lines)}

    # 7) SPF long-term inflation expectations candidate
    st, lines = get("SPF/Q.U2.HICP.POINT.LT.Q.AVG", "?format=csvdata&lastNObservations=3")
    out["spf_lt"] = {"status": st, "latest": latest_of(lines)}

    # 8) Negotiated wages candidates
    for nm, key in [("wages_i8", "STS/Q.I8.N.INWR000.4.000"), ("wages_i9", "STS/Q.I9.N.INWR000.4.000")]:
        st, lines = get(key, "?format=csvdata&lastNObservations=3")
        out[nm] = {"status": st, "latest": latest_of(lines)}

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
                            Code={"ZipFile": buf.getvalue()}, Timeout=150, MemorySize=256)
    for _ in range(25):
        time.sleep(2)
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") in ("Successful", None):
            break
    out = json.loads(lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e:
    out = {"err": str(e)[:150]}
open("aws/ops/reports/1523_keys.json", "w").write(json.dumps(out, indent=2, default=str))
print("done")
