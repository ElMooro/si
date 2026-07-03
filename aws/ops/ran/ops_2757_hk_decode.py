"""ops 2757 — HK decode diagnostic: what encoding is HKEX serving to AWS?
Fetch data_tab_daily js from Lambda, dump Content-Encoding + first bytes hex,
try gzip/zlib/brotli/utf-16 decodings. Also probe AAStocks Southbound as a
non-Akamai fallback. Read-only. Report: 2757_hk_decode.json.
"""
import os, io, json, gzip, zlib, time
from datetime import datetime, timezone, timedelta
import boto3
from botocore.config import Config
lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=180))
R = {"ops": 2757, "ts": datetime.now(timezone.utc).isoformat()}
# run the probe INSIDE the apac lambda so we test from the SAME IP that fails
CODE = '''
import json, gzip, zlib, urllib.request
from datetime import datetime, timezone, timedelta
def handler(event, ctx):
    ymd = (datetime.now(timezone.utc)+timedelta(hours=8)).strftime("%Y%m%d")
    out = {}
    url = "https://www.hkex.com.hk/eng/csm/DailyStat/data_tab_daily_%se.js" % ymd
    for tag, hdrs in [("plain", {"User-Agent":"Mozilla/5.0"}),
                      ("identity", {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/121 Safari/537.36","Accept-Encoding":"identity","Referer":"https://www.hkex.com.hk/"})]:
        try:
            req = urllib.request.Request(url, headers=hdrs)
            with urllib.request.urlopen(req, timeout=20) as r:
                raw = r.read(); enc = r.headers.get("Content-Encoding"); ct = r.headers.get("Content-Type")
            rec = {"len": len(raw), "enc": enc, "ctype": ct, "hex8": raw[:8].hex()}
            for name, fn in [("gzip", lambda b: gzip.decompress(b)),
                             ("zlib", lambda b: zlib.decompress(b)),
                             ("deflate", lambda b: zlib.decompress(b, -15))]:
                try:
                    d = fn(raw); rec[name] = d[:60].decode("utf-8","ignore")
                except Exception as e:
                    rec[name] = "x:" + str(e)[:30]
            for cdc in ("utf-8","utf-16","utf-16-le"):
                try: rec["dec_"+cdc] = raw[:80].decode(cdc,"ignore").replace(chr(0),"")[:50]
                except Exception as e: rec["dec_"+cdc] = "x"
            out[tag] = rec
        except Exception as e:
            out[tag] = {"err": str(e)[:80]}
    # AAStocks southbound page
    try:
        req = urllib.request.Request("http://www.aastocks.com/en/stocks/market/dtsc/dtsc.aspx?type=sb",
                                     headers={"User-Agent":"Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            aa = r.read().decode("utf-8","ignore")
        out["aastocks"] = {"len": len(aa), "has_sb": ("Southbound" in aa or "port股通" in aa or "Turnover" in aa),
                           "head": aa[:150]}
    except Exception as e:
        out["aastocks"] = {"err": str(e)[:80]}
    return out
'''
# deploy a throwaway function? simpler: temporarily add handler to apac lambda via env is messy.
# Instead invoke apac lambda with a special event that triggers this probe — but adapter doesn't support it.
# So: create/update a tiny standalone probe lambda reusing apac role.
import io as _io, zipfile
role = lam.get_function_configuration(FunctionName="justhodl-apac-flows")["Role"]
buf = _io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", CODE.replace("def handler", "def lambda_handler"))
zipb = buf.getvalue()
FN = "justhodl-hk-probe-tmp"
from botocore.exceptions import ClientError
try:
    lam.get_function(FunctionName=FN)
    lam.update_function_code(FunctionName=FN, ZipFile=zipb); time.sleep(4)
except ClientError:
    lam.create_function(FunctionName=FN, Runtime="python3.12", Role=role,
                        Handler="lambda_function.lambda_handler", Code={"ZipFile": zipb},
                        Timeout=120, MemorySize=256, Publish=True); time.sleep(6)
time.sleep(3)
resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
out = json.loads(resp["Payload"].read() or b"{}")
R["probe"] = out
print(json.dumps(out, indent=1)[:1400])
try:
    lam.delete_function(FunctionName=FN); print("tmp fn deleted")
except Exception: pass
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2757_hk_decode.json", "w"), indent=1, default=str)
print("OPS 2757 COMPLETE")
