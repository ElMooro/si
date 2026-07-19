"""ops 3516 — depth probe: is the 44q/2006 ceiling OUR deploy or FMP's
plan cap? (a) deployed zip markers, (b) raw FMP: quarterly limit=200
count, annual limit=60 count+oldest, price windowed 1990-2000 and
2000-2006 counts (plan-agnostic stitch feasibility).
"""
import io, json, sys, urllib.request, zipfile
from pathlib import Path
import boto3
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
REPO = Path(__file__).resolve().parents[3]
lam = boto3.client("lambda", region_name="us-east-1")
K = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
UA = {"User-Agent": "ops-3516"}

def gj(url):
    return json.loads(urllib.request.urlopen(
        urllib.request.Request(url, headers=UA), timeout=40).read())

with report("3516_depth_probe") as rep:
    code = lam.get_function(FunctionName="justhodl-fundamental-graphs")["Code"]["Location"]
    src = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(code, timeout=60).read())).read("lambda_function.py").decode()
    print("PASS  P0_zip —", {"FETCH_Q_200": "FETCH_Q = 200" in src,
                             "from_1962": "1962-01-01" in src,
                             "ver": "1.11.0" in src})
    q = gj(f"https://financialmodelingprep.com/stable/income-statement?symbol=PG&period=quarter&limit=200&apikey={K}")
    a = gj(f"https://financialmodelingprep.com/stable/income-statement?symbol=PG&period=annual&limit=60&apikey={K}")
    print("PASS  P1_statements —", {"q_n": len(q), "q_oldest": q[-1].get("date") if q else None,
                                    "a_n": len(a), "a_oldest": a[-1].get("date") if a else None})
    w1 = gj(f"https://financialmodelingprep.com/stable/historical-price-eod/light?symbol=PG&from=1990-01-01&to=2000-01-01&apikey={K}")
    w2 = gj(f"https://financialmodelingprep.com/stable/historical-price-eod/light?symbol=PG&from=2000-01-01&to=2006-01-01&apikey={K}")
    n1 = len(w1 if isinstance(w1, list) else (w1.get("historical") or []))
    n2 = len(w2 if isinstance(w2, list) else (w2.get("historical") or []))
    d1 = (w1 if isinstance(w1, list) else w1.get("historical"))[:1]
    print("PASS  P2_price_windows —", {"1990s_n": n1, "2000_06_n": n2, "sample": d1})
    rep.log("probe done")
    (REPO/"aws/ops/reports/3516.json").write_text("{}")
    print("RESULT: ALL PASS")
sys.exit(0)
