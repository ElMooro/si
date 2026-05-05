"""
Diagnose why EPS-velocity returned 0 results after the universe patch.
Hypothesis: the universe seed is causing a different MAX_TICKERS slice
that doesn't include big-tech with many estimates.
"""
import io, json, os, time, base64, zipfile
import boto3

REGION = "us-east-1"
L = boto3.client("lambda", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def main():
    section("1) Check what's in EPS universe today")
    # Read deployed source to confirm universe-loading
    code_url = L.get_function(FunctionName="justhodl-eps-revision-velocity")["Code"]["Location"]
    import urllib.request
    zb = urllib.request.urlopen(code_url, timeout=15).read()
    src = zipfile.ZipFile(io.BytesIO(zb)).read("lambda_function.py").decode()
    log(f"  has 'universe.json': {'data/universe.json' in src}")
    log(f"  has 'PRIMARY: unified': {'PRIMARY: unified' in src}")

    section("2) Read universe.json — what tickers are in there")
    obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="data/universe.json")
    d = json.loads(obj["Body"].read())
    stocks = d.get("stocks", [])
    log(f"  universe has {len(stocks)} tickers")

    # Check if AAPL/MSFT/NVDA are in there (these have many estimates)
    syms = [s.get("symbol") for s in stocks]
    test_syms = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA",
                  "MU", "SNDK", "PLTR", "CSGP", "EPAM"]
    for ts in test_syms:
        log(f"    {ts} in universe: {ts in syms}")

    section("3) Probe FMP /stable/analyst-estimates directly")
    # Test a few names we know have estimates
    FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
    for sym in ["AAPL", "MSFT", "NVDA", "PLTR", "CSGP"]:
        url = f"https://financialmodelingprep.com/stable/analyst-estimates?symbol={sym}&period=annual&limit=5&apikey={FMP_KEY}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                resp = json.loads(r.read())
                if isinstance(resp, list):
                    log(f"    {sym}: {len(resp)} estimates returned")
                    if resp:
                        log(f"       sample keys: {list(resp[0].keys())[:8]}")
                        log(f"       sample date: {resp[0].get('date')}, eps_avg: {resp[0].get('epsAvg') or resp[0].get('estimatedEpsAvg')}")
                else:
                    log(f"    {sym}: non-list response: {str(resp)[:100]}")
        except Exception as e:
            log(f"    {sym}: ERROR {e}")

    section("4) Re-invoke EPS with verbose log capture")
    t0 = time.time()
    r = L.invoke(FunctionName="justhodl-eps-revision-velocity",
                  InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
    log(f"  status: {r['StatusCode']}, dur: {time.time()-t0:.1f}s")
    body = json.loads(r["Payload"].read())
    log(f"  body: {body}")
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode()
        log("  ── full tail ──")
        for ln in tail.splitlines():
            log(f"    {ln.rstrip()}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_f2_diagnose_eps.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
