"""ops 1937 — Deploy 299-ETF universe + radar v3.0.0 (46 complexes) and verify.

Order matters: etf-fund-flows must run FIRST to populate the 53 new tickers
into etf-flows/daily.json + history, THEN the radar consumes them.

- Force update_function_code on both (deploy-lambdas can miss source diffs).
- etf-fund-flows zip = lambda_function.py only (no shared deps).
- radar zip = lambda_function.py + massive.py (price divergence read).
- Invoke each, wait Active, verify.
- Surface ETF Global publish date (freshness honesty flag).
"""
import io, json, time, zipfile, os
import boto3

REGION = "us-east-1"
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
BUCKET = "justhodl-dashboard-live"

def zip_bytes(files):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for arc, path in files.items():
            z.write(path, arc)
    buf.seek(0)
    return buf.read()

def update_code(fn, zb):
    for i in range(24):
        try:
            lam.update_function_code(FunctionName=fn, ZipFile=zb, Publish=False)
            print(f"  update OK ({fn}) attempt {i}")
            return True
        except lam.exceptions.ResourceConflictException:
            print(f"  conflict {i} on {fn}, retry...")
            time.sleep(5)
    return False

def wait_active(fn):
    for _ in range(40):
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
            return c
        time.sleep(3)
    return lam.get_function_configuration(FunctionName=fn)

ROOT = os.getcwd()
EFF = f"{ROOT}/aws/lambdas/justhodl-etf-fund-flows/source/lambda_function.py"
RAD = f"{ROOT}/aws/lambdas/justhodl-capital-flow-radar/source/lambda_function.py"
MAS = f"{ROOT}/aws/shared/massive.py"

# ── 1) etf-fund-flows ──
print("=== etf-fund-flows deploy ===")
update_code("justhodl-etf-fund-flows", zip_bytes({"lambda_function.py": EFF}))
c = wait_active("justhodl-etf-fund-flows")
print("  state:", c.get("State"), c.get("LastUpdateStatus"), "size:", c.get("CodeSize"))
print("  invoking (sync, populates 299 ETFs)...")
r = lam.invoke(FunctionName="justhodl-etf-fund-flows", InvocationType="RequestResponse")
body = json.loads(r["Payload"].read())
try:
    inner = json.loads(body.get("body", "{}"))
    print("  n_etfs_ok:", inner.get("n_etfs_ok"), "| regime:", inner.get("regime"), "| elapsed:", inner.get("elapsed_s"))
except Exception as e:
    print("  invoke raw:", str(body)[:300])

# verify daily.json
daily = json.loads(s3.get_object(Bucket=BUCKET, Key="etf-flows/daily.json")["Body"].read())
metrics = daily.get("metrics", [])
have = {m["ticker"] for m in metrics if m.get("daily_flow_usd") is not None}
NEW = ["MUU","MUD","NFXL","NFXS","BABX","TSMX","BRKU","CRWL","HOOX","AVL","AAPB","NVDD",
       "SMCX","PLTD","COII","AMUU","TSLZ","URTY","SRTY","UMDD","SMDD","QQQU","TTT","XLG",
       "FTEC","RXD","SDP","SIJ","UGE","SZK","SCC","XSD","TBT","UBT","TBF","PST","UST",
       "GDXU","GDXD","BTCL","ETU","ETHD","CHAU","XPP","FXP","BUG","QTUM","ROBO","MSOS",
       "NLR","PPA","ARKX","PHO"]
new_have = [t for t in NEW if t in have]
new_miss = [t for t in NEW if t not in have]
# freshness
dates = sorted({m.get("processed_date") for m in metrics if m.get("processed_date")}, reverse=True)
print(f"\n  daily.json total metrics: {len(metrics)} | with flow: {len(have)}")
print(f"  NEW tickers populated: {len(new_have)}/{len(NEW)}")
if new_miss:
    print(f"  NEW missing (no flow): {new_miss}")
print(f"  ETF Global latest processed_date: {dates[0] if dates else 'NONE'} (freshness)")

# ── 2) radar v3 ──
print("\n=== capital-flow-radar v3 deploy ===")
update_code("justhodl-capital-flow-radar", zip_bytes({"lambda_function.py": RAD, "massive.py": MAS}))
c = wait_active("justhodl-capital-flow-radar")
print("  state:", c.get("State"), c.get("LastUpdateStatus"), "size:", c.get("CodeSize"))
print("  invoking radar...")
r = lam.invoke(FunctionName="justhodl-capital-flow-radar", InvocationType="RequestResponse")
body = json.loads(r["Payload"].read())
print("  invoke:", str(body)[:260])

radar = json.loads(s3.get_object(Bucket=BUCKET, Key="data/capital-flow-radar.json")["Body"].read())
print("\n  version:", radar.get("version"), "| n_complexes:", radar.get("n_complexes"))
lp = radar.get("leveraged_positioning", {})
allb = lp.get("all", [])
ss = [e for e in allb if e.get("kind") == "single_stock"]
print(f"  single-stock board names: {len(ss)}")
for e in sorted(ss, key=lambda x: -(x.get('net_lev_positioning_5d') or 0)):
    print(f"    {e['name']:7s} net=${round((e.get('net_lev_positioning_5d') or 0)/1e6)}M  {e.get('stance')}  legs={','.join(e.get('legs',[]))}")
print("  risk_appetite:", lp.get("risk_appetite"))
newc = [c2["complex"] for c2 in radar.get("complexes", []) if c2["complex"] in
        ("Long Treasuries (20Y)","Intermediate Treasuries (7-10Y)","Cybersecurity","Mid Caps","Cannabis","Quantum Computing","Robotics/AI")]
print("  new complexes live:", newc)
print("\nDONE 1937")
