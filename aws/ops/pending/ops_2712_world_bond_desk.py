"""ops 2712 — Bond Desk v2.0: WORLD fixed-income anxiety (Khalid: 'tap all my
data, go worldwide'). Adds full AAA..CCC OAS ladder + spreads-of-spreads,
ICI bond FUND flows (z on fleet history), COT curve positioning, TIC foreign
demand, and three new regions on owned engines: EU (Euro HY + fragmentation),
JP (yen-carry/BOJ), EM (EM corp/HY OAS + ETF flows + dollar regime) ->
availability-renormalized WORLD anxiety + divergence flag + ranked drivers.
Page rebuilt (region sections, ladder tables, self-drawn sparkline fixing the
'No time-series' enhancer gap). Report: aws/ops/reports/2712_world_bond_desk.json.
"""
import os, io, json, time, zipfile, re, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
FRED_KEY = "2f057499936072679d8843d7fce99989"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=200, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2712, "ts": datetime.now(timezone.utc).isoformat()}
def sect(t): print("\n" + "=" * 8 + " " + t + " " + "=" * 8)
def get(url, timeout=25):
    with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "jh/1"}), timeout=timeout) as r:
        return r.read().decode("utf-8", "ignore")
def zip_fn(fn):
    src = "aws/lambdas/%s/source" % fn
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src):
            if "__pycache__" in root: continue
            for f in files:
                z.write(os.path.join(root, f), os.path.relpath(os.path.join(root, f), src))
        for f in sorted(os.listdir("aws/shared")):
            if f.endswith(".py"): z.write(os.path.join("aws/shared", f), f)
    return buf.getvalue()
def wait_ok(fn, budget=240):
    t0 = time.time()
    while time.time() - t0 < budget:
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None, "Successful"): return
        time.sleep(5)
def retry(call, what, tries=6):
    for i in range(tries):
        try: return call()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"): time.sleep(18)
            else: raise
    raise RuntimeError(what)

sect("1/4 GLOBAL OAS PROBES")
for sid in ("BAMLHE00EHYIOAS", "BAMLEMCBPIOAS", "BAMLEMHBHYCRPIOAS"):
    try:
        j = json.loads(get("https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s&file_type=json&sort_order=desc&limit=2" % (sid, FRED_KEY)))
        ob = [o for o in j.get("observations", []) if o.get("value") not in (".", None)]
        print(" ", sid, ob[0]["date"], ob[0]["value"] if ob else "EMPTY")
        R.setdefault("probes", {})[sid] = ob[0] if ob else "empty"
        if sid != "BAMLEMHBHYCRPIOAS":
            assert ob, sid + " required"
    except Exception as e:
        print(" ", sid, "ERR", str(e)[:60])
        assert sid == "BAMLEMHBHYCRPIOAS", sid + " required but failed"

sect("2/4 DEPLOY + RUN v2")
print("  settling 30s…"); time.sleep(30)
retry(lambda: (wait_ok("justhodl-bond-desk"), lam.update_function_code(FunctionName="justhodl-bond-desk", ZipFile=zip_fn("justhodl-bond-desk")))[-1], "desk")
wait_ok("justhodl-bond-desk")
r = lam.invoke(FunctionName="justhodl-bond-desk", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay)[:200])
assert not r.get("FunctionError"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/bond-desk.json")["Body"].read())
W, RG = d["world"], d["regions"]
US, EU, JP, EM = RG["US"], RG["EU"], RG["JP"], RG["EM"]
R["world"] = {"anxiety": W["anxiety"], "regime": d["regime"], "regional": W["regional"],
              "divergence": W.get("divergence"), "drivers": W["drivers"]}
R["us"] = {"ladder_n": len(US["ratings_ladder"]), "sos": US["spreads_of_spreads"],
           "ici": US["ici_fund_flows"], "cot": {k: US["cot_duration"].get(k) for k in ("status", "contracts_n", "avg_spec_z")},
           "tic": US["tic_foreign"].get("status"), "eq_to_bond_b": round(US["flows"]["equity_to_bond_5d_usd"] / 1e9, 2)}
R["eu"] = {"euro_hy": EU.get("euro_hy"), "euro_minus_us": EU.get("euro_minus_us_hy_bps"),
           "sovereign": {k: (EU.get("sovereign") or {}).get(k) for k in ("core_avg_spread_bp", "oat_bund_bp", "ecb_stance")}}
R["jp"] = {k: JP.get(k) for k in ("carry_regime", "boj_stance", "anxiety")}
R["em"] = {"em_corp": EM.get("em_corp"), "em_hy": EM.get("em_hy"), "dollar": EM.get("dollar_regime"), "anxiety": EM.get("anxiety")}
print(json.dumps({"world": R["world"], "us_ladder": R["us"]["ladder_n"], "eu": R["eu"], "jp": R["jp"], "em": R["em"]}, indent=1, default=str)[:1500])
assert d["version"] == "2.0.0" and 0 <= W["anxiety"] <= 100
live_regions = [k for k, v in W["regional"].items() if v is not None]
assert len(live_regions) >= 3, "regions live: %s" % live_regions
assert len(US["ratings_ladder"]) >= 8, "ladder thin"
for k in ("BBB_minus_A", "B_minus_BB", "CCC_minus_B", "CCC_minus_BB"):
    assert k in US["spreads_of_spreads"], k
assert US["ici_fund_flows"].get("status") == "OK" and isinstance(US["ici_fund_flows"].get("bond_flow_latest_b"), (int, float))
assert EU.get("euro_hy") and isinstance(EU["euro_hy"].get("bps"), (int, float)) and 150 <= EU["euro_hy"]["bps"] <= 1500
assert isinstance((EU.get("sovereign") or {}).get("core_avg_spread_bp"), (int, float))
assert EM.get("em_corp") and isinstance(EM["em_corp"].get("bps"), (int, float))
assert len(W["drivers"]) >= 4
assert d["anxiety_score"] == W["anxiety"]  # board compatibility

sect("3/4 BOARD + PAGE")
r = lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse")
assert not r.get("FunctionError")
sb = s3.get_object(Bucket=BUCKET, Key="data/signal-board.json")["Body"].read().decode()
m = re.search(r'\{[^{}]*Bond Desk[^{}]*\}', sb)
R["board_row"] = m.group(0)[:210] if m else "present"
print("  board:", R["board_row"])
assert "Bond Desk" in sb
time.sleep(70)
try:
    pg = get("https://justhodl.ai/bond-desk.html", timeout=20)
    R["page"] = "LIVE" if "WORLD BOND DESK" in pg else "200_no_marker"
except Exception as e:
    R["page"] = "propagating: " + str(e)[:60]
print("  page:", R["page"])

sect("4/4 REPORT")
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2712_world_bond_desk.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2712 COMPLETE — world bond desk live")

# rev v2: ICI classes.bond direct read + rot z
