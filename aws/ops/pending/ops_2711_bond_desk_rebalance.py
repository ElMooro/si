"""ops 2711 — BOND DESK + REBALANCE RADAR (Khalid's fixed-income & Q-end directive).

A) justhodl-bond-desk: FI FLOW & CREDIT-APPETITE desk (bond-vol v2 already
owns stress) — duration ladder, HY/loans/EM vs govvies, CCC-BB micro (new
FRED), equity->bond rotation, cross-checked vs owned stress engines ->
anxiety 0-100 + plain-English equity read.
B) justhodl-rebalance-radar: measured T-5..T+5 event study (~10y, 8 proxies,
cached 30d) + live window forensics (mechanical QTD-based pressure vs
observed radar flows) + leadership->crypto ROTATION_RISK flag — today is
T+1 of the Q2-end window, so the first run adjudicates Khalid's AI->crypto
hypothesis with real numbers.
Report: aws/ops/reports/2711_bond_desk_rebalance.json.
"""
import os, io, json, time, zipfile, re, urllib.request
from datetime import datetime, timezone, timedelta
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
FRED_KEY = "2f057499936072679d8843d7fce99989"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=290, retries={"max_attempts": 1}))
ev = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2711, "ts": datetime.now(timezone.utc).isoformat()}
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
def ensure_fn(name):
    cfg = json.load(open("aws/lambdas/%s/config.json" % name))
    zb = zip_fn(name)
    try:
        lam.get_function(FunctionName=name); wait_ok(name)
        retry(lambda: lam.update_function_code(FunctionName=name, ZipFile=zb), name)
        wait_ok(name)
        retry(lambda: lam.update_function_configuration(FunctionName=name, Timeout=cfg["timeout"], MemorySize=cfg["memory"]), name + " cfg")
    except lam.exceptions.ResourceNotFoundException:
        retry(lambda: lam.create_function(FunctionName=name, Runtime=cfg["runtime"], Role=cfg["role"],
              Handler=cfg["handler"], Code={"ZipFile": zb}, Timeout=cfg["timeout"], MemorySize=cfg["memory"],
              Architectures=cfg["architectures"], Description=cfg["description"]), name + " create")
    wait_ok(name)
    sch = cfg["schedule"]
    arn = "arn:aws:lambda:%s:857687956942:function:%s" % (REGION, name)
    ra = ev.put_rule(Name=sch["name"], ScheduleExpression=sch["expression"], State="ENABLED", Description=sch["description"])["RuleArn"]
    try:
        lam.add_permission(FunctionName=name, StatementId="evt-" + sch["name"], Action="lambda:InvokeFunction",
                           Principal="events.amazonaws.com", SourceArn=ra)
    except lam.exceptions.ResourceConflictException: pass
    ev.put_targets(Rule=sch["name"], Targets=[{"Id": "1", "Arn": arn}])
    print("  ensured", name, "+", sch["name"])

sect("1/5 PROBES — flows feed key, FRED CCC/BB")
def head(k):
    try:
        s3.head_object(Bucket=BUCKET, Key=k); return True
    except Exception: return False
flows_key_ok = head("etf-flows/daily.json")
print("  etf-flows/daily.json exists:", flows_key_ok)
if not flows_key_ok:
    alt = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix="etf-flows/", MaxKeys=1000).get("Contents", [])
           if "flow" in o["Key"] and o["Key"].endswith(".json")][:8]
    print("  flow-ish keys:", alt)
assert flows_key_ok, "flows feed key mismatch — fix engine constant before deploy"
for sid in ("BAMLH0A3HYC", "BAMLH0A1HYBB"):
    j = json.loads(get("https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s&file_type=json&sort_order=desc&limit=2" % (sid, FRED_KEY)))
    ob = [o for o in j.get("observations", []) if o.get("value") not in (".", None)]
    assert ob, sid + " unavailable"
    print(" ", sid, ob[0]["date"], ob[0]["value"])
R["probes"] = "ok"

sect("2/5 BOND DESK — create + run + prove")
print("  settling 30s…"); time.sleep(30)
for fn in ("justhodl-etf-true-flows",):
    retry(lambda f=fn: (wait_ok(f), lam.update_function_code(FunctionName=f, ZipFile=zip_fn(f)))[-1], fn)
    wait_ok(fn); print("  synced", fn, "(bond-ladder universe expansion)")
r = lam.invoke(FunctionName="justhodl-etf-true-flows", InvocationType="RequestResponse")
print("  true-flows seed invoke:", ("ERR " if r.get("FunctionError") else "ok ") + (r["Payload"].read() or b"")[:80].decode("utf-8","ignore"))
ensure_fn("justhodl-bond-desk")
r = lam.invoke(FunctionName="justhodl-bond-desk", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay)[:160])
assert not r.get("FunctionError"), pay
bd = json.loads(s3.get_object(Bucket=BUCKET, Key="data/bond-desk.json")["Body"].read())
F, M = bd["flows"], bd["credit_micro"]
R["bond_desk"] = {"anxiety": bd["anxiety_score"], "regime": bd["regime"],
                  "matched": F["matched_tickers"], "duration_tilt": F["duration_tilt"],
                  "credit_appetite_5d_b": round(F["credit_appetite_5d_usd"] / 1e9, 2),
                  "eq_to_bond_5d_b": round(F["equity_to_bond_5d_usd"] / 1e9, 2),
                  "ccc_bb": {k: M.get(k) for k in ("ccc_bb_bps", "pctile", "d21_bps", "read")},
                  "xchk": bd["stress_crosschecks"], "equity_read": bd["equity_read"][:220]}
print(json.dumps(R["bond_desk"], indent=1)[:900])
print("  per-bucket n:", {k: v["n"] for k, v in F["buckets"].items()})
assert F["matched_tickers"] >= 15, "too few flow tickers matched: %d" % F["matched_tickers"]
if F["matched_tickers"] < 30:
    print("  NOTE: coverage ramps to ~40 as true-flows snapshots accrue (new bond-ladder tickers need 1-2 daily closes for 5d flows)")
assert 0 <= bd["anxiety_score"] <= 100 and bd["regime"] in ("CALM", "UNEASY", "ANXIOUS", "STRESS")
assert M.get("status") == "OK" and isinstance(M.get("ccc_bb_bps"), (int, float)) and 50 <= M["ccc_bb_bps"] <= 2500
assert len(bd["equity_read"]) > 60
assert sum(1 for b in F["buckets"].values() if b["n"] > 0) >= 9, "buckets thin"

sect("3/5 REBALANCE RADAR — create + run + prove (T+1 of Q2-end)")
try: s3.delete_object(Bucket=BUCKET, Key="data/history/rebalance-eventstudy.json")
except Exception: pass
print("  purged cached event study (force 11y recompute)")
ensure_fn("justhodl-rebalance-radar")
r = lam.invoke(FunctionName="justhodl-rebalance-radar", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay)[:160])
assert not r.get("FunctionError"), pay
rb = json.loads(s3.get_object(Bucket=BUCKET, Key="data/rebalance-radar.json")["Body"].read())
C, ES, W, RR = rb["calendar"], rb["event_study"], rb["window_forensics"], rb["rotation_risk"]
spy_t = {t["offset"]: t["mean_pct"] for t in ES["assets"].get("SPY", {}).get("table", [])}
R["rebalance"] = {"calendar": C, "n_quarters": ES["n_quarters"],
                  "es_spy_tail": {("T%+d" % o): spy_t.get(o) for o in (-3, -2, -1, 0, 1, 2, 3)},
                  "legs": W["cross_asset_legs"], "n_complexes": W["n_complexes"],
                  "top_outflows": [{k: r_.get(k) for k in ("complex", "net_flow_5d_usd", "price_5d_pct", "qtd_vs_spy_pp", "classification")} for r_ in W["top_outflows"][:4]],
                  "rotation": {"flag": RR["flag"], "severity": RR["severity"], "evidence": RR["evidence"][:4]}}
print(json.dumps(R["rebalance"], indent=1, default=str)[:1400])
assert C["in_rebalance_window"] is True, "Jul-1 must be inside T+3 window: %s" % C
assert ES["n_quarters"] >= 34 and len(ES["assets"]) >= 6
for a in ES["assets"].values():
    assert len(a["table"]) == 11
assert W["n_complexes"] >= 20 and len(W["top_outflows"]) >= 5
assert isinstance(RR["flag"], bool) and RR["severity"] in ("NONE", "ELEVATED", "HIGH")
assert all(k in W["cross_asset_legs"] for k in ("ai_semis_5d_usd", "bonds_5d_usd", "crypto_5d_usd"))

sect("4/5 BOARD + PAGES")
retry(lambda: (wait_ok("justhodl-signal-board"), lam.update_function_code(FunctionName="justhodl-signal-board", ZipFile=zip_fn("justhodl-signal-board")))[-1], "board")
wait_ok("justhodl-signal-board")
r = lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse")
assert not r.get("FunctionError")
sb = s3.get_object(Bucket=BUCKET, Key="data/signal-board.json")["Body"].read().decode()
for marker in ("Bond Desk", "Rebalance Window"):
    assert marker in sb, "board row missing: " + marker
    m = re.search(r'\{[^{}]*' + marker + r'[^{}]*\}', sb)
    R.setdefault("board", {})[marker] = m.group(0)[:210] if m else "present"
    print("  board:", R["board"][marker])
time.sleep(70)
for pg, marker in (("bond-desk", "BOND DESK"), ("rebalance-radar", "REBALANCE RADAR")):
    try:
        R.setdefault("pages", {})[pg] = "LIVE" if marker in get("https://justhodl.ai/%s.html" % pg, 20) else "200_no_marker"
    except Exception as e:
        R.setdefault("pages", {})[pg] = "propagating: " + str(e)[:50]
    print(" ", pg, R["pages"][pg])

sect("5/5 REPORT")
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2711_bond_desk_rebalance.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2711 COMPLETE — FI flow desk + Q-end rotation regime live")
