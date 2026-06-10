# ops 1541 — canonical deep-SPX base (runner Stooq→Yahoo), analogs v2.2 deploy+verify, backtester re-run, honest walk-forward peek
import json, os, time, zipfile, io, urllib.request, boto3
from datetime import datetime, timezone
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1541, "errors": []}


def retry_conflict(fn, tries=10, wait=8):
    for i in range(tries):
        try:
            return fn()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
                time.sleep(wait); continue
            raise
    raise RuntimeError("retries exhausted")


# ── A. canonical deep base: Stooq first (runner-reachable), Yahoo retry fallback ──
def fetch_stooq():
    raw = urllib.request.urlopen(urllib.request.Request(
        "https://stooq.com/q/d/l/?s=%5Espx&i=d", headers={"User-Agent": "Mozilla/5.0"}), timeout=60).read()
    lines = raw.decode("utf-8", "replace").strip().split("\n")
    pts = []
    for ln in lines[1:]:
        c = ln.split(",")
        if len(c) >= 5 and c[0][:2] in ("19", "20"):
            try: pts.append([c[0], float(c[4])])
            except ValueError: pass
    pts.sort(); return pts

def fetch_yahoo():
    u = "https://query1.finance.yahoo.com/v8/finance/chart/%5EGSPC?range=max&interval=1d"
    for i in range(3):
        try:
            raw = urllib.request.urlopen(urllib.request.Request(
                u, headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}), timeout=60).read()
            j = json.loads(raw)
            r0 = (j.get("chart", {}).get("result") or [{}])[0]
            ts = r0.get("timestamp") or []
            cl = ((r0.get("indicators") or {}).get("quote") or [{}])[0].get("close") or []
            pts = sorted([[datetime.fromtimestamp(t, tz=timezone.utc).date().isoformat(), round(float(c), 2)]
                          for t, c in zip(ts, cl) if c is not None])
            if len(pts) > 8000:
                return pts
        except Exception as e:
            out["errors"].append(f"yahoo try{i}: {str(e)[:60]}")
            time.sleep(5)
    return []

base = []
try:
    base = fetch_stooq()
    out["base_source"] = "stooq"
except Exception as e:
    out["errors"].append(f"stooq: {str(e)[:70]}")
if len(base) < 5000 or (base and base[-1][0] < "2026-05-01"):
    yb = fetch_yahoo()
    if len(yb) > len(base):
        base = yb; out["base_source"] = "yahoo"
if len(base) >= 5000:
    s3.put_object(Bucket=B, Key="data/spx-history-deep.json",
                  Body=json.dumps({"id": "spx_deep", "source": out.get("base_source"), "built_at": datetime.now(timezone.utc).isoformat(),
                                   "n_points": len(base), "first": base[0][0], "last": base[-1][0], "points": base}).encode(),
                  ContentType="application/json")
    out["deep_base"] = {"n": len(base), "span": f"{base[0][0]}→{base[-1][0]}", "source": out.get("base_source")}
else:
    out["deep_base"] = f"INSUFFICIENT ({len(base)} pts) — key left untouched"

# ── B. deploy analogs v2.2, invoke, verify ──
def deploy(fn, src):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for r, _, fs in os.walk(src):
            for f in fs:
                if "__pycache__" not in r and not f.endswith(".pyc"):
                    zf.write(os.path.join(r, f), arcname=os.path.relpath(os.path.join(r, f), src))
    retry_conflict(lambda: lam.update_function_code(FunctionName=fn, ZipFile=buf.getvalue()))
    for _ in range(40):
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") in ("Successful", None): return
        time.sleep(3)

deploy("justhodl-historical-analogs", "aws/lambdas/justhodl-historical-analogs/source")
r = retry_conflict(lambda: lam.invoke(FunctionName="justhodl-historical-analogs", InvocationType="RequestResponse", Payload=b"{}"))
out["analogs_fn_error"] = r.get("FunctionError", "NONE")
time.sleep(2)
a = json.loads(s3.get_object(Bucket=B, Key="data/historical-analogs.json")["Body"].read())
yrs = sorted({x["date"][:4] for x in a.get("analogs", [])})
out["analogs_verify"] = {"version": a.get("version"), "n_pool": a.get("n_historical_dates_evaluated"),
                         "duration_s": a.get("duration_s"), "analog_years": yrs,
                         "top5": [(x["date"], x["distance"], x.get("forward_63d_pct")) for x in a.get("analogs", [])[:5]],
                         "fwd_dist": a.get("forward_distribution"), "call": a.get("directional_call"),
                         "today_spx": json.loads(a["today"]).get("spx_close") if isinstance(a.get("today"), str) else (a.get("today") or {}).get("spx_close")}

# ── C. backtester re-run (uses same deep base) ──
retry_conflict(lambda: lam.invoke(FunctionName="justhodl-alert-backtester", InvocationType="Event", Payload=b"{}"))
time.sleep(70)
try:
    bt = json.loads(s3.get_object(Bucket=B, Key="data/alert-backtests.json")["Body"].read())
    rows = bt.get("rules", bt.get("results", []))
    out["backtester_verify"] = {"generated_at": bt.get("generated_at"), "spy_span": bt.get("spy_span") or bt.get("spx_span"),
                                "rows": [{"rule": r.get("rule") or r.get("id"), "n": r.get("n_fires") or r.get("n"),
                                          "med21": r.get("median_fwd_21d_pct") or r.get("med_21d"),
                                          "first": r.get("first_fire"), "last": r.get("last_fire")} for r in rows][:12]}
except Exception as e:
    out["backtester_verify"] = str(e)[:120]

# ── D. honest walk-forward numbers ──
try:
    ws = json.loads(s3.get_object(Bucket=B, Key="backtest/summary.json")["Body"].read())
    out["walkforward"] = {"v": ws.get("v"), "generated_at": ws.get("generated_at"),
                          "summary_naive": {k: ws.get("summary", {}).get(k) for k in ("win_rate", "total_return_pct", "max_drawdown_pct", "sharpe_proxy", "alpha_vs_spy_pct")},
                          "honest_summary": ws.get("honest_summary"), "realistic_summary": ws.get("realistic_summary"),
                          "constants": ws.get("constants")}
except Exception as e:
    out["walkforward"] = str(e)[:120]

open("aws/ops/reports/1541_final.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({"base": out["deep_base"], "analogs": out["analogs_verify"], "bt_ok": not isinstance(out["backtester_verify"], str)}, default=str)[:900])
