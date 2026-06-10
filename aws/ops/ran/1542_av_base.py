# ops 1542 — canonical deep base via AlphaVantage SPY full (1999+), re-run analogs+backtester, dump real schemas
import json, os, time, urllib.request, boto3
from datetime import datetime, timezone
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
AV = "EOLGKSGAYZUXKPUL"
out = {"ops": 1542, "errors": []}


def retry_conflict(fn, tries=10, wait=8):
    for i in range(tries):
        try:
            return fn()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
                time.sleep(wait); continue
            raise
    raise RuntimeError("retries exhausted")


# ── A. AlphaVantage SPY full daily ──
base = []
try:
    u = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=SPY&outputsize=full&apikey={AV}"
    j = json.loads(urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0"}), timeout=90).read())
    ts = j.get("Time Series (Daily)") or {}
    if not ts:
        out["errors"].append(f"AV keys: {list(j.keys())[:3]} | {str(j)[:160]}")
    base = sorted([[d, round(float(v["4. close"]), 2)] for d, v in ts.items()])
except Exception as e:
    out["errors"].append(f"AV: {str(e)[:90]}")

if len(base) >= 5000 and base[-1][0] >= "2026-05-01":
    s3.put_object(Bucket=B, Key="data/spx-history-deep.json",
                  Body=json.dumps({"id": "spx_deep", "source": "alphavantage_SPY", "units": "SPY",
                                   "built_at": datetime.now(timezone.utc).isoformat(), "n_points": len(base),
                                   "first": base[0][0], "last": base[-1][0],
                                   "note": "Pre-1999 unavailable from free reachable sources (Yahoo/Stooq block AWS+GH). Forwards begin 1999.",
                                   "points": base}).encode(), ContentType="application/json")
    out["deep_base"] = {"n": len(base), "span": f"{base[0][0]}→{base[-1][0]}", "source": "alphavantage_SPY"}
else:
    out["deep_base"] = f"INSUFFICIENT n={len(base)} last={base[-1][0] if base else '—'}"

# ── B. re-run analogs ──
r = retry_conflict(lambda: lam.invoke(FunctionName="justhodl-historical-analogs", InvocationType="RequestResponse", Payload=b"{}"))
out["analogs_fn_error"] = r.get("FunctionError", "NONE")
time.sleep(2)
a = json.loads(s3.get_object(Bucket=B, Key="data/historical-analogs.json")["Body"].read())
yrs = sorted({x["date"][:4] for x in a.get("analogs", [])})
out["analogs_verify"] = {"version": a.get("version"), "n_pool": a.get("n_historical_dates_evaluated"),
                         "analog_years": yrs,
                         "top5": [(x["date"], x["distance"], x.get("forward_63d_pct")) for x in a.get("analogs", [])[:5]],
                         "fwd_21d": (a.get("forward_distribution") or {}).get("21d"),
                         "call": a.get("directional_call"), "duration_s": a.get("duration_s")}

# ── C. re-run backtester + dump REAL schema ──
retry_conflict(lambda: lam.invoke(FunctionName="justhodl-alert-backtester", InvocationType="Event", Payload=b"{}"))
time.sleep(75)
try:
    bt = json.loads(s3.get_object(Bucket=B, Key="data/alert-backtests.json")["Body"].read())
    out["bt_top_keys"] = sorted(bt.keys())
    rows = bt.get("rules") or bt.get("results") or []
    out["bt_row_schema"] = sorted(rows[0].keys()) if rows else []
    out["bt_rows_raw"] = rows[:4]
    out["bt_span"] = bt.get("spy_span") or bt.get("spx_span") or bt.get("price_span")
except Exception as e:
    out["bt_error"] = str(e)[:120]

open("aws/ops/reports/1542_av.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({"base": out["deep_base"], "pool": out["analogs_verify"]["n_pool"],
                  "years": out["analogs_verify"]["analog_years"], "bt_span": out.get("bt_span"), "err": out["errors"][:2]}, default=str))
