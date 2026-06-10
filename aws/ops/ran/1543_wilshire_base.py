# ops 1543 — deep base from FRED Wilshire 5000 daily 1971+ (WILL5000PR), lambda splices SPY on top; final verify
import json, time, urllib.request, boto3
from datetime import datetime, timezone
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
FRED = "2f057499936072679d8843d7fce99989"
out = {"ops": 1543, "errors": [], "tried": []}


def retry_conflict(fn, tries=10, wait=8):
    for i in range(tries):
        try:
            return fn()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
                time.sleep(wait); continue
            raise
    raise RuntimeError("retries exhausted")


def fred_series(sid, start="1971-01-01"):
    u = (f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}"
         f"&api_key={FRED}&file_type=json&observation_start={start}&limit=100000")
    j = json.loads(urllib.request.urlopen(u, timeout=90).read())
    pts = []
    for o in j.get("observations", []):
        if o.get("value") not in (".", "", None):
            try: pts.append([o["date"], round(float(o["value"]), 2)])
            except ValueError: pass
    pts.sort(); return pts

base, src = [], None
for sid in ("WILL5000PR", "WILL5000IND", "NASDAQCOM"):
    try:
        pts = fred_series(sid)
        out["tried"].append({sid: {"n": len(pts), "span": f"{pts[0][0]}→{pts[-1][0]}" if pts else "—"}})
        if len(pts) >= 8000:
            base, src = pts, sid
            break
    except Exception as e:
        out["tried"].append({sid: str(e)[:80]})

if base:
    s3.put_object(Bucket=B, Key="data/spx-history-deep.json",
                  Body=json.dumps({"id": "spx_deep", "source": f"FRED_{src}", "units": src,
                                   "built_at": datetime.now(timezone.utc).isoformat(), "n_points": len(base),
                                   "first": base[0][0], "last": base[-1][0],
                                   "note": "US broad-equity daily composite: FRED base; recent SPY ratio-spliced on top by consumers. Forwards are %, unit-free.",
                                   "points": base}).encode(), ContentType="application/json")
    out["deep_base"] = {"n": len(base), "span": f"{base[0][0]}→{base[-1][0]}", "source": src}
else:
    out["deep_base"] = "ALL FRED EQUITY SERIES INSUFFICIENT"

# re-run analogs (sync) + backtester (async), verify
r = retry_conflict(lambda: lam.invoke(FunctionName="justhodl-historical-analogs", InvocationType="RequestResponse", Payload=b"{}"))
out["analogs_fn_error"] = r.get("FunctionError", "NONE")
time.sleep(2)
a = json.loads(s3.get_object(Bucket=B, Key="data/historical-analogs.json")["Body"].read())
yrs = sorted({x["date"][:4] for x in a.get("analogs", [])})
out["analogs_verify"] = {"version": a.get("version"), "n_pool": a.get("n_historical_dates_evaluated"),
                         "analog_years": yrs, "duration_s": a.get("duration_s"),
                         "top5": [(x["date"], x["distance"], x.get("forward_63d_pct")) for x in a.get("analogs", [])[:5]],
                         "fwd_21d": (a.get("forward_distribution") or {}).get("21d"),
                         "fwd_63d": (a.get("forward_distribution") or {}).get("63d"),
                         "call": a.get("directional_call")}

retry_conflict(lambda: lam.invoke(FunctionName="justhodl-alert-backtester", InvocationType="Event", Payload=b"{}"))
time.sleep(80)
bt = json.loads(s3.get_object(Bucket=B, Key="data/alert-backtests.json")["Body"].read())
rows = bt.get("rules") or []
out["bt_span"] = bt.get("spy_span") or bt.get("spx_span") or bt.get("price_span")
out["bt_summary"] = [{"id": r["id"], "n": r.get("n_fires"), "last": r.get("last_fired"),
                      "med21": ((r.get("forward_spy") or {}).get("21d") or {}).get("median_pct"),
                      "n21": ((r.get("forward_spy") or {}).get("21d") or {}).get("n"),
                      "med63": ((r.get("forward_spy") or {}).get("63d") or {}).get("median_pct"),
                      "neg63": ((r.get("forward_spy") or {}).get("63d") or {}).get("pct_negative")} for r in rows]

open("aws/ops/reports/1543_wilshire.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({"base": out["deep_base"], "pool": out["analogs_verify"]["n_pool"], "years": yrs,
                  "bt_span": out["bt_span"], "bt_n": [(x['id'], x['n'], x['n21']) for x in out['bt_summary'][:6]]}, default=str)[:700])
