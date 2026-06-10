# ops 1541 — validated deep-SPX base (yahoo→stooq, n-gated), re-run consumers, read realistic walk-forward
import json, ssl, time, urllib.request, boto3
from datetime import datetime, timezone
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=300, retries={"max_attempts": 2})
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
_ctx = ssl.create_default_context()
out = {"ops": 1541, "errors": [], "tried": []}

def yahoo():
    u = "https://query1.finance.yahoo.com/v8/finance/chart/%5EGSPC?interval=1d&range=max&includePrePost=false"
    req = urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36", "Accept": "application/json"})
    j = json.loads(urllib.request.urlopen(req, timeout=60, context=_ctx).read())
    res = j["chart"]["result"][0]
    pts = [[datetime.fromtimestamp(t, tz=timezone.utc).date().isoformat(), round(float(c), 2)]
           for t, c in zip(res["timestamp"], res["indicators"]["quote"][0]["close"]) if c is not None]
    pts.sort(); return pts

def stooq():
    req = urllib.request.Request("https://stooq.com/q/d/l/?s=%5Espx&i=d", headers={"User-Agent": "Mozilla/5.0"})
    txt = urllib.request.urlopen(req, timeout=60, context=_ctx).read().decode("utf-8", "replace")
    pts = []
    for ln in txt.strip().split("\n")[1:]:
        c = ln.split(",")
        if len(c) >= 5 and c[0] and c[4]:
            try: pts.append([c[0], round(float(c[4]), 2)])
            except ValueError: pass
    pts.sort(); return pts

pts = []
for name, fn in (("yahoo", yahoo), ("stooq", stooq)):
    try:
        cand = fn()
        out["tried"].append({name: len(cand)})
        if len(cand) > 10000:
            pts = cand; out["source"] = name; break
    except Exception as e:
        out["tried"].append({name: str(e)[:80]})

if pts:
    doc = {"id": "spx_deep", "label": "S&P 500 daily close (deep canonical base)", "source": out["source"],
           "first_date": pts[0][0], "latest_date": pts[-1][0], "n_points": len(pts), "points": pts,
           "written_at": datetime.now(timezone.utc).isoformat()}
    s3.put_object(Bucket=B, Key="data/spx-history-deep.json", Body=json.dumps(doc).encode(),
                  ContentType="application/json", CacheControl="public, max-age=43200")
    out["deep_base"] = {"n": len(pts), "span": f"{pts[0][0]}→{pts[-1][0]}", "source": out["source"]}
    def retry_conflict(fn_call, tries=8, wait=8):
        for i in range(tries):
            try: return fn_call()
            except ClientError as e:
                if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
                    time.sleep(wait); continue
                raise
    for fn in ("justhodl-historical-analogs", "justhodl-alert-backtester"):
        try: retry_conflict(lambda: lam.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}"))
        except Exception as e: out["errors"].append(f"invoke {fn}: {str(e)[:70]}")
else:
    out["deep_base"] = "ALL SOURCES FAILED FROM RUNNER"

# realistic walk-forward numbers (independent of the above)
try:
    sm = json.loads(s3.get_object(Bucket=B, Key="backtest/summary.json")["Body"].read())
    out["wf_realistic"] = sm.get("realistic_summary")
    out["wf_honest"] = sm.get("honest_summary")
    out["wf_walkforward"] = sm.get("walkforward_summary")
    out["wf_top5"] = sm.get("top_5_contributors")
    out["wf_bottom5"] = sm.get("bottom_5_contributors")
except Exception as e:
    out["wf_realistic"] = str(e)[:90]

time.sleep(115)
try:
    an = json.loads(s3.get_object(Bucket=B, Key="data/historical-analogs.json")["Body"].read())
    yrs = sorted({a["date"][:4] for a in (an.get("analogs") or [])})
    out["analogs_verify"] = {"version": an.get("version"), "n_eval": an.get("n_historical_dates_evaluated"),
                             "analog_years": yrs, "sources": an.get("data_sources"),
                             "call": an.get("directional_call")}
except Exception as e:
    out["analogs_verify"] = str(e)[:90]
try:
    bt = json.loads(s3.get_object(Bucket=B, Key="data/alert-backtests.json")["Body"].read())
    rows = bt.get("rules") or bt.get("results") or []
    out["alertbt"] = {"spy_span": bt.get("spy_span"),
                      "rows": [{"rule": r.get("rule") or r.get("id"), "n": r.get("n_fires"),
                                "med21": (r.get("fwd_21d") or {}).get("median_pct")} for r in rows]}
except Exception as e:
    out["alertbt"] = str(e)[:90]

open("aws/ops/reports/1541_base.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({"deep_base": out["deep_base"], "tried": out["tried"],
                  "alertbt_span": (out.get("alertbt") or {}).get("spy_span") if isinstance(out.get("alertbt"), dict) else out.get("alertbt"),
                  "analog_years": (out.get("analogs_verify") or {}).get("analog_years") if isinstance(out.get("analogs_verify"), dict) else None}, default=str))
