# ops 1540 — canonical deep SPX base (runner-fetched Yahoo 1927+) → S3; deploy both consumers; verify depth + de-cluster + walk-forward shape
import json, ssl, time, io, os, zipfile, urllib.request, boto3
from datetime import datetime, timezone
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=300, retries={"max_attempts": 2})
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
_ctx = ssl.create_default_context()
out = {"ops": 1540, "errors": []}

# 1) runner fetch Yahoo ^GSPC max
pts = []
try:
    u = "https://query1.finance.yahoo.com/v8/finance/chart/%5EGSPC?range=max&interval=1d"
    req = urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"})
    j = json.loads(urllib.request.urlopen(req, timeout=60, context=_ctx).read())
    res = j["chart"]["result"][0]
    ts = res["timestamp"]; cl = res["indicators"]["quote"][0]["close"]
    for t, c in zip(ts, cl):
        if c is not None:
            pts.append([datetime.fromtimestamp(t, tz=timezone.utc).date().isoformat(), round(float(c), 2)])
    pts.sort()
except Exception as e:
    out["errors"].append(f"yahoo runner: {str(e)[:90]}")
if not pts:
    try:  # stooq fallback from runner
        req = urllib.request.Request("https://stooq.com/q/d/l/?s=%5Espx&i=d", headers={"User-Agent": "Mozilla/5.0"})
        txt = urllib.request.urlopen(req, timeout=60, context=_ctx).read().decode()
        for ln in txt.strip().split("\n")[1:]:
            c = ln.split(",")
            if len(c) >= 5 and c[0] and c[4]:
                pts.append([c[0], round(float(c[4]), 2)])
        pts.sort()
    except Exception as e:
        out["errors"].append(f"stooq runner: {str(e)[:90]}")

if pts:
    doc = {"id": "spx_deep", "label": "S&P 500 daily close (deep canonical base)", "source": "yahoo ^GSPC via ops-runner",
           "first_date": pts[0][0], "latest_date": pts[-1][0], "n_points": len(pts), "points": pts,
           "written_at": datetime.now(timezone.utc).isoformat()}
    s3.put_object(Bucket=B, Key="data/spx-history-deep.json", Body=json.dumps(doc).encode(),
                  ContentType="application/json", CacheControl="public, max-age=43200")
    out["deep_base"] = {"n": len(pts), "span": f"{pts[0][0]}→{pts[-1][0]}"}
else:
    out["deep_base"] = "FAILED — both runner sources blocked"

# 2) deploy + invoke both consumers
def zip_src(fn):
    buf = io.BytesIO(); src = f"aws/lambdas/{fn}/source"
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for r, _, fs in os.walk(src):
            for f in fs:
                if "__pycache__" not in r and not f.endswith(".pyc"):
                    zf.write(os.path.join(r, f), arcname=os.path.relpath(os.path.join(r, f), src))
    return buf.getvalue()

def retry_conflict(fn_call, tries=10, wait=8):
    for i in range(tries):
        try: return fn_call()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
                time.sleep(wait); continue
            raise
    raise RuntimeError("conflict")

for fn in ("justhodl-historical-analogs", "justhodl-alert-backtester"):
    try:
        retry_conflict(lambda: lam.update_function_code(FunctionName=fn, ZipFile=zip_src(fn)))
        for _ in range(40):
            if lam.get_function_configuration(FunctionName=fn).get("LastUpdateStatus") in ("Successful", None): break
            time.sleep(3)
        retry_conflict(lambda: lam.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}"))
    except Exception as e:
        out["errors"].append(f"{fn}: {str(e)[:80]}")

# 3) walk-forward summary shape (settles the deliverable question)
try:
    sm = json.loads(s3.get_object(Bucket=B, Key="backtest/summary.json")["Body"].read())
    out["walkforward_summary"] = {"top_keys": sorted(sm.keys()), "peek": json.dumps(sm, default=str)[:600]}
except Exception as e:
    out["walkforward_summary"] = str(e)[:90]

time.sleep(110)
# 4) verify analogs (dump real keys)
try:
    an = json.loads(s3.get_object(Bucket=B, Key="data/historical-analogs.json")["Body"].read())
    out["analogs_keys"] = sorted(an.keys())
    out["analogs_peek"] = {k: (an[k] if not isinstance(an[k], (list, dict)) else
                               (an[k][:3] if isinstance(an[k], list) else json.dumps(an[k], default=str)[:200]))
                           for k in list(an)[:14]}
except Exception as e:
    out["analogs_keys"] = str(e)[:90]
# 5) verify backtester depth
try:
    bt = json.loads(s3.get_object(Bucket=B, Key="data/alert-backtests.json")["Body"].read())
    rows = bt.get("rules") or bt.get("results") or []
    out["alertbt"] = {"spy_span": bt.get("spy_span"), "n_rules": len(rows),
                      "rows": [{"rule": r.get("rule") or r.get("id"), "n_fires": r.get("n_fires"),
                                "med21": (r.get("fwd_21d") or {}).get("median_pct"),
                                "hit21": (r.get("fwd_21d") or {}).get("pct_negative") or (r.get("fwd_21d") or {}).get("hit_rate")}
                               for r in rows]}
except Exception as e:
    out["alertbt"] = str(e)[:90]

open("aws/ops/reports/1540_deepspx.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({"deep_base": out["deep_base"], "errors": out["errors"],
                  "alertbt_span": (out.get("alertbt") or {}).get("spy_span") if isinstance(out.get("alertbt"), dict) else out.get("alertbt")}, default=str))
