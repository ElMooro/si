# ops 1538 — deep-SPX fix (stooq), redeploy analogs+backtester, contains-match rule enables, verify
import json, os, time, zipfile, io, boto3
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1538}

def retry_conflict(fn, tries=10, wait=8):
    for i in range(tries):
        try: return fn()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
                time.sleep(wait); continue
            raise
    raise RuntimeError("exhausted")

def zip_src(src):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for r, _, fs in os.walk(src):
            for f in fs:
                if "__pycache__" not in r and not f.endswith(".pyc"):
                    zf.write(os.path.join(r, f), arcname=os.path.relpath(os.path.join(r, f), src))
    return buf.getvalue()

def rd(k):
    try: return json.loads(s3.get_object(Bucket=B, Key=k)["Body"].read())
    except Exception as e: return {"_err": str(e)[:70]}

for fn, src in (("justhodl-historical-analogs", "aws/lambdas/justhodl-historical-analogs/source"),
                ("justhodl-alert-backtester", "aws/lambdas/justhodl-alert-backtester/source")):
    retry_conflict(lambda: lam.update_function_code(FunctionName=fn, ZipFile=zip_src(src)))
    for _ in range(40):
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") in ("Successful", None): break
        time.sleep(3)
    lam.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
out["deployed"] = ["analogs", "backtester"]

# contains-token rule enables
TOK = ("russell-recon", "opex-calendar", "stablecoin", "breadth-thrust", "rv-iv",
       "insider-buys-enriched", "buyback-scanner", "freshness", "fleet-monitor", "vol-surface")
rules, tok = [], None
while True:
    kw = {"Limit": 100}
    if tok: kw["NextToken"] = tok
    rr = ev.list_rules(**kw)
    rules += rr["Rules"]; tok = rr.get("NextToken")
    if not tok: break
out["disabled_all"] = sorted(r["Name"] for r in rules if r["State"] == "DISABLED")
enabled = []
for rule in rules:
    nm = rule["Name"].lower()
    if rule["State"] == "DISABLED" and any(t in nm for t in TOK):
        try:
            if "vol-surface" in nm:
                ev.put_rule(Name=rule["Name"], ScheduleExpression="cron(30 13 * * ? *)", State="ENABLED")
            else:
                ev.enable_rule(Name=rule["Name"])
            enabled.append(rule["Name"])
        except Exception as e:
            enabled.append(f"{rule['Name']} ERR {str(e)[:40]}")
out["rules_enabled"] = enabled

bs = rd("data/backtest-summary.json")
out["backtest_summary_keys"] = list(bs.keys())[:20]
out["backtest_summary_head"] = {k: bs[k] for k in list(bs.keys())[:8] if not isinstance(bs[k], (list, dict))}

time.sleep(120)
an = rd("data/historical-analogs.json")
out["analogs"] = {"n_dates": an.get("n_historical_dates_evaluated"),
                  "analog_dates": [a.get("date") for a in (an.get("analogs") or [])[:8]],
                  "fwd21": (an.get("forward_distribution") or {}).get("21d"),
                  "call": an.get("directional_call")}
bt = rd("data/alert-backtests.json")
out["alert_bt"] = {"spy_span": bt.get("spy_span"), "n_rules": bt.get("n_rules"),
                   "sample": [(x.get("id"), x.get("since"), x.get("n_fires"), x.get("last_fired"),
                               ((x.get("forward_spy") or {}).get("21d") or {}).get("median_pct"),
                               ((x.get("forward_spy") or {}).get("21d") or {}).get("pct_negative"))
                              for x in (bt.get("rules") or [])]}
open("aws/ops/reports/1538_depth.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({"rules_on": out["rules_enabled"], "spy": out["alert_bt"]["spy_span"],
                  "analog_dates": out["analogs"]["analog_dates"][:4]}, default=str))
