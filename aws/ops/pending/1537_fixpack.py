# ops 1537 — institutional fix-pack: kill-switch live, apex closed-loop canonical, regime scorecard,
# analogs deep-pool, alert-backtester, dark-rule re-enables
import json, os, time, zipfile, io, sys
from pathlib import Path
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
sys.path.insert(0, "aws/ops")
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda, ensure_eb_rule

cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
ssm = boto3.client("ssm", region_name="us-east-1", config=cfg)
dd = boto3.client("dynamodb", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
res = {"ops": 1537}

def rd(k):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=k)["Body"].read())
    except Exception as e:
        return {"_err": str(e)[:70]}

with report("1537-fixpack") as r:
    r.heading("Ops 1537 — institutional fix-pack")

    # 0) SSM kill-switch flag
    try:
        ssm.get_parameter(Name="/justhodl/kill-switch")
        res["flag"] = "existed"
    except ssm.exceptions.ParameterNotFound:
        ssm.put_parameter(Name="/justhodl/kill-switch", Value="OFF", Type="String")
        res["flag"] = "created OFF"
    r.ok(f"kill-switch flag: {res['flag']}")

    # 1) kill-switch lambda + 10-min check
    r.section("A. kill-switch")
    deploy_lambda(report=r, function_name="justhodl-kill-switch",
                  source_dir=Path("aws/lambdas/justhodl-kill-switch/source"),
                  env_vars={"S3_BUCKET": B},
                  eb_rule_name="justhodl-kill-switch-check", eb_schedule="rate(10 minutes)",
                  timeout=120, memory=256, description="Blast-radius governor: SSM-flag fleet halt/restore",
                  reserved_concurrency=1, create_function_url=False, smoke=True)
    rr = lam.invoke(FunctionName="justhodl-kill-switch", InvocationType="RequestResponse", Payload=b"{}")
    res["kill_switch_check"] = rr["Payload"].read().decode()[:160]

    # 2) apex v1.2 (smoke runs + logs canonical items)
    r.section("B. apex-fusion v1.2")
    deploy_lambda(report=r, function_name="justhodl-apex-fusion",
                  source_dir=Path("aws/lambdas/justhodl-apex-fusion/source"),
                  env_vars={"S3_BUCKET": B, "SIGNALS_TABLE": "justhodl-signals"},
                  timeout=120, memory=512, description="Learned cross-engine pump conviction (canonical loop v1.2)",
                  reserved_concurrency=1, create_function_url=False, smoke=False)
    rr = lam.invoke(FunctionName="justhodl-apex-fusion", InvocationType="RequestResponse",
                    Payload=json.dumps({"no_tg": True}).encode())
    res["apex_invoke"] = {"err": rr.get("FunctionError", "NONE"), "resp": rr["Payload"].read().decode()[:140]}

    # 3) outcome-checker + signal-scorecard (regime end-to-end) — async
    r.section("C. checker + scorecard")
    for fn, src in (("justhodl-outcome-checker", "aws/lambdas/justhodl-outcome-checker/source"),
                    ("justhodl-signal-scorecard", "aws/lambdas/justhodl-signal-scorecard/source")):
        deploy_lambda(report=r, function_name=fn, source_dir=Path(src),
                      env_vars={}, timeout=600, memory=1024,
                      description=f"{fn} (regime-conditioned, ops 1537)",
                      reserved_concurrency=1, create_function_url=False, smoke=False)
        lam.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
    r.ok("checker + scorecard deployed, async kicked")

    # 4) analogs v2 — async
    r.section("D. historical-analogs v2")
    deploy_lambda(report=r, function_name="justhodl-historical-analogs",
                  source_dir=Path("aws/lambdas/justhodl-historical-analogs/source"),
                  env_vars={"FRED_KEY": "2f057499936072679d8843d7fce99989",
                            "POLYGON_KEY": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"},
                  timeout=300, memory=1024, description="k-NN regime analogs v2 (Polygon SPY 1999+ pool)",
                  reserved_concurrency=1, create_function_url=False, smoke=False)
    lam.invoke(FunctionName="justhodl-historical-analogs", InvocationType="Event", Payload=b"{}")

    # 5) alert-backtester — new, daily 12 UTC, async first run
    r.section("E. alert-backtester")
    deploy_lambda(report=r, function_name="justhodl-alert-backtester",
                  source_dir=Path("aws/lambdas/justhodl-alert-backtester/source"),
                  env_vars={"S3_BUCKET": B, "FRED_KEY": "2f057499936072679d8843d7fce99989",
                            "POLYGON_KEY": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"},
                  eb_rule_name="justhodl-alert-backtester-daily", eb_schedule="cron(0 12 * * ? *)",
                  timeout=600, memory=1024, description="Per-rule historical alert edge audit",
                  reserved_concurrency=1, create_function_url=False, smoke=False)
    lam.invoke(FunctionName="justhodl-alert-backtester", InvocationType="Event", Payload=b"{}")

    # 6) re-enable dark daily-light rules
    r.section("F. dark-rule re-enables")
    PFX = ("justhodl-russell-recon", "justhodl-opex-calendar", "justhodl-stablecoin-flow",
           "justhodl-breadth-thrust", "justhodl-rv-iv", "justhodl-insider-buys-enriched",
           "justhodl-buyback-scanner", "justhodl-freshness", "justhodl-fleet-monitor",
           "justhodl-vol-surface")
    rules, tok = [], None
    while True:
        kw = {"Limit": 100}
        if tok: kw["NextToken"] = tok
        rr2 = ev.list_rules(**kw)
        rules += rr2["Rules"]; tok = rr2.get("NextToken")
        if not tok: break
    enabled = []
    for rule in rules:
        if rule["State"] == "DISABLED" and any(rule["Name"].startswith(p) for p in PFX):
            try:
                if rule["Name"].startswith("justhodl-vol-surface"):
                    ev.put_rule(Name=rule["Name"], ScheduleExpression="cron(30 13 * * ? *)", State="ENABLED")
                else:
                    ev.enable_rule(Name=rule["Name"])
                enabled.append(rule["Name"])
            except Exception as e:
                r.log(f"  {rule['Name']}: {str(e)[:60]}")
    res["rules_enabled"] = enabled
    r.ok(f"re-enabled {len(enabled)}: {enabled}")

    # 7) settle + verify
    r.section("G. verify")
    time.sleep(110)
    today = time.strftime("%Y-%m-%d", time.gmtime())
    ax = rd("data/apex-fusion.json")
    top = next((t for t in (ax.get("top") or []) if t.get("tier") in ("LIFTOFF", "IGNITION")), {})
    res["apex"] = {"version": ax.get("version"), "n_logged": ax.get("n_logged_to_ddb"),
                   "log_errors": ax.get("log_errors"), "top1": {k: top.get(k) for k in ("ticker", "tier")}}
    if top.get("ticker"):
        try:
            it = dd.get_item(TableName="justhodl-signals",
                             Key={"signal_id": {"S": f"apex-fusion#{top['ticker']}#{today}"}}).get("Item")
            res["apex_item"] = {"found": bool(it),
                                "fields": sorted(it.keys()) if it else None,
                                "status": (it or {}).get("status", {}).get("S"),
                                "signal_type": (it or {}).get("signal_type", {}).get("S")}
        except Exception as e:
            res["apex_item"] = str(e)[:90]
    an = rd("data/historical-analogs.json")
    res["analogs"] = {"version": an.get("version"), "n_dates": an.get("n_historical_dates_evaluated"),
                      "first_analog": (an.get("analogs") or [{}])[0].get("date"),
                      "fwd": an.get("forward_distribution"), "call": an.get("directional_call")}
    bt = rd("data/alert-backtests.json")
    res["alert_bt"] = {"n_rules": bt.get("n_rules"), "spy_span": bt.get("spy_span"),
                       "sample": [(x.get("id"), x.get("n_fires"),
                                   ((x.get("forward_spy") or {}).get("21d") or {}).get("median_pct"))
                                  for x in (bt.get("rules") or [])[:6]],
                       "err": bt.get("_err")}
    sc = rd("data/signal-scorecard.json")
    rows = sc.get("scorecard") or []
    wr = next((x for x in rows if x.get("by_regime")), None)
    res["scorecard"] = {"age_ok": sc.get("generated_at"), "n_rows": len(rows),
                        "regime_sample": {"signal_type": wr.get("signal_type"), "by_regime": wr.get("by_regime")} if wr else "PENDING (async)"}
    res["backtest_summary_peek"] = {k: v for k, v in (rd("data/backtest-summary.json") or {}).items()
                                    if k in ("generated_at", "sharpe", "net_sharpe", "gross_sharpe", "method",
                                             "annualized_return_pct", "max_drawdown_pct", "cost_model", "n_trades",
                                             "total_return_pct", "win_rate", "version", "methodology")}

open("aws/ops/reports/1537_fixpack.json", "w").write(json.dumps(res, indent=2, default=str))
print(json.dumps({"kill": res["kill_switch_check"][:60], "apex_item": res.get("apex_item", {}),
                  "analogs_first": res["analogs"].get("first_analog"), "bt_rules": res["alert_bt"].get("n_rules"),
                  "rules_on": len(res["rules_enabled"])}, default=str))
