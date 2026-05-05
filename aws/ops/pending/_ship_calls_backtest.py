"""Ship #3 — justhodl-calls-backtest. Create Lambda, schedule cron(15 14 * * ? *),
smoke-invoke, verify backtest/calls-results.json output."""
import io
import json
import os
import time
import zipfile
import boto3
from ops_report import report

REGION = "us-east-1"
ACCOUNT = "857687956942"
FUNC = "justhodl-calls-backtest"
SOURCE_DIR = "aws/lambdas/justhodl-calls-backtest/source"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"
SCHEDULE_RULE = f"{FUNC}-daily"
SCHEDULE_EXPR = "cron(15 14 * * ? *)"  # 14:15 UTC daily — after position-sizer (14:00) but before backtest engine (every 6h)
POLYGON_KEY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"

LAM = boto3.client("lambda", region_name=REGION)
EVENTS = boto3.client("events", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)


def make_zip(source_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(source_dir):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root:
                    continue
                full = os.path.join(root, f)
                rel = os.path.relpath(full, source_dir)
                zf.write(full, rel)
    buf.seek(0)
    return buf.read()


def main():
    with report("ship_calls_backtest") as r:
        # 1. Create or update Lambda
        r.heading("1) Create or update Lambda")
        zb = make_zip(SOURCE_DIR)
        r.log(f"  zip size: {len(zb):,}b")

        try:
            cfg = LAM.get_function(FunctionName=FUNC)["Configuration"]
            r.log(f"  ↻ updating existing function (last mod {cfg.get('LastModified')})")
            LAM.update_function_code(FunctionName=FUNC, ZipFile=zb)
            for _ in range(25):
                cfg = LAM.get_function(FunctionName=FUNC)["Configuration"]
                if cfg.get("LastUpdateStatus") in (None, "Successful"):
                    break
                time.sleep(2)
            LAM.update_function_configuration(
                FunctionName=FUNC,
                Timeout=180,
                MemorySize=512,
                Environment={"Variables": {"POLYGON_KEY": POLYGON_KEY}},
            )
            for _ in range(25):
                cfg = LAM.get_function(FunctionName=FUNC)["Configuration"]
                if cfg.get("LastUpdateStatus") in (None, "Successful"):
                    break
                time.sleep(2)
            r.ok("  ✓ updated")
        except LAM.exceptions.ResourceNotFoundException:
            r.log(f"  + creating fresh function")
            resp = LAM.create_function(
                FunctionName=FUNC,
                Runtime="python3.11",
                Role=ROLE_ARN,
                Handler="lambda_function.lambda_handler",
                Code={"ZipFile": zb},
                Timeout=180,
                MemorySize=512,
                Environment={"Variables": {"POLYGON_KEY": POLYGON_KEY}},
                Description="Replays decisive-call ledger as SPY-exposure backtest. Daily.",
            )
            r.ok(f"  ✓ created: {resp.get('FunctionArn')}")
            for _ in range(20):
                cfg = LAM.get_function(FunctionName=FUNC)["Configuration"]
                if cfg.get("State") == "Active":
                    break
                time.sleep(2)

        # 2. Schedule EventBridge rule
        r.heading("2) EventBridge schedule")
        EVENTS.put_rule(
            Name=SCHEDULE_RULE,
            ScheduleExpression=SCHEDULE_EXPR,
            State="ENABLED",
            Description="Daily 14:15 UTC — replay decisive-call ledger as SPY-exposure backtest.",
        )
        rule_arn = EVENTS.describe_rule(Name=SCHEDULE_RULE)["Arn"]
        r.log(f"  rule: {SCHEDULE_RULE}  expr={SCHEDULE_EXPR}")

        # Permission
        try:
            LAM.add_permission(
                FunctionName=FUNC,
                StatementId=f"{SCHEDULE_RULE}-invoke",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=rule_arn,
            )
            r.ok("  ✓ added invoke permission")
        except LAM.exceptions.ResourceConflictException:
            r.log("  ✓ invoke permission already exists")

        # Target
        EVENTS.put_targets(
            Rule=SCHEDULE_RULE,
            Targets=[{"Id": "1", "Arn": f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{FUNC}"}],
        )
        r.ok("  ✓ target set")

        # 3. Smoke invoke
        r.heading("3) Smoke invoke")
        t0 = time.time()
        resp = LAM.invoke(FunctionName=FUNC, InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
        try:
            outer = json.loads(body)
            inner = json.loads(outer.get("body", "{}"))
            r.log(f"  ok: {inner.get('ok')}")
            r.log(f"  n_calls: {inner.get('n_calls')}")
            r.log(f"  n_trading_days: {inner.get('n_trading_days')}")
            r.log(f"  total_return_pct: {inner.get('total_return_pct')}")
            r.log(f"  spy_return_pct: {inner.get('spy_return_pct')}")
            r.log(f"  alpha_vs_spy_pct: {inner.get('alpha_vs_spy_pct')}")
            r.log(f"  max_dd_pct: {inner.get('max_dd_pct')}")
        except Exception as e:
            r.log(f"  parse: {e}")
            r.log(f"  raw: {body[:500]}")

        # 4. Verify S3 output
        r.heading("4) Verify backtest/calls-results.json")
        try:
            obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="backtest/calls-results.json")
            d = json.loads(obj["Body"].read())
            r.ok(f"  ✓ written  ({obj['ContentLength']:,}b mod={obj['LastModified']})")
            summ = d.get("summary") or {}
            r.log(f"  method: {d.get('method')}")
            r.log(f"  n_calls: {summ.get('n_calls')}")
            r.log(f"  first_call: {summ.get('first_call_date')}")
            r.log(f"  last_date: {summ.get('last_date')}")
            r.log(f"  n_changes: {summ.get('n_changes')}")
            r.log(f"  total_return_pct: {summ.get('total_return_pct')}")
            r.log(f"  spy_return_pct: {summ.get('spy_return_pct')}")
            r.log(f"  alpha_vs_spy_pct: {summ.get('alpha_vs_spy_pct')}")
            r.log(f"  sharpe_proxy: {summ.get('sharpe_proxy')}")
            r.log(f"  max_dd_pct: {summ.get('max_drawdown_pct')}")
            r.log("")
            r.log("  Per-call breakdown:")
            for c in (d.get("calls") or [])[:20]:
                r.log(f"    {c.get('verb'):16s}  {c.get('start_date')} → {c.get('end_date')}  "
                      f"days={c.get('n_days')} expo={c.get('exposure'):.2f} "
                      f"SPY={c.get('spy_change_pct'):+.3f}% strat={c.get('strat_change_pct'):+.3f}%")
            r.log("")
            r.log(f"  nav_curve has {len(d.get('nav_curve') or [])} datapoints")
            for n in (d.get("nav_curve") or [])[:5]:
                r.log(f"    {n.get('date')}  nav={n.get('nav')}  spy_nav={n.get('spy_nav')} active={n.get('active_verb')}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # 5. Verify backtest.html has calls section
        r.heading("5) Verify backtest.html section")
        try:
            with open("backtest.html", "r", encoding="utf-8") as f:
                html = f.read()
            checks = [
                ("Calls section heading", "Brief calls backtest" in html),
                ("loadCallsBacktest", "loadCallsBacktest" in html),
                ("renderCallsNavChart", "renderCallsNavChart" in html),
                ("VERB_COLOR map", "VERB_COLOR" in html),
                ("calls-results.json fetch", "calls-results.json" in html),
                ("calls-table tbody", 'id="calls-body"' in html),
                ("calls-nav-chart svg", 'id="calls-nav-chart"' in html),
            ]
            for label, ok in checks:
                r.log(f"    {'✓' if ok else '✗'} {label}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
