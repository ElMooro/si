"""
Create justhodl-signal-portfolio Lambda + daily schedule + smoke test.
"""
import os
import time
import zipfile
import io
import json
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-signal-portfolio"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
SOURCE_DIR = "aws/lambdas/justhodl-signal-portfolio/source"
POLYGON_KEY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def make_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(SOURCE_DIR):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root:
                    continue
                full = os.path.join(root, f)
                rel = os.path.relpath(full, SOURCE_DIR)
                zf.write(full, rel)
    buf.seek(0)
    return buf.read()


def main():
    with report("create_signal_portfolio") as r:
        r.heading("Create justhodl-signal-portfolio + smoke test")

        zb = make_zip()
        r.log(f"  zip size: {len(zb):,}b")

        try:
            lam.get_function(FunctionName=LAMBDA_NAME)
            r.log(f"  function exists — updating")
            lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zb)
            time.sleep(3)
            lam.update_function_configuration(
                FunctionName=LAMBDA_NAME,
                Runtime="python3.12",
                Handler="lambda_function.lambda_handler",
                MemorySize=512,
                Timeout=600,
                Environment={"Variables": {"POLYGON_KEY": POLYGON_KEY}},
            )
            r.ok(f"  ✓ updated")
        except lam.exceptions.ResourceNotFoundException:
            lam.create_function(
                FunctionName=LAMBDA_NAME,
                Runtime="python3.12",
                Handler="lambda_function.lambda_handler",
                Role=ROLE_ARN,
                Code={"ZipFile": zb},
                MemorySize=512,
                Timeout=600,
                Environment={"Variables": {"POLYGON_KEY": POLYGON_KEY}},
                Description="Per-signal paper portfolio with PnL tracking",
            )
            r.ok(f"  ✓ created")

        r.section("EventBridge schedule (daily 22:30 UTC, after market close)")
        try:
            rule_name = f"{LAMBDA_NAME}-daily"
            events.put_rule(
                Name=rule_name,
                ScheduleExpression="cron(30 22 * * ? *)",
                State="ENABLED",
                Description=f"Daily 22:30 UTC mark-to-market + signal harvest for {LAMBDA_NAME}",
            )
            arn = lam.get_function(FunctionName=LAMBDA_NAME)["Configuration"]["FunctionArn"]
            events.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": arn}])
            try:
                lam.add_permission(
                    FunctionName=LAMBDA_NAME,
                    StatementId=f"{rule_name}-perm",
                    Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",
                    SourceArn=f"arn:aws:events:{REGION}:857687956942:rule/{rule_name}",
                )
            except lam.exceptions.ResourceConflictException:
                pass
            r.ok(f"  ✓ wired")
        except Exception as e:
            r.fail(f"  ✗ {e}")

        time.sleep(8)

        r.section("Smoke test (mtm + harvest + open new positions)")
        try:
            t0 = time.time()
            inv = lam.invoke(FunctionName=LAMBDA_NAME, Payload=b"{}")
            payload = inv["Payload"].read().decode()
            r.log(f"  status: {inv['StatusCode']} duration: {time.time()-t0:.1f}s")
            r.log(f"  resp: {payload[:600]}")
            if inv.get("FunctionError"):
                r.fail(f"  ✗ {inv['FunctionError']}")
                return
        except Exception as e:
            r.fail(f"  ✗ {e}")
            return

        r.section("S3 verify — state")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="portfolio/signal-portfolio-state.json")
            data = json.loads(obj["Body"].read())
            r.log(f"  initial_nav: ${data.get('initial_nav', 0):,.2f}")
            r.log(f"  current_nav: ${data.get('current_nav', 0):,.2f} ({data.get('current_nav_pct_chg', 0):+.2f}%)")
            r.log(f"  unrealized_pnl: ${data.get('unrealized_pnl_dollars', 0):,.2f}")
            r.log(f"  n_open: {len(data.get('open_positions', []))}")
            r.log(f"  n_closed_today: {len(data.get('recently_closed', []))}")
            r.log(f"  n_closed_total: {len(data.get('all_closed_positions', []))}")
            stats = data.get("stats", {})
            if stats:
                r.log(f"  win_rate: {stats.get('win_rate')}%")
                r.log(f"  profit_factor: {stats.get('profit_factor')}")
                r.log(f"  expectancy_pct: {stats.get('expectancy_pct')}")
                r.log(f"  max_dd_pct: {stats.get('max_drawdown_pct')}")
            r.section("Open positions sample")
            for p in (data.get("open_positions") or [])[:10]:
                r.log(
                    f"    {p['source']:18s} {p['ticker']:6s} {p['direction']:5s} "
                    f"entry=${p['entry_price']:>7.2f} now=${p['current_price']:>7.2f} "
                    f"pnl={p['current_pnl_pct']:>+5.2f}% qty={p['qty']}"
                )
            r.section("By-source stats")
            for src, v in (stats.get("by_source") or {}).items():
                r.log(f"    {src:20s} n={v.get('n')} win_rate={v.get('win_rate')}% avg_pnl={v.get('avg_pnl_pct')}% total=${v.get('total_pnl_dollars'):.2f}")
        except Exception as e:
            r.fail(f"  ✗ {e}")


if __name__ == "__main__":
    main()
