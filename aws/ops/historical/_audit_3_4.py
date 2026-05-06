"""Audit deployed state of #3 (calls-backtest) and #4 (realistic-backtest improvements)
before building. Never duplicate work."""
import json
import boto3
from ops_report import report

REGION = "us-east-1"
LAM = boto3.client("lambda", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)
EVENTS = boto3.client("events", region_name=REGION)


def main():
    with report("audit_3_4") as r:
        # ---------- #3: Calls-backtest ----------
        r.heading("#3 — Calls-backtest deployment state")
        try:
            cfg = LAM.get_function(FunctionName="justhodl-calls-backtest")["Configuration"]
            r.ok(f"  Lambda exists: {cfg['FunctionName']}")
            r.log(f"    runtime: {cfg.get('Runtime')}")
            r.log(f"    last modified: {cfg.get('LastModified')}")
            r.log(f"    state: {cfg.get('State')} ({cfg.get('LastUpdateStatus')})")
            r.log(f"    timeout: {cfg.get('Timeout')}s, mem: {cfg.get('MemorySize')}MB")
            envs = (cfg.get("Environment") or {}).get("Variables") or {}
            r.log(f"    env keys: {list(envs.keys())}")
            r.log(f"    has POLYGON_KEY: {'POLYGON_KEY' in envs or 'POLYGON_API_KEY' in envs}")
        except LAM.exceptions.ResourceNotFoundException:
            r.log("  ✗ Lambda DOES NOT EXIST — needs creation")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # EventBridge schedule
        r.log("")
        r.log("  EventBridge schedule:")
        try:
            rules = EVENTS.list_rule_names_by_target(
                TargetArn=f"arn:aws:lambda:{REGION}:857687956942:function:justhodl-calls-backtest"
            )
            for name in rules.get("RuleNames", []):
                rule = EVENTS.describe_rule(Name=name)
                r.log(f"    {name}: {rule.get('ScheduleExpression')}  state={rule.get('State')}")
            if not rules.get("RuleNames"):
                r.log("    ✗ No EventBridge rule targets this Lambda")
        except Exception as e:
            r.log(f"    ✗ {e}")

        # S3 output
        r.log("")
        r.log("  S3 output:")
        try:
            obj = S3.head_object(Bucket="justhodl-dashboard-live", Key="backtest/calls-results.json")
            r.log(f"    ✓ backtest/calls-results.json exists ({obj['ContentLength']:,}b, mod={obj['LastModified']})")
            data = json.loads(S3.get_object(Bucket="justhodl-dashboard-live", Key="backtest/calls-results.json")["Body"].read())
            summ = data.get("summary") or {}
            r.log(f"    method: {summ.get('method') or '—'}")
            r.log(f"    n_calls: {summ.get('n_calls')}")
            r.log(f"    n_trading_days: {summ.get('n_trading_days')}")
            r.log(f"    total_return_pct: {summ.get('total_return_pct')}")
            r.log(f"    spy_return_pct: {summ.get('spy_return_pct')}")
            r.log(f"    alpha_pct: {summ.get('alpha_pct')}")
        except S3.exceptions.NoSuchKey:
            r.log("    ✗ backtest/calls-results.json does NOT exist — Lambda hasn't run successfully")
        except Exception as e:
            r.log(f"    ✗ {e}")

        # backtest.html — does it have the calls-backtest section?
        r.log("")
        r.log("  backtest.html calls-backtest section:")
        try:
            html_bytes = S3.get_object(Bucket="justhodl-dashboard-live", Key="backtest.html")["Body"].read()
            html = html_bytes.decode("utf-8", errors="replace")
            checks = [
                ("loadCallsBacktest function", "loadCallsBacktest" in html),
                ("renderCallsNavChart function", "renderCallsNavChart" in html),
                ("calls-results.json fetch", "calls-results.json" in html),
                ("Brief calls section heading", "Brief calls" in html or "calls backtest" in html.lower()),
                ("VERB_COLOR map", "VERB_COLOR" in html),
            ]
            for label, ok in checks:
                r.log(f"    {'✓' if ok else '✗'} {label}")
        except Exception as e:
            r.log(f"    ✗ {e}")

        # Local source check
        r.log("")
        r.log("  Local source (aws/lambdas/justhodl-calls-backtest/source/lambda_function.py):")
        try:
            with open("aws/lambdas/justhodl-calls-backtest/source/lambda_function.py", "r") as f:
                local_src = f.read()
            r.log(f"    ✓ exists ({len(local_src):,} chars, {local_src.count(chr(10))} lines)")
            r.log(f"    has decisive_call_replay: {'decisive_call_replay' in local_src}")
            r.log(f"    has VERB_EXPOSURE: {'VERB_EXPOSURE' in local_src}")
        except Exception as e:
            r.log(f"    ✗ {e}")

        # ---------- #4: Realistic backtest improvements ----------
        r.heading("#4 — Realistic backtest improvements (slippage / gross cap / leverage cost)")
        try:
            with open("aws/lambdas/justhodl-backtest-engine/source/lambda_function.py", "r") as f:
                src = f.read()
            checks = [
                ("slippage constant", "SLIPPAGE_BPS" in src or "slippage_bps" in src),
                ("gross exposure cap", "GROSS_EXPOSURE_CAP" in src or "gross_exposure_cap" in src),
                ("leverage cost", "LEVERAGE_COST" in src or "leverage_cost" in src),
                ("concentration cap", "CONCENTRATION_CAP" in src or "concentration_cap" in src),
                ("realistic flag", "realistic" in src.lower()),
                ("v1.2 marker", "v1.2" in src or "v1_2" in src),
            ]
            for label, ok in checks:
                r.log(f"    {'✓' if ok else '✗'} {label}")
            # Method ID currently in use
            for line in src.split("\n"):
                if "method" in line and ("calibrated" in line or "alpha_replay" in line) and "=" in line:
                    if "'" in line or '"' in line:
                        r.log(f"    current method line: {line.strip()[:120]}")
                        break
        except Exception as e:
            r.log(f"    ✗ {e}")

        # Latest backtest summary — what does today's deploy say?
        r.log("")
        r.log("  Current backtest/summary.json:")
        try:
            data = json.loads(S3.get_object(Bucket="justhodl-dashboard-live", Key="backtest/summary.json")["Body"].read())
            r.log(f"    method: {data.get('method')}")
            r.log(f"    total_return_pct: {data.get('total_return_pct')}")
            r.log(f"    sharpe_ratio: {data.get('sharpe_ratio')}")
            r.log(f"    n_horizon_weighted: {data.get('n_horizon_weighted')}")
            r.log(f"    has slippage_bps: {'slippage_bps' in data}")
            r.log(f"    has gross_exposure_cap_pct: {'gross_exposure_cap_pct' in data}")
            r.log(f"    has v1_2_realistic: {'v1_2_realistic' in str(data)}")
        except Exception as e:
            r.log(f"    ✗ {e}")


if __name__ == "__main__":
    main()
