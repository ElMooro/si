"""
Create justhodl-yield-curve Lambda + 6h schedule + smoke test.
"""
import os
import time
import zipfile
import io
import json
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-yield-curve"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
SOURCE_DIR = "aws/lambdas/justhodl-yield-curve/source"
FRED_KEY = "2f057499936072679d8843d7fce99989"

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
    with report("create_yield_curve") as r:
        r.heading("Create justhodl-yield-curve + smoke test")

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
                Environment={"Variables": {"FRED_KEY": FRED_KEY}},
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
                Environment={"Variables": {"FRED_KEY": FRED_KEY}},
                Description="Yield Curve Shape Decomposition — full nominal + real + breakeven curve",
            )
            r.ok(f"  ✓ created")

        r.section("EventBridge schedule (6h)")
        try:
            rule_name = f"{LAMBDA_NAME}-6h"
            events.put_rule(
                Name=rule_name,
                ScheduleExpression="rate(6 hours)",
                State="ENABLED",
                Description=f"6h trigger for {LAMBDA_NAME}",
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

        r.section("Smoke test")
        try:
            t0 = time.time()
            inv = lam.invoke(FunctionName=LAMBDA_NAME, Payload=b"{}")
            payload = inv["Payload"].read().decode()
            r.log(f"  status: {inv['StatusCode']} duration: {time.time()-t0:.1f}s")
            r.log(f"  resp: {payload[:400]}")
            if inv.get("FunctionError"):
                r.fail(f"  ✗ {inv['FunctionError']}")
                return
        except Exception as e:
            r.fail(f"  ✗ {e}")
            return

        r.section("S3 verify")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/yield-curve.json")
            data = json.loads(obj["Body"].read())
            r.log(f"  as_of: {data.get('as_of_date')}")
            r.log(f"  regime: {data.get('regime')}")
            r.log(f"  desc: {data.get('regime_description')}")

            decomp = data.get("decomposition", {})
            r.log(f"  level: {decomp.get('level_pct')}%")
            r.log(f"  slope (2s10s): {decomp.get('slope_2s10s_bps')}bps")
            r.log(f"  curvature (butterfly): {decomp.get('curvature_butterfly_bps')}bps")

            spreads = data.get("spreads_bps", {})
            r.section("Key spreads (bps)")
            for k, v in spreads.items():
                r.log(f"    {k:25s} {v:>+8} bps" if v is not None else f"    {k:25s} (no data)")

            inv_flags = data.get("inversion_flags", {})
            r.section("Inversion flags")
            for k, v in inv_flags.items():
                emoji = "🚨" if v else "✓"
                r.log(f"    {emoji} {k}: {v}")

            r.section("Curve points (yield + 5d chg)")
            for p in data.get("curve_points", []):
                r.log(f"    {p['tenor']:5s} ({p['years']:5.2f}y) {p['yield_pct']:>5.2f}% chg5d={p.get('chg_5d_bps')}bps")

            real = data.get("real_yields", {})
            if real:
                r.section("Real yields (TIPS)")
                for label, v in real.items():
                    r.log(f"    {label:14s} {v.get('value_pct'):>+6.2f}% chg5d={v.get('chg_5d_bps')}bps")

            be = data.get("inflation_expectations", {})
            if be:
                r.section("Break-evens / inflation expectations")
                for label, v in be.items():
                    r.log(f"    {label:18s} {v.get('value_pct'):>+6.2f}% chg5d={v.get('chg_5d_bps')}bps")

            tp = data.get("term_premium_proxy_bps")
            if tp is not None:
                r.log(f"  term premium proxy: {tp:+}bps")

            sigs = data.get("signals", [])
            if sigs:
                r.section(f"🔔 SIGNALS ({len(sigs)})")
                for s in sigs:
                    r.log(f"    [{s['severity']:6s}] {s['name']:30s} {s['message']}")
            else:
                r.section("Signals")
                r.log("  (none)")
        except Exception as e:
            r.fail(f"  ✗ {e}")


if __name__ == "__main__":
    main()
