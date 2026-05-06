"""
Create justhodl-historical-analogs Lambda + daily schedule + smoke test.
"""
import os
import time
import zipfile
import io
import json
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-historical-analogs"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
SOURCE_DIR = "aws/lambdas/justhodl-historical-analogs/source"
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
    with report("create_historical_analogs") as r:
        r.heading("Create justhodl-historical-analogs + smoke test")

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
                MemorySize=1024,   # heavy data fetch
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
                MemorySize=1024,
                Timeout=600,
                Environment={"Variables": {"FRED_KEY": FRED_KEY}},
                Description="Historical analog finder — 6-dim regime nearest neighbors",
            )
            r.ok(f"  ✓ created")

        r.section("EventBridge schedule (daily 13 UTC)")
        try:
            rule_name = f"{LAMBDA_NAME}-daily"
            events.put_rule(
                Name=rule_name,
                ScheduleExpression="cron(0 13 * * ? *)",
                State="ENABLED",
                Description=f"Daily trigger for {LAMBDA_NAME}",
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
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/historical-analogs.json")
            data = json.loads(obj["Body"].read())
            today = data.get("today", {})
            r.log(f"  today_date: {today.get('date')}")
            r.log(f"  vix: {today.get('vix')}")
            r.log(f"  2s10s: {today.get('twos_tens_bps')} bps")
            r.log(f"  hy_oas: {today.get('hy_oas_pct')}%")
            r.log(f"  usd_index: {today.get('usd_index')}")
            r.log(f"  10Y yield: {today.get('ten_year_yield_pct')}%")
            r.log(f"  spx_1m_return: {today.get('spx_1m_return_pct')}%")
            r.log(f"  call: {data.get('directional_call')}")
            r.log(f"  desc: {data.get('directional_description')}")
            r.log(f"  n_historical_evaluated: {data.get('n_historical_dates_evaluated')}")

            r.section("📊 Forward return distribution")
            for h, dist in (data.get("forward_distribution") or {}).items():
                if dist:
                    r.log(
                        f"    {h:5s}  n={dist['n']:>3} mean={dist['mean_pct']:>+5.2f}% median={dist['median_pct']:>+5.2f}% "
                        f"hit_rate={dist['hit_rate_pct']:>4.1f}% range=[{dist['min_pct']:+.1f}, {dist['max_pct']:+.1f}]%"
                    )

            r.section(f"🔍 Top {min(8, len(data.get('analogs') or []))} analogs")
            for a in (data.get("analogs") or [])[:8]:
                r21 = a.get("forward_21d_pct")
                r63 = a.get("forward_63d_pct")
                r.log(
                    f"    {a['date']} dist={a['distance']:.3f} sim={a['similarity']:.2f} "
                    f"21d={r21:+.2f}% 63d={r63:+.2f}%"
                    if r21 is not None and r63 is not None
                    else f"    {a['date']} dist={a['distance']:.3f} sim={a['similarity']:.2f}"
                )
        except Exception as e:
            r.fail(f"  ✗ {e}")


if __name__ == "__main__":
    main()
