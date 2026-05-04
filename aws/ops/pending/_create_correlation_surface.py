"""
Create justhodl-correlation-surface Lambda + daily schedule + smoke test.

Cross-asset rolling correlation matrix (30d/90d/252d) across 14 proxies, with
regime-break + decoupling flags. Output: data/correlation-surface.json.
"""
import os
import time
import zipfile
import io
import json
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-correlation-surface"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
SOURCE_DIR = "aws/lambdas/justhodl-correlation-surface/source"
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
    with report("create_correlation_surface") as r:
        r.heading("Create justhodl-correlation-surface + smoke test")

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
                MemorySize=1024,
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
                MemorySize=1024,
                Timeout=600,
                Environment={"Variables": {"POLYGON_KEY": POLYGON_KEY}},
                Description="Cross-asset correlation surface — 30d/90d/252d + regime breaks",
            )
            r.ok(f"  ✓ created")

        r.section("EventBridge schedule (daily 15 UTC)")
        try:
            rule_name = f"{LAMBDA_NAME}-daily"
            events.put_rule(
                Name=rule_name,
                ScheduleExpression="cron(0 15 * * ? *)",
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
            r.log(f"  resp: {payload[:500]}")
            if inv.get("FunctionError"):
                r.fail(f"  ✗ {inv['FunctionError']}")
                return
        except Exception as e:
            r.fail(f"  ✗ {e}")
            return

        r.section("S3 verify")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/correlation-surface.json")
            data = json.loads(obj["Body"].read())
            r.log(f"  as_of: {data.get('as_of_date')}")
            r.log(f"  macro_regime: {data.get('macro_regime')}")
            r.log(f"  avg_30d_abs_corr: {data.get('avg_30d_abs_correlation')}")
            r.log(f"  n_regime_breaks: {data.get('n_regime_breaks')}")
            r.log(f"  n_decouplings: {data.get('n_decouplings')}")

            r.section("📊 Headline pairs")
            for hp in (data.get("headline_pairs") or [])[:10]:
                r.log(
                    f"  {hp.get('pair'):15s} c30={hp.get('corr_30d'):>+5.2f} "
                    f"c90={hp.get('corr_90d'):>+5.2f} c252={hp.get('corr_252d'):>+5.2f} "
                    f"Δ30v90={hp.get('delta_30d_vs_90d'):>+5.2f}  flag={hp.get('flag')}"
                )

            if data.get("regime_breaks"):
                r.section(f"🚨 Regime breaks ({len(data['regime_breaks'])})")
                for rb in data["regime_breaks"][:8]:
                    r.log(
                        f"  {rb.get('pair'):15s} c30={rb.get('corr_30d'):>+5.2f} "
                        f"c90={rb.get('corr_90d'):>+5.2f} Δ={rb.get('delta_30d_vs_90d'):>+5.2f}"
                    )

            if data.get("decouplings"):
                r.section(f"⚠️  Decouplings ({len(data['decouplings'])})")
                for dc in data["decouplings"][:8]:
                    r.log(
                        f"  {dc.get('pair'):15s} c30={dc.get('corr_30d'):>+5.2f} "
                        f"c252={dc.get('corr_252d'):>+5.2f} Δ={dc.get('delta_30d_vs_252d'):>+5.2f}"
                    )
        except Exception as e:
            r.fail(f"  ✗ {e}")


if __name__ == "__main__":
    main()
