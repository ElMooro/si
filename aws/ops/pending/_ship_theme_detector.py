"""Deploy justhodl-theme-detector — auto-detect theme lifecycle from 70+ thematic ETFs."""
import io
import json
import os
import time
import zipfile
import boto3
from ops_report import report

REGION = "us-east-1"
SOURCE_DIR = "aws/lambdas/justhodl-theme-detector/source"
LAM = boto3.client("lambda", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)
EVENTS = boto3.client("events", region_name=REGION)
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"


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
    with report("ship_theme_detector") as r:
        # 1. Create or update Lambda
        r.heading("1) Create or update Lambda")
        zb = make_zip(SOURCE_DIR)
        r.log(f"  zip size: {len(zb):,}b")
        try:
            existing = LAM.get_function(FunctionName="justhodl-theme-detector")
            r.log(f"  ✓ Lambda exists, updating code (mod={existing['Configuration']['LastModified']})")
            LAM.update_function_code(FunctionName="justhodl-theme-detector", ZipFile=zb)
            new_lambda = False
        except LAM.exceptions.ResourceNotFoundException:
            r.log("  Creating new Lambda...")
            LAM.create_function(
                FunctionName="justhodl-theme-detector",
                Runtime="python3.12",
                Role=ROLE_ARN,
                Handler="lambda_function.lambda_handler",
                Code={"ZipFile": zb},
                Timeout=300,  # 5 min — fetches ~270 tickers
                MemorySize=1024,
                Environment={"Variables": {
                    "POLYGON_KEY": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d",
                }},
                Description="Auto-detect theme lifecycle from 70+ thematic ETFs (DORMANT/EMERGING/ACCELERATING/EXTENDED/PEAKING/COOLING/DYING)",
            )
            new_lambda = True

        # Wait for active
        for _ in range(30):
            cfg = LAM.get_function(FunctionName="justhodl-theme-detector")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                r.ok(f"  ✓ Lambda active, mod={cfg.get('LastModified')}")
                break
            time.sleep(2)

        # 2. Configure timeout/memory if existed
        if not new_lambda:
            try:
                LAM.update_function_configuration(
                    FunctionName="justhodl-theme-detector",
                    Timeout=300,
                    MemorySize=1024,
                    Environment={"Variables": {
                        "POLYGON_KEY": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d",
                    }},
                )
                for _ in range(15):
                    cfg = LAM.get_function(FunctionName="justhodl-theme-detector")["Configuration"]
                    if cfg.get("LastUpdateStatus") in (None, "Successful"):
                        break
                    time.sleep(2)
                r.log("  ✓ Lambda config updated")
            except Exception as e:
                r.log(f"  config update: {e}")

        # 3. Schedule — daily 06:00 UTC
        r.heading("2) Schedule daily 06:00 UTC")
        try:
            EVENTS.put_rule(
                Name="justhodl-theme-detector-daily",
                ScheduleExpression="cron(0 6 * * ? *)",
                State="ENABLED",
                Description="Daily theme lifecycle detection from thematic ETFs",
            )
            try:
                LAM.add_permission(
                    FunctionName="justhodl-theme-detector",
                    StatementId="EventsInvoke",
                    Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",
                    SourceArn=f"arn:aws:events:{REGION}:857687956942:rule/justhodl-theme-detector-daily",
                )
            except LAM.exceptions.ResourceConflictException:
                pass
            EVENTS.put_targets(
                Rule="justhodl-theme-detector-daily",
                Targets=[{
                    "Id": "1",
                    "Arn": f"arn:aws:lambda:{REGION}:857687956942:function:justhodl-theme-detector",
                }],
            )
            r.log("  ✓ Schedule wired")
        except Exception as e:
            r.log(f"  ✗ schedule: {e}")

        # 4. Smoke invoke
        r.heading("3) Smoke invoke — fetch 270 tickers and classify 70+ themes")
        t0 = time.time()
        resp = LAM.invoke(FunctionName="justhodl-theme-detector", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
        if resp["StatusCode"] != 200:
            r.log(f"  body: {body[:500]}")
        try:
            outer = json.loads(body)
            if "errorType" in outer:
                r.log(f"  ✗ ERROR: {outer.get('errorType')}: {outer.get('errorMessage')}")
                return
            inner = json.loads(outer.get("body", "{}"))
            r.log(f"  n_themes:            {inner.get('n_themes')}")
            r.log(f"  duration_s:          {inner.get('duration_s')}")
            r.log(f"  phase_distribution:  {inner.get('phase_distribution')}")
            r.log(f"  HOTTEST themes:      {inner.get('hottest')}")
            r.log(f"  TIER-2 hunt grounds: {inner.get('tier2_hunt')}")
            r.log(f"  EMERGING themes:     {inner.get('emerging')}")
            r.log(f"  DYING themes:        {inner.get('dying')}")
        except Exception as e:
            r.log(f"  parse error: {e}, body[:500]: {body[:500]}")

        # 5. Verify S3
        r.heading("4) Verify S3 data/themes-detected.json")
        try:
            obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="data/themes-detected.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  v:                  {d.get('v')}")
            r.log(f"  method:             {d.get('method')}")
            r.log(f"  duration_s:         {d.get('duration_s')}")
            r.log(f"  fetch_stats:        {d.get('fetch_stats')}")
            r.log(f"  n_themes_classified:{len(d.get('themes', []))}")
            summ = d.get("summary") or {}
            r.log(f"  phase_distribution: {summ.get('phase_distribution')}")
            r.log("")

            # Show top hottest themes with full detail
            r.log("  ── HOTTEST THEMES (EXTENDED + ACCELERATING) ─────────────")
            themes = d.get("themes") or []
            shown = 0
            for t in themes:
                if t["phase"] not in ("EXTENDED", "ACCELERATING"):
                    continue
                if shown >= 8:
                    break
                m = t["metrics"]
                r.log(f"    {t['etf']:5s} {t['name']:32s} {t['phase']:13s} score={t['phase_score']}")
                r.log(f"      returns: 5d={m.get('ret_5d')}% 30d={m.get('ret_30d')}% 90d={m.get('ret_90d')}% 180d={m.get('ret_180d')}% 365d={m.get('ret_365d')}%")
                r.log(f"      RS vs SPY: 30d={m.get('rs_30d')}% 90d={m.get('rs_90d')}% 180d={m.get('rs_180d')}%")
                r.log(f"      vol_pct={m.get('vol_pct_90d')}  breadth_30d={m.get('breadth_30d')}")
                r.log(f"      → {t['interpretation']}")
                r.log("")
                shown += 1

            # Show emerging
            r.log("  ── EMERGING THEMES (early entry zone) ─────────────")
            shown = 0
            for t in themes:
                if t["phase"] != "EMERGING" or shown >= 5:
                    if t["phase"] != "EMERGING":
                        continue
                    break
                m = t["metrics"]
                r.log(f"    {t['etf']:5s} {t['name']:30s} score={t['phase_score']} | 30d={m.get('ret_30d')}% 90d={m.get('ret_90d')}% RS30d={m.get('rs_30d')}%")
                shown += 1
            if shown == 0:
                r.log("    (none currently emerging)")
            r.log("")

            # Show dying
            r.log("  ── DYING THEMES (avoid / consider short) ─────────────")
            shown = 0
            for t in themes:
                if t["phase"] != "DYING":
                    continue
                if shown >= 5:
                    break
                m = t["metrics"]
                r.log(f"    {t['etf']:5s} {t['name']:30s} score={t['phase_score']} | 180d={m.get('ret_180d')}% 365d={m.get('ret_365d')}% breadth={m.get('breadth_30d')}")
                shown += 1
            if shown == 0:
                r.log("    (none currently dying)")

        except Exception as e:
            r.log(f"  ✗ {e}")
            import traceback
            r.log(traceback.format_exc())


if __name__ == "__main__":
    main()
