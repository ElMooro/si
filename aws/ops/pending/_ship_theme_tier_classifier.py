"""
Combined ops:
  A) Patch supply-inflection-scanner: add retry-with-backoff to fetch_fred so
     FRED's transient 500s don't lose us 5 macro signals.
  B) Ship theme-tier-classifier (Layer 3 of nobrainer hunter)

Steps:
  1. Patch Layer 2 source in-place + redeploy Lambda
  2. Smoke-invoke Layer 2 to confirm 22/22 signals (or close)
  3. Build Layer 3 zip from aws/lambdas/justhodl-theme-tier-classifier/source/
  4. Create or update Layer 3 Lambda
  5. Schedule daily 08:00 UTC
  6. Smoke-invoke Layer 3
  7. Verify S3 output for both layers
"""
import io
import json
import time
import zipfile
import os
import re

import boto3

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"

L2_NAME = "justhodl-supply-inflection-scanner"
L2_SRC  = "aws/lambdas/justhodl-supply-inflection-scanner/source/lambda_function.py"

L3_NAME = "justhodl-theme-tier-classifier"
L3_SRC  = "aws/lambdas/justhodl-theme-tier-classifier/source/lambda_function.py"
L3_SCHED_NAME = "justhodl-theme-tier-classifier-daily"
L3_SCHED_EXPR = "cron(0 8 * * ? *)"  # daily 08:00 UTC

ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"

REPORT = []


def log(msg):
    ts = time.strftime("%H:%M:%S")
    print(f"- `{ts}`   {msg}")
    REPORT.append(f"- `{ts}`   {msg}")


def section(title):
    print(f"\n# {title}\n")
    REPORT.append(f"\n# {title}\n")


# ─────────────────────────────────────────────────────────────────────────────
# A) PATCH LAYER 2 — add FRED retry
# ─────────────────────────────────────────────────────────────────────────────
def patch_layer2_fred_retry():
    src = open(L2_SRC, "r", encoding="utf-8").read()

    OLD = '''def fetch_fred(series_id, days_back=600):
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days_back)
    url = (
        f"{FRED_BASE}/series/observations"
        f"?series_id={series_id}&api_key={FRED_KEY}"
        f"&observation_start={start.isoformat()}&observation_end={end.isoformat()}"
        f"&file_type=json&sort_order=asc"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-supply-inflection/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        obs = data.get("observations") or []
        if not obs:
            print(f"[fred] {series_id} no observations")
            return []
        out = []
        for o in obs:
            try:
                v = float(o["value"])
                d = datetime.fromisoformat(o["date"]).date()
                out.append({"date": d, "close": v})
            except (ValueError, TypeError):
                continue  # FRED uses "." for missing values
        return out
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:400] if hasattr(e, "read") else ""
        print(f"[fred] {series_id} HTTPError {e.code} body={body}")
        return []
    except urllib.error.URLError as e:
        print(f"[fred] {series_id} URLError {e.reason}")
        return []
    except Exception as e:
        print(f"[fred] {series_id} other_error {type(e).__name__} {e}")
        return []'''

    NEW = '''def fetch_fred(series_id, days_back=600, retries=4):
    """Fetch FRED series with retry/backoff on 5xx and 429."""
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days_back)
    url = (
        f"{FRED_BASE}/series/observations"
        f"?series_id={series_id}&api_key={FRED_KEY}"
        f"&observation_start={start.isoformat()}&observation_end={end.isoformat()}"
        f"&file_type=json&sort_order=asc"
    )
    last_err = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "justhodl-supply-inflection/1.0"})
            with urllib.request.urlopen(req, timeout=20) as resp:
                data = json.loads(resp.read().decode())
            obs = data.get("observations") or []
            if not obs:
                print(f"[fred] {series_id} no observations")
                return []
            out = []
            for o in obs:
                try:
                    v = float(o["value"])
                    d = datetime.fromisoformat(o["date"]).date()
                    out.append({"date": d, "close": v})
                except (ValueError, TypeError):
                    continue  # FRED uses "." for missing values
            return out
        except urllib.error.HTTPError as e:
            last_err = f"HTTP{e.code}"
            if e.code in (429, 500, 502, 503, 504):
                wait = 0.7 * (2 ** attempt)
                print(f"[fred] {series_id} {last_err} retry {attempt+1}/{retries} wait={wait:.1f}s")
                time.sleep(wait)
                continue
            body = e.read().decode("utf-8", errors="replace")[:200] if hasattr(e, "read") else ""
            print(f"[fred] {series_id} HTTPError {e.code} body={body}")
            return []
        except urllib.error.URLError as e:
            last_err = f"URL:{e.reason}"
            time.sleep(0.7 * (2 ** attempt))
            continue
        except Exception as e:
            last_err = f"{type(e).__name__}:{e}"
            time.sleep(0.5)
            continue
    print(f"[fred] {series_id} all_retries_failed err={last_err}")
    return []'''

    if OLD not in src:
        log("⚠️ Layer 2 patch: OLD signature not found — skipping (already patched?)")
        return False
    new_src = src.replace(OLD, NEW)
    open(L2_SRC, "w", encoding="utf-8").write(new_src)
    log("✓ Layer 2 patched: fetch_fred now has retry/backoff (4 attempts on 5xx/429)")
    return True


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def build_zip(src_path):
    src = open(src_path, "r", encoding="utf-8").read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    return buf.getvalue()


def deploy_lambda(lam, name, zip_bytes, env_vars, memory=1024, timeout=300):
    exists = False
    try:
        lam.get_function(FunctionName=name)
        exists = True
    except lam.exceptions.ResourceNotFoundException:
        exists = False

    if exists:
        log(f"{name} exists — updating code")
        lam.update_function_code(FunctionName=name, ZipFile=zip_bytes)
        for _ in range(20):
            cfg = lam.get_function_configuration(FunctionName=name)
            if cfg.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(1)
        lam.update_function_configuration(
            FunctionName=name,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=memory,
            Timeout=timeout,
            Environment={"Variables": env_vars},
        )
        for _ in range(20):
            cfg = lam.get_function_configuration(FunctionName=name)
            if cfg.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(1)
    else:
        log(f"{name} does not exist — creating")
        lam.create_function(
            FunctionName=name,
            Runtime="python3.12",
            Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zip_bytes},
            MemorySize=memory,
            Timeout=timeout,
            Environment={"Variables": env_vars},
        )
        for _ in range(20):
            cfg = lam.get_function_configuration(FunctionName=name)
            if cfg.get("State") == "Active":
                break
            time.sleep(1)
    log(f"✅ {name} deployed")


def schedule_lambda(events, lam, name, sched_name, sched_expr):
    try:
        events.put_rule(
            Name=sched_name,
            ScheduleExpression=sched_expr,
            State="ENABLED",
            Description=f"Daily run of {name}",
        )
        log(f"Rule put: {sched_name} ({sched_expr})")
        try:
            lam.add_permission(
                FunctionName=name,
                StatementId=f"{sched_name}-invoke",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{sched_name}",
            )
            log("Lambda invoke permission added")
        except lam.exceptions.ResourceConflictException:
            log("Permission already exists (ok)")
        events.put_targets(
            Rule=sched_name,
            Targets=[{"Id": "1", "Arn": f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{name}"}],
        )
        log("Target attached")
    except Exception as e:
        log(f"⚠️ schedule warning: {type(e).__name__} {e}")


def smoke_invoke(lam, name):
    invoke_started = time.time()
    resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
    invoke_dur = round(time.time() - invoke_started, 1)
    log(f"{name} invoke status={resp['StatusCode']} duration={invoke_dur}s")
    payload = json.loads(resp["Payload"].read())
    body = json.loads(payload.get("body", "{}")) if isinstance(payload, dict) else {}
    log("── Response body ──")
    for k, v in body.items():
        log(f"  {k}: {v}")
    if "LogResult" in resp:
        import base64
        log_text = base64.b64decode(resp["LogResult"]).decode("utf-8", errors="replace")
        log("── Log tail (last 25) ──")
        for line in log_text.splitlines()[-25:]:
            log(f"  {line}")
    return body


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    lam = boto3.client("lambda", region_name=REGION)
    events = boto3.client("events", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)

    # ─── A) Patch + redeploy Layer 2 ────────────────────────────────────────
    section("A1) Patch Layer 2 — FRED retry/backoff")
    patched = patch_layer2_fred_retry()

    if patched:
        section("A2) Redeploy Layer 2")
        zip_bytes = build_zip(L2_SRC)
        log(f"Layer 2 zip size: {len(zip_bytes):,}b")
        deploy_lambda(lam, L2_NAME, zip_bytes, {
            "POLYGON_KEY": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d",
            "FRED_KEY": "2f057499936072679d8843d7fce99989",
        })

        section("A3) Smoke invoke Layer 2 — verify FRED retry works")
        body = smoke_invoke(lam, L2_NAME)
        log(f"Layer 2 result: {body.get('n_signals')} signals scored "
            f"({body.get('n_strong_tightening')} strong tightening)")

    # ─── B) Ship Layer 3 ────────────────────────────────────────────────────
    section("B1) Build Layer 3 zip")
    zip_bytes = build_zip(L3_SRC)
    log(f"Layer 3 zip size: {len(zip_bytes):,}b")

    section("B2) Deploy Layer 3 Lambda")
    deploy_lambda(lam, L3_NAME, zip_bytes, {
        "FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
    }, memory=1024, timeout=600)

    section("B3) Schedule Layer 3 daily 08:00 UTC")
    schedule_lambda(events, lam, L3_NAME, L3_SCHED_NAME, L3_SCHED_EXPR)

    section("B4) Smoke invoke Layer 3")
    body = smoke_invoke(lam, L3_NAME)

    section("B5) Verify Layer 3 S3 output")
    try:
        head = s3.head_object(Bucket=BUCKET, Key="data/theme-tiers.json")
        log(f"S3 size: {head['ContentLength']:,}b")
        log(f"S3 last_modified: {head['LastModified']}")
        obj = s3.get_object(Bucket=BUCKET, Key="data/theme-tiers.json")
        data = json.loads(obj["Body"].read())
        log(f"v: {data.get('schema_version')}")
        log(f"n_themes_classified: {data.get('n_themes_classified')}")
        s = data.get("summary", {})
        log(f"n_total_classifications: {s.get('n_total_classifications')}")
        log(f"n_deep_asymmetry: {s.get('n_deep_asymmetry')}")
        log(f"n_asymmetric: {s.get('n_asymmetric')}")
        log("")
        log("── Top 10 asymmetric leaderboard ──")
        for x in (s.get("top_asymmetric_leaderboard") or [])[:10]:
            mc = x.get("mcap_to_rev")
            mc_str = f"{mc:.2f}" if mc is not None else "n/a"
            log(f"  {x['ticker']:<6} ({x['theme_etf']:<5} {x['theme_phase']:<13}) "
                f"tier={x['tier']} score={x['asymmetry_score']:>5.1f} "
                f"flag={x['flag']:<16} mcap_to_rev={mc_str}")
        log("")
        log("── MU-grade leaderboard (mcap_to_rev <= 3) ──")
        for x in (s.get("mu_grade_leaderboard") or [])[:10]:
            mc = x.get("mcap_to_rev")
            mc_str = f"{mc:.2f}" if mc is not None else "n/a"
            log(f"  {x['ticker']:<6} ({x['theme_etf']:<5} {x['theme_phase']:<13}) "
                f"score={x['asymmetry_score']:>5.1f} "
                f"mcap_to_rev={mc_str} p_s={x.get('p_s')}")
        log("")
        log("── Tier-2 leaderboard ──")
        for x in (s.get("tier2_leaderboard") or [])[:10]:
            mc = x.get("mcap_to_rev")
            mc_str = f"{mc:.2f}" if mc is not None else "n/a"
            log(f"  {x['ticker']:<6} ({x['theme_etf']:<5}) score={x['asymmetry_score']:>5.1f} "
                f"mcap_to_rev={mc_str}")
    except Exception as e:
        log(f"⚠️ Layer 3 S3 verify failed: {e}")


if __name__ == "__main__":
    main()
    out_dir = "aws/ops/reports/latest"
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "ship_theme_tier_classifier.md")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print(f"\n[report written] {out_path}")
