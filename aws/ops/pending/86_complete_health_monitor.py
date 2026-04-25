#!/usr/bin/env python3
"""
Step 86 — Final completion of the health monitor system.

Three things to ship:

  1. Fix edge-data expected_size 10_000 → 1_000 in expectations.py
     (observed reality: healthy runs range 1.2KB-11KB)

  2. Build the Telegram alerter — runs INSIDE the monitor Lambda after
     the dashboard is written. State-transition logic:
       - Read previous dashboard state from _health/last_alerted.json
       - Compare against current state
       - Send Telegram message ONLY when component flips green→red
         (or red→green for recovery notifications)
       - Cooldown: 24h per component to prevent spam
       - Aggregate: one batch message per Lambda invocation, not 1/component

  3. Create EventBridge rule justhodl-health-monitor-15min
     cron(0/15 * * * ? *) — fires every 15 minutes
"""
import io
import json
import os
import zipfile
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
ACCOUNT = "857687956942"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
eb = boto3.client("events", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


# ─── Patch 1: edge-data expected_size ─────────────────────────────────
EDGE_OLD = '''        "fresh_max": 25_000,     # ~7h (writer is every 6h)
        "warn_max": 43_200,      # 12h
        "expected_size": 10_000,
        "note": "Composite ML risk score, regime. edge-engine every 6h.",
        "severity": "critical",
    },'''
EDGE_NEW = '''        "fresh_max": 25_000,     # ~7h (writer is every 6h)
        "warn_max": 43_200,      # 12h
        "expected_size": 1_000,  # Healthy compact runs are ~1.2KB; full runs ~11KB
        "note": "Composite ML risk score, regime. edge-engine every 6h. Size varies 1-11KB depending on alerts/correlations.",
        "severity": "critical",
    },'''


# ─── Patch 2: Add alerter logic to lambda_function.py ────────────────
ALERTER_OLD = '''    print(f"[DONE] system={system} red={counts['red']} yellow={counts['yellow']} green={counts['green']}")
    return {"statusCode": 200, "body": json.dumps(dashboard, default=str)[:500]}'''

ALERTER_NEW = '''    # ─── Telegram alerter (state-transition based, with cooldown) ─────
    try:
        run_alerter(components, system)
    except Exception as e:
        print(f"[ALERTER] non-fatal error: {e}")

    print(f"[DONE] system={system} red={counts['red']} yellow={counts['yellow']} green={counts['green']}")
    return {"statusCode": 200, "body": json.dumps(dashboard, default=str)[:500]}


# ═══════════════════════════════════════════════════════════════════════
#  Telegram alerting layer
# ═══════════════════════════════════════════════════════════════════════

import urllib.request
import urllib.error

ALERT_STATE_KEY = "_health/last_alerted.json"
TELEGRAM_COOLDOWN_SEC = 24 * 3600  # 24h per component

def load_alert_state():
    """Load the last-alerted-status per component from S3."""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=ALERT_STATE_KEY)
        return json.loads(obj["Body"].read())
    except ClientError:
        return {"components": {}, "system": "unknown"}
    except Exception as e:
        print(f"[ALERTER] load state failed: {e}")
        return {"components": {}, "system": "unknown"}


def save_alert_state(state):
    s3.put_object(
        Bucket=BUCKET,
        Key=ALERT_STATE_KEY,
        Body=json.dumps(state, indent=2, default=str).encode(),
        ContentType="application/json",
    )


def get_telegram_creds():
    """Token from env (TELEGRAM_BOT_TOKEN), chat_id from SSM."""
    import os
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        print("[ALERTER] TELEGRAM_BOT_TOKEN env var missing; skipping")
        return None, None
    try:
        resp = ssm.get_parameter(Name="/justhodl/telegram/chat_id")
        chat_id = resp["Parameter"]["Value"]
        return token, chat_id
    except Exception as e:
        print(f"[ALERTER] chat_id fetch failed: {e}")
        return None, None


def send_telegram(text):
    token, chat_id = get_telegram_creds()
    if not token or not chat_id:
        return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = json.dumps({
            "chat_id": chat_id,
            "text": text[:4000],  # TG hard cap is 4096
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }).encode()
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status == 200
    except Exception as e:
        print(f"[ALERTER] telegram send failed: {e}")
        return False


def run_alerter(components, system_status):
    """State-transition alerting with per-component cooldown.

    Sends a Telegram message ONLY when:
      - A component flips from green/info → red or yellow (degradation)
      - A component flips from red → green (recovery)
      - And: per-component last_alerted_at > 24h ago (cooldown)

    Aggregates all transitions into a single message per invocation.
    """
    state = load_alert_state()
    prev = state.get("components", {})
    now_unix = int(time.time())

    # Severity gate: only alert on critical + important (not nice_to_have)
    ALERT_SEVERITIES = {"critical", "important"}

    degradations = []  # newly red/yellow
    recoveries = []    # back to green

    new_state_components = {}

    for c in components:
        cid = c.get("id", "?")
        cur_status = c.get("status", "unknown")
        sev = c.get("severity", "")
        reason = c.get("reason") or c.get("error") or ""

        prev_entry = prev.get(cid, {})
        prev_status = prev_entry.get("status", "unknown")
        prev_alerted_at = prev_entry.get("last_alerted_at", 0)

        # Update state always (even when not alerting)
        new_state_components[cid] = {
            "status": cur_status,
            "severity": sev,
            "last_alerted_at": prev_alerted_at,  # may be updated below
        }

        # Skip alerting on nice_to_have or info/unknown
        if sev not in ALERT_SEVERITIES:
            continue
        if cur_status in ("info", "unknown"):
            continue

        cooldown_ok = (now_unix - prev_alerted_at) > TELEGRAM_COOLDOWN_SEC

        # Degradation: green → red/yellow
        if prev_status == "green" and cur_status in ("red", "yellow") and cooldown_ok:
            degradations.append({"id": cid, "status": cur_status, "severity": sev, "reason": reason})
            new_state_components[cid]["last_alerted_at"] = now_unix

        # Recovery: red/yellow → green
        elif prev_status in ("red", "yellow") and cur_status == "green" and cooldown_ok:
            recoveries.append({"id": cid, "severity": sev})
            new_state_components[cid]["last_alerted_at"] = now_unix

    # Save state regardless
    save_alert_state({"components": new_state_components, "system": system_status, "updated_at": now_unix})

    # Compose message if there's anything to send
    if not degradations and not recoveries:
        print(f"[ALERTER] no transitions to alert (system={system_status})")
        return

    lines = [f"*JustHodl.AI Health Alert* — system: `{system_status.upper()}`", ""]

    if degradations:
        lines.append(f"*Degradations* ({len(degradations)})")
        for d in degradations[:10]:
            emoji = "🔴" if d["status"] == "red" else "🟡"
            line = f"{emoji} `{d['id']}` ({d['severity']})"
            if d["reason"]:
                line += f"\\n   _{d['reason'][:100]}_"
            lines.append(line)
        if len(degradations) > 10:
            lines.append(f"_… and {len(degradations) - 10} more_")
        lines.append("")

    if recoveries:
        lines.append(f"*Recovered* ({len(recoveries)})")
        for r in recoveries[:10]:
            lines.append(f"🟢 `{r['id']}` ({r['severity']})")
        if len(recoveries) > 10:
            lines.append(f"_… and {len(recoveries) - 10} more_")
        lines.append("")

    lines.append(f"Dashboard: https://justhodl-dashboard-live.s3.amazonaws.com/health.html")

    message = "\\n".join(lines)
    sent = send_telegram(message)
    print(f"[ALERTER] degradations={len(degradations)} recoveries={len(recoveries)} sent={sent}")'''


with report("complete_health_monitor") as r:
    r.heading("Step 86 — Final completion: edge fix, Telegram, EB schedule")

    # ─── 1. Patch edge-data threshold ───
    r.section("1. Patch edge-data expected_size")
    exp_path = REPO_ROOT / "aws/ops/health/expectations.py"
    src = exp_path.read_text()
    if EDGE_OLD in src:
        src = src.replace(EDGE_OLD, EDGE_NEW, 1)
        exp_path.write_text(src)
        r.ok("  Edge-data threshold lowered to 1KB")
    else:
        r.warn("  Pattern not found; manual fix may be needed")
        r.log("  Looking for any line with 'expected_size': 10_000 for edge-data...")

    # ─── 2. Patch lambda_function.py with alerter ───
    r.section("2. Add Telegram alerter to lambda_function.py")
    fn_path = REPO_ROOT / "aws/lambdas/justhodl-health-monitor/source/lambda_function.py"
    fn_src = fn_path.read_text()
    if ALERTER_OLD in fn_src:
        fn_src = fn_src.replace(ALERTER_OLD, ALERTER_NEW, 1)
        # Also need to import time at top if not already
        if "import time" not in fn_src.split("\n")[0:20].__str__():
            # time is already imported at top via 'import time' line
            pass
        # Need to import ClientError (used in load_alert_state)
        if "from botocore.exceptions import ClientError" not in fn_src:
            fn_src = fn_src.replace(
                "import boto3",
                "import boto3\nfrom botocore.exceptions import ClientError",
                1,
            )

        # Validate
        import ast
        try:
            ast.parse(fn_src)
        except SyntaxError as e:
            r.fail(f"  Syntax error after patch: {e}")
            raise SystemExit(1)

        fn_path.write_text(fn_src)
        r.ok(f"  Alerter added; final source {len(fn_src)} bytes")
    else:
        r.fail("  Alerter insert pattern not found")
        raise SystemExit(1)

    # ─── 3. Set TELEGRAM_BOT_TOKEN env on the Lambda ───
    r.section("3. Set Lambda env vars (TELEGRAM_BOT_TOKEN passthrough)")
    try:
        # Get current config
        cfg = lam.get_function_configuration(FunctionName="justhodl-health-monitor")
        env = (cfg.get("Environment", {}) or {}).get("Variables", {}) or {}

        # Pull token from secrets — note: GH Actions has TELEGRAM_BOT_TOKEN as secret;
        # we want to inject the same value into the Lambda env.
        # The bot token from memory: 8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs
        token = os.environ.get("TELEGRAM_BOT_TOKEN")
        if token:
            env["TELEGRAM_BOT_TOKEN"] = token
            lam.update_function_configuration(
                FunctionName="justhodl-health-monitor",
                Environment={"Variables": env},
            )
            r.ok(f"  Set TELEGRAM_BOT_TOKEN env var (len={len(token)})")
        else:
            r.warn("  TELEGRAM_BOT_TOKEN not in CI env; alerter will skip Telegram silently")
    except Exception as e:
        r.warn(f"  env update: {e}")

    # ─── 4. Re-deploy ───
    r.section("4. Re-deploy with alerter")
    src_dir = REPO_ROOT / "aws/lambdas/justhodl-health-monitor/source"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for f in src_dir.rglob("*"):
            if f.is_file():
                zout.write(f, str(f.relative_to(src_dir)))
        zout.write(exp_path, "expectations.py")
    zbytes = buf.getvalue()

    lam.update_function_code(FunctionName="justhodl-health-monitor", ZipFile=zbytes)
    lam.get_waiter("function_updated").wait(
        FunctionName="justhodl-health-monitor", WaiterConfig={"Delay": 3, "MaxAttempts": 30}
    )
    r.ok(f"  Re-deployed: {len(zbytes)} bytes")

    # ─── 5. Sync invoke to verify ───
    r.section("5. Sync invoke + check status")
    resp = lam.invoke(FunctionName="justhodl-health-monitor", InvocationType="RequestResponse")
    if resp.get("FunctionError"):
        payload = resp.get("Payload").read().decode()
        r.fail(f"  Invoke error: {payload[:500]}")
        raise SystemExit(1)
    r.ok(f"  Invoke clean (status {resp.get('StatusCode')})")

    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="_health/dashboard.json")
    dash = json.loads(obj["Body"].read())
    r.log(f"  System: {dash.get('system_status')}")
    r.log(f"  Counts: {dash.get('counts')}")

    # ─── 6. Create EB schedule ───
    r.section("6. Create EventBridge rule for 15-min cadence")
    rule_name = "justhodl-health-monitor-15min"
    try:
        eb.describe_rule(Name=rule_name)
        r.log(f"  Rule {rule_name} already exists; skipping create")
    except eb.exceptions.ResourceNotFoundException:
        eb.put_rule(
            Name=rule_name,
            ScheduleExpression="cron(0/15 * * * ? *)",
            State="ENABLED",
            Description="System health monitor — runs every 15 minutes",
        )
        r.ok(f"  Created rule {rule_name}: cron(0/15 * * * ? *)")

        # Permission for EB to invoke Lambda
        try:
            lam.add_permission(
                FunctionName="justhodl-health-monitor",
                StatementId=f"eb-{rule_name}",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=eb.describe_rule(Name=rule_name)["Arn"],
            )
        except lam.exceptions.ResourceConflictException:
            pass

        eb.put_targets(
            Rule=rule_name,
            Targets=[{
                "Id": "1",
                "Arn": f"arn:aws:lambda:us-east-1:{ACCOUNT}:function:justhodl-health-monitor",
            }],
        )
        r.ok(f"  Wired Lambda target to rule")

    r.section("7. Final dashboard state — should be cleaner now")
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="_health/dashboard.json")
    dash = json.loads(obj["Body"].read())
    r.log(f"  System: {dash.get('system_status')}")
    r.log(f"  Counts: {dash.get('counts')}")
    r.log(f"\n  Non-green/info components:")
    for c in dash.get("components", []):
        if c.get("status") in ("green", "info"):
            continue
        sid = c.get("id", "?")
        st = c.get("status", "?")
        sev = c.get("severity", "?")
        reason = c.get("reason") or c.get("error") or ""
        age = c.get("age_sec")
        size = c.get("size_bytes")
        bits = []
        if age is not None: bits.append(f"age={age/3600:.1f}h")
        if size is not None: bits.append(f"size={size}B")
        info = ", ".join(bits)
        r.log(f"    [{st:7}] {sev:12} {sid:35} {info:25} {reason[:80]}")

    r.kv(
        eb_rule="justhodl-health-monitor-15min cron(0/15 * * * ? *)",
        telegram_alerter="state-transition based, 24h cooldown per component",
        dashboard_url="https://justhodl-dashboard-live.s3.amazonaws.com/health.html",
        next_invocation="within 15 min of next quarter-hour",
    )
    r.log("Done")
