"""ops 1149 — Per-cardinal early-warning alert rollout.

Extends the alert priority ladder from 4 → 6 tiers with two new
transition-based partial-fingerprint alert kinds:

  PRIORITY 3: PARTIAL_BUILDUP — upward crossing into N=2 cardinals stressed
              (macro: 2/3 pillars · equity: 2/4 cardinals)
              Bypasses cooldown when transitioning. Fires once per crossing.

  PRIORITY 5: PARTIAL_EARLY  — upward crossing into N=1 stressed
              (MACRO ONLY — equity 1/4 too noisy)
              Bypasses cooldown when transitioning. Catches the
              first cardinal moving, often the earliest signal by
              several days of the full Aug 2007 / Sep 2019 fingerprint.

Updated priority ladder (final):
  1. CONVERGENCE_FIRE  full fingerprint (3/3 macro, 3-4/4 equity)
  2. REGIME_UP/DOWN     NORMAL/ELEVATED/EXTREME transitions
  3. PARTIAL_BUILDUP    N→2 cardinals upward crossing  [NEW]
  4. EXTREME            regime EXTREME persisting (60-min cooldown)
  5. PARTIAL_EARLY      0→1 macro pillar upward [NEW, macro only]
  6. SCORE_JUMP         |Δ| ≥ 15 (60-min cooldown)

State change: alert state files now persist `last_cardinal_count` per
cycle so transitions can be detected. Even cycles that don't fire an
alert update this field (silent state write) so the next cycle has
accurate prev_n for transition logic.

Message format:
  PARTIAL_BUILDUP title: ⚠️ MACRO BUILDUP — 2/3 PILLARS STRESSED
                          ⚠️ EQUITY BUILDUP — 2/4 CARDINALS FIRING
                          + 3 or 4 cardinal status block (✓/✗ for each)
                          + 'if 1 more moves: Aug 2007 / Jan 2021 signature'

  PARTIAL_EARLY title:   🔵 MACRO EARLY WARNING — first pillar moved
                          + 3 pillar status block (one ✓, two ✗)
                          + 'earliest signal in the pattern, watch the others'

This op:
  1. Redeploys router with retry-on-conflict
  2. Fires equity + macro sniffers synchronously to:
     - Exercise the new code path
     - Update both state files with last_cardinal_count
     - Potentially fire a PARTIAL_EARLY alert for macro since funding
       was already at STRESS in the previous run (1/3 = early warning
       state). The transition logic will check if last_cardinal_count
       was previously 0 → fire alert. If it was already 1+ → no fire.
  3. Read back both alert state files to confirm cardinal-count
     tracking is live.

Context: the previous digest showed macro at 1/3 (funding STRESS).
After this deploy, the macro sniffer will track that 1/3 state and on
NEXT transition (0→1, 1→2, 2→3) will fire the appropriate partial alert.
"""
import io, json, os, time, traceback, zipfile, base64
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

REGION = "us-east-1"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
FN = "justhodl-ai-brief-router"
BUCKET = "justhodl-dashboard-live"

_cfg = Config(connect_timeout=10, read_timeout=300, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=_cfg)
s3 = boto3.client("s3", region_name=REGION)


def zip_src(d):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(d):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root: continue
                fp = os.path.join(root, f)
                z.write(fp, os.path.relpath(fp, d))
    return buf.getvalue()


def wait_active(t=300):
    end = time.time() + t
    while time.time() < end:
        try:
            c = lam.get_function_configuration(FunctionName=FN)
            if (c.get("State") == "Active"
                and c.get("LastUpdateStatus") in ("Successful", None)):
                return True
            if c.get("LastUpdateStatus") == "Failed": return False
        except ClientError: pass
        time.sleep(3)
    return False


def update_code_with_retries(zipped, max_tries=8):
    last_err = None
    for attempt in range(1, max_tries + 1):
        wait_active()
        try:
            lam.update_function_code(FunctionName=FN, ZipFile=zipped, Publish=False)
            return {"attempt": attempt, "status": "OK"}
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            last_err = str(e)[:200]
            if code == "ResourceConflictException":
                time.sleep(10 + attempt * 2)
                continue
            raise
    return {"attempt": max_tries, "status": "EXHAUSTED", "err": last_err}


def fire_context(ctx_name):
    wait_active()
    inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                     Payload=json.dumps({"contexts": [ctx_name]}).encode(),
                     LogType="Tail")
    body_resp = json.loads(inv["Payload"].read() or b"{}")
    if isinstance(body_resp, dict) and "body" in body_resp:
        try: body_resp = json.loads(body_resp["body"])
        except Exception: pass
    return {
        "fn_err": inv.get("FunctionError"),
        "n_ok": body_resp.get("n_ok") if isinstance(body_resp, dict) else None,
        "duration_s": body_resp.get("duration_s") if isinstance(body_resp, dict) else None,
        "results": body_resp.get("results") if isinstance(body_resp, dict) else None,
        "log_tail": base64.b64decode(inv.get("LogResult","")).decode("utf-8","replace")[-1200:],
    }


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # 1) Redeploy router
        src_dir = os.path.join(REPO_ROOT, "aws/lambdas", FN, "source")
        zipped = zip_src(src_dir)
        rpt["redeploy"] = update_code_with_retries(zipped)
        wait_active()

        # 2) Fire macro sniffer — this exercises the new PARTIAL logic
        print("[1149] firing macro sniffer (will potentially trigger PARTIAL_EARLY) …")
        rpt["fire_macro"] = fire_context("macro-frontrun-sniffer")
        # Surface just the alert-related lines from the log
        if rpt["fire_macro"].get("log_tail"):
            alert_lines = [L for L in rpt["fire_macro"]["log_tail"].split("\n")
                            if "[macro" in L or "alert" in L.lower() or "cardinal" in L.lower()]
            rpt["fire_macro_alert_log"] = "\n".join(alert_lines[:20])
        time.sleep(3)
        wait_active()

        # 3) Fire equity sniffer
        print("[1149] firing equity sniffer …")
        rpt["fire_equity"] = fire_context("frontrun-sniffer")
        if rpt["fire_equity"].get("log_tail"):
            alert_lines = [L for L in rpt["fire_equity"]["log_tail"].split("\n")
                            if "[frontrun" in L or "alert" in L.lower() or "cardinal" in L.lower()]
            rpt["fire_equity_alert_log"] = "\n".join(alert_lines[:20])
        time.sleep(3)

        # 4) Read back both alert states to verify last_cardinal_count is set
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/_alerts/macro-frontrun-sniffer-alert-state.json")
            ms = json.loads(obj["Body"].read())
            rpt["macro_state"] = {
                "last_regime":          ms.get("last_regime"),
                "last_score":           ms.get("last_score"),
                "last_cardinal_count":  ms.get("last_cardinal_count"),
                "last_alert_kind":      ms.get("last_alert_kind"),
                "last_alert_reason":    ms.get("last_alert_reason"),
                "last_alert_at":        ms.get("last_alert_at"),
                "alerts_today":         ms.get("alerts_today"),
            }
        except ClientError as e:
            rpt["macro_state"] = f"NOT_FOUND: {e.response['Error']['Code']}"

        try:
            obj2 = s3.get_object(Bucket=BUCKET, Key="data/_alerts/frontrun-sniffer-alert-state.json")
            es = json.loads(obj2["Body"].read())
            rpt["equity_state"] = {
                "last_regime":          es.get("last_regime"),
                "last_score":           es.get("last_score"),
                "last_cardinal_count":  es.get("last_cardinal_count"),
                "last_alert_kind":      es.get("last_alert_kind"),
                "last_alert_reason":    es.get("last_alert_reason"),
                "last_alert_at":        es.get("last_alert_at"),
                "alerts_today":         es.get("alerts_today"),
            }
        except ClientError as e:
            rpt["equity_state"] = f"NOT_FOUND: {e.response['Error']['Code']}"

    except Exception as e:
        rpt["fatal_err"] = str(e)[:500]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1149.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    out = {k: v for k, v in rpt.items() if k != "traceback"}
    # Trim log_tail in fire_* results for readability
    for k in ("fire_macro", "fire_equity"):
        if isinstance(out.get(k), dict):
            out[k] = {x: y for x, y in out[k].items() if x != "log_tail"}
    print(json.dumps(out, indent=2, default=str)[:5000])


if __name__ == "__main__":
    main()
