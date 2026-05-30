"""ops 1142 — Equity convergence fingerprint rollout.

Adds equity-symmetric priority-1 alert: when 3+ of these 4 cardinal
equity-microstructure categories fire in the top setup's smoking guns:
  1. DEALER_GEX (dealer gamma exposure shifts)
  2. OPTIONS_FLOW or OPTIONS_GAMMA (unusual options activity)
  3. SKEW / VOL / TAIL HEDGING (skew steepening, tail-hedge bidding, IV crush)
  4. CFTC / SHORT_INTEREST / SQUEEZE (commercials flip, short stress)

That's the Jan 2021 GME / Feb 2018 vol unwind / Dec 2018 dealer cascade
fingerprint — fires a priority-1 Telegram alert that bypasses cooldown
symmetric to the macro fingerprint pattern.

New helper functions in the router:
  _equity_convergence_fingerprint(brief)
    Scans suspected_setups[0..2].smoking_gun_signals[] looking for the
    4 cardinals. Returns (fired, detail_dict with cardinals_fired booleans
    + contributing_signals + top_setup_target).

Updated:
  _format_equity_alert(brief, ..., conv_fired, conv_detail)
    Adds CONVERGENCE title prefix when fingerprint fires + a 4-cardinal
    breakdown block in the Telegram message:
      ✓ Dealer GEX
      ✓ Options Flow / Gamma
      ✓ Skew / Vol / Tail Hedge
      ✗ CFTC / Short Interest
      → target: AMD UPSIDE

  _maybe_alert_equity_sniffer()
    Now runs _equity_convergence_fingerprint() and passes the result to
    _decide_alert_kind which prioritizes CONVERGENCE > REGIME > EXTREME > SCORE_JUMP.
    State writes the same fields macro uses: last_convergence_fingerprint_at,
    last_convergence_detail.

Front-end: ai-alerts-cockpit-kit.js detectEquityConvergence() mirrors the
backend logic and renders a 4-pillar dashboard alongside the existing
macro convergence dashboard. Cockpit at /alerts.html now shows BOTH
fingerprints stacked vertically.

This ops redeploys router with the new code, invokes equity sniffer to
verify convergence detection runs without errors, captures the detail
shape if anything fires.
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


def wait_active(t=180):
    end = time.time() + t
    while time.time() < end:
        try:
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") in ("Successful", None):
                return True
            if c.get("LastUpdateStatus") == "Failed": return False
        except ClientError: pass
        time.sleep(2)
    return False


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # 1) Redeploy router
        src_dir = os.path.join(REPO_ROOT, "aws/lambdas", FN, "source")
        wait_active()
        lam.update_function_code(FunctionName=FN, ZipFile=zip_src(src_dir), Publish=False)
        wait_active()
        rpt["redeploy"] = "OK"

        # 2) Invoke equity sniffer — this exercises the new convergence code
        print("[1142] invoking equity sniffer with convergence detector …")
        inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                         Payload=json.dumps({"contexts": ["frontrun-sniffer"]}).encode(),
                         LogType="Tail")
        body_resp = json.loads(inv["Payload"].read() or b"{}")
        if isinstance(body_resp, dict) and "body" in body_resp:
            try: body_resp = json.loads(body_resp["body"])
            except Exception: pass
        log_tail = base64.b64decode(inv.get("LogResult","")).decode("utf-8","replace")[-2400:]

        rpt["invoke"] = {
            "fn_err": inv.get("FunctionError"),
            "n_ok": body_resp.get("n_ok") if isinstance(body_resp, dict) else None,
            "duration_s": body_resp.get("duration_s") if isinstance(body_resp, dict) else None,
        }
        # Capture alert-related log lines (including convergence detection results)
        rpt["alert_log_lines"] = "\n".join(
            L for L in log_tail.split("\n") if "[frontrun]" in L or "[macro_frontrun]" in L
        )[:2200]

        # 3) Verify the brief has smoking-gun categories the detector recognizes
        time.sleep(2)
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/frontrun-sniffer.json")
            brief = json.loads(obj["Body"].read())
            setups = brief.get("suspected_setups") or []

            # Run the same detection logic the Lambda runs, locally, to verify shape
            cards = {"DEALER_GEX": False, "OPTIONS_FLOW_GAMMA": False,
                     "SKEW_VOL": False, "CFTC_SHORT": False}
            contributing = []
            for sx in setups[:2]:
                for g in (sx.get("smoking_gun_signals") or []):
                    cat = (g.get("category") or "").upper()
                    sig = (g.get("signal") or "")[:120]
                    if ("DEALER_GEX" in cat or "GEX" in cat) and not cards["DEALER_GEX"]:
                        cards["DEALER_GEX"] = True
                        contributing.append({"cardinal":"DEALER_GEX","category":g.get("category"),"signal":sig})
                    elif ("OPTIONS_FLOW" in cat or "OPTIONS_GAMMA" in cat or cat == "GAMMA") and not cards["OPTIONS_FLOW_GAMMA"]:
                        cards["OPTIONS_FLOW_GAMMA"] = True
                        contributing.append({"cardinal":"OPTIONS_FLOW_GAMMA","category":g.get("category"),"signal":sig})
                    elif ("SKEW" in cat or "IV_CRUSH" in cat or "TAIL" in cat or cat == "VOL" or "CATALYST_SKEW" in cat) and not cards["SKEW_VOL"]:
                        cards["SKEW_VOL"] = True
                        contributing.append({"cardinal":"SKEW_VOL","category":g.get("category"),"signal":sig})
                    elif ("CFTC" in cat or "SHORT_INTEREST" in cat or "SHORT" == cat or "SQUEEZE" in cat) and not cards["CFTC_SHORT"]:
                        cards["CFTC_SHORT"] = True
                        contributing.append({"cardinal":"CFTC_SHORT","category":g.get("category"),"signal":sig})

            n_present = sum(1 for v in cards.values() if v)
            fired = n_present >= 3

            rpt["equity_convergence_detection"] = {
                "fired": fired,
                "n_cardinal_present": n_present,
                "n_cardinal_total": 4,
                "cardinals_fired": cards,
                "top_setup_target": (setups[0].get("target_asset") if setups else None),
                "top_setup_direction": (setups[0].get("target_direction") if setups else None),
                "contributing_signals": contributing,
                "all_smoking_gun_categories_seen": sorted(set(
                    (g.get("category") or "") for sx in setups for g in (sx.get("smoking_gun_signals") or [])
                )),
            }
        except ClientError as e:
            rpt["equity_convergence_detection"] = f"NO_BRIEF: {e.response['Error']['Code']}"

        # 4) Check alert state file — did anything change?
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/_alerts/frontrun-sniffer-alert-state.json")
            state = json.loads(obj["Body"].read())
            rpt["equity_alert_state"] = state
        except ClientError:
            rpt["equity_alert_state"] = "NOT_WRITTEN_YET (no alert condition met — fine)"

    except Exception as e:
        rpt["fatal_err"] = str(e)[:500]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1142.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k: v for k, v in rpt.items() if k != "traceback"}, indent=2, default=str)[:4500])


if __name__ == "__main__":
    main()
