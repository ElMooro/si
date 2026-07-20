"""ops 3579 — backfill fix-forward: diagnose 3578 via CloudWatch (expect 600s
Task timed out on the 100-page sweep), raise fn timeout to 900, re-run the
backfill SYNCHRONOUSLY so failure is visible, then normal run → feed carries
base_rates. [skip-deploy] push: engine code unchanged."""
import json, sys, time
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=920, retries={"max_attempts": 0}))
LOGS = boto3.client("logs", "us-east-1")
S3C = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-deal-scanner"

with report("3579_backfill_v2") as rep:
    rep.heading("ops 3579 — backfill v2 (900s + sync)")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:500]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:460]
        print(line); rep.log(line)
        if not ok:
            fails.append(n)

    # G0 diagnose the 3578 attempt from CloudWatch (informational — never fails the run)
    try:
        ev = LOGS.filter_log_events(
            logGroupName=f"/aws/lambda/{FN}",
            startTime=int((time.time() - 5400) * 1000),
            filterPattern='?"BACKFILL" ?"Task timed out" ?"REPORT"',
            limit=50)
        lines = [e["message"].strip()[:160] for e in ev.get("events", [])
                 if "timed out" in e["message"] or "BACKFILL" in e["message"]
                 or ("REPORT" in e["message"] and "Duration" in e["message"])]
        gate("G0_diagnosis", True, " || ".join(lines[-6:]) or "no matching log lines (group may be quiet)")
    except Exception as e:
        gate("G0_diagnosis", True, f"log read skipped: {str(e)[:120]}")

    # G1 timeout -> 900 (Lambda max), settle
    try:
        cfg = LAM.get_function_configuration(FunctionName=FN)
        if cfg.get("Timeout") != 900 or cfg.get("MemorySize") != 1024:
            LAM.update_function_configuration(FunctionName=FN, Timeout=900, MemorySize=1024)
            dl = time.time() + 180
            while time.time() < dl:
                cfg = LAM.get_function_configuration(FunctionName=FN)
                if cfg.get("LastUpdateStatus") == "Successful" and cfg.get("Timeout") == 900:
                    break
                time.sleep(6)
        gate("G1_timeout_900", cfg.get("Timeout") == 900 and cfg.get("MemorySize") == 1024,
             f"timeout={cfg.get('Timeout')} mem={cfg.get('MemorySize')}")
    except Exception as e:
        gate("G1_timeout_900", False, str(e)[:200])

    # G2 SYNC backfill — failure becomes visible instead of vanishing async
    try:
        r = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       Payload=json.dumps({"backfill_pages": 100}).encode())
        pl = json.loads(r["Payload"].read() or b"{}")
        if isinstance(pl, dict) and pl.get("errorMessage"):
            gate("G2_backfill_sync", False, f"fn error: {pl.get('errorMessage')[:220]}")
        else:
            body = json.loads(pl.get("body", "{}"))
            h = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/deal-history.json")["Body"].read())
            br = h.get("base_rates") or {}
            ok = (body.get("ledger_n") or 0) >= 60 and any((v.get("n5") or 0) >= 5 for v in br.values())
            gate("G2_backfill_sync", ok,
                 f"prs={body.get('n_prs')} deals={body.get('n_deals')} ledger={body.get('ledger_n')} "
                 f"filled={body.get('filled')} · base_rates: " +
                 (" | ".join(f"{k}: n5={v.get('n5')} med5={v.get('med_fwd5_ex')}% n21={v.get('n21')} "
                             f"med21={v.get('med_fwd21_ex')}% hit21={v.get('hit21')}%"
                             for k, v in br.items()) or "EMPTY"))
            out["base_rates"] = br
            out["backfill"] = body
    except Exception as e:
        gate("G2_backfill_sync", False, str(e)[:300])

    # G3 normal run → live feed carries base_rates
    t1 = datetime.now(timezone.utc)
    try:
        LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        ok3 = False; det = ""; dl = time.time() + 480
        while time.time() < dl:
            try:
                j = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/deal-scanner.json")["Body"].read())
                if j.get("generated_at", "") > t1.isoformat()[:19] and (j.get("base_rates") or {}):
                    ok3 = True
                    det = (f"feed base_rate types={list((j.get('base_rates') or {}).keys())} "
                           f"history.n={(j.get('history') or {}).get('n_entries')}")
                    break
            except Exception:
                pass
            time.sleep(15)
        gate("G3_feed_base_rates", ok3, det or "feed did not refresh with base_rates")
    except Exception as e:
        gate("G3_feed_base_rates", False, str(e)[:200])

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3579.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
