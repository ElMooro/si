"""ops 3571 — deal-scanner v2.0.0: FULL-MARKET coverage + graded deal-win family.
Gates: engine settled (zip marker) · schedule 3x/day on the SAME classic rule
(cap-safe) · live invoke writes v2 feed (by_sector 11 / by_cap 6 / coverage /
signals) · DDB row for any logged deal-win (regime-stamped) · page serves the
new coverage boards. Triggered-by-push: deploy-lambdas + pages.yml run in
parallel; this script polls settle."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=120, retries={"max_attempts": 2}))
S3C = boto3.client("s3", "us-east-1")
EVT = boto3.client("events", "us-east-1")
DDB = boto3.resource("dynamodb", "us-east-1")
UA = {"User-Agent": "Mozilla/5.0 (ops-3571)"}
FN = "justhodl-deal-scanner"
BUCKET = "justhodl-dashboard-live"

with report("3571_deal_scanner_v2") as rep:
    rep.heading("ops 3571 — deal-scanner v2.0.0 full-market + graded family")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:400]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:360]
        print(line); rep.log(line)
        if not ok:
            fails.append(n)

    # G1 — deployed code settled on v2 marker (deploy-lambdas runs in parallel)
    ok1 = False; dl = time.time() + 660
    while time.time() < dl:
        try:
            if LAM.get_function_configuration(FunctionName=FN).get("LastUpdateStatus") == "Successful":
                info = LAM.get_function(FunctionName=FN)
                with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"], headers=UA), timeout=60) as r:
                    src = zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8", "replace")
                if 'VERSION = "2.0.0"' in src and "deal-win" in src and "by_sector" in src:
                    ok1 = True; break
        except Exception:
            pass
        time.sleep(12)
    gate("G1_settled_v2", ok1, "zip markers VERSION 2.0.0 + deal-win + by_sector")

    # G2 — schedule now 3x/day on the SAME classic rule (no new rule; cap saturated)
    try:
        cr = EVT.describe_rule(Name="deal-scanner-daily").get("ScheduleExpression")
    except Exception as e:
        cr = str(e)[:80]
    gate("G2_schedule_3x", cr == "cron(30 13,17,22 * * ? *)", f"rule deal-scanner-daily = {cr}")

    # G3 — live invoke → v2 feed with full-market boards
    t0 = datetime.now(timezone.utc).isoformat()
    LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    feed = {}; dl = time.time() + 520
    while time.time() < dl:
        try:
            j = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/deal-scanner.json")["Body"].read())
            if j.get("version") == "2.0.0" and (j.get("generated_at") or "") > t0:
                feed = j; break
        except Exception:
            pass
        time.sleep(15)
    sm = feed.get("summary") or {}
    cov = feed.get("coverage") or {}
    ok3 = (feed.get("version") == "2.0.0"
           and (sm.get("n_prs_scanned") or 0) >= 1000
           and len(feed.get("by_sector") or {}) >= 11
           and len(feed.get("by_cap") or {}) >= 6
           and all(k in (cov.get("sources") or {}) for k in ("fmp_pr", "fmp_news", "polygon"))
           and len(feed.get("deals") or []) > 0)
    gate("G3_feed_v2_live", ok3,
         f"prs={sm.get('n_prs_scanned')} deals={sm.get('n_deals')} sectors_boards={len(feed.get('by_sector') or {})} "
         f"caps_boards={len(feed.get('by_cap') or {})} sectors_hit={cov.get('sectors_with_deals')}/11 "
         f"caps_hit={cov.get('caps_with_deals')}/6 tape_tickers={cov.get('n_unique_tickers_in_tape')} "
         f"sources={cov.get('sources')}")

    # G4 — graded family: signals fields present; verify a real DDB row if any logged
    logged = sm.get("signals") or []
    ok4 = isinstance(sm.get("signals_logged"), int) and isinstance(logged, list)
    detail4 = f"signals_logged={sm.get('signals_logged')} list={[x.get('ticker') for x in logged]}"
    if logged:
        try:
            tk = logged[0]["ticker"]
            sid = f"deal-win#{tk}#{datetime.now(timezone.utc).date().isoformat()}"
            row = DDB.Table("justhodl-signals").get_item(Key={"signal_id": sid}).get("Item")
            ok4 = ok4 and bool(row) and row.get("schema_version") == "2" and bool((row.get("metadata") or {}).get("regime") is not None)
            detail4 += (f" · DDB {sid}: {'FOUND' if row else 'MISSING'}"
                        + (f" conf={row.get('confidence')} base={row.get('baseline_price')} regime={bool((row.get('metadata') or {}).get('regime'))}" if row else ""))
        except Exception as e:
            ok4 = False; detail4 += f" · ddb err {str(e)[:60]}"
    else:
        detail4 += " · none crossed the bar this run (structural fields verified; family fires on transformative wins only)"
    gate("G4_graded_family", ok4, detail4)

    # G5 — page serves the new coverage boards (pages.yml deploys in parallel; bare URL)
    ok5 = False; dl = time.time() + 330
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/deal-scanner.html", headers=UA), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            if "Market Coverage — every sector" in html and "Graded Signals" in html and "All Cap Tiers" in html:
                ok5 = True; break
        except Exception:
            pass
        time.sleep(15)
    gate("G5_page_boards", ok5, "served markers: Market Coverage / All Cap Tiers / Graded Signals")

    out["feed_snapshot"] = {"n_deals": sm.get("n_deals"), "n_green": sm.get("n_green"),
                            "n_ai": sm.get("n_ai"), "n_ai_mega": sm.get("n_ai_mega"),
                            "signals_logged": sm.get("signals_logged"), "signals": logged,
                            "coverage": cov}
    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3571.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
