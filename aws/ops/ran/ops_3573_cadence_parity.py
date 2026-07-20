"""ops 3573 — close 3571's single gap: config/live-rule cadence parity.
Live rule was ALREADY cron(5 */3 * * ? *) (every 3h — superior to the 3x/day
in the stale config); repo now matches live. Gate: describe_rule == config
cron, engine redeployed with 8-runs/day strings, feed still v2-fresh."""
import io, json, sys, time, urllib.request, zipfile
from pathlib import Path
import boto3
from ops_report import report
LAM = boto3.client("lambda", "us-east-1"); EVT = boto3.client("events", "us-east-1")
S3C = boto3.client("s3", "us-east-1"); UA = {"User-Agent": "Mozilla/5.0 (ops-3573)"}
with report("3573_cadence_parity") as rep:
    rep.heading("ops 3573 — deal-scanner cadence parity (config == live rule)")
    out = {"gates": {}}; fails = []
    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:300]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:260]
        print(line); rep.log(line)
        if not ok: fails.append(n)
    cfg = json.loads(Path("aws/lambdas/justhodl-deal-scanner/config.json").read_text())
    want = cfg["schedule"]["cron"]
    live = ""
    dl = time.time() + 300
    while time.time() < dl:
        try:
            live = EVT.describe_rule(Name="deal-scanner-daily").get("ScheduleExpression")
            if live == want: break
        except Exception: pass
        time.sleep(12)
    gate("G1_rule_parity", live == want == "cron(5 */3 * * ? *)", f"config={want} live={live}")
    ok2 = False; dl = time.time() + 420
    while time.time() < dl:
        try:
            if LAM.get_function_configuration(FunctionName="justhodl-deal-scanner").get("LastUpdateStatus") == "Successful":
                info = LAM.get_function(FunctionName="justhodl-deal-scanner")
                with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"], headers=UA), timeout=60) as r:
                    src = zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8", "replace")
                if '"runs_per_day": 8,' in src and 'every 3 hours' in src:
                    ok2 = True; break
        except Exception: pass
        time.sleep(12)
    gate("G2_engine_8x_strings", ok2, "zip markers runs_per_day=8 + every-3-hours")
    try:
        j = json.loads(S3C.get_object(Bucket="justhodl-dashboard-live", Key="data/deal-scanner.json")["Body"].read())
        gate("G3_feed_v2_intact", j.get("version") == "2.0.0" and len(j.get("by_sector") or {}) >= 11,
             f"version={j.get('version')} boards={len(j.get('by_sector') or {})}")
    except Exception as e:
        gate("G3_feed_v2_intact", False, str(e)[:120])
    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3573.json").write_text(json.dumps(out, indent=2, default=str)); sys.exit(0)
