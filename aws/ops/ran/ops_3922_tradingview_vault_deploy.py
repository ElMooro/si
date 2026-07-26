"""
ops_3922 — DEPLOY justhodl-tradingview (the Metric Vault). Gates: marker
settles + full env-settle discipline; invoke succeeds; registry >= 500
symbols parsed FROM THE BRAIN; >= 150 resolved LIVE; famous symbols spot-
checked (DXY-family/RRPONTSYD/BAMLH0A3HYC live; CL1! honestly UNRESOLVED);
JPLG present with notes; daily Scheduler armed; served page renders.
"""
import io, json, sys, time, urllib.request, zipfile
from pathlib import Path
import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

FN, KEY = "justhodl-tradingview", "data/tradingview.json"
MARK = "tradingview-vault v1.0"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=880, retries={"max_attempts": 0}))
sched = boto3.client("scheduler", region_name="us-east-1")
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0"


def main():
    with report("3922_tradingview_vault_deploy") as rep:
        rep.heading("ops 3922 — TradingView Metric Vault deploy")
        checks = []
        settled = False
        for attempt in range(1, 41):
            try:
                loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
                blob = urllib.request.urlopen(loc, timeout=60).read()
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    if MARK in z.read("lambda_function.py").decode("utf-8", "ignore"):
                        settled = True; rep.ok(f"  settled attempt {attempt}"); break
            except Exception: pass
            time.sleep(15)
        checks.append(("deploy settled", settled))
        if not settled: rep.fail("never settled"); sys.exit(1)
        cfg = lam.get_function_configuration(FunctionName=FN)
        for _ in range(25):
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") != "InProgress": break
            time.sleep(8); cfg = lam.get_function_configuration(FunctionName=FN)
        rep.kv(state=cfg.get("State"), timeout=cfg.get("Timeout"),
               has_keys=all(k in ((cfg.get("Environment") or {}).get("Variables") or {})
                            for k in ("FRED_KEY", "FMP_KEY")))

        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        raw = json.loads(r["Payload"].read())
        if r.get("FunctionError"):
            rep.fail(f"FunctionError: {json.dumps(raw)[:900]}"); sys.exit(1)
        rep.log(f"  invoke: {json.dumps(raw, default=str)[:300]}")
        checks.append(("invoke ok", True))

        doc = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key=KEY)["Body"].read())
        rep.kv(n_symbols=doc.get("n_symbols"), n_live=doc.get("n_live"),
               coverage_pct=doc.get("coverage_pct"),
               by_cat=json.dumps(doc.get("by_category_counts"))[:250])
        checks.append(("registry >= 500 symbols from brain", (doc.get("n_symbols") or 0) >= 500))
        checks.append((">= 150 resolved LIVE", (doc.get("n_live") or 0) >= 150))
        idx = {r_["symbol"]: r_ for r_ in doc.get("symbols") or []}
        for s_ in ("RRPONTSYD", "BAMLH0A3HYC", "DTWEXBGS"):
            row = idx.get(s_) or {}
            rep.log(f"  {s_}: {row.get('status')} value={row.get('value')} notes={row.get('n_notes')}")
            checks.append((f"{s_} LIVE", row.get("status") == "LIVE" and row.get("value") is not None))
        cl = idx.get("CL1!") or {}
        checks.append(("CL1! honestly UNRESOLVED (futures, no free API)",
                       cl.get("status") == "UNRESOLVED"))
        jp = idx.get("JPLG") or {}
        rep.log(f"  JPLG: status={jp.get('status')} n_notes={jp.get('n_notes')} ids={jp.get('note_ids')}")
        checks.append(("JPLG present w/ its brain notes", (jp.get("n_notes") or 0) >= 5))

        sched_ok = False
        try:
            sched.create_schedule(Name="tradingview-vault-daily",
                ScheduleExpression="cron(35 11 * * ? *)",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target={"Arn": cfg["FunctionArn"],
                        "RoleArn": "arn:aws:iam::857687956942:role/justhodl-scheduler-role",
                        "Input": "{}"},
                State="ENABLED", Description="TV Metric Vault daily 11:35 UTC")
            sched_ok = True; rep.ok("  Scheduler created")
        except sched.exceptions.ConflictException:
            sched_ok = True; rep.ok("  Scheduler exists")
        except Exception as e:
            rep.fail(f"  Scheduler: {str(e)[:150]}")
        checks.append(("daily schedule armed", sched_ok))

        page_ok = False
        for a in range(9):
            try:
                req = urllib.request.Request(
                    f"https://justhodl.ai/tradingview.html?v={int(time.time())}{a}",
                    headers={"User-Agent": UA, "Cache-Control": "no-cache"})
                html = urllib.request.urlopen(req, timeout=25).read().decode("utf-8", "ignore")
                if "TradingView Metric Vault" in html and "by_category_counts" in html:
                    page_ok = True; rep.ok(f"  page live attempt {a+1}, {len(html)}b"); break
            except Exception: pass
            time.sleep(20)
        checks.append(("served page renders", page_ok))

        failed = [l for l, ok in checks if not ok]
        for l, ok in checks: (rep.ok if ok else rep.fail)(f"  {l}")
        if failed: rep.fail(f"FAILED: {failed}"); sys.exit(1)
        rep.ok(f"PASS_ALL — vault live: {doc.get('n_symbols')} symbols, "
               f"{doc.get('n_live')} LIVE ({doc.get('coverage_pct')}%)")


if __name__ == "__main__":
    main()
