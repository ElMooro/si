"""ops 3580 — historical base-rate backfill via Polygon date-windowed tape
(v3.1.1). 3579 finding: FMP latest-feeds reach only ~3-7 days → zero matured
entries. Now: 90-day window, fill cap 300, top-up passes until base rates
populate, then normal run refreshes the live feed."""
import json, sys, time
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=920, retries={"max_attempts": 0}))
S3C = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-deal-scanner"
import io, urllib.request, zipfile
UA = {"User-Agent": "Mozilla/5.0 (ops-3580)"}

with report("3580_backfill_hist") as rep:
    rep.heading("ops 3580 — historical backfill (Polygon 90d window)")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:520]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:480]
        print(line); rep.log(line)
        if not ok:
            fails.append(n)

    # G1 settle v3.1.1 + config heal (workflow stomps: enforce 1024/900)
    ok1 = False; dl = time.time() + 660
    while time.time() < dl:
        try:
            cfg = LAM.get_function_configuration(FunctionName=FN)
            if cfg.get("LastUpdateStatus") == "Successful":
                info = LAM.get_function(FunctionName=FN)
                with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"], headers=UA), timeout=60) as r:
                    src = zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8", "replace")
                if all(m in src for m in ('VERSION = "3.1.1"', "def fetch_polygon_window", "backfill_days")):
                    ok1 = True; break
        except Exception:
            pass
        time.sleep(12)
    try:
        cfg = LAM.get_function_configuration(FunctionName=FN)
        if cfg.get("MemorySize") != 1024 or cfg.get("Timeout") != 900:
            LAM.update_function_configuration(FunctionName=FN, MemorySize=1024, Timeout=900)
            dl2 = time.time() + 180
            while time.time() < dl2:
                cfg = LAM.get_function_configuration(FunctionName=FN)
                if cfg.get("LastUpdateStatus") == "Successful" and cfg.get("Timeout") == 900:
                    break
                time.sleep(6)
    except Exception:
        pass
    gate("G1_settled_3111", ok1 and cfg.get("Timeout") == 900,
         f"markers ok={ok1} mem={cfg.get('MemorySize')} timeout={cfg.get('Timeout')}")

    # G2 historical backfill + top-up passes until base rates populate
    def sync(payload):
        r = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       Payload=json.dumps(payload).encode())
        pl = json.loads(r["Payload"].read() or b"{}")
        if isinstance(pl, dict) and pl.get("errorMessage"):
            return None, pl.get("errorMessage")
        return json.loads(pl.get("body", "{}")), None

    br, ledger_n, passes, err = {}, 0, [], None
    body, err = sync({"backfill_days": 90})
    if body:
        passes.append(f"hist90: prs={body.get('n_prs')} deals={body.get('n_deals')} "
                      f"ledger={body.get('ledger_n')} filled={body.get('filled')}")
        for i in range(3):
            h = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/deal-history.json")["Body"].read())
            br, ledger_n = h.get("base_rates") or {}, h.get("n") or 0
            if any((v.get("n21") or 0) >= 5 for v in br.values()) or i == 2:
                break
            b2, e2 = sync({"backfill_days": 2})   # cheap top-up: fills-only pass over existing entries
            passes.append(f"topup{i+1}: filled={(b2 or {}).get('filled')}" + (f" err={e2[:60]}" if e2 else ""))
    ok2 = ledger_n >= 120 and any((v.get("n5") or 0) >= 5 for v in br.values())
    gate("G2_hist_backfill", ok2,
         (f"err={err[:180]} · " if err else "") + " || ".join(passes) +
         f" · ledger={ledger_n} · base_rates: " +
         (" | ".join(f"{k}: n5={v.get('n5')} med5={v.get('med_fwd5_ex')}% n21={v.get('n21')} "
                     f"med21={v.get('med_fwd21_ex')}% hit21={v.get('hit21')}%"
                     for k, v in br.items()) or "EMPTY"))
    out["base_rates"] = br
    out["ledger_n"] = ledger_n

    # G3 normal run → live feed carries base rates
    t1 = datetime.now(timezone.utc)
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

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3580.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
