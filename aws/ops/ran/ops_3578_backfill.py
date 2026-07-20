"""ops 3578 — deal-history backfill: one deep sweep (100 pages ≈ days-weeks of
tape) through the SAME classify/parse pipeline so the base-rate event study is
born POPULATED instead of waiting 21 trading days. Then a normal run refreshes
the live feed with base_rates included. Runs after 3577 alphabetically."""
import json, sys, time
from datetime import datetime, timezone
from pathlib import Path
import boto3
from ops_report import report

LAM = boto3.client("lambda", "us-east-1")
S3C = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-deal-scanner"

with report("3578_backfill") as rep:
    rep.heading("ops 3578 — base-rate ledger backfill (population study born live)")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:420]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:380]
        print(line); rep.log(line)
        if not ok:
            fails.append(n)

    # G1 backfill run → ledger jump + populated base rates
    try:
        pre = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/deal-history.json")["Body"].read())
        pre_n = pre.get("n") or 0
    except Exception:
        pre_n = 0
    LAM.invoke(FunctionName=FN, InvocationType="Event",
               Payload=json.dumps({"backfill_pages": 100}).encode())
    h = None; dl = time.time() + 560
    while time.time() < dl:
        try:
            cand = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/deal-history.json")["Body"].read())
            br = cand.get("base_rates") or {}
            if (cand.get("n") or 0) > max(pre_n, 40) and any((v.get("n5") or 0) >= 5 for v in br.values()):
                h = cand; break
        except Exception:
            pass
        time.sleep(20)
    if h:
        br = h.get("base_rates") or {}
        gate("G1_ledger_populated", True,
             f"ledger {pre_n}→{h.get('n')} entries · base_rates: " +
             " | ".join(f"{k}: n5={v.get('n5')} med5={v.get('med_fwd5_ex')}% "
                        f"n21={v.get('n21')} med21={v.get('med_fwd21_ex')}% hit21={v.get('hit21')}%"
                        for k, v in br.items()))
        out["base_rates"] = br
        out["ledger_n"] = h.get("n")
    else:
        gate("G1_ledger_populated", False, f"ledger did not populate (pre={pre_n})")

    # G2 normal run refreshes live feed with base rates
    t1 = datetime.now(timezone.utc)
    LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    ok2 = False; det = ""; dl = time.time() + 480
    while time.time() < dl:
        try:
            j = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/deal-scanner.json")["Body"].read())
            if j.get("generated_at", "") > t1.isoformat()[:19] and (j.get("base_rates") or {}):
                ok2 = True
                det = (f"feed base_rate types={list((j.get('base_rates') or {}).keys())} "
                       f"history.n={((j.get('history') or {}).get('n_entries'))}")
                break
        except Exception:
            pass
        time.sleep(15)
    gate("G2_feed_base_rates", ok2, det or "feed did not refresh with base_rates")

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3578.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
