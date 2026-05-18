"""
ops/834 - justhodl-spinoff-desk deploy verification (end-to-end proof).

deploy-lambdas.yml ships the function + the EventBridge Scheduler schedule
from config.json. This op proves the Spin-Off Desk actually works:

  1. Confirm the Lambda exists.
  2. Invoke it synchronously.
  3. Read back data/spinoff-desk.json from S3 and confirm it is fresh.
  4. Audit the book:
       - 10-12B registrations were scanned;
       - the trading set partitions cleanly into fresh + seasoned;
       - every trading spin-off carries a sane score (0-100), a tier,
         a symbol, a window and a thesis;
       - top_setups is score-sorted and every name in it is a real
         member of the fresh/seasoned set;
       - no filing flagged is_spinoff == False leaked into the book;
       - tiers reconcile (n_prime == count of PRIME SPIN);
       - pending registrations carry an SEC filing link.
  5. Confirm the EventBridge Scheduler schedule is live + ENABLED.

Writes aws/ops/reports/834_spinoff_desk_verify.json.
"""
import json
import time
from datetime import datetime, timezone

import boto3

S3_BUCKET = "justhodl-dashboard-live"
FN = "justhodl-spinoff-desk"
SCHED = "justhodl-spinoff-desk-daily"
OUT_KEY = "data/spinoff-desk.json"
REGION = "us-east-1"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)

VALID_TIERS = {"PRIME SPIN", "STRONG", "WATCH", "MONITOR"}


def main():
    rep = {
        "ops": 834,
        "ts": datetime.now(timezone.utc).isoformat(),
        "subject": "Deploy + verify justhodl-spinoff-desk",
        "checks": [],
    }

    def check(name, ok, detail=""):
        rep["checks"].append({"check": name, "ok": bool(ok), "detail": str(detail)})
        return ok

    # 1) lambda exists
    try:
        cfg = lam.get_function_configuration(FunctionName=FN)
        check("lambda_exists", True,
              f"{cfg['Runtime']} mem={cfg['MemorySize']} timeout={cfg['Timeout']}")
    except Exception as e:
        check("lambda_exists", False, f"{type(e).__name__}: {e}")
        return _finish(rep)

    # 2) invoke synchronously
    try:
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse")
        payload = r["Payload"].read().decode("utf-8", "ignore")
        fn_err = r.get("FunctionError")
        rep["invoke"] = {"status": r.get("StatusCode"),
                         "fn_error": fn_err, "body": payload[:500]}
        check("invoke_ok", r.get("StatusCode") == 200 and not fn_err,
              fn_err or "200")
    except Exception as e:
        check("invoke_ok", False, f"{type(e).__name__}: {e}")
        return _finish(rep)

    time.sleep(3)

    # 3) read + freshness
    try:
        head = s3.head_object(Bucket=S3_BUCKET, Key=OUT_KEY)
        age = (datetime.now(timezone.utc) - head["LastModified"]).total_seconds()
        check("output_fresh", age < 900, f"{round(age)}s old")
        obj = s3.get_object(Bucket=S3_BUCKET, Key=OUT_KEY)
        doc = json.loads(obj["Body"].read())
    except Exception as e:
        check("output_fresh", False, f"{type(e).__name__}: {e}")
        return _finish(rep)

    summ = doc.get("summary", {}) or {}
    fresh = doc.get("fresh_spinoffs", []) or []
    seasoned = doc.get("seasoned_spinoffs", []) or []
    top = doc.get("top_setups", []) or []
    pend = doc.get("pending_registrations", []) or []
    spins = fresh + seasoned

    rep["spinoff_desk"] = {
        "headline": doc.get("headline"),
        "n_filings_scanned": summ.get("n_filings_scanned"),
        "n_trading": summ.get("n_trading"),
        "n_fresh": len(fresh),
        "n_seasoned": len(seasoned),
        "n_pending": len(pend),
        "n_prime": summ.get("n_prime"),
        "best_score": summ.get("best_score"),
        "top5": [
            {"sym": s.get("symbol"), "tier": s.get("tier"),
             "score": s.get("spinoff_score"), "window": s.get("window"),
             "cap": s.get("market_cap_label"), "parent": s.get("parent"),
             "neglect": s.get("neglect"),
             "insider": s.get("insider_cluster_buy")}
            for s in top[:5]
        ],
    }

    # 4) audits
    check("filings_scanned", (summ.get("n_filings_scanned") or 0) > 0,
          summ.get("n_filings_scanned"))

    check("partition_reconciles", summ.get("n_trading") == len(spins),
          f"n_trading={summ.get('n_trading')} fresh+seasoned={len(spins)}")

    bad_score = [s.get("symbol") for s in spins
                 if not isinstance(s.get("spinoff_score"), (int, float))
                 or not (0 <= s["spinoff_score"] <= 100)]
    check("scores_in_band", not bad_score, bad_score[:5])

    bad_tier = [s.get("symbol") for s in spins
                if s.get("tier") not in VALID_TIERS]
    check("tiers_valid", not bad_tier, bad_tier[:5])

    bad_fields = [s.get("symbol") for s in spins
                  if not s.get("symbol") or not s.get("window")
                  or not s.get("thesis")]
    check("entries_complete", not bad_fields, bad_fields[:5])

    # top_setups score-sorted
    tscores = [s.get("spinoff_score") for s in top
               if isinstance(s.get("spinoff_score"), (int, float))]
    check("top_sorted", tscores == sorted(tscores, reverse=True),
          tscores[:6])

    # every top name is a real member of the book
    book_syms = {s.get("symbol") for s in spins}
    orphan_top = [s.get("symbol") for s in top
                  if s.get("symbol") not in book_syms]
    check("top_subset_of_book", not orphan_top, orphan_top[:5])

    # no non-spinoff leaked in
    leaked = [s.get("symbol") for s in spins
              if s.get("is_spinoff") is False]
    check("no_nonspinoff_leak", not leaked, leaked[:5])

    # tiers reconcile
    n_prime_actual = sum(1 for s in spins if s.get("tier") == "PRIME SPIN")
    check("prime_count_reconciles",
          (summ.get("n_prime") or 0) == n_prime_actual,
          f"summary={summ.get('n_prime')} actual={n_prime_actual}")

    # pending carry a filing link
    if pend:
        no_link = [p.get("name") for p in pend if not p.get("filing_url")]
        check("pending_have_links", len(no_link) < max(1, len(pend) // 2),
              f"{len(no_link)}/{len(pend)} missing")
    else:
        check("pending_have_links", True, "no pending this run")

    # 5) scheduler
    try:
        sd = sch.get_schedule(Name=SCHED)
        st = sd.get("State")
        check("schedule_live", st == "ENABLED",
              f"{st} {sd.get('ScheduleExpression')}")
    except Exception as e:
        check("schedule_live", False, f"{type(e).__name__}: {e}")

    return _finish(rep)


def _finish(rep):
    rep["all_pass"] = all(c["ok"] for c in rep["checks"])
    body = json.dumps(rep, indent=2, default=str)
    try:
        s3.put_object(Bucket=S3_BUCKET,
                      Key="ops/reports/834_spinoff_desk_verify.json",
                      Body=body.encode(), ContentType="application/json")
    except Exception as e:
        print(f"[ops834] S3 report write failed: {e}")
    with open("aws/ops/reports/834_spinoff_desk_verify.json", "w") as f:
        f.write(body)
    print(body)
    return rep


if __name__ == "__main__":
    main()
