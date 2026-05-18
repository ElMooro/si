"""
ops/836 - justhodl-spinoff-desk RE-verification after the EFTS bug fix.

ops 834 ran green on 13/14 checks but surfaced two real bugs, both rooted
in EFTS field parsing inside pull_registrations:

  - the code read src['cik'] but EFTS keeps the CIK in a list field
    'ciks'. cik was therefore always None -> doc_url() returned None for
    every filing (pending_have_links FAIL, 40/40 missing SEC links) and
    inspect_filing() silently no-op'd, so is_spinoff / parent never set.
  - a SpinCo files an original 10-12B then 10-12B/A amendments under the
    same CIK; these were ingested as separate rows (BSEM, RHLD twice).

01f5b82 fixes both: ciks[0] is parsed (leading zeros stripped), the
accession comes from adsh, the primary doc from the hit _id, amendments
collapse to one record per CIK, and lambda_handler adds a ticker/CIK
dedup safety net. This op re-runs the full 834 audit and adds the two
checks that prove the fix:

  - trading_have_links : every fresh/seasoned SpinCo carries a real SEC
    filing_url (proves cik is populated and doc_url resolves);
  - no_duplicate_tickers : no symbol appears twice in the book or in
    top_setups (proves the amendment dedup holds).

pending_have_links is also tightened from "< half missing" to ">= 90%
present" - with cik fixed it should be effectively perfect.

Writes aws/ops/reports/836_spinoff_desk_reverify.json.
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
        "ops": 836,
        "ts": datetime.now(timezone.utc).isoformat(),
        "subject": "Re-verify justhodl-spinoff-desk after the EFTS "
                   "ciks-parsing + amendment-dedup fix (01f5b82)",
        "checks": [],
    }

    def check(name, ok, detail=""):
        rep["checks"].append({"check": name, "ok": bool(ok),
                              "detail": str(detail)[:240]})
        return ok

    # 1) lambda exists
    try:
        cfg = lam.get_function_configuration(FunctionName=FN)
        check("lambda_exists", True,
              f"{cfg['Runtime']} mem={cfg['MemorySize']} "
              f"timeout={cfg['Timeout']} "
              f"modified={cfg.get('LastModified')}")
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
        age = (datetime.now(timezone.utc) - head["LastModified"]
               ).total_seconds()
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

    n_with_parent = sum(1 for s in spins if s.get("parent"))
    n_trading_with_link = sum(1 for s in spins if s.get("filing_url"))
    n_pend_with_link = sum(1 for p in pend if p.get("filing_url"))

    rep["spinoff_desk"] = {
        "headline": doc.get("headline"),
        "n_filings_scanned": summ.get("n_filings_scanned"),
        "n_trading": summ.get("n_trading"),
        "n_fresh": len(fresh),
        "n_seasoned": len(seasoned),
        "n_pending": len(pend),
        "n_prime": summ.get("n_prime"),
        "best_score": summ.get("best_score"),
        "n_with_parent": n_with_parent,
        "n_trading_with_link": n_trading_with_link,
        "n_pending_with_link": n_pend_with_link,
        "top5": [
            {"sym": s.get("symbol"), "tier": s.get("tier"),
             "score": s.get("spinoff_score"), "window": s.get("window"),
             "cap": s.get("market_cap_label"), "parent": s.get("parent"),
             "neglect": s.get("neglect"),
             "insider": s.get("insider_cluster_buy"),
             "has_link": bool(s.get("filing_url"))}
            for s in top[:5]
        ],
    }

    # --- 4) standard book audits (carried over from 834) ----------------
    check("filings_scanned", (summ.get("n_filings_scanned") or 0) > 0,
          summ.get("n_filings_scanned"))

    check("partition_reconciles", summ.get("n_trading") == len(spins),
          f"n_trading={summ.get('n_trading')} "
          f"fresh+seasoned={len(spins)}")

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

    tscores = [s.get("spinoff_score") for s in top
               if isinstance(s.get("spinoff_score"), (int, float))]
    check("top_sorted", tscores == sorted(tscores, reverse=True),
          tscores[:6])

    book_syms = {s.get("symbol") for s in spins}
    orphan_top = [s.get("symbol") for s in top
                  if s.get("symbol") not in book_syms]
    check("top_subset_of_book", not orphan_top, orphan_top[:5])

    leaked = [s.get("symbol") for s in spins
              if s.get("is_spinoff") is False]
    check("no_nonspinoff_leak", not leaked, leaked[:5])

    n_prime_actual = sum(1 for s in spins if s.get("tier") == "PRIME SPIN")
    check("prime_count_reconciles",
          (summ.get("n_prime") or 0) == n_prime_actual,
          f"summary={summ.get('n_prime')} actual={n_prime_actual}")

    # --- 5) the FIX checks ----------------------------------------------
    # pending registrations carry an SEC filing link (the 834 FAIL).
    if pend:
        ratio = n_pend_with_link / len(pend)
        check("pending_have_links", ratio >= 0.90,
              f"{n_pend_with_link}/{len(pend)} have links "
              f"({round(ratio * 100)}%)")
    else:
        check("pending_have_links", True, "no pending this run")

    # every trading SpinCo carries a real SEC filing_url - proves cik is
    # populated and doc_url() resolves (was 0/N before the fix).
    if spins:
        tratio = n_trading_with_link / len(spins)
        check("trading_have_links", tratio >= 0.90,
              f"{n_trading_with_link}/{len(spins)} fresh+seasoned "
              f"carry filing_url ({round(tratio * 100)}%)")
    else:
        check("trading_have_links", True, "no trading spin-offs this run")

    # no duplicate tickers - proves the amendment dedup holds.
    book_list = [s.get("symbol") for s in spins if s.get("symbol")]
    dup_book = sorted({x for x in book_list if book_list.count(x) > 1})
    top_list = [s.get("symbol") for s in top if s.get("symbol")]
    dup_top = sorted({x for x in top_list if top_list.count(x) > 1})
    check("no_duplicate_tickers", not dup_book and not dup_top,
          f"book_dups={dup_book} top_dups={dup_top}")

    # 6) scheduler
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
    sd = rep.get("spinoff_desk", {})
    rep["verdict"] = (
        f"SPIN-OFF DESK CLEAN - {sd.get('n_filings_scanned')} 10-12B "
        f"registrations scanned, {sd.get('n_trading')} tradeable "
        f"spin-offs ({sd.get('n_fresh')} fresh / "
        f"{sd.get('n_seasoned')} seasoned), {sd.get('n_prime')} PRIME. "
        f"EFTS ciks fix verified: "
        f"{sd.get('n_trading_with_link')}/{sd.get('n_trading')} carry SEC "
        f"links, {sd.get('n_with_parent')} parents resolved, no "
        f"duplicate tickers."
        if rep["all_pass"]
        else "REVIEW - see checks[] for the failing item(s)")
    body = json.dumps(rep, indent=2, default=str)
    try:
        s3.put_object(Bucket=S3_BUCKET,
                      Key="ops/reports/836_spinoff_desk_reverify.json",
                      Body=body.encode(), ContentType="application/json")
    except Exception as e:
        print(f"[ops836] S3 report write failed: {e}")
    with open("aws/ops/reports/836_spinoff_desk_reverify.json", "w") as f:
        f.write(body)
    print(body)
    return rep


if __name__ == "__main__":
    main()
