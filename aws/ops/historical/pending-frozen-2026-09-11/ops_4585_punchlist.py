"""ops 4585 — wo4585 punch-list verification.

Three revs landed: activist-13d BUG-4 gate (rev-A), grid-queue ISO-NE
redirect walk (rev-B), N-PORT fund CIK map (rev-C). This op settles,
invokes the three touched engines + congress-direct, and asserts:

  activist   : data_sufficiency block present; state honest (QUIET only
               with >=70% filer fetches answered — else INSUFFICIENT_DATA)
  grid-queue : ISO-NE either LIVE or the gap now carries the redirect
               CHAIN with Location targets (never a bare HTTP 302 again)
  etf-true-flows : N-PORT ground truth WIRED_INDEX with per_etf >= 1
               (the MF map covers fund trusts)
  congress   : house error VALUE on the record (evidence, not a gate —
               the house side ships filing metadata only by design)
"""
import json
import sys
import time

import boto3
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
B = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=120, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)

TARGETS = {
    "justhodl-activist-13d": "data/activist-13d.json",
    "justhodl-grid-queue": "data/grid-queue.json",
    "justhodl-etf-true-flows": "data/etf-true-flows.json",
    "justhodl-congress-direct": "data/congress-direct.json",
}


def get_json(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception:
        return None


def settle(r, fn, deadline_s=300):
    t0 = time.time()
    while time.time() - t0 < deadline_s:
        try:
            c = lam.get_function(FunctionName=fn)["Configuration"]
            if c.get("LastUpdateStatus") == "Successful" \
                    and c.get("State") == "Active":
                return True
        except Exception:
            pass
        time.sleep(6)
    r.warn("  %s did not settle" % fn)
    return False


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def main():
    with report("4585_punchlist") as r:
        r.heading("ops 4585 — wo4585 punch-list verification")
        misses = 0

        r.section("1. Settle + fire")
        before = {}
        for fn, key in TARGETS.items():
            settle(r, fn)
            j = get_json(key) or {}
            before[fn] = j.get("generated_at") or j.get("as_of") or ""
            try:
                lam.invoke(FunctionName=fn, InvocationType="Event")
                r.log("  fired %s" % fn)
            except Exception as e:
                misses += contract(r, fn, False, "invoke: %s" % str(e)[:100])

        outs, pending, t0 = {}, dict(TARGETS), time.time()
        while pending and time.time() - t0 < 700:
            time.sleep(12)
            for fn in list(pending):
                cur = get_json(pending[fn])
                ts = (cur or {}).get("generated_at") or \
                     (cur or {}).get("as_of") or ""
                if cur is not None and ts and ts != before[fn]:
                    outs[fn] = cur
                    r.log("  %s refreshed (%ss)" % (fn,
                                                    int(time.time() - t0)))
                    del pending[fn]
        for fn in pending:
            r.warn("  %s did not refresh in 700s" % fn)
            outs[fn] = get_json(pending[fn]) or {}
            misses += 1

        r.section("2. activist-13d — BUG-4 gate")
        j = outs.get("justhodl-activist-13d") or {}
        ds = j.get("data_sufficiency") or {}
        misses += contract(r, "activist",
                           "n_filers_fetched_ok" in ds,
                           "data_sufficiency published (ok=%s fail=%s)"
                           % (ds.get("n_filers_fetched_ok"),
                              ds.get("n_filers_fetch_failed")))
        ok_n = ds.get("n_filers_fetched_ok") or 0
        fail_n = ds.get("n_filers_fetch_failed") or 0
        tot = ok_n + fail_n
        st = j.get("state")
        blind = tot > 0 and ok_n < 0.7 * tot
        misses += contract(r, "activist",
                           (not blind) or st == "INSUFFICIENT_DATA",
                           "state honest: %s (blind=%s, %d setups)"
                           % (st, blind,
                              len(j.get("all_setups") or [])))

        r.section("3. grid-queue — ISO-NE chain evidence")
        j = outs.get("justhodl-grid-queue") or {}
        isone_live = "ISO-NE" in (j.get("iso_queues") or {})
        gap = next((g for g in (j.get("gaps") or []) if "ISO-NE" in str(g)),
                   "")
        chain_named = ("→" in str(gap) or "Location" in str(gap)
                       or "no-Location" in str(gap))
        misses += contract(r, "grid-queue",
                           isone_live or chain_named,
                           ("ISO-NE LIVE (%d rows)" %
                            ((j.get("iso_queues") or {}).get("ISO-NE") or {})
                            .get("n_projects", 0))
                           if isone_live else
                           "gap carries the redirect chain: %s"
                           % str(gap)[:220])
        if not isone_live and gap:
            r.log("  full ISO-NE gap: %s" % str(gap)[:400])

        r.section("4. etf-true-flows — N-PORT fund index")
        j = outs.get("justhodl-etf-true-flows") or {}
        gt = j.get("ground_truth") or {}
        misses += contract(r, "etf-true-flows",
                           gt.get("status") == "WIRED_INDEX"
                           and len(gt.get("per_etf") or []) >= 1,
                           "N-PORT %s — %d funds indexed via MF map"
                           % (gt.get("status"),
                              len(gt.get("per_etf") or [])))
        for e in (gt.get("per_etf") or [])[:6]:
            r.log("    %s cik=%s latest NPORT-P %s"
                  % (e.get("etf"), e.get("cik"),
                     e.get("latest_nport_date")))

        r.section("5. congress-direct — house error on the record")
        j = outs.get("justhodl-congress-direct") or {}
        h = j.get("house") or {}
        r.log("  house n_ptr_filings=%s error=%r"
              % (h.get("n_ptr_filings"), str(h.get("error"))[:200]))
        sen = j.get("senate") or {}
        r.log("  senate n_transactions=%s (the ticker-bearing side)"
              % sen.get("n_transactions"))
        if h.get("error"):
            r.warn("  house side erroring — evidence above; fix rides the "
                   "next iteration (metadata-only side, congress leg is "
                   "senate-fed and live)")

        r.section("verdict")
        if misses:
            r.fail("punch list: %d red" % misses)
            sys.exit(1)
        r.ok("wo4585 punch list GREEN — activist gate honest, ISO-NE "
             "evidence-or-live, N-PORT funds indexed, house error on record")


if __name__ == "__main__":
    main()
