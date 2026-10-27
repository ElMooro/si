"""ops 6696 - universe-builder v4.1.0 (with the listing rule) first light on the real banks, then the first real run.

  1. {"dry_run": true, "force_bank": true}: no FMP call, nothing written - the bank-built universe, limited to the live
     publication's members and passed through the listing rule, vs the live file;
  2. {} - a real run, exactly what the 18:06 UTC schedule does: the screener (or, on 429, the bank fill), the listing
     rule, the publication guard, the write.  The live universe then stops carrying the ~172 notes / preferreds /
     warrants / rights / units that FMP lists with their issuer's market cap (ops 6694/6695);
  3. the new publication vs the version it replaced: totals by bucket and source, the listing-rule drops by reason,
     every other membership change, and spot checks (TBB / SOMN / STRC must be gone; GWW / NWSA / ARM / ASTS / TPC / BCPC
     must stay).
One attempt per invoke (no SDK retry: a re-sent invoke would run the builder twice).  No key involved.
"""
from pathlib import Path
import base64
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

FN = "justhodl-universe-builder"
LIVE = "justhodl-dashboard-live"
KEY = "data/universe.json"
GONE = ("TBB", "SOMN", "SOJE", "STRC", "STRF", "SMCIP", "TMUSL", "PFH")
STAY = ("GWW", "NWSA", "NWS", "ARM", "ASTS", "TPC", "BCPC", "AAPL", "MSFT", "BRK-B", "JPM", "MNR")


def invoke(lam, payload):
    resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", LogType="Tail", Payload=json.dumps(payload).encode())
    tail = base64.b64decode(resp.get("LogResult") or b"").decode("utf-8", "replace")
    body = json.loads(resp["Payload"].read() or b"{}")
    return resp.get("FunctionError"), body, tail


def main():
    lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=600, connect_timeout=10, retries={"max_attempts": 0}))
    s3 = boto3.client("s3", region_name="us-east-1", config=Config(retries={"max_attempts": 6, "mode": "standard"}))
    with report("ops_6696_universe_rule_light") as r:
        r.heading("ops 6696 - universe-builder v4.1.0 with the listing rule: dry run on the bank, then the first real run")
        cfg = lam.get_function_configuration(FunctionName=FN)
        r.log("deployed LastModified %s, CodeSha256 %s" % (cfg["LastModified"], cfg["CodeSha256"][:16]))
        before = s3.get_object(Bucket=LIVE, Key=KEY)
        old = json.loads(before["Body"].read())
        r.log("live before: %d stocks, generated %s, version %s" % (len(old.get("stocks", [])), old.get("generated_at"), before.get("VersionId")))

        r.section("1. dry run, force_bank (no FMP call, nothing written)")
        err, payload, tail = invoke(lam, {"dry_run": True, "force_bank": True})
        if err:
            r.fail("dry run FunctionError %s: %s" % (err, json.dumps(payload)[:600]))
            raise SystemExit(1)
        body = json.loads(payload.get("body") or "{}")
        srcs = body.get("sources") or {}
        if "listing_rule" not in srcs:
            r.fail("the deployed code has no listing rule - the lane has not deployed this commit yet")
            raise SystemExit(1)
        fill, lr, cmp = srcs.get("bank_profile_bulk") or {}, srcs.get("listing_rule") or {}, body.get("compare") or {}
        (r.ok if body.get("would_publish") else r.warn)("would_publish %s %s | total %s | by bucket %s | by source %s" % (
            body.get("would_publish"), body.get("violations"), body.get("total_stocks"), json.dumps(body.get("by_bucket")), json.dumps(body.get("by_source"))))
        r.log("bank: as_of %s (age %s d), membership %s, in spans %s, not members %s, not equity %s" % (
            fill.get("as_of"), fill.get("age_days"), fill.get("membership"), fill.get("in_failed_spans"), fill.get("dropped_not_member"), fill.get("dropped_not_equity")))
        r.log("vs live: both %s (%s%% of live), only live %s, only bank %s, same bucket %s; only-live sample %s" % (
            cmp.get("both"), cmp.get("both_pct_of_previous"), cmp.get("only_previous"), cmp.get("only_new"), cmp.get("same_bucket"),
            cmp.get("only_previous_sample")))
        for line in tail.splitlines():
            if line.startswith("REPORT"):
                r.log("   " + line[:300].replace("\t", " "))

        r.section("2. the real run")
        err, payload, tail = invoke(lam, {})
        for line in tail.splitlines():
            if "[universe]" in line or line.startswith("REPORT"):
                r.log("   " + line[:400].replace("\t", " "))
        if err:
            r.fail("real run FunctionError %s: %s" % (err, json.dumps(payload)[:600]))
            raise SystemExit(1)
        r.ok("real run: %s" % (payload.get("body") or "")[:400])

        r.section("3. the new publication vs the one it replaced")
        after = s3.get_object(Bucket=LIVE, Key=KEY)
        new = json.loads(after["Body"].read())
        st, srcs = new.get("stats") or {}, new.get("sources") or {}
        r.log("new: %d stocks, generated %s, engine %s, version %s | by bucket %s | by source %s" % (
            st.get("total_stocks"), new.get("generated_at"), new.get("engine_version"), after.get("VersionId"),
            json.dumps(st.get("by_cap_bucket")), json.dumps(st.get("by_source"))))
        sc, lr = srcs.get("screener") or {}, srcs.get("listing_rule") or {}
        r.log("screener: rows %s, ranges failed %s | bank fill: %s | listing rule: applied %s, book %s, dropped %s by reason %s" % (
            sc.get("rows"), sc.get("ranges_failed"), (srcs.get("bank_profile_bulk") or {}).get("status"), lr.get("applied"),
            lr.get("generated_at"), lr.get("dropped"), json.dumps(lr.get("by_reason"))))
        for k, v in sorted((lr.get("samples") or {}).items()):
            r.log("   %s: %s" % (k, v))
        o = {s["symbol"]: s for s in old.get("stocks", [])}
        n = {s["symbol"]: s for s in new.get("stocks", [])}
        ruled = {x for v in (lr.get("samples") or {}).values() for x in v}
        removed, added = sorted(set(o) - set(n)), sorted(set(n) - set(o))
        r.log("membership: removed %d (of which shown as listing-rule samples %d), added %d" % (len(removed), len(set(removed) & ruled), len(added)))
        r.log("   removed (first 60): %s" % removed[:60])
        r.log("   added: %s" % added[:60])
        bad_gone = [s for s in GONE if s in n]
        bad_stay = [s for s in STAY if s in o and s not in n]
        (r.ok if not bad_gone else r.fail)("must be gone: %s -> still present %s" % (list(GONE), bad_gone))
        (r.ok if not bad_stay else r.fail)("must stay (when they were live before): %s -> lost %s" % (list(STAY), bad_stay))
        r.ok("ops 6696 complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
