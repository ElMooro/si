"""ops 6692 - universe-builder v4.1.0 first light, on the real banks: one invoke with {"dry_run": true, "force_bank":
true} - the screener is NOT called (no FMP request, no bandwidth spent) and nothing is written - builds the whole
universe from the FMP bank's profile-bulk + the factor book's security types, exactly as a 429 day would, and compares
it with the live data/universe.json (today's 18:06 UTC screener publication).  Reported: the deployed version, would-
publish / guard verdict, totals by bucket and source, the bank provenance (as_of, age, rows), membership overlap and
bucket agreement with the live universe, and the run's duration / memory from its REPORT line.  No key involved.
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


def main():
    # one attempt, long read: a re-sent invoke would run the builder twice (the ~285 s botocore re-invoke trap)
    lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=600, connect_timeout=10, retries={"max_attempts": 0}))
    with report("ops_6692_universe_bank_fill_light") as r:
        r.heading("ops 6692 - universe-builder v4.1.0: a bank-built universe (dry run, force_bank) vs the live publication")
        cfg = lam.get_function_configuration(FunctionName=FN)
        r.log("deployed: %s MB / %s s, LastModified %s, description %s" % (cfg["MemorySize"], cfg["Timeout"], cfg["LastModified"], cfg.get("Description", "")[:90]))
        if not cfg.get("Description", "").startswith("v4.1"):
            r.fail("v4.1.0 is not deployed yet")
            raise SystemExit(1)
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", LogType="Tail",
                          Payload=json.dumps({"dry_run": True, "force_bank": True}).encode())
        tail = base64.b64decode(resp.get("LogResult") or b"").decode("utf-8", "replace")
        payload = json.loads(resp["Payload"].read() or b"{}")
        if resp.get("FunctionError"):
            r.fail("FunctionError %s: %s" % (resp["FunctionError"], json.dumps(payload)[:800]))
            for line in tail.splitlines()[-15:]:
                r.log("   " + line[:300])
            raise SystemExit(1)
        body = json.loads(payload.get("body") or "{}")
        r.section("verdict")
        (r.ok if body.get("would_publish") else r.warn)("would_publish %s; violations %s" % (body.get("would_publish"), body.get("violations")))
        r.log("total %s | by bucket %s | by source %s | %ss" % (body.get("total_stocks"), json.dumps(body.get("by_bucket")), json.dumps(body.get("by_source")), body.get("duration_s")))
        bank = (body.get("sources") or {}).get("bank_profile_bulk") or {}
        r.log("bank: status %s, as_of %s, age %s d, rows %s, in the failed spans %s, dropped as not common %s, book %s" % (
            bank.get("status"), bank.get("as_of"), bank.get("age_days"), bank.get("rows"), bank.get("in_failed_spans"), bank.get("dropped_not_common"),
            json.dumps(bank.get("book_identity"))[:200]))
        cmp = body.get("compare") or {}
        r.section("vs the live publication")
        r.log("live %s (generated %s) vs bank-built %s: both %s (%s%% of live), only live %s, only bank %s, same cap bucket %s of %s" % (
            cmp.get("previous_total"), cmp.get("previous_generated_at"), cmp.get("new_total"), cmp.get("both"), cmp.get("both_pct_of_previous"),
            cmp.get("only_previous"), cmp.get("only_new"), cmp.get("same_bucket"), cmp.get("both")))
        r.log("only live (sample): %s" % cmp.get("only_previous_sample"))
        r.log("only bank (sample): %s" % cmp.get("only_new_sample"))
        r.section("the run")
        for line in tail.splitlines():
            if line.startswith("REPORT") or "[universe]" in line:
                r.log("   " + line[:400].replace("\t", " "))
        r.ok("ops 6692 complete (nothing written by the builder)")


if __name__ == "__main__":
    main()
    sys.exit(0)
