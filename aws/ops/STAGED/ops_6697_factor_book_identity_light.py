"""ops 6697 - bank-factors v1.0.7 first light: one asynchronous kick of the factor book (the same {} event as the 02:50
UTC Scheduler; InvocationType=Event so no SDK re-invoke), the manifest polled until the run lands, then the identity
of the names the old heuristics mis-flagged and of the instruments that must stay non-common, plus the manifest's
common-stock count before and after.  Expect GWW / NWSA / NWS / FLWS / WWW / ARM / PONY / PFBC / ASTS / EXR / MDA
is_common True; TMUSL / AFGB / TBB / BAC-PL / SOMN False.  The run's REPORT line closes it.  No key involved.
"""
from pathlib import Path
import gzip
import json
import sys
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

FN = "justhodl-bank-factors"
LIVE = "justhodl-dashboard-live"
BOOK = "data/warm/factor-book/"
COMMON = ("GWW", "NWSA", "NWS", "FLWS", "WWW", "ARM", "PONY", "PFBC", "ASTS", "EXR", "MDA", "AAPL")
NOT_COMMON = ("TMUSL", "AFGB", "TBB", "BAC-PL", "SOMN", "STRC")
CFG = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=120)


def jget(s3, key):
    try:
        raw = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
        try:
            raw = gzip.decompress(raw)
        except Exception:  # noqa: BLE001
            pass
        return json.loads(raw)
    except Exception as e:  # noqa: BLE001
        return {"_error": type(e).__name__}


def main():
    s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
    lam = boto3.client("lambda", region_name="us-east-1", config=CFG)
    logs = boto3.client("logs", region_name="us-east-1", config=CFG)
    with report("ops_6697_factor_book_identity_light") as r:
        r.heading("ops 6697 - bank-factors v1.0.7: a Polygon common type outweighs the symbol/name heuristics (kick + identity check)")
        cfg = lam.get_function_configuration(FunctionName=FN)
        r.log("deployed LastModified %s, %s MB / %s s" % (cfg["LastModified"], cfg["MemorySize"], cfg["Timeout"]))
        before = jget(s3, BOOK + "manifest.json")
        r.log("manifest before: version %s, updated %s, common_stocks %s, universe %s" % (
            before.get("version"), before.get("updated_at"), before.get("common_stocks"), before.get("universe")))
        start = int(time.time() * 1000)
        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        r.log("kicked (async) at %s" % datetime.now(timezone.utc).isoformat()[:19])
        after, deadline = before, time.time() + 1080
        while time.time() < deadline:
            time.sleep(30)
            after = jget(s3, BOOK + "manifest.json")
            if after.get("updated_at") and after.get("updated_at") != before.get("updated_at"):
                break
        if after.get("updated_at") == before.get("updated_at"):
            r.fail("the manifest did not change within 18 min")
            raise SystemExit(1)
        r.log("manifest after: version %s, updated %s, common_stocks %s (was %s), universe %s" % (
            after.get("version"), after.get("updated_at"), after.get("common_stocks"), before.get("common_stocks"), after.get("universe")))
        if after.get("version") != "1.0.7":
            r.warn("manifest version %s - expected 1.0.7" % after.get("version"))
        bad = []
        for sym, want in [(s, True) for s in COMMON] + [(s, False) for s in NOT_COMMON]:
            doc = jget(s3, BOOK + "symbols/%s.json.gz" % sym)
            ident = doc.get("identity") or {}
            got = ident.get("is_common")
            flags = [k for k in ("is_preferred", "is_warrant_unit", "is_shell", "is_debt_or_structured", "is_etf", "is_fund", "is_otc") if ident.get(k)]
            r.log("   %-7s is_common %s (want %s) | type %s | flags %s | %s" % (sym, got, want, ident.get("security_type"), flags, (ident.get("name") or doc.get("_error") or "")[:50]))
            if doc.get("_error") is None and got is not want:
                bad.append(sym)
        (r.ok if not bad else r.fail)("identity check: %d names, wrong: %s" % (len(COMMON) + len(NOT_COMMON), bad))
        evs = logs.filter_log_events(logGroupName="/aws/lambda/" + FN, startTime=start, filterPattern='"REPORT RequestId"').get("events", [])
        for e in evs[-2:]:
            r.log("   " + e["message"].strip()[:300].replace("\t", " "))
        r.ok("ops 6697 complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
