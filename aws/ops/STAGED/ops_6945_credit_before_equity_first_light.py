"""ops 6945 - FIRST LIGHT of Credit Before Equity 2.0.0: the deployed function, one dry run inside Lambda, one publication.

  1. Guard: the deployed package's files are this checkout's, byte for byte; memory, timeout and the once-a-day rule.
     If not, nothing is invoked.
  2. One invoke with {"dry_run": true}: the engine builds the whole board inside Lambda (its own role, its own S3 reads)
     and writes nothing. If that fails, or it sees fewer companies than the runner's dry run did, nothing is published.
  3. One invoke with {}: the engine publishes data/credit-before-equity.json, its ledger days and the model ledger. This is
     a one-off run outside the engine's 22:35 UTC schedule, disclosed to Khalid; the schedule itself is untouched and
     runs as usual (the same holdings day, so it publishes the same board again).
  4. Read back: the document is 2.0, its counts are its rows, the ledger holds the days it should, every key the first
     engine published is there for the model's names, and the site serves it.

Safe to run again: each run is one more dry run and one more publication of the kind the schedule makes.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import hashlib
import io
import json
import sys
import time
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "aws" / "lambdas" / "justhodl-credit-before-equity" / "source"
sys.path[:0] = [str(ROOT / "aws" / "ops")]
FN = "justhodl-credit-before-equity"
LIVE = "justhodl-dashboard-live"
OUT_KEY, HIST_KEY, LEDGER_ROOT = "data/credit-before-equity.json", "credit/credit-before-equity-history.json", "credit/cbe-spreads/"
FILES = ("lambda_function.py", "cbe_core.py", "cbe_names.py", "leaders_tape.py")
UA = "Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe"
LEADS = ("CREDIT_LEADS_DOWN", "CREDIT_LEADS_UP")


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report
    lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=320, retries={"max_attempts": 0}))
    ev = boto3.client("events", region_name="us-east-1")
    s3 = boto3.client("s3", region_name="us-east-1")
    fails = []

    def get(key):
        o = s3.get_object(Bucket=LIVE, Key=key)
        return o["Body"].read(), o["LastModified"]

    def ledger():
        return sorted(o["Key"][len(LEDGER_ROOT):-8] for o in s3.list_objects_v2(Bucket=LIVE, Prefix=LEDGER_ROOT).get("Contents") or [])

    with report("ops_6945_credit_before_equity_first_light") as r:
        r.heading("ops 6945 - Credit Before Equity 2.0.0: first light of the deployed engine, %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))

        r.section("1. Guard: the deployed function is this checkout's")
        g = lam.get_function(FunctionName=FN)
        cfg = g["Configuration"]
        with urllib.request.urlopen(g["Code"]["Location"], timeout=120) as resp:
            z = zipfile.ZipFile(io.BytesIO(resp.read()))
        same = {}
        for name in FILES:
            hits = [x for x in z.namelist() if x == name]
            same[name] = bool(hits) and z.read(hits[0]) == (SOURCE / name).read_bytes()
        shared = [x for x in z.namelist() if x in ("bank_enrich.py",)]
        r.log("%s updated %s | memory %s MB, timeout %s s | files as in this checkout: %s | shared modules in the package: %s" % (
            FN, (cfg.get("LastModified") or "")[:19], cfg.get("MemorySize"), cfg.get("Timeout"), same, shared))
        if not all(same.values()) or not shared:
            r.fail("the deployed package is not this checkout's (or lacks the factor-book reader); nothing was invoked")
            sys.exit(1)
        try:
            rule = ev.describe_rule(Name="justhodl-credit-before-equity-daily")
            r.log("schedule: %s %s %s" % (rule.get("Name"), rule.get("ScheduleExpression"), rule.get("State")))
            if rule.get("ScheduleExpression") != "cron(35 22 * * ? *)" or rule.get("State") != "ENABLED":
                fails.append("the engine's daily rule is not cron(35 22 * * ? *) ENABLED")
        except Exception as e:  # noqa: BLE001
            r.log("schedule: the rule could not be read (%s)" % type(e).__name__)
        raw0, when0 = get(OUT_KEY)
        was = json.loads(raw0)
        days0 = ledger()
        hist0 = hashlib.sha256(get(HIST_KEY)[0]).hexdigest()
        r.log("before: %s version %s, %d bytes, generated %s, %s names, %s leads | ledger days %d (%s .. %s)" % (
            OUT_KEY, was.get("version"), len(raw0), was.get("generated_at"), was.get("n_names"), was.get("n_leads"), len(days0), days0[0] if days0 else None, days0[-1] if days0 else None))

        r.section("2. One dry run inside Lambda (nothing written)")
        t0 = time.time()
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", LogType="Tail", Payload=json.dumps({"dry_run": True}).encode())
        dry = json.loads(resp["Payload"].read() or b"{}")
        r.log("HTTP %s, function error %s, %.1f s | %s" % (resp.get("StatusCode"), resp.get("FunctionError"), time.time() - t0, json.dumps(dry)[:700]))
        try:
            tail = base64.b64decode(resp.get("LogResult") or b"").decode("utf-8", "replace")
            for line in tail.splitlines():
                if "[cbe]" in line or "REPORT" in line or "Error" in line or "Traceback" in line:
                    r.log("  lambda: %s" % line.strip()[:330])
        except Exception:  # noqa: BLE001
            pass
        if resp.get("FunctionError") or not dry.get("dry_run") or (dry.get("companies") or 0) < 2500 or (dry.get("listed") or 0) < 900:
            r.fail("the dry run inside Lambda failed or saw too few companies; nothing was published: %s" % json.dumps(dry)[:600])
            sys.exit(1)
        if hashlib.sha256(get(OUT_KEY)[0]).hexdigest() != hashlib.sha256(raw0).hexdigest() or ledger() != days0 or hashlib.sha256(get(HIST_KEY)[0]).hexdigest() != hist0:
            r.fail("the dry run wrote something")
            sys.exit(1)
        r.log("the document, the ledger and the model ledger are what they were: the dry run wrote nothing")

        r.section("3. One publication (a one-off run outside the 22:35 UTC schedule)")
        t0 = time.time()
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
        out = json.loads(resp["Payload"].read() or b"{}")
        r.log("HTTP %s, function error %s, %.1f s | %s" % (resp.get("StatusCode"), resp.get("FunctionError"), time.time() - t0, json.dumps(out)[:400]))
        try:
            tail = base64.b64decode(resp.get("LogResult") or b"").decode("utf-8", "replace")
            for line in tail.splitlines():
                if "[cbe]" in line or "REPORT" in line:
                    r.log("  lambda: %s" % line.strip()[:330])
        except Exception:  # noqa: BLE001
            pass
        if resp.get("FunctionError") or out.get("statusCode") != 200:
            r.fail("the engine did not publish: %s" % json.dumps(out)[:600])
            sys.exit(1)

        r.section("4. Read back")
        raw, when = get(OUT_KEY)
        doc = json.loads(raw)
        cov, names = doc["coverage"], doc["names"]
        leads = [x for x in names if x["signal"] in LEADS]
        days = ledger()
        r.log("%s: %d bytes (%d gzipped) written %s | %s %s | generated %s | bonds of %s, stocks to %s" % (
            OUT_KEY, len(raw), len(gzip.compress(raw)), when.strftime("%H:%M:%S"), doc.get("contract"), doc.get("version"), doc.get("generated_at"), doc.get("as_of"), doc.get("stock_as_of")))
        r.log("%d companies (%d listed, %d credit only), %d bonds with a spread, %d leads, %d CDS levels (%d tied) | degraded %s" % (
            cov["companies"], cov["listed"], cov["unlisted"], cov["bonds_with_spread"], len(leads), cov["cds_priced"], cov["cds_priced_tied"], doc["degraded"]))
        r.log("windows: %s" % {k: (v.get("ref"), v.get("source") or v.get("why")) for k, v in doc["windows"].items()})
        r.log("ledger days now %d: %s" % (len(days), days))
        checks = {
            "the document is the 2.0 contract": doc.get("contract") == "credit-before-equity.v2" and doc.get("version") == "2.0.0",
            "the counts are the rows": doc["n_names"] == len(names) and doc["n_leads"] == len(leads) and doc["leads"] == leads[:20] and cov["companies"] == len(doc["issuers"]["rows"]),
            "it is the size the runner's dry run saw": cov["companies"] == dry["companies"] and cov["listed"] == dry["listed"] and doc["n_leads"] == dry["leads"],
            "every key the first engine published is there": all(k in doc for k in ("version", "generated_at", "n_names", "n_leads", "n_awaiting_history", "names", "leads", "degraded", "gaps", "thresholds", "method", "attribution")),
            "the model's names kept their fields": sum(1 for x in names if isinstance(x.get("distance_to_default"), (int, float)) and "d_distance_to_default" in x and "hist_n" in x) >= 18,
            "the run was not degraded": not doc["degraded"],
            "the ledger holds today's holdings day and the two reference days": doc["as_of"] in days and all(v.get("ref") in days for v in doc["windows"].values() if v.get("ref")),
            "a one-week comparison exists from the board's own ledger": doc["windows"]["w1"].get("source") == "the board's own ledger",
            "no ledger day was removed": set(days0) <= set(days),
            "the model ledger was appended": hashlib.sha256(get(HIST_KEY)[0]).hexdigest() != hist0,
        }
        for what, ok in checks.items():
            r.log("check %-62s %s" % (what, "ok" if ok else "FAILED"))
            if not ok:
                fails.append(what)
        site = None
        for attempt in range(12):
            try:
                req = urllib.request.Request("https://justhodl.ai/data/credit-before-equity.json?probe=%d" % (int(time.time()) + attempt), headers={"User-Agent": UA, "Cache-Control": "no-cache"})
                site = json.loads(urllib.request.urlopen(req, timeout=60).read())
                if site.get("generated_at") == doc.get("generated_at"):
                    break
            except Exception as e:  # noqa: BLE001
                site = {"error": str(e)[:80]}
            time.sleep(10)
        ok = bool(site) and site.get("generated_at") == doc.get("generated_at")
        r.log("the site serves the document generated %s -> %s" % ((site or {}).get("generated_at") or site, "the same" if ok else "NOT the warehouse's"))
        if not ok:
            fails.append("the site does not serve the new document")
        for x in leads[:8]:
            r.log("  lead %-6s %-32s spread %s bp, %+.1f bp in the month (%+.1f past its book), stock %+.2f%% -> %s" % (
                x["ticker"], str(x["name"])[:32], x.get("spread_bp"), x.get("d_spread_bp") or 0, x.get("x_spread_bp") or 0, x.get("d_price_pct") or 0, x["signal"]))
        r.kv(companies=cov["companies"], listed=cov["listed"], leads=len(leads), cds_levels=cov["cds_priced"], ledger_days=len(days), failed_checks=len(fails), document=doc["generated_at"][:16])
        for f in fails:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok("FIRST LIGHT: 2.0.0 published %d companies (%d listed) with %d leads and %d CDS levels; one dry run and one publication, both by hand, disclosed" % (cov["companies"], cov["listed"], len(leads), cov["cds_priced"]))


if __name__ == "__main__":
    main()
