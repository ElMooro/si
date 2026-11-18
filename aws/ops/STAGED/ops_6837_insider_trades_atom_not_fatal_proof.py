"""ops 6837 - live proof that justhodl-insider-trades writes again when EDGAR's atom feed fails (daily health run 2026-11-18).

The 21:10 UTC runs of Nov 16 and Nov 17 ended `FATAL atom feed fetch failed: HTTP Error 400: Bad Request` (handled, 0 errors
in the metrics) and returned 502 before the daily-index backfill, the merge and the write: data/insider-trades.json was 62 h
old at the Nov 18 health run (ops 6836). The same commit makes the atom failure non-fatal (the daily form.idx backfill feeds
the window alone; the error is kept in stats.diagnostics.atom_feed_error). This ops:

  1. waits for deploy-lambdas.yml to land that commit (CodeSha256 differs from the one read at start, LastUpdateStatus
     Successful, State Active) - up to 15 min;
  2. reads the three outputs' LastModified, invokes the engine ONCE with InvocationType=Event (its own 21:10 UTC slot is
     untouched; reserved concurrency 1 serialises it) and polls data/insider-trades.json for a newer LastModified - up to
     8 min (the function wall is 300 s);
  3. reads the new document: generated_at, stats.atom_feed_count, stats.diagnostics.atom_feed_error (present if EDGAR's
     atom endpoint is still answering 400, absent if it recovered), coverage, transaction counts; and the newest log stream's
     tail (redacted: the SEC User-Agent carries a contact address).
Writes nothing but this report. Fails (exit 1) if the deploy never lands or the engine still does not write.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import re
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
FN = "justhodl-insider-trades"
KEYS = ("data/insider-trades.json", "data/insider-trades-full.json", "data/_insider/backfill-cursor.json")
EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
SECRET_KV = re.compile(r"(?i)\b(api[_-]?key|apikey|token|secret|password|authorization)(\s*[=:]\s*)[^\s&\"',}]+")


def redact(line):
    line = EMAIL.sub("***@***", str(line))
    return SECRET_KV.sub(lambda m: m.group(1) + m.group(2) + "***", line)


def selftest():
    assert redact("User-Agent: JustHodl ops@example.com") == "User-Agent: JustHodl ***@***"
    assert redact("token=abc rest") == "token=*** rest"
    assert redact("FATAL atom feed fetch failed: HTTP Error 400: Bad Request") == "FATAL atom feed fetch failed: HTTP Error 400: Bad Request"
    print("selftest ok")


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    selftest()
    cfg = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=60)
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    logs = boto3.client("logs", region_name=REGION, config=cfg)
    facts = {}
    ok = True
    with report("ops_6837_insider_trades_atom_not_fatal_proof") as r:
        r.heading("ops 6837 - justhodl-insider-trades writes through a failed EDGAR atom feed (live proof after the deploy)")

        r.section("1. the deploy")
        conf = lam.get_function_configuration(FunctionName=FN)
        sha0, lm0 = conf.get("CodeSha256"), conf.get("LastModified")
        r.log("at start: CodeSha256 %s LastModified %s LastUpdateStatus %s State %s" % (sha0, lm0, conf.get("LastUpdateStatus"), conf.get("State")))
        # the commit that carries the fix is the one this script was pushed with; deploy-lambdas.yml runs on the same push
        t0 = time.time()
        landed = False
        while time.time() - t0 < 15 * 60:
            conf = lam.get_function_configuration(FunctionName=FN)
            if conf.get("CodeSha256") != sha0 and conf.get("LastUpdateStatus") == "Successful" and conf.get("State") == "Active":
                landed = True
                break
            time.sleep(20)
        facts["deploy_landed"] = landed
        if landed:
            r.ok("deploy landed after %.0f s: CodeSha256 %s LastModified %s" % (time.time() - t0, conf.get("CodeSha256"), conf.get("LastModified")))
            time.sleep(15)   # settle (AUTONOMY.md: LastModified is not 'deployed')
        else:
            r.fail("deploy-lambdas.yml did not replace the code within 15 min (CodeSha256 still %s, status %s/%s) - nothing invoked" % (
                sha0, conf.get("LastUpdateStatus"), conf.get("State")))
            r.kv(**{k: str(v) for k, v in facts.items()})
            sys.exit(1)

        r.section("2. one Event invoke, then the write")
        before = {}
        for k in KEYS:
            try:
                h = s3.head_object(Bucket=LIVE, Key=k)
                before[k] = h["LastModified"]
                r.log("%s before: LastModified %s (%d B)" % (k, h["LastModified"].isoformat(), h["ContentLength"]))
            except Exception as exc:  # noqa: BLE001
                before[k] = None
                r.warn("%s before: %s" % (k, type(exc).__name__))
        inv = lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        r.log("invoke: HTTP %s at %s" % (inv.get("StatusCode"), datetime.now(timezone.utc).isoformat(timespec="seconds")))
        t1 = time.time()
        wrote = False
        while time.time() - t1 < 8 * 60:
            try:
                h = s3.head_object(Bucket=LIVE, Key=KEYS[0])
                if before[KEYS[0]] is None or h["LastModified"] > before[KEYS[0]]:
                    wrote = True
                    break
            except Exception:  # noqa: BLE001
                pass
            time.sleep(15)
        facts["wrote"] = wrote
        if not wrote:
            ok = False
            r.fail("data/insider-trades.json did not change within 8 min of the invoke (still %s)" % (before[KEYS[0]].isoformat() if before[KEYS[0]] else None))
        else:
            r.ok("data/insider-trades.json written %.0f s after the invoke" % (time.time() - t1))
            for k in KEYS:
                try:
                    h = s3.head_object(Bucket=LIVE, Key=k)
                    r.log("%s after: LastModified %s (%d B)%s" % (k, h["LastModified"].isoformat(), h["ContentLength"],
                                                                "" if before[k] is None or h["LastModified"] > before[k] else "  (unchanged)"))
                except Exception as exc:  # noqa: BLE001
                    r.warn("%s after: %s" % (k, type(exc).__name__))

        r.section("3. the new document and the log tail")
        if wrote:
            doc = json.loads(s3.get_object(Bucket=LIVE, Key=KEYS[0])["Body"].read())
            st = doc.get("stats") or {}
            diag = st.get("diagnostics") or {}
            facts.update(generated_at=doc.get("generated_at"), version=doc.get("version"), atom_feed_count=st.get("atom_feed_count"),
                         atom_feed_error=diag.get("atom_feed_error"), total_buys=st.get("total_buys"), fetch_errors=st.get("fetch_errors"),
                         fetch_duration_s=st.get("fetch_duration_s"), transactions=len(doc.get("transactions") or []),
                         sell_transactions=len(doc.get("sell_transactions") or []), coverage=json.dumps(doc.get("coverage"), default=str)[:400])
            r.log("generated_at %s version %s | atom_feed_count %s | atom_feed_error %s" % (
                doc.get("generated_at"), doc.get("version"), st.get("atom_feed_count"), diag.get("atom_feed_error")))
            r.log("stats: total_buys %s unique_companies %s clusters %s big_buys %s fetch_errors %s fetch_duration_s %s" % (
                st.get("total_buys"), st.get("unique_companies"), st.get("cluster_count"), st.get("big_buy_count"), st.get("fetch_errors"), st.get("fetch_duration_s")))
            r.log("coverage: %s" % facts["coverage"])
            r.log("transactions (buys) %d | sell_transactions %d" % (facts["transactions"], facts["sell_transactions"]))
            if diag.get("atom_feed_error"):
                r.warn("EDGAR's getcurrent atom feed is still failing (%s) - the daily-index backfill carried the run; the atom path itself still wants a look" % diag.get("atom_feed_error"))
            else:
                r.log("the atom feed answered this time (%s filings) - the fix is a safety net now" % st.get("atom_feed_count"))
            if not (doc.get("transactions") or doc.get("sell_transactions")):
                r.warn("the window is empty - the backfill found nothing to merge; look at coverage")
        try:
            streams = logs.describe_log_streams(logGroupName="/aws/lambda/" + FN, orderBy="LastEventTime", descending=True, limit=1).get("logStreams") or []
            if streams:
                ev = logs.get_log_events(logGroupName="/aws/lambda/" + FN, logStreamName=streams[0]["logStreamName"], limit=40, startFromHead=False).get("events") or []
                for e in ev[-25:]:
                    msg = redact(e.get("message", "").strip().replace("\n", " "))[:300]
                    if msg.startswith("START") or msg.startswith("END"):
                        continue
                    r.log("  log %s: %s" % (datetime.fromtimestamp(e["timestamp"] / 1000, timezone.utc).strftime("%m-%d %H:%M:%S"), msg))
        except Exception as exc:  # noqa: BLE001
            r.warn("log tail: %s: %s" % (type(exc).__name__, str(exc)[:160]))

        r.kv(**{k: str(v) for k, v in facts.items()})
        if ok:
            r.ok("insider-trades writes again; nothing else changed")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
