"""ops 6789 - justhodl-corporate-actions 403 carry-forward proof (daily health run 2026-11-12; verify-only, the deploy is
deploy-lambdas.yml's).

Commit 06b7b410: a 403 on the pre-ex-close aggs call (the session is older than the Polygon plan's history) stops the
3-session walk at the first call, and the dividend is recorded with pending_reason plan_history_403 + checked_at and
carried forward from the prior artifact for 30 days instead of being asked for again (3 calls + 3 sleeps) every run.
This op climbs the three-rung ladder (repo has it, artifact has it, function is READY), then invokes the engine
synchronously TWICE for the same three dividend-rich tickers:

  - run 1: every old dividend costs one 403, no 401, pending dividends carry pending_reason plan_history_403;
  - run 2: the 403 dividends are carried forward - zero http failures, the invocation finishes in seconds, the three
    artifacts are regenerated again with the same pending count and the same pre_ex_close count.
Changes nothing but the artifacts the engine writes on its own daily path (its normal real-data output).
"""
from datetime import datetime, timezone
from pathlib import Path
import io
import json
import re
import sys
import time
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION = "us-east-1"
FN = "justhodl-corporate-actions"
LIVE = "justhodl-dashboard-live"
ART_PREFIX = "data/corporate-actions/"
MARKER = 'PLAN_HISTORY_403 = "plan_history_403"'
TICKERS = ["AAPL", "MSFT", "KO"]
SECRET_KV = re.compile(r"(?i)(apiKey|api[_-]?key|token|secret)(\s*[=:]\s*)[^\s&\"',}]+")


def redact(line):
    return SECRET_KV.sub(lambda m: m.group(1) + m.group(2) + "***", str(line))


def selftest():
    assert redact("GET x?ticker=AAPL&apiKey=abc&limit=5") == "GET x?ticker=AAPL&apiKey=***&limit=5"
    assert redact("[corpact] http failed https://api.polygon.io/v2/aggs: HTTPError 401") == "[corpact] http failed https://api.polygon.io/v2/aggs: HTTPError 401"
    print("selftest ok")


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    selftest()
    cfg = Config(retries={"max_attempts": 5, "mode": "standard"}, connect_timeout=10, read_timeout=330)
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    logs = boto3.client("logs", region_name=REGION, config=cfg)
    now = datetime.now(timezone.utc)
    facts = {}
    with report("ops_6789_corporate_actions_403_carry_forward_proof") as r:
        r.heading("ops 6789 - justhodl-corporate-actions 403 carry-forward proof %s (ladder, then two synchronous invocations for %s)" % (now.date(), ", ".join(TICKERS)))

        r.section("1. the three-rung ladder")
        src = (ROOT / "aws/lambdas/justhodl-corporate-actions/source/lambda_function.py").read_text(encoding="utf-8")
        if MARKER not in src:
            r.fail("rung 1: the repo source does not carry the marker - wrong checkout")
            sys.exit(1)
        r.log("rung 1: repo has the marker")
        ready = False
        for attempt in range(40):   # deploy-lambdas.yml runs on the same push; allow ~10 min
            conf = lam.get_function_configuration(FunctionName=FN)
            try:
                loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
                blob = urllib.request.urlopen(loc, timeout=60).read()
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    deployed = z.read("lambda_function.py").decode("utf-8", "replace")
                has_marker = MARKER in deployed
            except Exception as exc:  # noqa: BLE001
                has_marker = False
                r.log("  artifact read: %s" % type(exc).__name__)
            state_ok = conf.get("State") == "Active" and conf.get("LastUpdateStatus") == "Successful"
            if has_marker and state_ok:
                ready = True
                r.log("rung 2+3 at attempt %d: artifact has the marker, State %s / LastUpdateStatus %s, LastModified %s" % (
                    attempt + 1, conf.get("State"), conf.get("LastUpdateStatus"), conf.get("LastModified")))
                break
            r.log("  attempt %d: marker %s, State %s / %s - waiting 15 s" % (attempt + 1, has_marker, conf.get("State"), conf.get("LastUpdateStatus")))
            time.sleep(15)
        if not ready:
            r.fail("the deployed artifact never carried the marker (or never settled) within 10 min - deploy-lambdas.yml for 06b7b410 did not land")
            sys.exit(1)
        time.sleep(5)
        facts["deployed_last_modified"] = conf.get("LastModified")

        import base64

        def invoke(label):
            before = {}
            for t in TICKERS:
                try:
                    before[t] = s3.head_object(Bucket=LIVE, Key=ART_PREFIX + "%s.json" % t)["LastModified"]
                except Exception:  # noqa: BLE001
                    before[t] = None
            t0 = time.time()
            resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", LogType="Tail",
                              Payload=json.dumps({"tickers": TICKERS, "max_tickers": len(TICKERS)}).encode())
            took = time.time() - t0
            payload = json.loads(resp["Payload"].read() or b"{}")
            if resp.get("FunctionError"):
                r.fail("%s: invoke FunctionError %s: %s" % (label, resp["FunctionError"], redact(json.dumps(payload))[:500]))
                sys.exit(1)
            body = payload.get("body") if isinstance(payload, dict) else None
            if isinstance(body, str):
                try:
                    body = json.loads(body)
                except ValueError:
                    body = {"raw": body[:300]}
            stats = (body or {}).get("stats") or {}
            r.log("%s: invoke took %.0f s | statusCode %s | stats %s" % (label, took, payload.get("statusCode"), redact(json.dumps(stats, default=str))[:600]))
            tail = base64.b64decode(resp.get("LogResult") or b"").decode("utf-8", "replace")
            lines = [ln for ln in tail.splitlines() if ln.strip()]
            n401 = sum(1 for ln in lines if "HTTPError 401" in ln)
            n403 = sum(1 for ln in lines if "HTTPError 403" in ln)
            nfail = sum(1 for ln in lines if "[corpact] http failed" in ln)
            codes = {}
            for ln in lines:
                m = re.search(r"http failed \S+: (\w+(?: \d+)?)", ln)
                if m:
                    codes[m.group(1)] = codes.get(m.group(1), 0) + 1
            r.log("%s: log tail: %d lines, %d http failures (%s), %d of them 401, %d of them 403 (the 4 KB tail may truncate)" % (
                label, len(lines), nfail, json.dumps(codes), n401, n403))
            for ln in lines[-6:]:
                r.log("  " + redact(" ".join(ln.split()))[:240])
            arts = {}
            for t in TICKERS:
                try:
                    obj = s3.get_object(Bucket=LIVE, Key=ART_PREFIX + "%s.json" % t)
                    doc = json.loads(obj["Body"].read() or b"{}")
                    fresh = before[t] is None or obj["LastModified"] > before[t]
                    evs = doc.get("events") or []
                    cash = [e for e in evs if e.get("type") == "dividend_cash"]
                    pending = [e for e in cash if e.get("factor_pending")]
                    p403 = [e for e in pending if e.get("pending_reason") == "plan_history_403"]
                    deferred = [e for e in pending if "deferred" in str(e.get("note") or "")]
                    with_close = [e for e in cash if e.get("pre_ex_close")]
                    r.log("%s %s: regenerated %s (%s) | cash dividends %d, with pre_ex_close %d, pending %d (plan_history_403 %d, deferred %d) | factors %d rows | factor_gaps %s | agreement %s" % (
                        label, t, fresh, doc.get("generated_at"), len(cash), len(with_close), len(pending), len(p403), len(deferred),
                        len(doc.get("factors") or []), doc.get("factor_gaps"), doc.get("agreement_rate")))
                    if p403[:1]:
                        r.log("  example 403 dividend: %s" % redact(json.dumps(p403[0], default=str))[:300])
                    arts[t] = {"fresh": fresh, "cash": len(cash), "with_close": len(with_close), "pending": len(pending), "p403": len(p403), "deferred": len(deferred)}
                except Exception as exc:  # noqa: BLE001
                    r.warn("%s %s artifact: %s: %s" % (label, t, type(exc).__name__, str(exc)[:120]))
                    arts[t] = None
            return took, stats, nfail, n401, n403, arts

        r.section("2. run 1 - the 403 dividends are recorded once (one call each)")
        took1, stats1, nfail1, n401_1, n403_1, arts1 = invoke("run1")
        facts.update(run1_s=round(took1), run1_http_failures=nfail1, run1_401=n401_1, run1_403=n403_1,
                     run1_stopped_early=stats1.get("stopped_early"), run1_written=stats1.get("written"))
        if n401_1:
            r.fail("%d keyless 401s in run 1 - b81da0e1 regressed" % n401_1)
            sys.exit(1)
        time.sleep(3)

        r.section("3. run 2 - the 403 dividends are carried forward (no call)")
        took2, stats2, nfail2, n401_2, n403_2, arts2 = invoke("run2")
        facts.update(run2_s=round(took2), run2_http_failures=nfail2, run2_403=n403_2,
                     run2_stopped_early=stats2.get("stopped_early"), run2_written=stats2.get("written"))
        all_ok = True
        for t in TICKERS:
            a1, a2 = arts1.get(t), arts2.get(t)
            facts[t + "_cash"] = (a2 or {}).get("cash")
            facts[t + "_with_close"] = (a2 or {}).get("with_close")
            facts[t + "_p403"] = (a2 or {}).get("p403")
            if not a1 or not a2 or not a2["fresh"] or a2["with_close"] == 0:
                all_ok = False
                r.warn("%s: run 2 did not regenerate the artifact with pre-ex closes (%s / %s)" % (t, a1, a2))
            elif a2["p403"] != a1["p403"] or a2["with_close"] != a1["with_close"]:
                all_ok = False
                r.warn("%s: run 2 changed the counts (with_close %d->%d, plan_history_403 %d->%d) - carry-forward or cache did not hold" % (
                    t, a1["with_close"], a2["with_close"], a1["p403"], a2["p403"]))
            elif a2["deferred"]:
                r.warn("%s: %d dividends still deferred by the time budget after run 2" % (t, a2["deferred"]))
        if nfail2:
            all_ok = False
            r.warn("run 2 still made %d failing http calls (%d x 403) - the carry-forward did not take" % (nfail2, n403_2))
        if took2 > 60:
            r.warn("run 2 took %.0f s for three tickers with everything cached - more than the 60 s expected" % took2)
        try:
            h = s3.head_object(Bucket=LIVE, Key="data/corporate-actions-index.json")
            r.log("index data/corporate-actions-index.json: %.1f h old (%d B)" % ((datetime.now(timezone.utc) - h["LastModified"]).total_seconds() / 3600, h["ContentLength"]))
        except Exception as exc:  # noqa: BLE001
            r.warn("index: %s" % type(exc).__name__)

        r.kv(**{k: str(v) for k, v in facts.items()})
        if all_ok:
            r.ok("fixed: run 1 recorded the plan-history 403 dividends once (%d x 403, %.0f s); run 2 carried them forward with 0 http failures in %.0f s and regenerated %s with the same counts" % (
                n403_1, took1, took2, ", ".join(TICKERS)))
        else:
            r.fail("the carry-forward did not hold - read sections 2 and 3")
            sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
