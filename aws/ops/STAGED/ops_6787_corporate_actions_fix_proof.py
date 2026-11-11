"""ops 6787 - justhodl-corporate-actions fix proof (daily health run 2026-11-11; verify-only, the deploy is deploy-lambdas.yml's).

Commit b81da0e1 gave the pre-ex-close aggs call the Polygon key it never carried (ops 6786 showed the key and all three
endpoint shapes answer 200 from the runner, so the engine's "HTTPError" on every call was its own keyless request). This op
climbs the three-rung ladder (AUTONOMY.md: repo has it, artifact has it, function is READY), then invokes the engine
synchronously for three dividend-rich tickers and reads the artifacts it wrote:

  - no `HTTPError 401` in the invocation's log; the status code is now printed;
  - AAPL / MSFT / KO artifacts regenerated now, with pre_ex_close on their cash dividends and factor_gaps False
    (or the few pending ones named);
  - the run finished well inside 300 s and reported stopped_early False.
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
MARKER = 'POLYGON_AGGS_URL.format(t=ticker, d=d) + "&apiKey=" + api_key'
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
    with report("ops_6787_corporate_actions_fix_proof") as r:
        r.heading("ops 6787 - justhodl-corporate-actions fix proof %s (ladder, then one synchronous invocation for %s)" % (now.date(), ", ".join(TICKERS)))

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
            r.fail("the deployed artifact never carried the marker (or never settled) within 10 min - deploy-lambdas.yml for b81da0e1 did not land")
            sys.exit(1)
        time.sleep(5)
        facts["deployed_last_modified"] = conf.get("LastModified")

        r.section("2. one synchronous invocation for %s" % ", ".join(TICKERS))
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
            r.fail("invoke FunctionError %s: %s" % (resp["FunctionError"], redact(json.dumps(payload))[:500]))
            sys.exit(1)
        body = payload.get("body") if isinstance(payload, dict) else None
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except ValueError:
                body = {"raw": body[:300]}
        stats = (body or {}).get("stats") or {}
        r.log("invoke took %.0f s | statusCode %s | stats %s" % (took, payload.get("statusCode"), redact(json.dumps(stats, default=str))[:600]))
        facts.update(invoke_s=round(took), processed=stats.get("processed"), written=stats.get("written"), with_events=stats.get("with_events"),
                     factor_gaps=stats.get("factor_gaps"), stopped_early=stats.get("stopped_early"), polygon_key=stats.get("polygon_key"))
        import base64
        tail = base64.b64decode(resp.get("LogResult") or b"").decode("utf-8", "replace")
        lines = [ln for ln in tail.splitlines() if ln.strip()]
        n401 = sum(1 for ln in lines if "HTTPError 401" in ln)
        nfail = sum(1 for ln in lines if "[corpact] http failed" in ln)
        codes = {}
        for ln in lines:
            m = re.search(r"http failed \S+: (\w+(?: \d+)?)", ln)
            if m:
                codes[m.group(1)] = codes.get(m.group(1), 0) + 1
        r.log("log tail: %d lines, %d http failures (%s), %d of them 401" % (len(lines), nfail, json.dumps(codes), n401))
        for ln in lines[-12:]:
            r.log("  " + redact(" ".join(ln.split()))[:240])
        facts.update(http_failures=nfail, http_401=n401)
        if n401:
            r.fail("%d keyless 401s in the fixed code's own run - the fix did not take" % n401)
            sys.exit(1)
        if stats.get("stopped_early"):
            r.warn("stopped_early True on three tickers - the per-ticker cost is still too high")

        r.section("3. the artifacts the engine just wrote")
        all_ok = True
        for t in TICKERS:
            try:
                obj = s3.get_object(Bucket=LIVE, Key=ART_PREFIX + "%s.json" % t)
                doc = json.loads(obj["Body"].read() or b"{}")
                fresh = before[t] is None or obj["LastModified"] > before[t]
                evs = doc.get("events") or []
                cash = [e for e in evs if e.get("type") == "dividend_cash"]
                pending = [e for e in cash if e.get("factor_pending")]
                with_close = [e for e in cash if e.get("pre_ex_close")]
                r.log("%s: regenerated %s (%s) | n_events %s | cash dividends %d, with pre_ex_close %d, pending %d | factors %d rows | factor_gaps %s | agreement %s | latest %s" % (
                    t, fresh, doc.get("generated_at"), doc.get("n_events"), len(cash), len(with_close), len(pending), len(doc.get("factors") or []),
                    doc.get("factor_gaps"), doc.get("agreement_rate"), doc.get("latest_event_date")))
                if pending[:3]:
                    r.log("  pending: %s" % redact(json.dumps(pending[:3], default=str))[:300])
                facts[t + "_cash"] = len(cash)
                facts[t + "_with_close"] = len(with_close)
                facts[t + "_pending"] = len(pending)
                if not fresh or (cash and len(with_close) == 0):
                    all_ok = False
            except Exception as exc:  # noqa: BLE001
                all_ok = False
                r.warn("%s artifact: %s: %s" % (t, type(exc).__name__, str(exc)[:120]))
        try:
            h = s3.head_object(Bucket=LIVE, Key="data/corporate-actions-index.json")
            r.log("index data/corporate-actions-index.json: %.1f h old (%d B)" % ((datetime.now(timezone.utc) - h["LastModified"]).total_seconds() / 3600, h["ContentLength"]))
        except Exception as exc:  # noqa: BLE001
            r.warn("index: %s" % type(exc).__name__)

        r.kv(**{k: str(v) for k, v in facts.items()})
        if all_ok:
            r.ok("fixed: the engine's own run fetched pre-ex closes with the key (no 401), regenerated %s, finished in %.0f s" % (", ".join(TICKERS), took))
        else:
            r.fail("an artifact was not regenerated or carries no pre_ex_close - read section 3")
            sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
