"""ops 5019 — TSLA regen probe.

Symptom on the live desk: TSLA layers sit on the honest stale-schema
note past the poll window → the v2.9.3 regeneration for TSLA is not
landing. This op invokes TSLA (plus two controls) with refresh=1,
reports status / schema / timing / gf_extras availability, and tails
the function's recent CloudWatch errors so the root cause is in the
report, not in guesswork. Diagnostics only — no deploy, no asserts
beyond transport.
"""
import json
import time

import boto3
from botocore.config import Config

from ops_report import report

FN = "justhodl-equity-research"
CFG = Config(connect_timeout=10, read_timeout=600,
             retries={"max_attempts": 0})
lam = boto3.client("lambda", region_name="us-east-1", config=CFG)
logs = boto3.client("logs", region_name="us-east-1")


def invoke(ticker):
    t0 = time.time()
    r = lam.invoke(FunctionName=FN,
                   Payload=json.dumps({"queryStringParameters": {
                       "ticker": ticker, "refresh": "1"}}).encode())
    body = json.loads(r["Payload"].read() or b"{}")
    dt = round(time.time() - t0, 1)
    return body, dt


def tail_errors(rep, since_min=30, want=6):
    try:
        streams = logs.describe_log_streams(
            logGroupName="/aws/lambda/" + FN, orderBy="LastEventTime",
            descending=True, limit=4)["logStreams"]
        got = 0
        for st in streams:
            ev = logs.get_log_events(
                logGroupName="/aws/lambda/" + FN,
                logStreamName=st["logStreamName"],
                startTime=int((time.time() - since_min * 60) * 1000),
                limit=100)["events"]
            for e in ev:
                m = e["message"]
                if ("ERROR" in m or "Traceback" in m or
                        "Task timed out" in m or "[jh" in m and
                        "failed" in m):
                    rep.log("cw: " + m.strip()[:220])
                    got += 1
                    if got >= want:
                        return
        if not got:
            rep.log("cw: no ERROR/Traceback lines in the last "
                    "%d min" % since_min)
    except Exception as e:
        rep.warn("cloudwatch tail failed: %s" % e)


with report("ops_5019_tsla_probe") as rep:
    rep.heading("ops 5019 -- TSLA v2.9.3 regen probe")
    rep.section("P1 fresh invokes")
    bad = False
    for t in ("TSLA", "KO", "ORCL"):
        try:
            body, dt = invoke(t)
        except Exception as e:
            rep.fail("%s: invoke transport error: %s" % (t, e))
            bad = True
            continue
        if isinstance(body, dict) and body.get("errorMessage"):
            rep.fail("%s: lambda errored in %.1fs: %s" %
                     (t, dt, str(body.get("errorMessage"))[:200]))
            rep.log("%s errorType=%s" % (t, body.get("errorType")))
            bad = True
            continue
        # function returns the doc directly or an HTTP-shaped wrapper
        doc = body
        if isinstance(body, dict) and "body" in body and \
                isinstance(body["body"], str):
            rep.kv(ticker=t, status=body.get("statusCode"))
            try:
                doc = json.loads(body["body"])
            except Exception:
                rep.fail("%s: body not JSON (status %s): %r" %
                         (t, body.get("statusCode"),
                          body["body"][:160]))
                bad = True
                continue
        X = (doc.get("gf_extras") or {}) if isinstance(doc, dict) \
            else {}
        rep.kv(ticker=t, gen_s=dt,
               schema=(doc.get("schema_version")
                       if isinstance(doc, dict) else None),
               from_cache=(doc.get("from_cache")
                           if isinstance(doc, dict) else None),
               gfx=bool(X.get("available")),
               gfx_reason=(X.get("reason") or "")[:60],
               doc_kb=len(json.dumps(doc)) // 1024
               if isinstance(doc, dict) else None)
        if not isinstance(doc, dict) or \
                doc.get("schema_version") != "2.9.3":
            rep.fail("%s: doc did not land on 2.9.3" % t)
            bad = True

    rep.section("P2 CloudWatch error tail")
    tail_errors(rep)
    if bad:
        raise SystemExit("TSLA probe found failures — see report")
    rep.ok("all three tickers regenerate on 2.9.3 with gf_extras")
