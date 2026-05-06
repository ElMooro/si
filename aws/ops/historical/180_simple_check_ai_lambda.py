#!/usr/bin/env python3
"""
Step 180 — Simple check: deployed Lambda state + AAPL test.

Step 179 has been pending for 40+ min without completion. Either CI
is stuck or the screener poll is taking forever. Skip the heavy
verification and just answer two questions:

  1. Is the AI Lambda running the new code (safe_record helper)?
     → Check LastModified on the function.
  2. Does AAPL work right now?
     → One direct invoke, print result.

Also peek at screener Lambda's LastModified — if old, we know the
deploy hasn't taken yet.
"""
import json
import time
from ops_report import report
import boto3

REGION = "us-east-1"
AI_LAMBDA = "justhodl-stock-ai-research"
SCREENER_LAMBDA = "justhodl-stock-screener"

lam = boto3.client("lambda", region_name=REGION)


with report("simple_check_ai_lambda") as r:
    r.heading("Quick state check on both Lambdas")

    # ─── A. AI Lambda state ─────────────────────────────────────────────
    r.section("A. AI Lambda configuration")
    cfg = lam.get_function_configuration(FunctionName=AI_LAMBDA)
    r.log(f"  CodeSha256:    {cfg.get('CodeSha256','?')[:24]}...")
    r.log(f"  LastModified:  {cfg.get('LastModified','?')[:19]}")
    r.log(f"  CodeSize:      {cfg.get('CodeSize',0)} bytes")
    r.log(f"  Timeout:       {cfg.get('Timeout',0)}s")
    r.log(f"  ReservedConc:  {cfg.get('Concurrency',{}).get('ReservedConcurrentExecutions','default')}")

    # ─── B. AI Lambda smoke test with AAPL ──────────────────────────────
    r.section("B. AI Lambda smoke test — ticker=AAPL")
    t0 = time.time()
    resp = lam.invoke(
        FunctionName=AI_LAMBDA,
        InvocationType="RequestResponse",
        Payload=json.dumps({
            "queryStringParameters": {"ticker": "AAPL", "force": "true"},
            "requestContext": {"http": {"method": "GET"}},
        }),
    )
    elapsed = time.time() - t0
    payload = resp.get("Payload").read().decode()
    if resp.get("FunctionError"):
        r.fail(f"  ✗ FunctionError ({elapsed:.1f}s): {payload[:600]}")
        raise SystemExit(1)

    body = json.loads(payload)
    status = body.get("statusCode")
    inner = json.loads(body.get("body", "{}"))

    if status != 200:
        r.fail(f"  ✗ Status {status} ({elapsed:.1f}s)")
        r.log(f"    body: {body.get('body','')[:500]}")
        # Show CloudWatch tail to see the new Shapes log
        try:
            logs = boto3.client("logs", region_name=REGION)
            streams = logs.describe_log_streams(
                logGroupName=f"/aws/lambda/{AI_LAMBDA}",
                orderBy="LastEventTime",
                descending=True,
                limit=1,
            )["logStreams"]
            if streams:
                evts = logs.get_log_events(
                    logGroupName=f"/aws/lambda/{AI_LAMBDA}",
                    logStreamName=streams[0]["logStreamName"],
                    startFromHead=False,
                    limit=20,
                )["events"]
                r.log(f"\n  Recent CloudWatch events:")
                for ev in evts[-15:]:
                    msg = (ev.get("message") or "").strip()
                    if msg and "Runtime" not in msg and "REPORT" not in msg:
                        r.log(f"    {msg[:200]}")
        except Exception as e:
            r.log(f"  (log fetch failed: {e})")
        raise SystemExit(1)

    r.ok(f"  ✅ HTTP 200 in {elapsed:.1f}s")
    co = inner.get("company", {})
    sn = inner.get("snapshot", {})
    ai = inner.get("ai", {})
    r.log(f"")
    r.log(f"  Company: {co.get('name','?')} ({co.get('sector','?')})")
    r.log(f"  Price:   ${sn.get('price')}  P/E={sn.get('pe')}")
    r.log(f"  Cached:  {inner.get('from_cache')}  Model: {inner.get('model','?')}")
    r.log(f"  Lambda runtime: {inner.get('elapsed_seconds')}s")
    r.log(f"")

    desc = ai.get("description") or ""
    r.log(f"  AI Description: {desc[:200]}...")
    r.log(f"")
    bull = ai.get("bull_case") or {}
    bear = ai.get("bear_case") or {}
    r.log(f"  Bull thesis:  {(bull.get('thesis') or '')[:150]}")
    r.log(f"  Bear thesis:  {(bear.get('thesis') or '')[:150]}")
    r.log(f"")
    sc = ai.get("scenarios") or {}
    for h in ("horizon_1m", "horizon_1q", "horizon_1y"):
        sh = sc.get(h) or {}
        r.log(f"  {h:14}: bull=${sh.get('bull')}  base=${sh.get('base')}  bear=${sh.get('bear')}")

    # ─── C. Screener Lambda state ───────────────────────────────────────
    r.section("C. Screener Lambda configuration (just state, no invoke)")
    cfg2 = lam.get_function_configuration(FunctionName=SCREENER_LAMBDA)
    r.log(f"  CodeSha256:    {cfg2.get('CodeSha256','?')[:24]}...")
    r.log(f"  LastModified:  {cfg2.get('LastModified','?')[:19]}")
    r.log(f"  CodeSize:      {cfg2.get('CodeSize',0)} bytes")
    r.log(f"  Timeout:       {cfg2.get('Timeout',0)}s")

    r.kv(
        ai_status=status,
        ai_elapsed=round(elapsed, 1),
        ai_company=co.get("name", "?"),
        has_description=bool(desc),
        has_bull=bool(bull.get("thesis")),
        has_bear=bool(bear.get("thesis")),
        has_scenarios=bool(sc.get("horizon_1y")),
    )
    r.log("Done")
