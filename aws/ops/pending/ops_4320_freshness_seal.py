"""ops_4320 -- the freshness seal: force-regenerate TSM (the 27-day
victim), poll for a CHANGED generated_at, assert the fresh doc's price
within 2% of live FMP; page carries the stale-render+hot-swap layer.
The client bug (poll accepted the same stale file back) is the ledger
lesson: 'poll for presence' must be 'poll for CHANGE'."""
import json, sys, time, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from ops_report import report
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=60, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
fails = []
with report("4320_freshness_seal") as r:
    r.heading("ops 4320 -- stale docs die today")
    old = json.loads(s3.get_object(
        Bucket=B, Key="equity-research/TSM.json")["Body"].read())
    g0 = old.get("generated_at")
    r.log("TSM before: generated_at=%s price=%s"
          % (g0, (old.get("quote") or {}).get("price")))
    lam.invoke(FunctionName="justhodl-equity-research",
               InvocationType="Event",
               Payload=json.dumps({"_internal": "1", "ticker": "TSM",
                                   "force_refresh": True}).encode())
    r.log("async regen fired; polling for CHANGED generated_at "
          "(90-220s typical)")
    fresh = None
    t0 = time.time()
    while time.time() - t0 < 340:
        time.sleep(15)
        try:
            d = json.loads(s3.get_object(
                Bucket=B, Key="equity-research/TSM.json"
            )["Body"].read())
            if d.get("generated_at") and d["generated_at"] != g0:
                fresh = d
                break
        except Exception:
            pass
    if not fresh:
        fails.append("no changed generated_at within 340s")
    else:
        q = fresh.get("quote") or {}
        r.ok("TSM after %.0fs: generated_at=%s price=%s day=%s-%s"
             % (time.time() - t0, fresh["generated_at"],
                q.get("price"), q.get("day_low"), q.get("day_high")))
        kd = lam.get_function_configuration(
            FunctionName="justhodl-commodity-curves")
        env = ((kd.get("Environment") or {}).get("Variables") or {})
        fk = env.get("FMP_KEY") or env.get("FMP_API_KEY")
        lq = json.loads(urllib.request.urlopen(
            "https://financialmodelingprep.com/stable/quote"
            "?symbol=TSM&apikey=%s" % fk, timeout=25).read())
        lq = lq[0] if isinstance(lq, list) and lq else lq
        dv = abs((q.get("price") or 0) - (lq.get("price") or 0)) / \
            max(lq.get("price") or 1, 1) * 100
        r.ok("live=%s · Δ=%.2f%%" % (lq.get("price"), dv))
        if dv > 2.0:
            fails.append("fresh doc still off by %.2f%%" % dv)
        ia = fresh.get("institutional_activity") or {}
        r.log("13D/13G: total=%s recent24m=%s filers=%s (sellers "
              "structurally not in this feed -- page now says so)"
              % (ia.get("n_filings_total"),
                 ia.get("n_filings_recent_24m"),
                 ia.get("n_unique_filers_24m")))
    body = ""
    for _ in range(13):
        try:
            body = urllib.request.urlopen(urllib.request.Request(
                "https://justhodl.ai/why.html",
                headers={"User-Agent": "ops/4320",
                         "Cache-Control": "no-cache"}),
                timeout=25).read().decode("utf-8", "ignore")
            if "jh-stale-banner" in body:
                break
        except Exception:
            pass
        time.sleep(20)
    for mk in ("jh-stale-banner",
               "doc.generated_at !== cached.generated_at",
               "markStale", "SNAPSHOT "):
        if mk not in body:
            fails.append("edge missing %s" % mk)
    if "jh-stale-banner" in body:
        r.ok("page freshness layer LIVE (%d bytes)" % len(body))
    if fails:
        for f in fails:
            r.fail("  %s" % f)
        sys.exit(1)
    r.ok("OPS 4320 PASS -- research pages can no longer lie about "
         "when they were born")
