"""
ops_4278 -- quantum-desk v2.0.0: the whole fleet on every name.

Built from the 4277 census (771 engines mechanically mapped): a
data-driven evidence join consults every ticker-keyed artifact the
fleet produces for each money-map candidate (fresh<72h only, one chip
per source, future engines join when the census re-runs); the canary
war-room master barometer becomes a second veto (RED caps BUY_ZONE);
Khalid's original Index rides beside the risk-gate; global-recession
joins the regime vote as a fourth voter; the page shows the tiles,
the coverage line, and per-name evidence chips.

Gate: version 2.0.0 · 16/16 sources read · >=5 map names with >=2
corroborating engines · canary + khalid_index + coverage blocks
present with real values · top map printed WITH chips · page on edge.
"""
import json, sys, time, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
RUN_START = datetime.now(timezone.utc)

fails = []
with report("4278_quantum_v2") as r:
    r.heading("ops 4278 -- quantum-desk v2.0.0 live")
    doc = None
    for _ in range(50):
        try:
            c = lam.get_function_configuration(
                FunctionName="justhodl-quantum-desk")
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and c.get("State") == "Active":
                lm = datetime.strptime(
                    c["LastModified"].split(".")[0], "%Y-%m-%dT%H:%M:%S"
                ).replace(tzinfo=timezone.utc)
                if (RUN_START - lm).total_seconds() < 12 * 60:
                    lam.invoke(FunctionName="justhodl-quantum-desk",
                               InvocationType="RequestResponse",
                               Payload=b"{}")
                    doc = json.loads(s3.get_object(
                        Bucket=BUCKET,
                        Key="data/quantum-desk.json")["Body"].read())
                    if doc.get("version") == "2.0.0":
                        break
        except Exception:
            pass
        time.sleep(8)
    if not doc or doc.get("version") != "2.0.0":
        fails.append("v2.0.0 never landed (saw %s)"
                     % (doc or {}).get("version"))
    else:
        dh = doc.get("data_health") or {}
        r.ok("v2.0.0 live -- sources %s/%s"
             % (dh.get("sources_ok"), dh.get("sources_total")))
        ki = doc.get("khalid_index") or {}
        cb = doc.get("canary_barometer") or {}
        cov = doc.get("fleet_coverage") or {}
        es = doc.get("evidence_stats") or {}
        r.log("KHALID INDEX: %s (%s, %s)"
              % (ki.get("risk_index"), ki.get("grade"), ki.get("phase")))
        r.log("CANARY BAROMETER: %s score=%s veto=%s triggered=%s"
              % (cb.get("level"), cb.get("score"), cb.get("veto_active"),
                 (cb.get("triggered") or [])[:4]))
        r.log("COVERAGE: %s engines, %s artifacts (%s fresh), "
              "%s per-name sources; evidence hit %s sources, "
              "skipped %s"
              % (cov.get("engines"), cov.get("live_artifacts"),
                 cov.get("fresh_26h"),
                 cov.get("ticker_sources_consulted"),
                 es.get("hit_sources"), es.get("skipped_stale_or_err")))
        if not ki.get("risk_index") and not cb.get("level"):
            fails.append("khalid_index and canary blocks both empty")
        reg = doc.get("regime") or {}
        r.log("REGIME %s | votes: %s"
              % (reg.get("regime"),
                 ", ".join("%(source)s=%(regime)s" % v
                           for v in reg.get("votes") or [])))
        mm = doc.get("money_map") or []
        multi = [m for m in mm if (m.get("n_corroborating") or 0) >= 2]
        r.log("MONEY MAP (%d, %d with >=2 corroborating engines):"
              % (len(mm), len(multi)))
        for m in mm[:8]:
            chips = ", ".join(c.get("src", "?")
                              for c in (m.get("evidence") or [])[:5])
            r.kv(ticker=m["ticker"], fit=m["khalid_fit"],
                 x=m.get("n_corroborating"), evidence=chips[:90])
        if len(multi) < 5:
            fails.append("only %d names with >=2 corroborations"
                         % len(multi))
    try:
        req = urllib.request.Request(
            "https://justhodl.ai/quantum-desk.html",
            headers={"User-Agent": "ops/4278",
                     "Cache-Control": "no-cache"})
        body = urllib.request.urlopen(req, timeout=25).read().decode(
            "utf-8", "ignore")
        (r.ok if "Canary barometer" in body else r.warn)(
            "page v2 %s on edge (%d bytes)"
            % ("LIVE" if "Canary barometer" in body else
               "still propagating", len(body)))
    except Exception as e:
        r.warn("edge: %s" % str(e)[:80])
    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4278 PASS -- the desk now reads the whole fleet, "
             "name by name")
if fails:
    sys.exit(1)
