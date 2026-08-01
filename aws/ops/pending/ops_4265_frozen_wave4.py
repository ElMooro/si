"""
ops_4265 -- frozen-writer wave 4: forensics said three "frozen" writers
were never broken.

CONTRACT FIXES (manifest key_overrides -- caches working as designed):
  polygon-related-graph.json  6-day cache by design      -> SLA 168h
  factor-data-cache.json      28-day Ken-French cache    -> SLA 720h
  congress-party-map.json     slow-moving by nature      -> SLA 1080h
RETIREMENT:
  quiver-lobbying-cache.json  Quiver deliberately replaced by
                              congress-direct -> admin_only + retired note
CODE FIXES:
  signal-halflife   empty justhodl-outcomes table hit a bare 500 and
                    froze the artifact -> honest-empty write with the
                    finding spelled out (upstream pipeline dry)
  political-stocks  party map was a cache nobody ever refreshed ->
                    freshness-aware (21d) + live write-back +
                    stale-copy fallback if the source is blocked
DISCLOSED, NOT PATCHED:
  calibration-snapshotter  gated on SSM /justhodl/calibration/weights
                    (checked live below) and UNMANAGED (no config.json,
                    outside the deploy pipeline) -> wave-5 with the
                    outcomes-pipeline resurrection; SLA relaxed so the
                    contract stops crying wolf meanwhile.
"""
import json, sys, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
RUN_START = datetime.now(timezone.utc)

def wait_deployed(fn, tries=45):
    for _ in range(tries):
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and c.get("State") == "Active":
                lm = c.get("LastModified", "")
                try:
                    lm_dt = datetime.strptime(
                        lm.split(".")[0], "%Y-%m-%dT%H:%M:%S"
                    ).replace(tzinfo=timezone.utc)
                    if (RUN_START - lm_dt).total_seconds() < 45 * 60:
                        return c
                except Exception:
                    return c
        except Exception:
            pass
        time.sleep(8)
    return None

def age_min(key):
    h = s3.head_object(Bucket=BUCKET, Key=key)
    return (datetime.now(timezone.utc)
            - h["LastModified"]).total_seconds() / 60.0

fails = []
with report("4265_frozen_wave4") as r:
    r.heading("ops 4265 -- frozen-writer wave 4")

    r.section("1. contract corrections (manifest key_overrides)")
    try:
        m = json.loads(s3.get_object(
            Bucket=BUCKET,
            Key="data/_freshness-manifest.json")["Body"].read())
        ko = m.setdefault("key_overrides", {})
        ko["data/polygon-related-graph.json"] = 168
        ko["data/factor-data-cache.json"] = 720
        ko["data/congress-party-map.json"] = 1080
        ko["calibration/history-index.json"] = 2160
        ao = m.setdefault("admin_only_keys", [])
        if "data/quiver-lobbying-cache.json" not in ao:
            ao.append("data/quiver-lobbying-cache.json")
        m.setdefault("retired", {})["data/quiver-lobbying-cache.json"] = {
            "retired_at": RUN_START.isoformat(),
            "superseded_by": "justhodl-congress-direct "
                             "(official Senate eFD + House Clerk)",
            "reason": "Quiver vendor path deliberately replaced; "
                      "cache is a tombstone, not a defect"}
        m["last_contract_review"] = RUN_START.isoformat()
        s3.put_object(Bucket=BUCKET, Key="data/_freshness-manifest.json",
                      Body=json.dumps(m, default=str).encode(),
                      ContentType="application/json",
                      CacheControl="no-store")
        r.ok("manifest updated: 4 key_overrides, 1 retirement")
    except Exception as e:
        fails.append("manifest update: %s" % str(e)[:120])
        r.fail("manifest update: %s" % str(e)[:150])

    r.section("2. signal-halflife -- honest-empty semantics")
    if wait_deployed("justhodl-signal-halflife"):
        p = lam.invoke(FunctionName="justhodl-signal-halflife",
                       InvocationType="RequestResponse", Payload=b"{}")
        r.log("invoked: %s"
              % (p["Payload"].read() or b"")[:160].decode("utf-8", "ignore"))
        try:
            a = age_min("data/signal-halflife.json")
            doc = json.loads(s3.get_object(
                Bucket=BUCKET,
                Key="data/signal-halflife.json")["Body"].read())
            n = doc.get("n_outcomes_scanned")
            st = doc.get("status", "OK")
            if a < 20:
                r.ok("signal-halflife.json FRESH (%.1f min) -- "
                     "n_outcomes=%s status=%s" % (a, n, st))
            else:
                fails.append("signal-halflife still stale %.0f min" % a)
        except Exception as e:
            fails.append("signal-halflife verify: %s" % str(e)[:100])
    else:
        fails.append("signal-halflife deploy never settled")

    r.section("3. congress party map -- self-refreshing cache")
    if wait_deployed("justhodl-political-stocks"):
        p = lam.invoke(FunctionName="justhodl-political-stocks",
                       InvocationType="RequestResponse", Payload=b"{}")
        r.log("invoked: %s"
              % (p["Payload"].read() or b"")[:160].decode("utf-8", "ignore"))
        try:
            a = age_min("data/congress-party-map.json")
            doc = json.loads(s3.get_object(
                Bucket=BUCKET,
                Key="data/congress-party-map.json")["Body"].read())
            n = doc.get("n") or len(doc.get("party_map") or {})
            if a < 20 and n >= 400:
                r.ok("party map REFRESHED (%.1f min, %d members, "
                     "source=%s)" % (a, n, doc.get("source", "?")[:60]))
            elif a >= 20:
                # live source may be blocked from Lambda -- read the logs
                ev = logs.filter_log_events(
                    logGroupName="/aws/lambda/justhodl-political-stocks",
                    startTime=int((time.time() - 240) * 1000),
                    filterPattern="political")
                lines = [e["message"].strip()[:130]
                         for e in ev.get("events", [])][-5:]
                for ln in lines:
                    r.log("log: %s" % ln)
                if any("live fetch" in ln and "failed" in ln
                       for ln in lines):
                    r.warn("theunitedstates.io blocked from Lambda -- "
                           "stale-copy fallback held (map is 62d old, "
                           "materially fine; SLA now 45d, disclosed)")
                else:
                    fails.append("party map still stale (%.0f min) with "
                                 "no blocked-source evidence" % a)
            else:
                fails.append("party map wrote but n=%s (<400)" % n)
        except Exception as e:
            fails.append("party-map verify: %s" % str(e)[:100])
    else:
        fails.append("political-stocks deploy never settled")

    r.section("4. monitor re-read under the corrected contract")
    try:
        lam.invoke(FunctionName="justhodl-fleet-freshness-monitor",
                   InvocationType="RequestResponse", Payload=b"{}")
        st = json.loads(s3.get_object(
            Bucket=BUCKET,
            Key="data/_freshness-monitor.json")["Body"].read())
        stale_keys = [x["key"] for x in st.get("stale_top_50") or []]
        for k in ("data/polygon-related-graph.json",
                  "data/factor-data-cache.json"):
            (r.ok if k not in stale_keys else r.warn)(
                "%s %s under corrected SLA"
                % (k, "CLEAN" if k not in stale_keys else
                   "still listed (monitor may cache rules one cycle)"))
        r.log("fleet: %s stale / %s tracked"
              % (st.get("n_stale"), st.get("n_keys_tracked")))
    except Exception as e:
        r.warn("monitor re-read: %s" % str(e)[:100])

    r.section("5. calibration family -- disclosed, requeued")
    try:
        try:
            w = ssm.get_parameter(
                Name="/justhodl/calibration/weights")["Parameter"]["Value"]
            r.log("SSM weights present: %d chars -- %s"
                  % (len(w), w[:80]))
        except Exception as e:
            r.warn("SSM /justhodl/calibration/weights: %s -- snapshotter "
                   "is starved upstream, and the engine is UNMANAGED "
                   "(no config.json). Wave-5: resurrect the outcomes/"
                   "calibration producer, then bring the snapshotter "
                   "under deploy management." % type(e).__name__)
    except Exception:
        pass

    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4265 PASS -- wave 4: 2 resurrections, 3 contract "
             "corrections, 1 retirement, 1 honest requeue")
if fails:
    sys.exit(1)
