"""ops 6680 - first light of the five reviewed-contract engines with the bank rung (eps-revision-velocity 1.4.0,
momentum-leaders 2.1.0, earnings-pead 1.3.0, estimate-revisions 3.4.0, pump-positioning schema 2.1).

Each is invoked ONCE asynchronously (InvocationType=Event: a synchronous invoke's HTTP connection drops at ~285 s and
botocore re-invokes, which ran signal-genealogy three times in ops 6676), then the probe polls CloudWatch for each
request's REPORT line (duration / memory / status) and its error lines, and reads every published head's bank_rung
block and version / generated_at.  Under the FMP bandwidth block the honest outcome for a bank vintage that is not newer
than the previous retained original is "preserve previous publication" - the probe says which it was.  Writes nothing
except what the engines write.  No key involved.
"""
from pathlib import Path
import gzip
import json
import os
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 4, "mode": "standard"}, connect_timeout=10, read_timeout=60)
ENGINES = {
    "justhodl-eps-revision-velocity": "data/eps-revision-velocity.json",
    "justhodl-momentum-leaders": "data/momentum-leaders.json",
    "justhodl-earnings-pead": "data/earnings-pead.json",
    "justhodl-estimate-revisions": "data/estimate-revisions.json",
    "justhodl-pump-positioning": "data/pump-positioning.json",
}
WAIT_S = int(os.environ.get("WAIT_S") or "420")


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
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    logs = boto3.client("logs", region_name=REGION, config=CFG)
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    with report("ops_6680_contract_engines_first_light") as r:
        r.heading("ops 6680 - reviewed-contract engines: first light of the bank rung (one async run each)")
        r.section("0. deployed configuration + the head before the run")
        before = {}
        for fn, key in ENGINES.items():
            try:
                c = lam.get_function_configuration(FunctionName=fn)
                r.log("%s: %s MB / %s s, modified %s" % (fn, c.get("MemorySize"), c.get("Timeout"), (c.get("LastModified") or "")[:19]))
            except Exception as e:  # noqa: BLE001
                r.warn("%s: %s" % (fn, type(e).__name__))
            doc = jget(s3, key)
            before[fn] = (doc.get("generated_at"), doc.get("version") or doc.get("schema_version"))
            r.log("   head %s: generated_at %s version %s bank_rung %s" % (key, before[fn][0], before[fn][1], "present" if isinstance(doc.get("bank_rung"), dict) else "absent"))
        r.section("1. async invocations")
        since_ms = int(time.time() * 1000) - 5000
        rids = {}
        for fn in ENGINES:
            try:
                resp = lam.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
                rids[fn] = resp.get("ResponseMetadata", {}).get("RequestId")
                r.log("%s: accepted %s (request %s)" % (fn, resp.get("StatusCode"), rids[fn]))
            except Exception as e:  # noqa: BLE001
                r.warn("%s: invoke %s" % (fn, type(e).__name__))
        r.section("2. REPORT lines + error lines (polling up to %d s)" % WAIT_S)
        done = {}
        t0 = time.time()
        while time.time() - t0 < WAIT_S and len(done) < len(rids):
            for fn, rid in rids.items():
                if fn in done or not rid:
                    continue
                try:
                    ev = logs.filter_log_events(logGroupName="/aws/lambda/" + fn, startTime=since_ms, filterPattern='"REPORT RequestId: %s"' % rid, limit=3).get("events", [])
                except Exception:  # noqa: BLE001
                    ev = []
                if ev:
                    m = ev[-1].get("message", "")
                    done[fn] = " ".join(p.strip() for p in m.split("\t") if p.strip().startswith(("Duration:", "Max Memory", "Status:", "Init Duration")))
            if len(done) < len(rids):
                time.sleep(15)
        for fn in ENGINES:
            r.log("%s: %s" % (fn, done.get(fn, "(no REPORT line within the wait)")))
            try:
                ev = logs.filter_log_events(logGroupName="/aws/lambda/" + fn, startTime=since_ms, limit=12,
                                            filterPattern='?Traceback ?"[ERROR]" ?"Task timed out" ?"preserve" ?"errorMessage" ?"[fmp-bankfirst]"').get("events", [])
                for e in ev[-8:]:
                    r.log("   %s | %s" % (time.strftime("%H:%M:%S", time.gmtime(e["timestamp"] / 1000)), e.get("message", "").strip().replace("\n", " ⏎ ")[:330]))
            except Exception as e:  # noqa: BLE001
                r.warn("   logs: %s" % type(e).__name__)
        r.section("3. the heads after the run")
        published = 0
        for fn, key in ENGINES.items():
            doc = jget(s3, key)
            gen = doc.get("generated_at")
            new = gen is not None and gen != before[fn][0]
            published += int(new)
            br = doc.get("bank_rung") if isinstance(doc.get("bank_rung"), dict) else ((doc.get("controls") or {}).get("bank_rung") if isinstance(doc.get("controls"), dict) else None)
            r.log("%s: %s | generated_at %s version %s | bank_rung %s" % (
                key, "NEW publication" if new else "previous publication preserved", gen, doc.get("version") or doc.get("schema_version"),
                json.dumps({k: v for k, v in (br or {}).items() if k != "rule"}, default=str)[:600] if br else "absent"))
        r.log("new publications: %d of %d (a preserved head under the FMP block means the bank held nothing newer than the retained original - the honest outcome)" % (published, len(ENGINES)))
        r.ok("ops 6680 complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
