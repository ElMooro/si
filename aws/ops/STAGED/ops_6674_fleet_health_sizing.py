"""ops 6674 - read-only sizing for the five fleet-health fixes (Khalid, 2026-10-26: "fix these").

Each fix is aimed at a measured cause, not a guess:
  1. analytics-snapshot (600 s timeouts, 282 MB): how many objects / bytes sit under equity-research/, edgar-insiders/,
     equity-critique/, split by first sub-folder and by age (<=1 d, <=7 d, <=30 d, older) - sizes the incremental rebuild.
  2. earnings-whisper (60 s timeouts at the 256 MB ceiling): ContentLength / LastModified of its seven inputs.
  3. event-flow-monitor (120 s timeouts at the 512 MB ceiling): sizes of system-events/audit/<day>.jsonl for 16 days.
  4. treasury-auto-updater (3 s timeouts): treasury-api's reserved concurrency (the governor's hold), both functions'
     live configuration, the updater's fan-out tick membership in the live manifest.
  5. signal-genealogy (205 captures fail record_ref_schema): the key set of records[0] in one failing capture and the
     research_context status mix - confirms the producer added a field the validator's exact key set rejects.
Nothing is written.  No key involved.
"""
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=120, max_pool_connections=16)
ANALYTICS_PREFIXES = ("equity-research/", "edgar-insiders/", "equity-critique/")
WHISPER_KEYS = ("data/earnings-tracker.json", "data/eps-revision-velocity.json", "data/options-flow-scanner.json", "data/insider-trades.json",
                "data/8k-filings.json", "data/revenue-acceleration.json", "data/smart-money-clusters.json")
FAILING_CAPTURE = "data/research-forecasts/captures/068b8058c871bb41625b5da5c6d2224f19c951635f86e838f0a7eae1fad8818d.json"


def head(s3, key):
    try:
        h = s3.head_object(Bucket=LIVE, Key=key)
        return h.get("ContentLength"), (h.get("LastModified") or datetime.min.replace(tzinfo=timezone.utc)).isoformat()[:19]
    except Exception as e:  # noqa: BLE001
        return None, type(e).__name__


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    now = datetime.now(timezone.utc)
    with report("ops_6674_fleet_health_sizing") as r:
        r.heading("ops 6674 - sizing the five fleet-health fixes (read-only)")
        r.section("1. analytics-snapshot: the three prefixes")
        for prefix in ANALYTICS_PREFIXES:
            n = total = n_json = 0
            ages = Counter()
            sub = Counter()
            sub_bytes = Counter()
            biggest = (0, None)
            for page in s3.get_paginator("list_objects_v2").paginate(Bucket=LIVE, Prefix=prefix):
                for o in page.get("Contents") or []:
                    n += 1
                    total += o["Size"]
                    key = o["Key"]
                    rest = key[len(prefix):]
                    folder = rest.split("/")[0] + "/" if "/" in rest else "(top)"
                    sub[folder] += 1
                    sub_bytes[folder] += o["Size"]
                    if key.endswith(".json") and not key.endswith("manifest.json") and not key.endswith("latest.json"):
                        n_json += 1
                    age = (now - o["LastModified"]).days
                    ages["<=1d" if age <= 1 else "<=7d" if age <= 7 else "<=30d" if age <= 30 else ">30d"] += 1
                    if o["Size"] > biggest[0]:
                        biggest = (o["Size"], key)
            r.log("%s: %d objects, %.1f MB, %d read by the engine (.json minus manifest/latest); ages %s; biggest %s (%d KB)" % (
                prefix, n, total / 1e6, n_json, dict(ages), biggest[1], biggest[0] // 1024))
            for folder, cnt in sub.most_common(12):
                r.log("   %s%s: %d objects, %.1f MB" % (prefix, folder, cnt, sub_bytes[folder] / 1e6))
        r.section("2. earnings-whisper: the seven inputs")
        for k in WHISPER_KEYS:
            size, lm = head(s3, k)
            r.log("%s: %s bytes (%.1f MB), last modified %s" % (k, size, (size or 0) / 1e6, lm))
        r.section("3. event-flow-monitor: system-events/audit/<day>.jsonl, 16 days")
        total = 0
        for i in range(16):
            d = (now - timedelta(days=i)).strftime("%Y-%m-%d")
            size, lm = head(s3, "system-events/audit/%s.jsonl" % d)
            total += size or 0
            r.log("%s: %s bytes (%.1f MB) %s" % (d, size, (size or 0) / 1e6, lm))
        r.log("16-day total %.1f MB" % (total / 1e6))
        n = 0
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=LIVE, Prefix="system-events/audit/"):
            n += len(page.get("Contents") or [])
        r.log("objects under system-events/audit/: %d" % n)
        r.section("4. treasury-auto-updater / treasury-api")
        for fn in ("treasury-auto-updater", "treasury-api"):
            try:
                c = lam.get_function_configuration(FunctionName=fn)
                r.log("%s: %s MB / %s s, handler %s, modified %s, state %s" % (fn, c.get("MemorySize"), c.get("Timeout"), c.get("Handler"), (c.get("LastModified") or "")[:19], c.get("State")))
            except Exception as e:  # noqa: BLE001
                r.warn("%s: get_function_configuration %s" % (fn, type(e).__name__))
            try:
                rc = lam.get_function_concurrency(FunctionName=fn)
                r.log("   reserved concurrency: %s" % rc.get("ReservedConcurrentExecutions", "(none)"))
            except Exception as e:  # noqa: BLE001
                r.warn("   get_function_concurrency %s" % type(e).__name__)
        try:
            fan = json.loads(s3.get_object(Bucket=LIVE, Key="config/fanout-manifest.json")["Body"].read())
            ticks = [t for t, fs in fan.get("ticks", {}).items() if "treasury-auto-updater" in fs]
            r.log("live fan-out manifest: treasury-auto-updater in ticks %s; assignment %s; manifest generated_at %s" % (
                ticks, fan.get("assignment", {}).get("treasury-auto-updater"), fan.get("generated_at")))
        except Exception as e:  # noqa: BLE001
            r.warn("fan-out manifest: %s" % type(e).__name__)
        r.section("5. signal-genealogy: one failing capture's record references")
        try:
            doc = json.loads(s3.get_object(Bucket=LIVE, Key=FAILING_CAPTURE)["Body"].read())
            refs = doc.get("records") or []
            r.log("capture generated_at %s, %d record refs, %d sources" % (doc.get("generated_at"), len(refs), len(doc.get("sources") or [])))
            if refs:
                r.log("records[0] keys: %s" % sorted(refs[0].keys()))
                rc = refs[0].get("research_context")
                r.log("records[0].research_context keys: %s" % (sorted(rc.keys()) if isinstance(rc, dict) else type(rc).__name__))
                r.log("research_context status mix: %s" % dict(Counter((x.get("research_context") or {}).get("status") if isinstance(x.get("research_context"), dict) else "absent" for x in refs)))
        except Exception as e:  # noqa: BLE001
            r.warn("capture read: %s" % type(e).__name__)
        r.ok("ops 6674 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
