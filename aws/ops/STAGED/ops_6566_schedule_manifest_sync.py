"""ops 6566 - make the deployed schedule manifest equal the committed one (doctrine: git is authoritative, S3 is the
deployed copy the reconciler reads).

ops 6563 disabled eight triggers live and the repo copy of config/schedule-manifest.json was edited to match, but the
S3 copy was not; ops 6565 found the two copies differ.  In full enforce mode the daily reconciler (07:30 UTC) resets
an events rule whose state differs from the manifest - i.e. it would switch the retired engines back on.  This script
lists every per-entry difference, uploads the repo copy when every difference is one of the expected eight state flips
(or generated_at / the ops 6565 edits), and reports the reconciler's SSM mode and its last drift summary.  Read-only
except for that one upload (previous copy backed up).
"""
from pathlib import Path
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
KEY = "config/schedule-manifest.json"
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=120)
PLAN = json.loads((ROOT / "config/retire-plan-2026-10-20.json").read_text(encoding="utf-8"))
EXPECTED_DISABLED = {(t["kind"], t["name"]) for spec in PLAN["switch_off"].values() for t in spec["triggers"] if t["state"] == "ENABLED"}


def entries(doc):
    out = {}
    for kind, sec in (("rule", "rules"), ("sched", "schedules")):
        for e in doc.get(sec, []):
            out[(kind, e["name"])] = e
    return out


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    ssm = boto3.client("ssm", region_name=REGION, config=CFG)
    with report("ops_6566_schedule_manifest_sync") as r:
        r.heading("ops 6566 - deployed schedule manifest == committed manifest")
        live_txt = s3.get_object(Bucket=LIVE, Key=KEY)["Body"].read().decode("utf-8")
        repo_txt = (ROOT / KEY).read_text(encoding="utf-8")
        live, repo = entries(json.loads(live_txt)), entries(json.loads(repo_txt))
        diffs = []
        for k in sorted(set(live) | set(repo)):
            a, b = live.get(k), repo.get(k)
            if a == b:
                continue
            if a is None:
                diffs.append((k, "only in repo", None, b.get("state")))
            elif b is None:
                diffs.append((k, "only on S3", a.get("state"), None))
            else:
                fields = [f for f in sorted(set(a) | set(b)) if a.get(f) != b.get(f)]
                diffs.append((k, "fields " + ",".join(fields), a.get("state"), b.get("state")))
        for k, what, sa, sb in diffs:
            r.log("%s %s: %s (S3 %s -> repo %s)" % (k[0], k[1], what, sa, sb))
        unexpected = [d for d in diffs if not (d[1] == "fields state" and d[0] in EXPECTED_DISABLED and d[3] == "DISABLED")]
        try:
            mode = ssm.get_parameter(Name="/justhodl/schedules/mode")["Parameter"]["Value"]
        except Exception as ex:  # noqa: BLE001
            mode = "unreadable (%s)" % type(ex).__name__
        try:
            drift = json.loads(s3.get_object(Bucket=LIVE, Key="data/schedule-drift.json")["Body"].read())
            drift_summary = {"generated_at": drift.get("generated_at"), "mode": drift.get("mode"), "drift_count": drift.get("drift_count"),
                             "by_class": drift.get("by_class"), "enforced": len(drift.get("enforced") or [])}
        except Exception as ex:  # noqa: BLE001
            drift_summary = {"error": type(ex).__name__}
        r.log("reconciler mode (SSM): %s; last drift report: %s" % (mode, json.dumps(drift_summary)))
        if not diffs:
            r.kv(differences=0, uploaded=False, reconciler_mode=mode)
            r.ok("already identical")
            return
        if unexpected:
            r.kv(differences=len(diffs), unexpected=len(unexpected), uploaded=False, reconciler_mode=mode)
            r.fail("unexpected differences - nothing uploaded: %s" % [(d[0][1], d[1]) for d in unexpected][:10])
            raise SystemExit(1)
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        s3.put_object(Bucket=PRIVATE, Key="backups/retire-2026-10-20/%s/manifest/schedule-manifest.before-sync.json" % stamp,
                      Body=live_txt.encode("utf-8"), ServerSideEncryption="AES256")
        s3.put_object(Bucket=LIVE, Key=KEY, Body=repo_txt.encode("utf-8"), ContentType="application/json", CacheControl="no-cache")
        chk = s3.get_object(Bucket=LIVE, Key=KEY)["Body"].read().decode("utf-8")
        r.kv(differences=len(diffs), uploaded=chk == repo_txt, reconciler_mode=mode, expected_state_flips=len(EXPECTED_DISABLED))
        r.ok("S3 copy replaced by the committed manifest (%d state flips from ops 6563 carried over)" % len(diffs))


if __name__ == "__main__":
    main()
