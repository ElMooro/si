"""ops 6614 - after deploy-lambdas ships 4b55630f (futures-curves paced + 900 s; fleet-integrity v1.2.1):
 1. the deployed S3 fan-out manifest := the committed one (justhodl-polygon-futures-curves leaves the session-5min tick;
    exactly that change, previous copy backed up, read back) - the scheduler reads the S3 copy every tick;
 2. wait until both functions carry the new code (LastModified after the dispatch; futures-curves timeout 900);
 3. run justhodl-polygon-futures-curves ONCE (async) and watch its request record: expect status=complete, phase=complete,
    published=true, quality with products/contracts; the public head data/futures-research.json regenerated (it has been
    on 2026-10-10 13:20 UTC since the provider limit appeared);
 4. run justhodl-fleet-integrity ONCE (sync) and read data/fleet-integrity.json: D4_orphan_rule expected 0 (the 20 were
    alias-qualified targets misread as a function named "live").
Limits: nothing faster than daily (the lane loses a member, nothing is added); no caps, budgets, IAM, lifecycle or
archive changes.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import Counter
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
FAN_KEY = "config/fanout-manifest.json"
FC, FI = "justhodl-polygon-futures-curves", "justhodl-fleet-integrity"
DISPATCHED_AT = datetime(2026, 10, 22, 16, 25, 0, tzinfo=timezone.utc)
REQ_PREFIX = "audit-private/20260909-originals/futures-research/requests/"
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=900)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def main():
    s3, lam = c("s3"), c("lambda")
    with report("ops_6614_futures_curves_relaunch") as r:
        r.heading("ops 6614 - lane manifest, deploy check, futures-curves first paced run, fleet-integrity re-run")
        r.section("1. S3 fan-out manifest := committed")
        repo_text = (ROOT / FAN_KEY).read_text(encoding="utf-8")
        repo = json.loads(repo_text)
        old_text = s3.get_object(Bucket=LIVE, Key=FAN_KEY)["Body"].read().decode("utf-8")
        old = json.loads(old_text)
        diffs = {t: (sorted(set(old["ticks"].get(t, [])) - set(repo["ticks"].get(t, []))), sorted(set(repo["ticks"].get(t, [])) - set(old["ticks"].get(t, []))))
                 for t in set(old["ticks"]) | set(repo["ticks"]) if sorted(old["ticks"].get(t, [])) != sorted(repo["ticks"].get(t, []))}
        r.log("tick differences S3 -> repo (removed, added): %s" % json.dumps(diffs))
        if diffs and (set(diffs) != {"session-5min"} or diffs["session-5min"] != ([FC], [])):
            r.fail("the fan-out manifest difference is not just the futures-curves removal")
            raise SystemExit(1)
        if old_text != repo_text:
            stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
            s3.put_object(Bucket=PRIVATE, Key="backups/fanout-manifest/%s/fanout-manifest.json" % stamp, Body=old_text.encode("utf-8"), ServerSideEncryption="AES256")
            s3.put_object(Bucket=LIVE, Key=FAN_KEY, Body=repo_text.encode("utf-8"), ContentType="application/json", CacheControl="no-cache")
            if s3.get_object(Bucket=LIVE, Key=FAN_KEY)["Body"].read().decode("utf-8") != repo_text:
                r.fail("read-back differs")
                raise SystemExit(1)
            r.ok("fan-out manifest uploaded: session-5min now %d members (backup backups/fanout-manifest/%s/)" % (len(repo["ticks"]["session-5min"]), stamp))
        else:
            r.ok("fan-out manifest already equal")

        r.section("2. the deploy landed")
        deadline = time.time() + 12 * 60
        while True:
            cf_fc = lam.get_function_configuration(FunctionName=FC)
            cf_fi = lam.get_function_configuration(FunctionName=FI)
            ok_fc = datetime.fromisoformat(cf_fc["LastModified"].replace("+0000", "+00:00")) >= DISPATCHED_AT and cf_fc.get("Timeout") == 900
            ok_fi = datetime.fromisoformat(cf_fi["LastModified"].replace("+0000", "+00:00")) >= DISPATCHED_AT
            if (ok_fc and ok_fi) or time.time() > deadline:
                break
            time.sleep(30)
        r.log("%s modified %s timeout %s -> new code: %s; %s modified %s -> new code: %s" % (FC, cf_fc["LastModified"][:19], cf_fc.get("Timeout"), ok_fc, FI, cf_fi["LastModified"][:19], ok_fi))
        if not (ok_fc and ok_fi):
            r.fail("deploy-lambdas has not shipped both functions yet - run again after it finishes")
            raise SystemExit(1)

        r.section("3. futures-curves: one paced run")
        head0 = s3.head_object(Bucket=LIVE, Key="data/futures-research.json")
        r.log("public head before: last modified %s" % str(head0.get("LastModified"))[:19])
        t0 = datetime.now(timezone.utc)
        resp = lam.invoke(FunctionName=FC, InvocationType="Event", Payload=json.dumps({"request_id": "ops6614-%s" % t0.strftime("%Y%m%dT%H%M%SZ")}).encode())
        r.log("async invoke status %s at %s" % (resp.get("StatusCode"), t0.strftime("%H:%M:%S")))
        rec, key = None, None
        deadline = time.time() + 15 * 60
        while time.time() < deadline:
            time.sleep(30)
            res = s3.list_objects_v2(Bucket=LIVE, Prefix=REQ_PREFIX, MaxKeys=1000)
            objs = [o for o in res.get("Contents", []) if o["Key"].count("/") == REQ_PREFIX.count("/") and o["LastModified"] >= t0 - timedelta(seconds=5)]
            # the records are named by a sha; find the newest one updated after the invoke
            objs.sort(key=lambda o: o["LastModified"])
            if objs:
                key = objs[-1]["Key"]
                rec = json.loads(s3.get_object(Bucket=LIVE, Key=key)["Body"].read())
                if rec.get("status") in ("complete", "failed"):
                    break
        if not rec:
            r.fail("no request record appeared within 15 min")
            raise SystemExit(1)
        r.log("request record %s: status %s phase %s published %s error %s failure_class %s started %s completed %s" % (
            key.rsplit("/", 1)[-1][:16], rec.get("status"), rec.get("phase"), rec.get("published"), rec.get("error"), rec.get("failure_class"), (rec.get("started_at") or "")[:19], (rec.get("completed_at") or "")[:19]))
        r.log("quality: %s; provider requests this execution: %s" % (json.dumps(rec.get("quality")), rec.get("provider_requests_this_execution")))
        ds_prefix = key[:-5] + "/datasets/"
        ds = s3.list_objects_v2(Bucket=LIVE, Prefix=ds_prefix, MaxKeys=200).get("Contents", [])
        stops = Counter()
        for o in ds:
            try:
                v = json.loads(s3.get_object(Bucket=LIVE, Key=o["Key"])["Body"].read())
                stops[v.get("stop")] += 1
            except Exception:  # noqa: BLE001
                stops["unreadable"] += 1
        r.log("dataset checkpoints: %d; stops: %s" % (len(ds), json.dumps(stops)))
        head1 = s3.head_object(Bucket=LIVE, Key="data/futures-research.json")
        r.log("public head after: last modified %s" % str(head1.get("LastModified"))[:19])
        fc_ok = rec.get("status") == "complete" and rec.get("published") is True
        r.kv(futures_status=rec.get("status"), futures_published=rec.get("published"), futures_quality=json.dumps(rec.get("quality"))[:200], datasets=len(ds), stops=json.dumps(stops))
        if rec.get("status") != "complete":
            r.warn("futures-curves did not complete: %s / %s" % (rec.get("error"), rec.get("failure_class")))

        r.section("4. fleet-integrity re-run")
        resp2 = lam.invoke(FunctionName=FI, Payload=b"{}")
        body = json.loads(resp2["Payload"].read() or b"{}")
        r.log("fleet-integrity invoke: error %s; body %s" % (resp2.get("FunctionError"), json.dumps(body)[:400]))
        fi = json.loads(s3.get_object(Bucket=LIVE, Key="data/fleet-integrity.json")["Body"].read())
        new = fi.get("new") or []
        cls = Counter(str(x).split("|")[0] for x in new)
        rows = [x for x in (fi.get("rows") or []) if isinstance(x, dict)]
        d4 = [x for x in rows if str(x.get("cls") or x.get("defect") or "").startswith("D4")]
        r.log("fleet-integrity.json generated %s version %s: new %d by class %s; D4 rows now %d" % (fi.get("generated_at") or fi.get("at"), fi.get("version"), len(new), json.dumps(cls.most_common(10)), len(d4)))
        r.kv(integrity_version=fi.get("version"), integrity_new=len(new), d4_rows=len(d4))
        if d4:
            r.warn("D4 rows remain: %s" % json.dumps(d4[:5])[:600])
        if fc_ok and not d4:
            r.ok("futures-curves complete and published under the paced run; fleet-integrity v%s reports 0 orphan rules" % fi.get("version"))
        else:
            r.fail("see above: futures complete=%s published=%s, D4=%d" % (rec.get("status"), rec.get("published"), len(d4)))
            raise SystemExit(1)


if __name__ == "__main__":
    main()
