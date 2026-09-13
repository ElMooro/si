"""ops 5523 -- factory audit fixes: wall ledger, grader quota, verifier isolation, snapshot retention (Claude, 2026-09-13).

Findings this push fixes (all measured, see tests/factory/test_wall_ledger.py + test_code_verify.py):
  * market_wall re-read the whole season every 5 minutes: at 10 agents x 6 names x 13 weeks one pass was
    ~3,100 S3 requests + 780 grader invocations (~85 s against the 40 s tick deadline, ~900k S3 requests/day),
    and the loop kept reading past the deadline toward the 120 s Lambda timeout. Now a per-season ledger
    (factory/runtime/wall-ledger.json, a memo of immutable objects) bounds a pass to the UNFINALIZED entries:
    ~100 requests steady, 8 once every week is final, each entry graded exactly once.
  * the grader counted pending polls ("prints not yet written") against its 50-verdict day: now only terminal
    verdicts count. The student probes once per week/symbol per hour until prints exist, then grades all.
  * MBPP reference solutions carry CRLF (463/464): normalized in the fetcher and at dataset build.
  * code candidates ran as root with the results directory writable: now `nobody`, own scratch dir.
  * factory/runtime/snapshots/ grew 1,440 objects/day with no retention: 3-day lifecycle rule (the store only
    ever reads the newest 8).
  * ops 5521 went RED because it demanded a receipt whose commit == HEAD; its commit touched no Lambda, so no
    deploy fired. This gate proves by CONTENT (receipt source sha256s == checkout, code_sha256 == live).

What this op does (runner only; the student never touches IAM):
  1. receipts by content for justhodl-student-rsi, justhodl-factory-grader, justhodl-ai (wait <= 45 min);
  2. student role: read factory/official-prints/* (+ list prefix) and write factory/runtime/wall-ledger.json;
  3. private bucket lifecycle: expire factory/runtime/snapshots/ after 3 days (merged, never replaces other rules);
  4. live: one synchronous tick after propagation -- ok, no errors, elapsed < 40 s, wall pass complete, ledger
     object present, state_version advanced;
  5. live: a market probe on the grader that must NOT consume the daily verdict quota;
  6. report only: Gear B public status, code-exam verified rows, holdout manifest.
"""
import json
import hashlib
import subprocess
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config

HERE = Path(__file__).resolve()
REPO = HERE.parents[3]
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
PUB, PRI = "justhodl-dashboard-live", "justhodl-ai-857687956942"
FUNCS = ("justhodl-student-rsi", "justhodl-factory-grader", "justhodl-ai")
ROLE, POLICY = "justhodl-student-rsi-role", "factory-gear-a"
LEDGER_KEY = "factory/runtime/wall-ledger.json"
SNAPSHOT_PREFIX = "factory/runtime/snapshots/"
LIFECYCLE_ID = "jh-factory-runtime-snapshots-3d"
RECEIPT_WAIT_S = 45 * 60


def _get_json(s3, bucket, key):
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except Exception:  # noqa: BLE001
        return None


def _head():
    return subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, text=True, capture_output=True).stdout.strip()


def _source_hashes(fn):
    src = REPO / "aws/lambdas" / fn / "source"
    return {str(p.relative_to(src)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(src.rglob("*")) if p.is_file() and "__pycache__" not in p.parts}


def _receipt_matches_checkout(s3, lam, fn):
    """Proof by release receipt: live CodeSha256 == receipt AND the receipt's source inventory == this checkout.
    A zip rebuilt here can never hash-match the deployed zip; commit equality is not required either."""
    rec = _get_json(s3, PUB, "data/ops/releases/%s.json" % fn) or {}
    live = lam.get_function_configuration(FunctionName=fn)["CodeSha256"]
    if rec.get("code_sha256") != live:
        return False, "receipt code_sha256 %s != live %s" % (str(rec.get("code_sha256"))[:10], live[:10])
    theirs = {k: v.get("sha256") for k, v in (rec.get("source") or {}).items()}
    if theirs != _source_hashes(fn):
        return False, "receipt source (commit %s) differs from this checkout" % str(rec.get("commit"))[:10]
    return True, "receipt commit %s run %s" % (str(rec.get("commit"))[:10], rec.get("run_id") or "-")


def _widen_student(iam, R):
    doc = iam.get_role_policy(RoleName=ROLE, PolicyName=POLICY)["PolicyDocument"]
    if isinstance(doc, str):
        from urllib.parse import unquote
        doc = json.loads(unquote(doc))
    pri = "arn:aws:s3:::" + PRI
    touched = 0
    for st in doc.get("Statement", []):
        if st.get("Effect") == "Allow" and st.get("Action") == "s3:ListBucket" and st.get("Resource") == pri:
            prefixes = st.setdefault("Condition", {}).setdefault("StringLike", {}).setdefault("s3:prefix", [])
            if "factory/official-prints/*" not in prefixes:
                prefixes.append("factory/official-prints/*"); touched += 1
    wanted = [
        {"Effect": "Allow", "Action": "s3:GetObject", "Resource": [pri + "/factory/official-prints/*"]},
        {"Effect": "Allow", "Action": "s3:PutObject", "Resource": [pri + "/" + LEDGER_KEY]},
    ]
    for st in wanted:
        if st not in doc["Statement"]:
            doc["Statement"].append(st); touched += 1
    if touched:
        iam.put_role_policy(RoleName=ROLE, PolicyName=POLICY, PolicyDocument=json.dumps(doc))
    R.ok("student role %s/%s: prints read + ledger write %s (%d changes)" % (ROLE, POLICY, "widened" if touched else "already allowed", touched))
    return touched


def _snapshot_lifecycle(s3, R):
    try:
        rules = s3.get_bucket_lifecycle_configuration(Bucket=PRI).get("Rules", [])
    except Exception as exc:  # noqa: BLE001
        if "NoSuchLifecycleConfiguration" not in str(exc):
            raise
        rules = []
    rule = {"ID": LIFECYCLE_ID, "Status": "Enabled", "Filter": {"Prefix": SNAPSHOT_PREFIX}, "Expiration": {"Days": 3}}
    others = [r for r in rules if r.get("ID") != LIFECYCLE_ID]
    if rule in rules:
        R.ok("lifecycle %s already set (%d other rules untouched)" % (LIFECYCLE_ID, len(others)))
        return
    s3.put_bucket_lifecycle_configuration(Bucket=PRI, LifecycleConfiguration={"Rules": others + [rule]})
    R.ok("lifecycle %s: expire %s after 3 days (%d other rules kept)" % (LIFECYCLE_ID, SNAPSHOT_PREFIX, len(others)))


def _count(s3, bucket, prefix, cap=20000):
    n, token = 0, None
    while True:
        args = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            args["ContinuationToken"] = token
        page = s3.list_objects_v2(**args)
        n += page.get("KeyCount", len(page.get("Contents", [])))
        token = page.get("NextContinuationToken")
        if not token or n >= cap:
            return n


def main() -> int:
    red = []
    cfg = Config(read_timeout=140, connect_timeout=5, retries={"max_attempts": 0})
    s3 = boto3.client("s3", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    iam = boto3.client("iam")
    head = _head()
    with report("ops_5523_factory_wall_ledger_gate") as R:
        R.heading("ops 5523 -- factory audit fixes: wall ledger, grader quota, verifier isolation, snapshot retention")
        R.kv(head=head[:10])

        # 1. receipts by content
        deadline, pending = time.time() + RECEIPT_WAIT_S, set(FUNCS)
        while pending and time.time() < deadline:
            for fn in list(pending):
                ok, why = _receipt_matches_checkout(s3, lam, fn)
                if ok:
                    R.ok("%s: %s" % (fn, why)); pending.discard(fn)
            if pending:
                time.sleep(30)
        for fn in pending:
            red.append("receipt:" + fn)
            R.fail("%s: %s (waited %d min)" % (fn, _receipt_matches_checkout(s3, lam, fn)[1], RECEIPT_WAIT_S // 60))
        if red:
            R.fail("RED -- " + ", ".join(red))
            return 1

        # 2. IAM widen (runner action) + 3. lifecycle
        touched = _widen_student(iam, R)
        _snapshot_lifecycle(s3, R)
        snapshots = _count(s3, PRI, SNAPSHOT_PREFIX)
        R.kv(snapshot_objects_now=snapshots, snapshot_ceiling_after_rule="~4,320 (3 days x 1,440)")
        if touched:
            R.ok("waiting 75 s for IAM propagation before the live tick")
            time.sleep(75)

        # 4. live tick
        before = _get_json(s3, PRI, "factory/runtime/current.json") or {}
        out = None
        for attempt in range(6):
            raw = lam.invoke(FunctionName="justhodl-student-rsi", InvocationType="RequestResponse", Payload=b"{}")
            out = json.loads(raw["Payload"].read() or b"{}")
            if out.get("status") == "already_running":
                time.sleep(20); continue
            errs = [e for e in out.get("errors", []) if e.get("phase") in ("wall", "wall-ledger", "projection")]
            if not errs:
                break
            R.warn("tick attempt %d: %s" % (attempt + 1, json.dumps(errs)[:300]))
            time.sleep(30)
        state = _get_json(s3, PRI, "factory/runtime/current.json") or {}
        wall, health = state.get("wall") or {}, state.get("health") or {}
        elapsed = health.get("elapsed_seconds")
        errs = out.get("errors", []) if isinstance(out, dict) else ["no response"]
        checks = {
            "tick_ok": bool(out) and out.get("ok") is True and out.get("status") in ("running", "live", "degraded"),
            "no_wall_errors": not [e for e in errs if isinstance(e, dict) and e.get("phase") in ("wall", "wall-ledger")],
            "elapsed_lt_40s": isinstance(elapsed, (int, float)) and elapsed < 40,
            "wall_pass_complete": wall.get("pass_complete") is True,
            "state_version_advanced": int(state.get("state_version") or 0) > int(before.get("state_version") or 0),
            "ledger_present_or_empty_season": (_get_json(s3, PRI, LEDGER_KEY) is not None) or (wall.get("entries", 0) == 0),
        }
        for name, passed in checks.items():
            (R.ok if passed else R.fail)("%s=%s" % (name, passed))
            if not passed:
                red.append(name)
        R.kv(tick_status=out.get("status") if isinstance(out, dict) else None, elapsed_seconds=elapsed,
             wall=json.dumps(wall)[:300], errors=json.dumps(errs)[:300])

        # 5. grader: a probe that cannot be a verdict must not consume the day's quota
        day = time.strftime("%Y-%m-%d", time.gmtime())
        qkey = "factory/grader/daily/%s.json" % day
        q0 = (_get_json(s3, PRI, qkey) or {}).get("count", 0)
        raw = lam.invoke(FunctionName="justhodl-factory-grader", InvocationType="RequestResponse",
                         Payload=json.dumps({"kind": "market", "id": "2026-09-14-student-SPY"}).encode())
        verdict = json.loads(raw["Payload"].read() or b"{}")
        q1 = (_get_json(s3, PRI, qkey) or {}).get("count", 0)
        free = verdict.get("status") in ("pending", "rejected") and q1 == q0
        (R.ok if free else R.fail)("grader probe status=%s reason=%s quota %d -> %d (%s)" % (
            verdict.get("status"), verdict.get("reason"), q0, q1, "not consumed" if free else "CONSUMED"))
        if not free:
            red.append("grader-quota")

        # 6. report only
        ai = _get_json(s3, PUB, "data/ai.json") or {}
        gear_b = ai.get("gear_b") or {}
        verified = _count(s3, PRI, "factory/curriculum/code/verified/")
        holdout = _get_json(s3, PRI, "factory/holdout/manifest.json") or {}
        R.kv(gear_b_status=json.dumps(gear_b)[:400], verified_code_rows=verified,
             holdout_frozen_at=holdout.get("frozen_at"), holdout_code_ids=len(((holdout.get("code") or {}).get("task_ids") or [])))
        R.ok("Gear B untouched by this op: no job launched, no budget changed; a job needs >= 1,500 verified rows")

        if red:
            R.fail("RED -- " + ", ".join(red))
            return 1
        R.ok("GREEN -- wall ledger live, grader polls free, verifier unprivileged, snapshots capped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
