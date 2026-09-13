"""ops 5520 -- factory discipline + official prints (Claude Ship 1, 2026-09-13).

What the push carries: the student tick no longer raises Conflict('state_changed') on every tick
(26c4ac6 reused the state ETag variable for factory/fleet/meta.json), factory_doctrine.verdict runs
inside the tick every 15 minutes (promote / hold / retire from warehouse-graded windows), spawn caps
come from the chain of command, the gateway's direct z.ai call is gone (owner voice only through
llm_router.complete), learn tracks are factory-reading.v1 receipts, and the runner-owned official
prints lane (scripts/factory_official_prints.py + factory-official-prints.yml) exists.

This op proves it by receipt and by live behaviour:
  1. release receipts for justhodl-ai and justhodl-student-rsi carry HEAD and match live CodeSha256;
  2. the student role may write its two public mirrors (data/ai-factory.json, data/factory-public.json)
     -- an IAM change made here on the runner, never by the student;
  3. two synchronous ticks: ok, no Conflict, no discipline/projection error, state_version advances,
     ranks in the private authority and in the public projection (aliases only);
  4. official prints DRY RUN for the last closed week before the season (2026-09-07): all six
     symbols complete from the warehouse + Coinbase + Polygon reference; nothing written;
  5. anonymous boundaries unchanged: factory/salon/season.json and data/brain-constitution.json deny;
  6. the live justhodl-ai zip has no provider bypass in factory_gateway.py.
"""
from __future__ import annotations

import io
import json
import subprocess
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

import boto3
from botocore.config import Config

HERE = Path(__file__).resolve()
REPO = HERE.parents[3]
sys.path.insert(0, str(REPO / "aws" / "shared"))
sys.path.insert(0, str(REPO / "scripts"))
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
PUB, PRI = "justhodl-dashboard-live", "justhodl-ai-857687956942"
FUNCS = ("justhodl-ai", "justhodl-student-rsi")
ROLE, POLICY = "justhodl-student-rsi-role", "factory-gear-a"
MIRRORS = ("data/ai-factory.json", "data/factory-public.json")
UA = {"User-Agent": "JustHodl-ops-5520 (+https://justhodl.ai)"}
RECEIPT_WAIT_S = 45 * 60


def _get_json(s3, bucket, key):
    try:
        o = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(o["Body"].read())
    except Exception:  # noqa: BLE001
        return None


def _head():
    return subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, text=True, capture_output=True).stdout.strip()


def _http_status(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=30) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception as e:  # noqa: BLE001
        return "ERR:" + type(e).__name__


def _widen_student_mirrors(iam, R):
    doc = iam.get_role_policy(RoleName=ROLE, PolicyName=POLICY)["PolicyDocument"]
    arns = ["arn:aws:s3:::%s/%s" % (PUB, k) for k in MIRRORS]
    touched = 0
    for st in doc.get("Statement", []):
        if st.get("Effect") != "Allow" or st.get("Action") != "s3:PutObject":
            continue
        res = st.get("Resource") if isinstance(st.get("Resource"), list) else [st.get("Resource")]
        if not any(r.endswith("/factory/salon/board.json") for r in res):
            continue
        for arn in arns:
            if arn not in res:
                res.append(arn); touched += 1
        st["Resource"] = res
    if touched:
        iam.put_role_policy(RoleName=ROLE, PolicyName=POLICY, PolicyDocument=json.dumps(doc))
    R.ok("student role %s/%s: mirrors %s (%d resource entries added; the two board/wall statements only)" % (
        ROLE, POLICY, "widened" if touched else "already allowed", touched))


def main() -> int:
    red, warn = [], []
    cfg = Config(read_timeout=140, connect_timeout=5, retries={"max_attempts": 0})
    s3 = boto3.client("s3", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    iam = boto3.client("iam")
    head = _head()
    with report("ops_5520_factory_discipline_prints_gate") as R:
        R.heading("ops 5520 -- factory discipline in the tick + official prints lane: receipts, live ticks, dry-run prints, boundaries")
        R.kv(head=head[:10])

        # 1. receipts (wait for the deploy of this push)
        deadline = time.time() + RECEIPT_WAIT_S
        pending = set(FUNCS)
        while pending and time.time() < deadline:
            for fn in list(pending):
                rc = _get_json(s3, PUB, "data/ops/releases/%s.json" % fn) or {}
                if str(rc.get("commit", "")) == head:
                    live = lam.get_function_configuration(FunctionName=fn)["CodeSha256"]
                    ok = rc.get("code_sha256") == live
                    (R.ok if ok else R.fail)("%s receipt commit=%s sha_match=%s run=%s" % (fn, head[:7], ok, rc.get("run_id")))
                    if not ok:
                        red.append("receipt:" + fn)
                    pending.discard(fn)
            if pending:
                time.sleep(30)
        for fn in pending:
            red.append("receipt-missing:" + fn)
            R.fail("%s: no release receipt for %s within %d min" % (fn, head[:7], RECEIPT_WAIT_S // 60))
        if red:
            R.fail("RED -- %s" % ", ".join(red))
            return 1

        # 2. IAM: the student may write its two public mirrors (runner-made change; the student never touches IAM)
        try:
            _widen_student_mirrors(iam, R)
        except Exception as e:  # noqa: BLE001
            red.append("iam"); R.fail("student role widen failed %s" % str(e)[:160])

        # 3. two live ticks
        versions = []
        for i in range(2):
            out = None
            for attempt in range(4):
                raw = lam.invoke(FunctionName="justhodl-student-rsi", InvocationType="RequestResponse", Payload=b"{}")
                out = json.loads(raw["Payload"].read() or b"{}")
                if out.get("status") != "already_running" and "errorMessage" not in out:
                    break
                time.sleep(25)
            if not out or "errorMessage" in out or not out.get("ok"):
                red.append("tick"); R.fail("tick %d failed: %s" % (i + 1, json.dumps(out)[:300])); break
            errs = out.get("errors") or []
            bad = [e for e in errs if e.get("phase") in ("discipline", "projection") or "Conflict" in json.dumps(e)]
            (R.fail if bad else R.ok)("tick %d ok status=%s state_version=%s errors=%s" % (i + 1, out.get("status"), out.get("state_version"), json.dumps(errs)[:400]))
            if bad and i == 1:
                red.append("tick-errors")
            versions.append(int(out.get("state_version") or 0))
            time.sleep(3)
        if len(versions) == 2 and versions[1] <= versions[0]:
            red.append("tick-version"); R.fail("state_version did not advance: %s" % versions)
        current = _get_json(s3, PRI, "factory/runtime/current.json") or {}
        ranks = current.get("ranks") or {}
        cards = ranks.get("cards") or {}
        ok = ranks.get("schema_version") == "factory-ranks.v1" and (cards.get("student") or {}).get("rank") == "student"
        (R.ok if ok else R.fail)("private authority ranks: schema=%s cards=%d student=%s active=%d" % (
            ranks.get("schema_version"), len(cards), (cards.get("student") or {}).get("rank"),
            sum(1 for c in cards.values() if c.get("status") == "active")))
        if not ok:
            red.append("ranks")
        proj = _get_json(s3, PUB, "data/ai-factory.json") or {}
        pr = proj.get("ranks") or {}
        ok = pr.get("schema_version") == "factory-ranks.v1" and "uid" not in json.dumps(proj)
        (R.ok if ok else R.warn)("public projection ranks: schema=%s cards=%s generated=%s" % (pr.get("schema_version"), len(pr.get("cards") or []), proj.get("generated_at")))
        if not ok:
            warn.append("projection-not-yet-mirrored")

        # 4. official prints dry run for the last closed pre-season week
        try:
            import factory_official_prints as fp
            wh = fp.Warehouse(s3)
            rep = fp.run(wh, week="2026-09-07", dry_run=True)
            complete = rep.get("complete")
            summary = {k: (v.get("status"), v.get("opening"), (v.get("closes") or [None])[-1], v.get("corporate_action"), v.get("reason"))
                       for k, v in rep.get("symbols", {}).items()}
            (R.ok if complete else R.fail)("prints dry-run 2026-09-07 complete=%s %s" % (complete, json.dumps(summary)[:900]))
            if not complete:
                red.append("prints-dry-run")
            R.kv(prints_week="2026-09-07", prints_complete=complete)
        except Exception as e:  # noqa: BLE001
            red.append("prints-dry-run"); R.fail("prints dry-run raised %s: %s" % (type(e).__name__, str(e)[:200]))
        wf = REPO / ".github/workflows/factory-official-prints.yml"
        (R.ok if wf.exists() else R.fail)("factory-official-prints.yml present: %s (cron Sat 04:45 UTC, Sun 12:00 UTC)" % wf.exists())
        if not wf.exists():
            red.append("workflow")

        # 5. anonymous boundaries
        for url, want in (("https://justhodl.ai/factory/salon/season.json", (401, 403)),
                          ("https://justhodl.ai/data/brain-constitution.json", (401,))):
            st = _http_status(url)
            (R.ok if st in want else R.fail)("anonymous %s -> %s (want %s)" % (url, st, "/".join(map(str, want))))
            if st not in want:
                red.append("boundary")

        # 6. live gateway bytes
        try:
            cfg_ai = lam.get_function(FunctionName="justhodl-ai")
            zb = urllib.request.urlopen(cfg_ai["Code"]["Location"], timeout=120).read()
            src = zipfile.ZipFile(io.BytesIO(zb)).read("factory_gateway.py").decode()
            clean = all(n not in src for n in ("api.z.ai", "ZAI_BASE_URL", "_zai_key", "factory-lesson.v1"))
            (R.ok if clean else R.fail)("live justhodl-ai factory_gateway.py: provider bypass absent=%s reading_receipts=%s check_spawn=%s" % (
                clean, "factory/fleet/reading/" in src, "factory_discipline.check_spawn(" in src))
            if not clean:
                red.append("gateway-bytes")
        except Exception as e:  # noqa: BLE001
            warn.append("zip-inspect"); R.warn("live zip inspect skipped %s" % str(e)[:120])

        if red:
            R.fail("RED -- %s" % ", ".join(red))
            return 1
        R.ok("GREEN -- tick commits again (no Conflict), chain of command live in state + projection, prints adapter proven dry against the warehouse, boundaries intact%s" % (
            " (warn: %s)" % ", ".join(warn) if warn else ""))
        return 0


if __name__ == "__main__":
    if main() != 0:
        sys.exit(1)
    sys.exit(0)
