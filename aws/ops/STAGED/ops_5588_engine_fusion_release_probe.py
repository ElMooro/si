"""ops 5588 -- engine-fusion release probe (Claude, 2026-09-17). READ-ONLY. Direct lane only (STAGED never runs serially).

Grok reported that deploy-lambdas.yml showed no run for 002bbd1e (the 2026-09-17 registry change: settlement_fails
max_age 240h -> 336h, freshness_basis weekly_observation, generated_at first). The Actions API shows run 35174108077
(push, success, 02:21 UTC) for that SHA. This probe proves it on AWS the doctrine way and reads the effect:

  1. release receipt data/ops/releases/justhodl-engine-fusion.json vs the LIVE function's CodeSha256 / LastModified
  2. the bundled registry the live zip carries: settlement_fails_treasury row (max_age_hours must read 336)
  3. data/engine-fusion.json: generated_at, status, coverage.freshness, and the settlement_fails_treasury trace row
     (freshness / age_h / max_age_h / error) -- i.e. did the false STALE clear
  4. data/settlement-fails.json timestamps the row is judged on (generated_at, as_of, treasury.as_of)

No writes, no invokes. RED only if the live function cannot be described.
"""
from __future__ import annotations

import hashlib
import io
import json
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
import urllib.request
from botocore.config import Config

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

REGION, PUB, FN = "us-east-1", "justhodl-dashboard-live", "justhodl-engine-fusion"
NOW = datetime.now(timezone.utc)


def _get_json(s3, key):
    try:
        return json.loads(s3.get_object(Bucket=PUB, Key=key)["Body"].read())
    except Exception as e:  # noqa: BLE001
        return {"_error": str(e)[:160]}


def main():
    cfg = Config(read_timeout=60, retries={"max_attempts": 3})
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    with report("5588_engine_fusion_release_probe") as r:
        r.heading("ops 5588 -- engine-fusion release probe (read-only)")

        r.section("1. Live function vs release receipt")
        try:
            fc = lam.get_function(FunctionName=FN)
        except Exception as e:  # noqa: BLE001
            r.fail("get_function %s: %s" % (FN, e))
            sys.exit(1)
        conf = fc["Configuration"]
        live_sha, live_mod = conf.get("CodeSha256"), conf.get("LastModified")
        receipt = _get_json(s3, "data/ops/releases/%s.json" % FN)
        r.kv(live_code_sha256=live_sha, live_last_modified=live_mod, state=conf.get("State"), update_status=conf.get("LastUpdateStatus"))
        if "_error" in receipt:
            r.warn("receipt unreadable: %s" % receipt["_error"])
        else:
            rc = {k: receipt.get(k) for k in ("commit", "code_sha256", "run_id", "workflow", "published_at", "deployed_at", "actor") if k in receipt}
            r.kv(**{"receipt_" + k: v for k, v in rc.items()})
            rsha = receipt.get("code_sha256") or (receipt.get("proof") or {}).get("code_sha256")
            if rsha and rsha == live_sha:
                r.ok("receipt CodeSha256 == live CodeSha256 -- AWS runs the receipted commit %s" % str(receipt.get("commit"))[:12])
            elif rsha:
                r.warn("receipt CodeSha256 %s != live %s (a later deploy or manual update)" % (rsha[:12], (live_sha or "")[:12]))

        r.section("2. Registry row inside the live zip")
        try:
            url = fc["Code"]["Location"]
            blob = urllib.request.urlopen(url, timeout=60).read()
            zf = zipfile.ZipFile(io.BytesIO(blob))
            reg = json.loads(zf.read("fusion-registry.v1.json"))
            row = next(s for s in reg["sources"] if s["id"] == "settlement_fails_treasury")
            r.kv(zip_sha256=hashlib.sha256(blob).hexdigest()[:16], settlement_max_age_hours=row.get("max_age_hours"),
                 freshness_basis=row.get("freshness_basis"), timestamp_paths=row.get("timestamp_paths"),
                 best_setups_evidence_level=next(s.get("evidence_level") for s in reg["sources"] if s["id"] == "best_setups_view"))
            (r.ok if row.get("max_age_hours") == 336 else r.warn)("live bundle carries max_age_hours=%s for settlement_fails_treasury" % row.get("max_age_hours"))
        except Exception as e:  # noqa: BLE001
            r.warn("could not read the live zip: %s" % str(e)[:160])

        r.section("3. data/engine-fusion.json -- the effect")
        ef = _get_json(s3, "data/engine-fusion.json")
        if "_error" in ef:
            r.warn("engine-fusion.json unreadable: %s" % ef["_error"])
        else:
            cov = ef.get("coverage") or {}
            r.kv(generated_at=ef.get("generated_at"), status=ef.get("status"), coverage_ratio=cov.get("ratio"),
                 independent_root_evidence=cov.get("independent_root_evidence"), freshness=json.dumps(cov.get("freshness")))
            rows = [t for t in (ef.get("trace") or []) if t.get("source_id") == "settlement_fails_treasury"]
            for t in rows:
                r.kv(sf_freshness=t.get("freshness"), sf_age_h=t.get("age_h"), sf_max_age_h=t.get("max_age_h"), sf_active=t.get("active"), sf_error=t.get("error"), sf_as_of=t.get("as_of"))
                (r.ok if t.get("freshness") == "FRESH" else r.warn)("settlement_fails_treasury is %s (age %sh vs SLA %sh)%s" % (
                    t.get("freshness"), t.get("age_h"), t.get("max_age_h"), (" -- " + str(t.get("error"))) if t.get("error") else ""))
            if not rows:
                r.warn("no trace row for settlement_fails_treasury in engine-fusion.json")
            stale = [t.get("source_id") for t in (ef.get("trace") or []) if t.get("freshness") != "FRESH"]
            r.log("non-FRESH sources now: %s" % (stale or "none"))

        r.section("4. data/settlement-fails.json timestamps")
        sf = _get_json(s3, "data/settlement-fails.json")
        if "_error" in sf:
            r.warn("settlement-fails.json unreadable: %s" % sf["_error"])
        else:
            r.kv(generated_at=sf.get("generated_at"), as_of=sf.get("as_of"), treasury_as_of=(sf.get("treasury") or {}).get("as_of"),
                 treasury_scope=(sf.get("treasury") or {}).get("scope"), treasury_regime=(sf.get("treasury") or {}).get("regime"))
        r.ok("probe complete (no writes, no invokes)")


if __name__ == "__main__":
    main()
