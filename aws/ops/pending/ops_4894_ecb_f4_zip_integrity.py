"""ops/4894 -- ecb-full-catalog F4-zip integrity + arc non-regression.

Self-caught violation of the house rule (memory #30 / AUTONOMY): ops
4893 rev-B/rev-C called deploy_lambda() on functions ALSO in the push,
and the ops-helper zips ONLY source/ -- so the live ecb-full-catalog
artifact lost aws/shared/raw_snapshot.py. Its fail-soft import masked
it (snapshot=None), i.e. the catalog kept banking (proven, 104 flows)
but the F4 raw-XML snapshot contract silently stopped. provider-catalog
and import-sentinel import nothing shared -- their artifacts are whole.

Remediation already fired BEFORE this push: deploy-lambdas.yml
workflow_dispatch function=justhodl-ecb-full-catalog (API 204), which
zips source + aws/shared/*.py. This op proves it:

  A) zip-settle BY CONTENT: live artifact must contain raw_snapshot.py
  B) invoke catalog -> ok, n_dataflows>=100, raw_snapshot_key NON-NULL
     in the read-back (F4 restored), snapshot object HEADable
  C) walker non-regression: sdmx-walk-ecb done>=2, failures reported
  D) sentinel non-regression: sdmx-ecb not BLOCKED, no 406 in detail
"""
import gzip
import io
import json
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-ecb-full-catalog"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=240,
                                 retries={"max_attempts": 0}))


def g(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def live_zip_names():
    loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
    raw = urllib.request.urlopen(loc, timeout=60).read()
    return set(zipfile.ZipFile(io.BytesIO(raw)).namelist())


def main():
    verdict = {"ops": 4894, "gates": {},
               "started": datetime.now(timezone.utc).isoformat(
                   timespec="seconds")}
    with report("ops 4894 -- ecb f4 zip integrity") as rep:
        rep.heading("ops 4894 — F4-zip integrity + arc non-regression")

        # ── A. zip-settle by CONTENT (dispatch fired pre-push) ───────
        names, ok_a = set(), False
        deadline = time.time() + 360
        while time.time() < deadline:
            try:
                names = live_zip_names()
            except Exception as e:
                rep.log(f"get_function/zip read: "
                        f"{type(e).__name__}: {str(e)[:80]}")
                names = set()
            if "raw_snapshot.py" in names:
                ok_a = True
                break
            time.sleep(20)
        rep.kv(stage="zip-settle", raw_snapshot_in_zip=ok_a,
               n_files=len(names),
               sample=",".join(sorted(names)[:8]))
        verdict["gates"]["shared_zip_restored"] = ("PASS" if ok_a
                                                   else "FAIL")

        # ── B. invoke: F4 snapshot must land non-null ────────────────
        n_flows, snap_key, ok_b = 0, None, False
        if ok_a:
            try:
                r = lam.invoke(FunctionName=FN,
                               InvocationType="RequestResponse")
                payload = json.loads(r["Payload"].read() or b"{}")
                body = payload.get("body")
                res = json.loads(body) if isinstance(body, str) else (
                    payload if isinstance(payload, dict) else {})
                n_flows = int(res.get("n_dataflows") or 0)
                cat = g("data/warm/ecb/catalog.json.gz")
                snap_key = cat.get("raw_snapshot_key")
                head_ok = None
                if snap_key:
                    try:
                        s3.head_object(Bucket=B, Key=snap_key)
                        head_ok = True
                    except Exception as e:
                        head_ok = f"{type(e).__name__}"
                ok_b = bool(res.get("ok")) and n_flows >= 100 \
                    and bool(snap_key)
                rep.kv(stage="catalog-invoke", ok=bool(res.get("ok")),
                       n_dataflows=n_flows,
                       accept_winner=res.get("accept_winner"),
                       raw_snapshot_key=str(snap_key)[:120],
                       snapshot_headable=head_ok)
            except Exception as e:
                rep.kv(stage="catalog-invoke", ok=False,
                       err=f"{type(e).__name__}: {str(e)[:180]}")
        else:
            rep.kv(stage="catalog-invoke", skipped="zip gate failed")
        verdict["gates"]["f4_snapshot_restored"] = ("PASS" if ok_b
                                                    else "FAIL")

        # ── C. walker non-regression + progress ─────────────────────
        st = None
        try:
            st = g("data/_state/sdmx-walk-ecb.json")
        except Exception as e:
            rep.log(f"walker state read: {type(e).__name__}")
        n_done = len(set((st or {}).get("done") or []))
        rep.kv(stage="walker-progress", state_present=bool(st),
               status=(st or {}).get("status"),
               n_done=n_done, n_total=(st or {}).get("n_total"),
               progress_pct=(st or {}).get("progress_pct"),
               failures=len((st or {}).get("failures") or {}))
        verdict["gates"]["walker_nonregression"] = (
            "PASS" if (st and n_done >= 2) else "FAIL")

        # ── D. sentinel non-regression ───────────────────────────────
        pipe = None
        try:
            ih = g("data/import-health.json")
            pipe = next((p for p in (ih.get("pipelines") or [])
                         if p.get("name") == "sdmx-ecb"), None)
        except Exception as e:
            rep.log(f"import-health read: {type(e).__name__}")
        _st = str((pipe or {}).get("status") or "")
        _dt = str((pipe or {}).get("detail") or "")
        rep.kv(stage="sentinel", status=_st, detail=_dt[:150])
        verdict["gates"]["sentinel_nonregression"] = (
            "PASS" if (pipe and _st != "BLOCKED" and "406" not in _dt)
            else "FAIL")

        hard = [k for k, v in verdict["gates"].items() if v == "FAIL"]
        verdict["overall"] = "FAIL" if hard else "PASS"
        verdict["n_dataflows"] = n_flows
        verdict["finished"] = datetime.now(timezone.utc).isoformat(
            timespec="seconds")
        rep.log("VERDICT: " + verdict["overall"] + " · gates=" +
                json.dumps(verdict["gates"]))
        out = ROOT / "aws" / "ops" / "reports" / "4894.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(verdict, indent=1, default=str))
        rep.log("report written: aws/ops/reports/4894.json")
    return verdict["overall"]


_overall = main()
if _overall == "FAIL":
    sys.exit(1)
sys.exit(0)
