"""ops 4944 -- US Census Bureau joins the fleet.

Khalid (2026-08-23): "add US Census Bureau to data.html (and import all
the data from there they have a lot of factory orders, manufacturying
etc) ... make sure you are importing all the historic data since
inception".

Ships justhodl-census-us v1.0.0 (EITS full-history walker), registers
the provider in justhodl-provider-catalog (which is what puts the card
on data.html -- the page renders from provider-catalog.json, ops 4893
lesson), and adds a census-us pipeline to justhodl-import-sentinel
with an allowlisted stalled-kick.

Gates (the 4940-4942 three-rung deploy ladder, verbatim):
  G0   repo carries all three markers
  G0b  provider-catalog + sentinel ARTIFACTS carry the markers AND are
       READY (State=Active, LastUpdateStatus=Successful) -- these two
       are deployed by deploy-lambdas.yml racing this ops on the same
       push; never invoke pre-deploy code
  G1   census-us created via house helper (rc=1, no URL, no smoke)
  G2   EventBridge Scheduler heartbeat rate(15 minutes)
  G3   first run: catalog banked, n_total >= 10 EITS datasets
  G4   drain driven up to 3 sync invokes: >= 6 datasets done, rows
       banked, and SINCE-INCEPTION PROOF -- min first-year across
       manifests <= 1995 (MARTS starts 1992)
  G5   provider-catalog re-run: census-us card live in
       provider-catalog.json with keys > 0 and the EITS note
  G6   sentinel re-run: census-us pipeline in import-health.json
The Scheduler completes any remaining drain autonomously; the sentinel
supervises it from this hour forward.
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
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION, B = "us-east-1", "justhodl-dashboard-live"
FN = "justhodl-census-us"
CAT_FN = "justhodl-provider-catalog"
SEN_FN = "justhodl-import-sentinel"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
SRC = ROOT / "aws" / "lambdas" / FN / "source"
STATE_KEY = "data/warm/census-us/_state/state.json"
CENSUS_KEY = "8423ffa543d0e95cdba580f2e381649b6772f515"

MARKERS = {
    FN: "justhodl-census-us v1.0.0 ops4944",
    CAT_FN: "EITS full history since inception:",
    SEN_FN: "census-us: async kick queued",
}

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=900, connect_timeout=20,
                                 retries={"max_attempts": 0}))
sch = boto3.client("scheduler", region_name=REGION)


def gj(key, default=None):
    try:
        raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
        if key.endswith(".gz"):
            raw = gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
        return json.loads(raw)
    except Exception:
        return default


def artifact_ready(fn, marker, budget=540):
    """Rungs 2+3 of the deploy ladder: the DEPLOYED zip carries the
    marker AND the function is READY. No LastModified heuristics."""
    t0 = time.time()
    while time.time() - t0 < budget:
        try:
            g = lam.get_function(FunctionName=fn)
            cfg = g["Configuration"]
            url = g["Code"]["Location"]
            with urllib.request.urlopen(url, timeout=60) as r:
                zbytes = r.read()
            src = zipfile.ZipFile(io.BytesIO(zbytes)).read(
                "lambda_function.py").decode("utf-8", "replace")
            if marker in src and cfg.get("State") == "Active" and \
                    cfg.get("LastUpdateStatus") == "Successful":
                return time.time() - t0
        except Exception:
            pass
        time.sleep(20)
    return None


def sync_run(fn):
    r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                   Payload=b"{}")
    body = r["Payload"].read()
    try:
        return json.loads(body)
    except Exception:
        return {"raw": body[:200].decode("utf-8", "replace")}


with report("ops_4944_census_us_birth") as R:
    t0 = datetime.now(timezone.utc)
    fails = []

    # G0 -- repo has it -------------------------------------------------
    R.section("G0 repo markers")
    for fn, marker in MARKERS.items():
        src = (ROOT / "aws" / "lambdas" / fn / "source"
               / "lambda_function.py").read_text()
        ok = marker in src
        R.log("G0 %s %s" % ("PASS" if ok else "FAIL", fn))
        if not ok:
            fails.append("G0 " + fn)
    if fails:
        R.log("ops 4944 RED early: " + "; ".join(fails))
        sys.exit(1)

    # G0b -- artifacts READY for the two racing deploys ------------------
    R.section("G0b artifact ladder (catalog + sentinel)")
    for fn in (CAT_FN, SEN_FN):
        took = artifact_ready(fn, MARKERS[fn])
        if took is None:
            fails.append("G0b " + fn)
            R.log("G0b FAIL %s artifact never carried marker+READY" % fn)
        else:
            R.log("G0b PASS %s ready after %.0fs" % (fn, took))
    if fails:
        R.log("ops 4944 RED: " + "; ".join(fails))
        sys.exit(1)

    # G1 -- create the engine -------------------------------------------
    R.section("G1 deploy justhodl-census-us")
    deploy_lambda(
        report=R, function_name=FN, source_dir=SRC,
        env_vars={"S3_BUCKET": B, "CENSUS_API_KEY": CENSUS_KEY,
                  "SPACING": "0.55", "MAX_CALLS": "260"},
        timeout=850, memory=1024,
        description=("US Census Bureau EITS walker -- full-history "
                     "import of every timeseries/eits dataset since "
                     "inception; adaptive slicing, daily refresh, "
                     "weekly rediscovery. State: data/warm/census-us/."),
        reserved_concurrency=1, create_function_url=False, smoke=False,
    )
    took = artifact_ready(FN, MARKERS[FN], budget=180)
    if took is None:
        R.log("G1 FAIL census artifact ladder")
        sys.exit(1)
    R.log("G1 PASS engine live + READY")

    # G2 -- Scheduler heartbeat -----------------------------------------
    R.section("G2 schedule")
    fn_arn = lam.get_function(FunctionName=FN)["Configuration"][
        "FunctionArn"]
    sched = {"Name": FN + "-15min",
             "ScheduleExpression": "rate(15 minutes)",
             "FlexibleTimeWindow": {"Mode": "OFF"},
             "Target": {"Arn": fn_arn, "RoleArn": SCHED_ROLE,
                        "Input": "{}"},
             "State": "ENABLED"}
    try:
        sch.create_schedule(**sched)
        R.log("G2 PASS schedule created")
    except sch.exceptions.ConflictException:
        sch.update_schedule(**sched)
        R.log("G2 PASS schedule updated")

    # G3 + G4 -- first run, then drive the drain ------------------------
    R.section("G3/G4 discovery + drain drive")
    last = {}
    for i in range(3):
        last = sync_run(FN)
        R.log("drive %d -> %s" % (i + 1, json.dumps(last)[:220]))
        st = gj(STATE_KEY) or {}
        if i == 0:
            n_total = st.get("n_total") or 0
            cat = gj("data/warm/census-us/catalog.json.gz") or {}
            ok3 = n_total >= 10 and len(cat.get("datasets") or []) == \
                n_total
            R.log("G3 %s catalog banked: n_total=%s universe=%s" % (
                "PASS" if ok3 else "FAIL", n_total,
                st.get("n_timeseries_universe")))
            if not ok3:
                fails.append("G3 catalog")
                break
        if st.get("phase") == "COMPLETE" or (st.get("n_done") or 0) >= 6:
            break
    st = gj(STATE_KEY) or {}
    y0s = []
    for slug in list((st.get("datasets") or {}).keys()):
        man = gj("data/warm/census-us/%s/manifest.json" % slug)
        if man and (man.get("years") or [None])[0]:
            y0s.append(str(man["years"][0]))
    min_y0 = min(y0s) if y0s else None
    ok4 = ((st.get("n_done") or 0) >= 6 and
           (st.get("rows_total") or 0) > 50000 and
           min_y0 is not None and min_y0 <= "1995")
    R.log("G4 %s n_done=%s/%s rows=%s min_first_year=%s phase=%s "
          "failures=%s" % ("PASS" if ok4 else "FAIL",
                           st.get("n_done"), st.get("n_total"),
                           st.get("rows_total"), min_y0,
                           st.get("phase"),
                           list((st.get("failures") or {}).items())[:4]))
    if not ok4:
        fails.append("G4 drain")

    # G5 -- the data.html card ------------------------------------------
    R.section("G5 provider-catalog card")
    prev = (gj("data/provider-catalog.json") or {}).get("as_of", "")
    lam.invoke(FunctionName=CAT_FN, InvocationType="Event", Payload=b"{}")
    card = None
    for _ in range(40):
        time.sleep(15)
        hub = gj("data/provider-catalog.json") or {}
        if hub.get("as_of", "") > prev:
            card = next((p for p in hub.get("providers", [])
                         if p.get("slug") == "census-us"), None)
            break
    ok5 = bool(card and (card.get("n_keys") or 0) > 0 and
               "EITS" in (card.get("catalog_note") or ""))
    R.log("G5 %s card=%s" % ("PASS" if ok5 else "FAIL",
                             json.dumps(card)[:260] if card else None))
    if not ok5:
        fails.append("G5 card")

    # G6 -- sentinel supervision ----------------------------------------
    R.section("G6 sentinel pipeline")
    lam.invoke(FunctionName=SEN_FN, InvocationType="Event", Payload=b"{}")
    pipe = None
    for _ in range(24):
        time.sleep(15)
        h = gj("data/import-health.json") or {}
        pipe = next((p for p in h.get("pipelines", [])
                     if p.get("name") == "census-us"), None)
        if pipe:
            break
    ok6 = bool(pipe and pipe.get("status") in ("RUNNING", "COMPLETE",
                                               "STALE"))
    R.log("G6 %s pipeline=%s" % ("PASS" if ok6 else "FAIL", pipe))
    if not ok6:
        fails.append("G6 sentinel")

    if fails:
        R.log("ops 4944 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(n_total=st.get("n_total"), n_done=st.get("n_done"),
         rows=st.get("rows_total"), min_first_year=min_y0,
         phase=st.get("phase"))
    R.log("ops 4944 GREEN -- US Census Bureau live on data.html; "
          "Scheduler finishes any remaining drain; sentinel supervises")
