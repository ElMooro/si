"""ops_4966 -- lane #4 dol-full launch + lane #5 bsrm TRUTH-CLOSE.

#4  justhodl-dol-full v1.0.0 (harness 12/12): every DataDownloads
    report CSV mirrored verbatim, conditional, self-extending.
#5  dissolved under audit: ops 4753's own report proves the "500
    parsed bsrm series" are an ACCIDENTAL DUPLICATE of ofr-hfm
    (ops 4752 bug), flagged in-bucket by _DUPLICATE_NOTE.json --
    no transform owed. src-mirror v1.2 rewrites refresh-orphans
    (bsrm -> closed-with-reason; nyfed-haircuts = sole phase-2);
    the card fossil dies (bsrm-truth).

  G-1 markers  G0 settle dol-full(+fallback) + src-mirror + catalog
  G0b rate(6 hours) dol schedule  G1 dol run: files>=60, mb>=15,
  ar539 readable  G2 bsrm truth: _DUPLICATE_NOTE verified in
  bucket, refresh-orphans closed-entry live, ofr-hfm canonical
  fresh (<26h)  G3 cards post-mark (dol FULL + bsrm-truth, fossil
  string forbidden)
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

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-dol-full"
MIR = "justhodl-src-mirror"
CAT = "justhodl-provider-catalog"
STATE_KEY = "data/warm/dol-full/_state/state.json"
ORPHANS = "data/warm/_audit/refresh-orphans.json"
DUPNOTE = "data/warm/ofr-bsrm/series/_DUPLICATE_NOTE.json"
HUB_KEY = "data/provider-catalog.json"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
MARKS = {FN: ("v1.0.0 ops4966",
              "aws/lambdas/justhodl-dol-full/source/"
              "lambda_function.py"),
         MIR: ("bsrm-truth ops 4966",
               "aws/lambdas/justhodl-src-mirror/source/"
               "lambda_function.py"),
         CAT: ("dol-note-v2",
               "aws/lambdas/justhodl-provider-catalog/source/"
               "lambda_function.py")}
ROOTP = Path(__file__).resolve().parents[2]

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)


def gj(key, default=None):
    try:
        raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:
        return default


def kick(fn):
    try:
        lam.invoke(FunctionName=fn, InvocationType="Event",
                   Payload=b"{}")
    except Exception:
        pass


def settle(R, fn, mk, budget=600):
    t0 = time.time()
    while time.time() - t0 < budget:
        try:
            f = lam.get_function(FunctionName=fn)
            req = urllib.request.Request(f["Code"]["Location"])
            with urllib.request.urlopen(req, timeout=90) as r:
                zb = r.read()
            src = zipfile.ZipFile(io.BytesIO(zb)).read(
                "lambda_function.py").decode("utf-8", "replace")
            if mk in src and \
                    f["Configuration"].get("State") == "Active":
                R.log("  %s settled (%ds)" % (fn, time.time() - t0))
                return True
        except lam.exceptions.ResourceNotFoundException:
            R.log("  %s t+%ds NOT FOUND" % (fn, time.time() - t0))
            if time.time() - t0 > 240:
                return None
        except Exception as e:
            R.log("  %s settle: %s" % (fn, str(e)[:90]))
        time.sleep(25)
    return False


with report("ops_4966_dol_full_and_bsrm_truth") as R:
    fails = []
    R.section("G-1 markers-in-checkout")
    for fn, (mk, rel) in MARKS.items():
        if mk not in (ROOTP.parent / rel).read_text():
            R.log("ABORT: %r absent from %s" % (mk, rel))
            sys.exit(1)
        R.log("  ok %-24s %r" % (fn, mk))

    R.section("G0 settle x3")
    r0 = settle(R, FN, MARKS[FN][0])
    if r0 is None:
        R.log("  create-branch skipped -> deploy from runner")
        cfg = json.load(open(ROOTP.parent /
                             "aws/lambdas/justhodl-dol-full/"
                             "config.json"))
        deploy_lambda(
            report=R, function_name=FN,
            source_dir=ROOTP / "lambdas/justhodl-dol-full/source",
            env_vars={"S3_BUCKET": B}, timeout=780, memory=512,
            description=cfg["description"],
            create_function_url=False, smoke=False)
        r0 = settle(R, FN, MARKS[FN][0], budget=300)
    ok_mir = settle(R, MIR, MARKS[MIR][0])
    ok_cat = settle(R, CAT, MARKS[CAT][0])
    if not (r0 and ok_mir and ok_cat):
        R.log("G0 FAIL")
        sys.exit(1)
    R.log("G0 PASS")

    R.section("G0b dol schedule rate(6 hours)")
    arn = lam.get_function_configuration(
        FunctionName=FN)["FunctionArn"]
    try:
        sch.create_schedule(
            Name="justhodl-dol-full-6h", GroupName="default",
            ScheduleExpression="rate(6 hours)",
            FlexibleTimeWindow={"Mode": "OFF"}, State="ENABLED",
            Target={"Arn": arn, "RoleArn": SCHED_ROLE,
                    "Input": "{}"})
        R.log("G0b created")
    except Exception as e:
        if "Conflict" in type(e).__name__ or "exists" in str(e):
            R.log("G0b exists (ok)")
        else:
            R.log("G0b FAIL %s" % str(e)[:100])
            fails.append("G0b")

    R.section("G1 dol-full run")
    kick(FN)
    st, t0 = {}, time.time()
    while time.time() - t0 < 9 * 60:
        time.sleep(25)
        st = gj(STATE_KEY) or {}
        if st.get("as_of") and \
                float(st.get("lease_until") or 1) == 0:
            break
        R.log("  t+%4ds files=%s" % (time.time() - t0,
                                     st.get("n_files")))
    files = st.get("n_files") or 0
    mb = (st.get("bytes_total") or 0) / 1e6
    ok1 = files >= 60 and mb >= 15
    R.log("G1 %s files=%d mb=%.1f universe=%s failures=%d" % (
        "PASS" if ok1 else "FAIL", files, mb,
        st.get("universe_n"), len(st.get("failures") or {})))
    try:
        raw = s3.get_object(
            Bucket=B, Key="data/warm/dol-full/src/ar539.csv"
        )["Body"].read(400)
        R.log("  ar539 head: %r" % raw[:80])
        ok1 = ok1 and b"," in raw
    except Exception as e:
        R.log("  ar539 read: %s" % str(e)[:80])
        ok1 = False
    if not ok1:
        fails.append("G1")

    R.section("G2 bsrm truth verified")
    dup = gj(DUPNOTE) or {}
    kick(MIR)
    orp, t0 = {}, time.time()
    while time.time() - t0 < 8 * 60:
        time.sleep(25)
        orp = gj(ORPHANS) or {}
        if "closed" in orp:
            break
        R.log("  t+%4ds orphans awaiting v1.2 write" % (
            time.time() - t0))
    hub0 = gj(HUB_KEY) or {}
    hfm = next((p for p in hub0.get("providers", [])
                if p.get("slug") == "ofr-hf"), {}) or {}
    hfm_h = abs(hfm.get("freshest_h") or 99)
    ok2 = bool(dup.get("canonical_prefix") ==
               "data/warm/ofr-hfm/series/") and \
        "ofr-bsrm-series" in (orp.get("closed") or {}) and \
        "ofr-bsrm-series" not in (orp.get("phase2_retransforms")
                                  or {}) and \
        "nyfed-haircuts-series" in (orp.get("phase2_retransforms")
                                    or {}) and hfm_h < 26
    R.log("G2 %s dupnote=%s closed=%s phase2=%s hfm=%.1fh" % (
        "PASS" if ok2 else "FAIL", bool(dup),
        list((orp.get("closed") or {})),
        list((orp.get("phase2_retransforms") or {})), hfm_h))
    if not ok2:
        fails.append("G2")

    R.section("G3 cards (post-mark)")
    t_mark = datetime.now(timezone.utc).isoformat(timespec="seconds")
    kick(CAT)
    hub, t0 = {}, time.time()
    while time.time() - t0 < 12 * 60:
        time.sleep(30)
        hub = gj(HUB_KEY) or {}
        if (hub.get("as_of") or "") >= t_mark:
            break
    dl = next((p for p in hub.get("providers", [])
               if p.get("slug") in ("dol", "dol-eta")), {}) or {}
    bs = next((p for p in hub.get("providers", [])
               if p.get("slug") == "ofr-bsrm"), {}) or {}
    ok3 = "FULL ETA DataDownloads corpus" in (dl.get("catalog_note")
                                              or "") and \
        "bsrm-truth" in (bs.get("catalog_note") or "") and \
        "re-transform = phase 2" not in (bs.get("catalog_note")
                                         or "")
    R.log("  dol : %s" % (dl.get("catalog_note") or "")[:160])
    R.log("  bsrm: %s" % (bs.get("catalog_note") or "")[:160])
    R.log("G3 %s" % ("PASS" if ok3 else "FAIL"))
    if not ok3:
        fails.append("G3")

    if fails:
        R.log("ops 4966 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(dol_files=files, dol_mb=round(mb, 1),
         phase2_remaining=list(
             (orp.get("phase2_retransforms") or {})),
         hfm_fresh_h=round(hfm_h, 1))
    R.log("ops 4966 GREEN -- lane #4 corpus FULL; lane #5 closed as "
          "truth (no transform owed); the five-lane program is "
          "complete on the build side")
