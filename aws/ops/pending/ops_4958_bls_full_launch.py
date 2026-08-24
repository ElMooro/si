"""ops_4958 -- launch the FULL BLS warehouse (first drain of the
depth-audit queue; Khalid's 5th ask, answered with GB not promises).

justhodl-bls-full v1.0.0 (harness 15/15): complete download.bls.gov/
pub/time.series mirror -- every survey, series maps + AllData history
since 1913, streamed verbatim, HEAD-conditional refresh, MIDAS-style
self-chain. provider-catalog bls-note-v2 composes the card from the
walker's manifest so 0.11MB-of-curation can never masquerade again.

  G-1 markers-in-checkout (engine + bls-note-v2)
  G0  settle BOTH; NEW-FUNCTION fallback: if deploy-lambdas created
      nothing in 240s (banked intermittent), deploy_lambda() from the
      runner (the usd-funding pattern) then settle again
  G0b Scheduler justhodl-bls-full-12h rate(12 hours)
  G1  kick -> chain-drive 22min: PASS on COMPLETE or PROGRESS
      (>=250 files AND >=1.0GB, chains alive) -- remainder drains
      autonomously; day-two verifies COMPLETE
  G2  substance: >=1 mirrored file >50MB (deep AllData streaming
      proven), a .series map readable
  G3  post-mark catalog: bls card carries the manifest note
"""
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
FN = "justhodl-bls-full"
CAT_FN = "justhodl-provider-catalog"
STATE_KEY = "data/warm/bls-full/_state/state.json"
MANIFEST_KEY = "data/warm/bls-full/manifest.json"
HUB_KEY = "data/provider-catalog.json"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
MARKS = {FN: ("v1.0.0 ops4958",
              "aws/lambdas/justhodl-bls-full/source/"
              "lambda_function.py"),
         CAT_FN: ("bls-note-v2",
                  "aws/lambdas/justhodl-provider-catalog/source/"
                  "lambda_function.py")}
ROOTP = Path(__file__).resolve().parents[2]

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)


def gj(key, default=None):
    try:
        return json.loads(
            s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception:
        return default


def kick(fn, payload=b"{}"):
    try:
        lam.invoke(FunctionName=fn, InvocationType="Event",
                   Payload=payload)
        return True
    except Exception:
        return False


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
            R.log("  %s t+%ds marker=%s" % (fn, time.time() - t0,
                                            mk in src))
        except lam.exceptions.ResourceNotFoundException:
            R.log("  %s t+%ds NOT FOUND" % (fn, time.time() - t0))
            if time.time() - t0 > 240:
                return None          # signal: create-branch skipped
        except Exception as e:
            R.log("  %s settle: %s" % (fn, str(e)[:100]))
        time.sleep(25)
    return False


with report("ops_4958_bls_full_launch") as R:
    fails = []

    R.section("G-1 markers-in-checkout")
    for fn, (mk, rel) in MARKS.items():
        src = (ROOTP.parent / rel).read_text()
        if mk not in src:
            R.log("ABORT: %r absent from %s" % (mk, rel))
            sys.exit(1)
        R.log("  ok %-28s %r" % (fn, mk))

    R.section("G0 settle (new-function fallback armed)")
    r0 = settle(R, FN, MARKS[FN][0])
    if r0 is None:
        R.log("  create-branch skipped (banked intermittent) -> "
              "deploy from runner")
        cfg = json.load(open(ROOTP.parent /
                             "aws/lambdas/justhodl-bls-full/"
                             "config.json"))
        deploy_lambda(
            report=R, function_name=FN,
            source_dir=ROOTP / "lambdas/justhodl-bls-full/source",
            env_vars={"S3_BUCKET": B},
            timeout=850, memory=1024,
            description=cfg["description"],
            create_function_url=False, smoke=False)
        r0 = settle(R, FN, MARKS[FN][0], budget=300)
    if not r0:
        R.log("G0 FAIL engine never settled")
        sys.exit(1)
    if not settle(R, CAT_FN, MARKS[CAT_FN][0]):
        R.log("G0 FAIL catalog never settled")
        sys.exit(1)
    R.log("G0 PASS")

    R.section("G0b schedule rate(12 hours)")
    arn = lam.get_function_configuration(
        FunctionName=FN)["FunctionArn"]
    try:
        sch.create_schedule(
            Name="justhodl-bls-full-12h", GroupName="default",
            ScheduleExpression="rate(12 hours)",
            FlexibleTimeWindow={"Mode": "OFF"}, State="ENABLED",
            Target={"Arn": arn, "RoleArn": SCHED_ROLE,
                    "Input": "{}"})
        R.log("G0b created")
    except Exception as e:
        if "ConflictException" in type(e).__name__ or \
                "already exists" in str(e):
            R.log("G0b exists (ok)")
        else:
            R.log("G0b FAIL %s" % str(e)[:120])
            fails.append("G0b")

    R.section("G1 chain-drive (22min budget; chains finish the rest)")
    kick(FN)
    t0, last_fp, last_move, kicks = time.time(), None, time.time(), 0
    st = {}
    while time.time() - t0 < 22 * 60:
        st = gj(STATE_KEY) or {}
        fp = (st.get("phase"), st.get("n_files"),
              st.get("bytes_total"), len(st.get("queue") or []))
        if fp != last_fp:
            last_fp, last_move = fp, time.time()
            R.log("  t+%4ds %s files=%s gb=%.2f q=%s fail=%s" % (
                time.time() - t0, st.get("phase"),
                st.get("n_files") or 0,
                (st.get("bytes_total") or 0) / 1e9,
                len(st.get("queue") or []),
                len(st.get("failures") or {})))
        if st.get("phase") == "COMPLETE":
            break
        lease_free = float(st.get("lease_until") or 0) <= time.time()
        if lease_free and time.time() - last_move > 240 and kicks < 6:
            kicks += 1
            kick(FN)
            last_move = time.time()
            R.log("  chain restart kick #%d" % kicks)
        time.sleep(25)
    files_n = st.get("n_files") or 0
    gb = (st.get("bytes_total") or 0) / 1e9
    ok1 = st.get("phase") == "COMPLETE" or \
        (files_n >= 250 and gb >= 1.0)
    R.log("G1 %s phase=%s files=%d gb=%.2f q=%s kicks=%d" % (
        "PASS" if ok1 else "FAIL", st.get("phase"), files_n, gb,
        len(st.get("queue") or []), kicks))
    if not ok1:
        fails.append("G1")

    R.section("G2 substance (deep AllData streaming proven)")
    have = st.get("have") or {}
    big = [(k, v["bytes"]) for k, v in have.items()
           if (v.get("bytes") or 0) > 50_000_000]
    ser = next((k for k in have if k.endswith(".series")), None)
    ok2 = bool(big) and bool(ser)
    if ser:
        head = s3.get_object(
            Bucket=B, Key="data/warm/bls-full/src/" + ser
        )["Body"].read(200).decode("utf-8", "replace")
        R.log("  %s head: %r" % (ser, head[:90]))
    for k, b_ in sorted(big, key=lambda x: -x[1])[:5]:
        R.log("  BIG %-34s %.1fMB" % (k, b_ / 1e6))
    R.log("G2 %s big_files=%d series_maps=%s" % (
        "PASS" if ok2 else "FAIL", len(big), bool(ser)))
    if not ok2:
        fails.append("G2")

    R.section("G3 catalog card (post-mark)")
    man = gj(MANIFEST_KEY) or {}
    t_mark = datetime.now(timezone.utc).isoformat(timespec="seconds")
    kick(CAT_FN)
    hub, t0 = {}, time.time()
    while time.time() - t0 < 12 * 60:
        time.sleep(30)
        hub = gj(HUB_KEY) or {}
        if (hub.get("as_of") or "") >= t_mark:
            break
    ce = next((p for p in hub.get("providers", [])
               if p.get("slug") == "bls"), {}) or {}
    note = ce.get("catalog_note") or ""
    ok3 = "FULL time.series warehouse" in note and \
        (ce.get("n_keys") or 0) >= files_n * 0.8
    R.log("G3 %s keys=%s note=%s" % ("PASS" if ok3 else "FAIL",
                                     ce.get("n_keys"), note[:200]))
    if not ok3:
        fails.append("G3")

    if fails:
        R.log("ops 4958 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(phase=st.get("phase"), files=files_n, gb=round(gb, 2),
         queue_left=len(st.get("queue") or []),
         failures=len(st.get("failures") or {}),
         surveys=st.get("n_surveys"),
         manifest_gb=man.get("gb"))
    R.log("ops 4958 GREEN -- the BLS warehouse is draining since "
          "1913; chains + rate(12h) finish and keep it fresh; "
          "day-two: phase COMPLETE + final GB + unchanged-proof")
