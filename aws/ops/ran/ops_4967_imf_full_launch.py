"""ops_4967 -- launch the FULL IMF SDMX-2.1 warehouse + day-two board.

justhodl-imf-full v1.0.0 (harness 14/14) on the ops-4961 grammar:
222 dataflows on api.imf.org (legacy DNS-dead), vintage snapshots
retained, lastN fallback tagged, daily rediscovery, weekly
non-vintage redrain. gov-sources carries the imf-api-v2 marker;
imf-note-v2 composes the card.

  G-1 markers  G0 settle(+new-fn fallback)  G0b rate(6 hours) +
  weekly redrain  G1 chain-drive 15min: catalog>=200, banked>=50
  G2 substance: BOP xml.gz holds SDMX Obs elements  G3 card
  DAY-TWO (info, non-gating): worldbank / gdelt / bls / dol states
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
FN = "justhodl-imf-full"
CAT = "justhodl-provider-catalog"
GOV = "justhodl-gov-sources"
STATE_KEY = "data/warm/imf-full/_state/state.json"
HUB_KEY = "data/provider-catalog.json"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
MARKS = {FN: ("v1.0.2 ops4967",
              "aws/lambdas/justhodl-imf-full/source/"
              "lambda_function.py"),
         CAT: ("imf-note-v2",
               "aws/lambdas/justhodl-provider-catalog/source/"
               "lambda_function.py"),
         GOV: ("imf-api-v2 ops4967",
               "aws/lambdas/justhodl-gov-sources/source/"
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


def ensure_sched(R, name, expr, arn, inp):
    try:
        sch.create_schedule(
            Name=name, GroupName="default",
            ScheduleExpression=expr,
            FlexibleTimeWindow={"Mode": "OFF"}, State="ENABLED",
            Target={"Arn": arn, "RoleArn": SCHED_ROLE,
                    "Input": inp})
        R.log("  created %s %s" % (name, expr))
        return True
    except Exception as e:
        if "Conflict" in type(e).__name__ or "exists" in str(e):
            R.log("  exists %s (ok)" % name)
            return True
        R.log("  sched FAIL %s: %s" % (name, str(e)[:90]))
        return False


with report("ops_4967_imf_full_launch") as R:
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
                             "aws/lambdas/justhodl-imf-full/"
                             "config.json"))
        deploy_lambda(
            report=R, function_name=FN,
            source_dir=ROOTP / "lambdas/justhodl-imf-full/source",
            env_vars={"S3_BUCKET": B}, timeout=850, memory=1024,
            description=cfg["description"],
            create_function_url=False, smoke=False)
        r0 = settle(R, FN, MARKS[FN][0], budget=300)
    ok_c = settle(R, CAT, MARKS[CAT][0])
    ok_g = settle(R, GOV, MARKS[GOV][0])
    if not (r0 and ok_c and ok_g):
        R.log("G0 FAIL")
        sys.exit(1)
    R.log("G0 PASS")

    R.section("G0b schedules")
    arn = lam.get_function_configuration(
        FunctionName=FN)["FunctionArn"]
    okb = ensure_sched(R, "justhodl-imf-full-6h", "rate(6 hours)",
                       arn, "{}")
    okb = ensure_sched(R, "justhodl-imf-full-weekly",
                       "rate(7 days)", arn,
                       json.dumps({"redrain": True})) and okb
    if not okb:
        fails.append("G0b")

    R.section("G1 chain-drive (15min)")
    kick(FN)
    t0, last_fp, last_move, kicks = time.time(), None, time.time(), 0
    st = {}
    while time.time() - t0 < 15 * 60:
        st = gj(STATE_KEY) or {}
        have = st.get("have") or {}
        fp = (st.get("phase"), len(have),
              len(st.get("queue") or []))
        if fp != last_fp:
            last_fp, last_move = fp, time.time()
            R.log("  t+%4ds %s banked=%d q=%s cat=%s fail=%s" % (
                time.time() - t0, st.get("phase"), len(have),
                len(st.get("queue") or []),
                len(st.get("universe") or {}),
                len(st.get("failures") or {})))
        if st.get("phase") == "COMPLETE":
            break
        if float(st.get("lease_until") or 0) <= time.time() and \
                time.time() - last_move > 220 and kicks < 4:
            kicks += 1
            kick(FN)
            last_move = time.time()
            R.log("  chain restart kick #%d" % kicks)
        time.sleep(25)
    have = st.get("have") or {}
    cat_n = len(st.get("universe") or {})
    # v3 evidence: flows are HUGE (BOP alone 1.87GB raw); launch-
    # verified bar -- chains + 6h schedule own completion (ETA hrs)
    ok1 = cat_n >= 200 and len(have) >= 8 and \
        st.get("phase") in ("DRAIN", "COMPLETE") and \
        len(st.get("failures") or {}) <= 5
    R.log("G1 %s phase=%s banked=%d catalog=%d failures=%d" % (
        "PASS" if ok1 else "FAIL", st.get("phase"), len(have),
        cat_n, len(st.get("failures") or {})))
    if not ok1:
        fails.append("G1")

    R.section("G2 substance: BOP SDMX payload")
    ok2 = False
    try:
        raw = gzip.decompress(s3.get_object(
            Bucket=B, Key="data/warm/imf-full/src/BOP.xml.gz"
        )["Body"].read())
        ok2 = raw.count(b"<Obs") >= 100 and b"Series" in raw
        R.log("  BOP raw=%.2fMB obs_tags=%d" % (
            len(raw) / 1e6, raw.count(b"<Obs")))
    except Exception as e:
        R.log("  substance err: %s" % str(e)[:100])
    R.log("G2 %s" % ("PASS" if ok2 else "FAIL"))
    if not ok2:
        fails.append("G2")

    R.section("G3 card (post-mark)")
    t_mark = datetime.now(timezone.utc).isoformat(timespec="seconds")
    kick(CAT)
    hub, t0 = {}, time.time()
    while time.time() - t0 < 12 * 60:
        time.sleep(30)
        hub = gj(HUB_KEY) or {}
        if (hub.get("as_of") or "") >= t_mark:
            break
    im = next((p for p in hub.get("providers", [])
               if p.get("slug") == "imf"), {}) or {}
    ok3 = "FULL SDMX-2.1 warehouse" in (im.get("catalog_note")
                                        or "")
    R.log("G3 %s note=%s" % ("PASS" if ok3 else "FAIL",
                             (im.get("catalog_note") or "")[:200]))
    if not ok3:
        fails.append("G3")

    R.section("DAY-TWO board (info)")
    wb = gj("data/warm/worldbank-full/_state/state.json") or {}
    R.log("  worldbank: phase=%s banked=%s q=%s" % (
        wb.get("phase"), wb.get("n_banked") or
        len(wb.get("have") or {}), len(wb.get("queue") or [])))
    gd = gj("data/warm/gdelt-full/_state/state.json") or {}
    R.log("  gdelt: phase=%s files=%s gb=%.2f cursor=%s gaps=%s "
          "v1=%s/%s" % (gd.get("phase"), gd.get("files"),
                        (gd.get("bytes") or 0) / 1e9,
                        str(gd.get("cursor"))[:10], gd.get("gaps"),
                        gd.get("v1_idx", 0), gd.get("v1_total")))
    bl = gj("data/warm/bls-full/manifest.json") or {}
    R.log("  bls: phase=%s files=%s gb=%s" % (
        bl.get("phase"), bl.get("files"), bl.get("gb")))
    dl = gj("data/warm/dol-full/manifest.json") or {}
    R.log("  dol: files=%s mb=%.1f fresh=%s unchanged=%s" % (
        dl.get("files"), (dl.get("bytes") or 0) / 1e6,
        dl.get("fresh"), dl.get("unchanged")))

    if fails:
        R.log("ops 4967 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(phase=st.get("phase"), banked=len(have), catalog=cat_n,
         vintages=sum(1 for v in have.values()
                      if v.get("vintage")),
         failures=len(st.get("failures") or {}))
    R.log("ops 4967 GREEN -- IMF full warehouse draining; daily "
          "rediscovery + weekly redrain own it; original drain "
          "queue resumes next (boe -> coinmetrics -> ...)")
