"""ops_4953 -- src-mirror v1.1: nyfed-research lane (last audit orphan).

The 4943 provider audit left ONE genuine orphan: data/warm/nyfed-
research/ (512 keys, 718MB -- SCE/HHDC/DSGE harvest + tri-party
haircut workbooks) seeded by ops 4757/4758/4759/4793-94 with NO
refresh engine; the board read 161.8h stale because nothing ever
re-touched it. Fix (audit-first, extend-don't-duplicate): a third
lane in justhodl-src-mirror --

  (a) the two tri-party haircut workbooks verbatim (medialibrary
      Interactives endpoints, exact ops-4759 URLs);
  (b) every file in _manifest.json conditionally re-mirrored from its
      recorded source_url -- the 4757/4758 sweep IS the refresh map;
  (c) light re-harvest of the five seed pages appends NEW first-party
      data files to the manifest (<=12/run);
  (d) _last-check.json stamped every run so an unchanged quarterly
      source reads healthy, not stale;
  (e) refresh-orphans.json now engine-owned: both phase-2
      re-transforms (bsrm 500-series, nyfed haircuts-series) stated.

provider-catalog nyfed-note-v2: the ORPHANED_TRANSFORM fossil is
replaced by live composition from the lane's _last-check (census-
note-v2 doctrine).

  G-1 markers exist in checkout (both engines -- trap h)
  G0  settle both zips   G1 Event-invoke src-mirror -> state as_of
      moves, lane present, haircuts fresh/unchanged, sources>=5
  G2  invoke provider-catalog -> nyfed card composed live, fossil
      forbidden, freshest under 2h
  G3  origin hub serves it
"""
import io
import json
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
MIR_FN = "justhodl-src-mirror"
CAT_FN = "justhodl-provider-catalog"
MIR_STATE = "data/_state/src-mirror.json"
LASTCHECK = "data/warm/nyfed-research/_last-check.json"
HUB_KEY = "data/provider-catalog.json"
MARKS = {MIR_FN: ("nyfed-research lane ops 4953",
                  "aws/lambdas/justhodl-src-mirror/source/"
                  "lambda_function.py"),
         CAT_FN: ("nyfed-note-v2",
                  "aws/lambdas/justhodl-provider-catalog/source/"
                  "lambda_function.py")}
UA = "justhodl-ops/4953 (raafouis@gmail.com)"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def gj(key, default=None):
    try:
        return json.loads(
            s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception:
        return default


def kick(fn):
    lam.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")


with report("ops_4953_nyfed_src_mirror_lane") as R:
    fails = []

    # G-1 -- markers exist in the checkout (trap h) --------------------
    R.section("G-1 markers-in-checkout")
    root = Path(__file__).resolve().parents[2]
    for fn, (mk, rel) in MARKS.items():
        src = (root.parent / rel).read_text() if not (
            root / rel).exists() else (root / rel).read_text()
        if mk not in src:
            R.log("ABORT: marker %r absent from %s" % (mk, rel))
            sys.exit(1)
        R.log("  ok %-28s %r" % (fn, mk))

    # G0 -- settle both --------------------------------------------
    R.section("G0 zip-settle both engines")
    for fn, (mk, _rel) in MARKS.items():
        ok, t0 = False, time.time()
        while time.time() - t0 < 600:
            try:
                f = lam.get_function(FunctionName=fn)
                req = urllib.request.Request(f["Code"]["Location"])
                with urllib.request.urlopen(req, timeout=90) as r:
                    zb = r.read()
                src = zipfile.ZipFile(io.BytesIO(zb)).read(
                    "lambda_function.py").decode("utf-8", "replace")
                if mk in src and \
                        f["Configuration"].get("State") == "Active":
                    ok = True
                    R.log("  %s settled after %ds" % (
                        fn, time.time() - t0))
                    break
            except Exception as e:
                R.log("  %s settle: %s" % (fn, str(e)[:100]))
            time.sleep(25)
        if not ok:
            R.log("G0 FAIL %s never landed" % fn)
            sys.exit(1)
    R.log("G0 PASS")

    # G1 -- run the mirror --------------------------------------------
    R.section("G1 src-mirror run (Event + state poll)")
    before = (gj(MIR_STATE) or {}).get("as_of") or ""
    kick(MIR_FN)
    st, ok1, t0 = {}, False, time.time()
    while time.time() - t0 < 16 * 60:
        time.sleep(30)
        st = gj(MIR_STATE) or {}
        if (st.get("as_of") or "") != before and \
                "nyfed-research" in (st.get("summary", {})
                                     .get("lanes") or {}):
            ok1 = True
            break
        R.log("  t+%4ds waiting (as_of=%s)" % (
            time.time() - t0, (st.get("as_of") or "")[:19]))
    lane = ((st.get("summary") or {}).get("lanes") or {}) \
        .get("nyfed-research") or {}
    lc = gj(LASTCHECK) or {}
    hc = lane.get("haircuts") or {}
    hc_ok = len(hc) == 2 and all(
        v.get("status") in ("fresh", "unchanged") or
        str(v.get("status", "")).startswith("err") is False
        for v in hc.values()) and all(
        v.get("status") in ("fresh", "unchanged") for v in hc.values())
    src_n = lc.get("sources") or 0
    err_ok = (lane.get("errors") or 0) <= max(
        3, int(0.3 * (lane.get("mirrored") or 1)))
    ok1 = ok1 and hc_ok and src_n >= 5 and err_ok and \
        lc.get("engine") == "src-mirror"
    R.log("G1 %s lane=%s lastcheck={sources:%s at:%s}" % (
        "PASS" if ok1 else "FAIL",
        json.dumps(lane)[:260], src_n, (lc.get("at") or "")[:19]))
    if not ok1:
        fails.append("G1 mirror")

    # G2 -- catalog card live ------------------------------------------
    R.section("G2 provider-catalog nyfed card")
    cbefore = (gj(HUB_KEY) or {}).get("as_of") or ""
    kick(CAT_FN)
    cat, ok2, t0 = {}, False, time.time()
    while time.time() - t0 < 11 * 60:
        time.sleep(30)
        cat = gj(HUB_KEY) or {}
        if (cat.get("as_of") or "") != cbefore:
            ok2 = True
            break
        R.log("  t+%4ds stamp unchanged" % (time.time() - t0))
    ce = next((p for p in (cat.get("providers") or [])
               if p.get("slug") == "nyfed-research"), {}) or {}
    note = ce.get("catalog_note") or ""
    fresh_h = ce.get("freshest_h")
    ok2 = ok2 and "src-mirror daily since ops 4953" in note and \
        "ORPHANED_TRANSFORM" not in note and \
        (fresh_h is not None and fresh_h < 2.0)
    R.log("G2 %s freshest_h=%s note=%s" % (
        "PASS" if ok2 else "FAIL", fresh_h, note[:220]))
    if not ok2:
        fails.append("G2 card")

    # G3 -- origin -----------------------------------------------------
    R.section("G3 origin")
    try:
        req = urllib.request.Request(
            "https://s3.amazonaws.com/%s/%s" % (B, HUB_KEY),
            headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=45) as r:
            body = r.read().decode("utf-8", "replace")
        ok3 = "src-mirror daily since ops 4953" in body and \
            "ORPHANED_TRANSFORM" not in body
    except Exception as e:
        ok3, body = False, str(e)
    R.log("G3 %s" % ("PASS" if ok3 else "FAIL"))
    if not ok3:
        fails.append("G3 origin")

    if fails:
        R.log("ops 4953 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(sources=src_n, mirrored=lane.get("mirrored"),
         fresh=lane.get("fresh"), unchanged=lane.get("unchanged"),
         new_harvested=lane.get("new_harvested"),
         haircuts=json.dumps(hc)[:160],
         freshest_h=fresh_h)
    R.log("ops 4953 GREEN -- the last audit orphan has a real import "
          "loop; nyfed-research refreshes daily beside the OFR lanes, "
          "phase-2 re-transforms stated in refresh-orphans")
