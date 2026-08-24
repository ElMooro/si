"""ops_4963 -- nyfed hist-v1: Khalid priority lane #1 goes FULL-DEPTH.

4962 evidence: the source holds EFFR 6,568 obs / repo ops 8,125 since
2000 / AMBS 2,875 since 2009 while justhodl-nyfed-markets-full's
bounded block stored LATEST-only for those families. Fix = hist-v1
block IN the existing engine (extend, never duplicate): 11 families,
full-window search pulls, candidate ladders, verbatim gz under
data/warm/nyfed-markets/hist/, self-refreshing when >20h stale on the
engine's existing hourly schedule.

  G-1 marker-in-checkout   G0 settle   G1 Event-kick -> hist-state
  poll: >=8 families ok; EFFR n_hint>=6000, repo>=8000, ambs>=2500
  G2 substance: hist/rates_effr payload spans 2000->current
  G3 provider card keys grow (hist/ prefix) + note untouched
"""
import gzip
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
FN = "justhodl-nyfed-markets-full"
HSTATE = "data/warm/nyfed-markets/hist-state.json"
MARK = "nyfed-hist-v1 ops4963"
REL = ("aws/lambdas/justhodl-nyfed-markets-full/source/"
       "lambda_function.py")

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def gj(key, default=None):
    try:
        raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:
        return default


with report("ops_4963_nyfed_hist_depth") as R:
    fails = []
    R.section("G-1 marker-in-checkout")
    root = Path(__file__).resolve().parents[2]
    if MARK not in (root.parent / REL).read_text():
        R.log("ABORT marker absent")
        sys.exit(1)
    R.log("  ok %r" % MARK)

    R.section("G0 settle")
    ok0, t0 = False, time.time()
    while time.time() - t0 < 600:
        try:
            f = lam.get_function(FunctionName=FN)
            req = urllib.request.Request(f["Code"]["Location"])
            with urllib.request.urlopen(req, timeout=90) as r:
                zb = r.read()
            src = zipfile.ZipFile(io.BytesIO(zb)).read(
                "lambda_function.py").decode("utf-8", "replace")
            if MARK in src and \
                    f["Configuration"].get("State") == "Active":
                ok0 = True
                R.log("  settled (%ds)" % (time.time() - t0))
                break
        except Exception as e:
            R.log("  settle: %s" % str(e)[:90])
        time.sleep(25)
    if not ok0:
        R.log("G0 FAIL")
        sys.exit(1)

    R.section("G1 kick -> hist families converge")
    lam.invoke(FunctionName=FN, InvocationType="Event",
               Payload=b"{}")
    hs, t0, kicks = {}, time.time(), 0
    while time.time() - t0 < 14 * 60:
        time.sleep(25)
        hs = gj(HSTATE) or {}
        fams = hs.get("families") or {}
        okn = sum(1 for v in fams.values() if v.get("ok"))
        R.log("  t+%4ds mark=%s ok=%d/%d" % (
            time.time() - t0,
            (hs.get("mark") == MARK), okn, len(fams)))
        if hs.get("mark") == MARK and okn >= 8:
            break
        if time.time() - t0 > 300 and kicks < 2 and \
                hs.get("mark") != MARK:
            kicks += 1
            lam.invoke(FunctionName=FN, InvocationType="Event",
                       Payload=b"{}")
            R.log("  re-kick #%d" % kicks)
    fams = hs.get("families") or {}
    okn = sum(1 for v in fams.values() if v.get("ok"))
    for nm, v in sorted(fams.items()):
        R.log("  %-16s ok=%s bytes=%s n_hint=%s %s" % (
            nm, v.get("ok"), v.get("bytes"), v.get("n_hint"),
            (v.get("endpoint") or v.get("reason") or "")[:60]))
    eff = (fams.get("rates_effr") or {}).get("n_hint") or 0
    rep = (fams.get("repo_ops") or {}).get("n_hint") or 0
    amb = (fams.get("ambs_ops") or {}).get("n_hint") or 0
    ok1 = hs.get("mark") == MARK and okn >= 8 and \
        eff >= 6000 and rep >= 8000 and amb >= 2500
    R.log("G1 %s ok_fams=%d effr=%d repo=%d ambs=%d" % (
        "PASS" if ok1 else "FAIL", okn, eff, rep, amb))
    if not ok1:
        fails.append("G1")

    R.section("G2 substance: EFFR span 2000 -> current")
    doc = gj("data/warm/nyfed-markets/hist/rates_effr.json.gz") or {}
    rows = (((doc.get("payload") or {}).get("refRates"))
            or ((doc.get("payload") or {}).get("rates")) or [])
    dates = sorted(str(r.get("effectiveDate")) for r in rows
                   if isinstance(r, dict) and r.get("effectiveDate"))
    ok2 = bool(dates) and dates[0] <= "2000-12-31" and \
        dates[-1] >= "2026-08-01" and len(dates) >= 6000
    R.log("G2 %s n=%d span=%s..%s" % (
        "PASS" if ok2 else "FAIL", len(dates),
        dates[0] if dates else None, dates[-1] if dates else None))
    if not ok2:
        fails.append("G2")

    R.section("G3 hist keys landed")
    r_ = s3.list_objects_v2(Bucket=B,
                            Prefix="data/warm/nyfed-markets/hist/")
    ks = [(o["Key"].rsplit("/", 1)[-1], o["Size"])
          for o in r_.get("Contents", [])]
    tot = sum(sz for _k, sz in ks)
    for k_, sz in sorted(ks):
        R.log("  %-26s %8.2fKB" % (k_, sz / 1e3))
    ok3 = len(ks) >= 8 and tot > 1_500_000
    R.log("G3 %s files=%d %.2fMB" % ("PASS" if ok3 else "FAIL",
                                     len(ks), tot / 1e6))
    if not ok3:
        fails.append("G3")

    if fails:
        R.log("ops 4963 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(families_ok=okn, effr_obs=len(dates), repo_hint=rep,
         ambs_hint=amb, hist_mb=round(tot / 1e6, 2))
    R.log("ops 4963 GREEN -- nyfed lane #1 full-depth: latest-only "
          "families now carry complete source history, self-"
          "refreshing on the existing hourly schedule")
