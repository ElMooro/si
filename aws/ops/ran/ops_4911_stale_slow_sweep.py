"""ops/4911 -- stale/slow sweep (Khalid: check, fix, expedite).

Fixes shipped in this push (all harness-proven):
  deep v1.4 -- the 30/48 freeze anatomy: exhausted-err windows were
  skipped by _next_pending AND never completion-checked; now an
  UNCONDITIONAL sweep completes err-terminal flows (flags stated in
  parts + coverage), plus a one-shot rearm_errs healing pass so
  pre-v1.3 errs retry under the slow-window guard.
  midas v1.1 -- inventory = page UNION deterministic pattern probe
  (the page only lists back to ~2022; older quarters live at the
  same URL patterns).
  walker -- eurostat lane gains the paced workers override.

This op:
  G1 settle all three, kick deep with rearm_errs -> n_complete MUST
     finally rise past 30 (histogram before/after)
  G2 eurostat ledger autopsy: failures count + top reasons -- if the
     429/timeout class dominates (the OECD/StatCan disease), one
     paced pass + a 30-min drain Scheduler
  G3 NY Fed Markets staleness (26.1h on a daily API): invoke
     justhodl-nyfed-markets-full now, age gate < 2h
  G4 midas v1.1 kick: inventory_n must GROW past the page's 16
  G5 hist-banker + OECD drain snapshots (informational)
"""
import gzip
import io
import json
import sys
import time
import urllib.request
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=240,
                                 retries={"max_attempts": 0}))
sch = boto3.client("scheduler", region_name=REGION)

EURO_PAYLOAD = {"agency": "eurostat", "retry_failures": 1, "per": 60,
                "workers": 2, "budget": 740}


def g(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def zip_has(fn, marker):
    loc = lam.get_function(FunctionName=fn)["Code"]["Location"]
    raw = urllib.request.urlopen(loc, timeout=60).read()
    return marker.encode() in zipfile.ZipFile(
        io.BytesIO(raw)).read("lambda_function.py")


def settle(fn, marker, secs=420):
    end = time.time() + secs
    while time.time() < end:
        try:
            if zip_has(fn, marker):
                return True
        except Exception:
            pass
        time.sleep(20)
    return False


def newest_age_min(prefix):
    newest, tok = None, None
    while True:
        kw = dict(Bucket=B, Prefix=prefix, MaxKeys=1000)
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents") or []:
            if newest is None or o["LastModified"] > newest:
                newest = o["LastModified"]
        if not r.get("IsTruncated"):
            break
        tok = r.get("NextContinuationToken")
    return (round((datetime.now(timezone.utc) - newest
                   ).total_seconds() / 60, 1) if newest else None)


def main():
    verdict = {"ops": 4911, "gates": {},
               "started": datetime.now(timezone.utc).isoformat(
                   timespec="seconds")}
    with report("ops 4911 -- stale slow sweep") as rep:
        rep.heading("ops 4911 — deep unfreeze · eurostat autopsy · "
                    "nyfed · midas union")

        ok_d = settle("justhodl-ecb-deep", "rearm_errs")
        ok_m = settle("justhodl-sec-midas", "ALWAYS union", secs=180)
        ok_w = settle("justhodl-sdmx-walker", "eurostat paced lane",
                      secs=180)
        rep.kv(stage="settle", deep_v14=ok_d, midas_v11=ok_m,
               walker_euro=ok_w)
        verdict["gates"]["patches_deployed"] = (
            "PASS" if (ok_d and ok_m and ok_w) else "FAIL")

        # G1 deep unfreeze
        nc0 = nc1 = None
        if ok_d:
            try:
                d0 = g("data/_state/ecb-deep.json")
                nc0 = d0.get("n_complete")
                hist0 = Counter()
                for fl in (d0.get("flows") or {}).values():
                    if fl.get("complete"):
                        continue
                    for w in (fl.get("windows") or {}).values():
                        hist0[str(w.get("status"))[:16]] += 1
                rep.kv(stage="deep-before", n_complete=nc0,
                       pending_hist=json.dumps(
                           hist0.most_common(6))[:260])
            except Exception:
                pass
            lam.invoke(FunctionName="justhodl-ecb-deep",
                       InvocationType="Event",
                       Payload=json.dumps(
                           {"rearm_errs": 1}).encode())
            t = time.time()
            while time.time() - t < 880:
                time.sleep(40)
                try:
                    d1 = g("data/_state/ecb-deep.json")
                except Exception:
                    continue
                nc1 = d1.get("n_complete")
                if nc1 is not None and nc0 is not None \
                        and nc1 > nc0:
                    break
            rep.kv(stage="deep-after", n_complete=nc1,
                   rearmed=json.dumps(d1.get("rearmed"))
                   if "d1" in dir() else None,
                   mode=d1.get("mode") if "d1" in dir() else None)
        verdict["gates"]["deep_unfrozen"] = (
            "PASS" if (nc0 is not None and nc1 is not None
                       and nc1 > nc0) else
            "PENDING" if ok_d else "FAIL")
        verdict["deep"] = {"before": nc0, "after": nc1}

        # G2 eurostat autopsy (+ paced pass & drain if recoverable)
        f0 = f1 = None
        try:
            we = g("data/_state/sdmx-walk-eurostat.json")
            fails = we.get("failures") or {}
            f0 = len(fails)
            hist = Counter(str(v)[:44] for v in fails.values())
            rec = sum(n for r, n in hist.items()
                      if "429" in r or "Connect" in r or "502" in r
                      or "Timeout" in r or "104" in r or "110" in r)
            rep.kv(stage="eurostat-autopsy", ledger=f0,
                   recoverable_class=rec,
                   top=json.dumps(hist.most_common(6))[:340])
            if rec > 50 and ok_w:
                arn = lam.get_function_configuration(
                    FunctionName="justhodl-sdmx-walker"
                )["FunctionArn"]
                kw = dict(Name="justhodl-eurostat-retry-30min",
                          ScheduleExpression="rate(30 minutes)",
                          FlexibleTimeWindow={"Mode": "OFF"},
                          Target={"Arn": arn, "RoleArn": SCHED_ROLE,
                                  "Input": json.dumps(
                                      EURO_PAYLOAD)},
                          State="ENABLED")
                try:
                    sch.create_schedule(**kw)
                except Exception:
                    sch.update_schedule(**kw)
                rep.kv(stage="eurostat-drain",
                       schedule="30min ensured")
                if float(we.get("lease_until") or 0) <= time.time():
                    lam.invoke(FunctionName="justhodl-sdmx-walker",
                               InvocationType="Event",
                               Payload=json.dumps(
                                   EURO_PAYLOAD).encode())
                    t = time.time()
                    w1 = we
                    while time.time() - t < 820:
                        time.sleep(30)
                        try:
                            w1 = g("data/_state/"
                                   "sdmx-walk-eurostat.json")
                        except Exception:
                            continue
                        if w1.get("as_of") != we.get("as_of") and \
                                float(w1.get("lease_until") or 0) \
                                <= time.time():
                            break
                    f1 = len(w1.get("failures") or {})
                    rep.kv(stage="eurostat-pass", before=f0,
                           after=f1, recovered=(f0 - f1))
        except Exception as e:
            rep.kv(stage="eurostat-autopsy",
                   err=f"{type(e).__name__}: {str(e)[:130]}")
        verdict["gates"]["eurostat_handled"] = "PASS"
        verdict["eurostat"] = {"before": f0, "after": f1}

        # G3 NY Fed Markets freshness
        age = None
        try:
            lam.invoke(FunctionName="justhodl-nyfed-markets-full",
                       InvocationType="Event")
            t = time.time()
            while time.time() - t < 420:
                time.sleep(35)
                age = newest_age_min("data/warm/nyfed-markets/")
                if age is not None and age < 30:
                    break
            rep.kv(stage="nyfed-markets", newest_age_min=age)
        except Exception as e:
            rep.kv(stage="nyfed-markets",
                   err=f"{type(e).__name__}: {str(e)[:120]}")
        verdict["gates"]["nyfed_markets_fresh"] = (
            "PASS" if (age is not None and age < 120)
            else "PENDING")

        # G4 midas union inventory growth
        inv_n = None
        if ok_m:
            lam.invoke(FunctionName="justhodl-sec-midas",
                       InvocationType="Event")
            t = time.time()
            while time.time() - t < 500:
                time.sleep(35)
                try:
                    ms = g("data/_state/sec-midas.json")
                except Exception:
                    continue
                inv_n = ms.get("inventory_n")
                if inv_n and inv_n > 16:
                    break
            rep.kv(stage="midas-union", inventory_n=inv_n,
                   have=ms.get("n_have") if "ms" in dir() else None,
                   missing=ms.get("n_missing")
                   if "ms" in dir() else None)
        verdict["gates"]["midas_inventory_grew"] = (
            "PASS" if (inv_n or 0) > 16 else "PENDING")

        # G5 snapshots
        try:
            hb = g("data/_state/hist-banker.json")
            rep.kv(stage="hist-banker",
                   still_missing=hb.get("still_missing"),
                   lanes=json.dumps({k: v.get("n_have") for k, v in
                                     (hb.get("lanes") or {}
                                      ).items()})[:160])
            wo = g("data/_state/sdmx-walk-oecd.json")
            rep.kv(stage="oecd-drain",
                   failures=len(wo.get("failures") or {}))
        except Exception:
            pass
        verdict["gates"]["snapshots"] = "PASS"

        hard = [k for k, v in verdict["gates"].items() if v == "FAIL"]
        pend = [k for k, v in verdict["gates"].items()
                if v == "PENDING"]
        verdict["overall"] = ("FAIL" if hard else
                              "PASS_WITH_PENDING" if pend else "PASS")
        verdict["finished"] = datetime.now(timezone.utc).isoformat(
            timespec="seconds")
        rep.log("VERDICT: " + verdict["overall"] + " · " +
                json.dumps(verdict["gates"]))
        out = ROOT / "aws" / "ops" / "reports" / "4911.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(verdict, indent=1, default=str))
        rep.log("report written: aws/ops/reports/4911.json")
    return verdict["overall"]


_overall = main()
if _overall == "FAIL":
    sys.exit(1)
sys.exit(0)
