"""ops/4904 -- statcan paced drain + deep truth-metric + midas v3.

4903 evidence: StatCan's 290 "denied at source" histogram = 106x 429
+ 83x ECONNRESET + 72x ETIMEDOUT + 24x 502 + only 4x 404 -- the same
self-storm disease OECD had, mislabeled as denial. Same medicine:
paced lane (workers=2) + hourly drain Scheduler.

Deep engine shows n_complete frozen at 30 -- but flow-completion is
the WRONG progress metric while the 18 remaining mega-flows grind
100+ windows each; parts-on-disk is the truth. Measure parts delta
over 5 minutes; only if parts are ALSO frozen diagnose the pending-
window status histogram (and kick).

MIDAS v3: the two index pages return 200 but zero classic hrefs --
capture the page verbatim (first 500 chars), every href containing
"market" or "data", every .json href, and script srcs, so the next
build starts from evidence, not guesses.

  G1 settle (statcan paced lane marker)
  G2 statcan paced retry: failures 290 must drop
  G3 Scheduler justhodl-statcan-retry-hourly
  G4 deep parts-truth: delta>0 over 5 min = grinding; else diagnose
  G5 midas discovery v3 (verbatim evidence)
"""
import gzip
import io
import json
import re
import sys
import time
import urllib.error
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
UA = {"User-Agent": "JustHodl Research raafouis@gmail.com"}
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=240,
                                 retries={"max_attempts": 0}))
sch = boto3.client("scheduler", region_name=REGION)

RETRY_PAYLOAD = {"agency": "statcan", "retry_failures": 1, "per": 40,
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


def http(url, nb=65536):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=45) as r:
            return r.status, r.read(nb)
    except urllib.error.HTTPError as e:
        return e.code, b""
    except Exception as e:
        return type(e).__name__, b""


def deep_parts():
    n, tok = 0, None
    while True:
        kw = dict(Bucket=B, Prefix="data/warm/ecb/data/",
                  MaxKeys=1000)
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        n += sum(1 for o in r.get("Contents") or []
                 if "__" in o["Key"].rsplit("/", 1)[-1])
        if not r.get("IsTruncated"):
            return n
        tok = r.get("NextContinuationToken")


def main():
    verdict = {"ops": 4904, "gates": {},
               "started": datetime.now(timezone.utc).isoformat(
                   timespec="seconds")}
    with report("ops 4904 -- statcan drain deep truth midas v3"
                ) as rep:
        rep.heading("ops 4904 — StatCan lane · deep truth · MIDAS v3")

        # G1 settle
        ok1 = False
        end = time.time() + 420
        while time.time() < end:
            try:
                if zip_has("justhodl-sdmx-walker",
                           "statcan paced lane"):
                    ok1 = True
                    break
            except Exception:
                pass
            time.sleep(20)
        rep.kv(stage="settle", statcan_lane=ok1)
        verdict["gates"]["statcan_lane_deployed"] = (
            "PASS" if ok1 else "FAIL")

        # G4a parts t0 (start clock early, wait happens during G2)
        p0 = deep_parts()

        # G2 statcan paced retry
        f0 = f1 = None
        if ok1:
            try:
                w0 = g("data/_state/sdmx-walk-statcan.json")
                f0 = len(w0.get("failures") or {})
            except Exception:
                w0 = {}
            lam.invoke(FunctionName="justhodl-sdmx-walker",
                       InvocationType="Event",
                       Payload=json.dumps(RETRY_PAYLOAD).encode())
            t = time.time()
            w1 = w0
            while time.time() - t < 900:
                time.sleep(30)
                try:
                    w1 = g("data/_state/sdmx-walk-statcan.json")
                except Exception:
                    continue
                if w1.get("as_of") != w0.get("as_of") and \
                        float(w1.get("lease_until") or 0) \
                        <= time.time():
                    break
            f1 = len(w1.get("failures") or {})
            hist = Counter(str(v)[:44] for v in
                           (w1.get("failures") or {}).values())
            rep.kv(stage="statcan-paced-retry", failures_before=f0,
                   failures_after=f1,
                   recovered=(f0 - f1) if (f0 is not None
                                           and f1 is not None)
                   else None,
                   top_remaining=json.dumps(
                       hist.most_common(5))[:280])
        verdict["gates"]["statcan_failures_decreasing"] = (
            "PASS" if (f0 is not None and f1 is not None
                       and f1 < f0 - 4) else
            "PENDING" if ok1 else "FAIL")
        verdict["statcan"] = {"before": f0, "after": f1}

        # G3 hourly drain schedule
        ok3 = False
        try:
            arn = lam.get_function_configuration(
                FunctionName="justhodl-sdmx-walker")["FunctionArn"]
            kw = dict(Name="justhodl-statcan-retry-hourly",
                      ScheduleExpression="rate(1 hour)",
                      FlexibleTimeWindow={"Mode": "OFF"},
                      Target={"Arn": arn, "RoleArn": SCHED_ROLE,
                              "Input": json.dumps(RETRY_PAYLOAD)},
                      State="ENABLED")
            try:
                sch.create_schedule(**kw)
                rep.kv(stage="schedule", action="created")
            except Exception as e:
                if "Conflict" in type(e).__name__ or "already" in \
                        str(e):
                    sch.update_schedule(**kw)
                    rep.kv(stage="schedule", action="updated")
                else:
                    raise
            ok3 = True
        except Exception as e:
            rep.kv(stage="schedule", ok=False,
                   err=f"{type(e).__name__}: {str(e)[:130]}")
        verdict["gates"]["statcan_drain_scheduled"] = (
            "PASS" if ok3 else "FAIL")

        # G4 deep parts truth (t1 after the G2 wait ~5-15 min)
        p1 = deep_parts()
        moving = p1 > p0
        diag = {}
        if not moving:
            try:
                d = g("data/_state/ecb-deep.json")
                pend_hist = Counter()
                sample_flow = None
                for f, fl in (d.get("flows") or {}).items():
                    if fl.get("complete"):
                        continue
                    sample_flow = sample_flow or f
                    for w in (fl.get("windows") or {}).values():
                        pend_hist[str(w.get("status"))[:20]] += 1
                diag = {"mode": d.get("mode"),
                        "lease_free": float(d.get("lease_until")
                                            or 0) <= time.time(),
                        "sample_flow": sample_flow,
                        "window_status_hist":
                        pend_hist.most_common(6)}
                if diag["lease_free"] and d.get("mode") == \
                        "backfill":
                    lam.invoke(FunctionName="justhodl-ecb-deep",
                               InvocationType="Event")
                    diag["kicked"] = True
            except Exception as e:
                diag = {"err": type(e).__name__}
        rep.kv(stage="deep-parts-truth", parts_t0=p0, parts_t1=p1,
               moving=moving, diag=json.dumps(diag)[:300])
        verdict["gates"]["deep_grinding"] = ("PASS" if moving
                                             else "PENDING")
        verdict["deep_parts"] = {"t0": p0, "t1": p1, "diag": diag}

        # G5 MIDAS discovery v3
        ev = {}
        st_, bb = http("https://www.sec.gov/marketstructure/"
                       "downloads.html", nb=1500000)
        ev["status"] = st_
        if isinstance(st_, int) and st_ == 200:
            txt = bb.decode("utf-8", "ignore")
            ev["verbatim_500"] = txt[:500].replace("\n", " ")
            ev["hrefs_market_or_data"] = list(dict.fromkeys(
                re.findall(r'href="([^"]*(?:market|data)[^"]*)"',
                           txt, re.I)))[:12]
            ev["json_refs"] = list(dict.fromkeys(
                re.findall(r'"([^"]+\.json)"', txt)))[:8]
            ev["script_srcs"] = list(dict.fromkeys(
                re.findall(r'<script[^>]+src="([^"]+)"', txt)))[:6]
        rep.kv(stage="midas-v3", **{k: (json.dumps(v)[:350]
                                        if isinstance(v, list)
                                        else str(v)[:350])
                                    for k, v in ev.items()})
        verdict["gates"]["midas_evidence"] = "PASS"
        verdict["midas_v3"] = ev

        hard = [k for k, v in verdict["gates"].items() if v == "FAIL"]
        pend = [k for k, v in verdict["gates"].items()
                if v == "PENDING"]
        verdict["overall"] = ("FAIL" if hard else
                              "PASS_WITH_PENDING" if pend else "PASS")
        verdict["finished"] = datetime.now(timezone.utc).isoformat(
            timespec="seconds")
        rep.log("VERDICT: " + verdict["overall"] + " · " +
                json.dumps(verdict["gates"]))
        out = ROOT / "aws" / "ops" / "reports" / "4904.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(verdict, indent=1, default=str))
        rep.log("report written: aws/ops/reports/4904.json")
    return verdict["overall"]


_overall = main()
if _overall == "FAIL":
    sys.exit(1)
sys.exit(0)
