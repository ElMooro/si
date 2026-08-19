"""ops/4905 -- deep autopsy (CloudWatch) + MIDAS second hop.

4904 hard symptom: deep parts frozen at 237, lease FREE, mode
backfill, CSEC holding 194 pending windows, n_complete stuck at 30
for ~80 min despite the 10-min Scheduler + v1.2 chain + a manual
kick. The missing eye is the LOG: tail /aws/lambda/justhodl-ecb-deep
for the last 45 min -- errors, chain lines, run summaries -- the
definitive evidence. Also recheck parts + newest-part age after the
4904 kick, and if a trivially fixable bug shows, it ships next op.

MIDAS: 4904 proved SEC's new Drupal IA; the datasets live behind
/data-research/sec-markets-data and
/featured-topics/market-structure-analytics -- harvest THOSE for
.zip/.csv/download hrefs (second hop, verbatim evidence).
"""
import gzip
import json
import re
import sys
import time
import urllib.error
import urllib.request
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
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=240,
                                 retries={"max_attempts": 0}))
logs = boto3.client("logs", region_name=REGION)


def g(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def http(url, nb=1500000):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=45) as r:
            return r.status, r.read(nb)
    except urllib.error.HTTPError as e:
        return e.code, b""
    except Exception as e:
        return type(e).__name__, b""


def parts_and_newest():
    n, newest, tok = 0, None, None
    while True:
        kw = dict(Bucket=B, Prefix="data/warm/ecb/data/",
                  MaxKeys=1000)
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents") or []:
            if "__" in o["Key"].rsplit("/", 1)[-1]:
                n += 1
                if newest is None or o["LastModified"] > newest:
                    newest = o["LastModified"]
        if not r.get("IsTruncated"):
            return n, newest
        tok = r.get("NextContinuationToken")


def main():
    verdict = {"ops": 4905, "gates": {},
               "started": datetime.now(timezone.utc).isoformat(
                   timespec="seconds")}
    with report("ops 4905 -- deep autopsy midas hop2") as rep:
        rep.heading("ops 4905 — deep CloudWatch autopsy · MIDAS hop2")

        # ── deep: parts + newest age + state ────────────────────────
        p0, newest = parts_and_newest()
        age_min = (round((datetime.now(timezone.utc) - newest
                          ).total_seconds() / 60, 1)
                   if newest else None)
        st = {}
        try:
            st = g("data/_state/ecb-deep.json")
        except Exception:
            pass
        rep.kv(stage="deep-now", parts=p0, newest_part_age_min=age_min,
               as_of=st.get("as_of"), mode=st.get("mode"),
               n_complete=st.get("n_complete"),
               lease_free=float(st.get("lease_until") or 0)
               <= time.time())

        # ── CloudWatch autopsy: last 50 min ─────────────────────────
        lines = []
        try:
            resp = logs.filter_log_events(
                logGroupName="/aws/lambda/justhodl-ecb-deep",
                startTime=int((time.time() - 3000) * 1000),
                limit=200)
            evs = resp.get("events") or []
            # keep errors, tracebacks, summaries, chain lines
            keep = re.compile(r"error|Error|Task timed|REPORT|"
                              r"chain|Traceback|coverage|\"ok\"",
                              re.I)
            for e in evs:
                m = (e.get("message") or "").strip()
                if keep.search(m):
                    lines.append(m[:220])
            rep.kv(stage="autopsy", events_scanned=len(evs),
                   kept=len(lines))
            for i, ln in enumerate(lines[:14]):
                rep.log(f"LOG[{i}] {ln}")
        except Exception as e:
            rep.kv(stage="autopsy", ok=False,
                   err=f"{type(e).__name__}: {str(e)[:150]}")
        verdict["deep_log_sample"] = lines[:14]
        verdict["gates"]["autopsy_captured"] = (
            "PASS" if lines else "PENDING")

        # if truly idle, kick and measure 4 min
        moving = None
        if age_min is not None and age_min > 12 and \
                float(st.get("lease_until") or 0) <= time.time():
            lam.invoke(FunctionName="justhodl-ecb-deep",
                       InvocationType="Event")
            time.sleep(240)
            p1, newest1 = parts_and_newest()
            moving = p1 > p0
            rep.kv(stage="kick-check", parts_after=p1,
                   moved=moving)
        verdict["gates"]["deep_status"] = (
            "PASS" if (age_min is not None and age_min <= 12)
            or moving else "PENDING")
        verdict["deep"] = {"parts": p0, "newest_age_min": age_min,
                           "moved_after_kick": moving}

        # ── MIDAS hop 2 ─────────────────────────────────────────────
        found = {}
        for label, page in (
                ("sec-markets-data",
                 "https://www.sec.gov/data-research/"
                 "sec-markets-data"),
                ("mstr-analytics",
                 "https://www.sec.gov/featured-topics/"
                 "market-structure-analytics")):
            st_, bb = http(page)
            row = {"status": st_}
            if isinstance(st_, int) and st_ == 200:
                txt = bb.decode("utf-8", "ignore")
                row["files"] = list(dict.fromkeys(re.findall(
                    r'href="([^"]+\.(?:zip|csv|xlsx))"', txt,
                    re.I)))[:12]
                row["next_hops"] = list(dict.fromkeys(
                    re.findall(r'href="(/[^"]*(?:market-structure|'
                               r'metrics|midas)[^"]*)"', txt,
                               re.I)))[:10]
            found[label] = row
            rep.kv(stage="midas-hop2", page=label,
                   **{k: (json.dumps(v)[:340]
                          if isinstance(v, list) else v)
                      for k, v in row.items()})
            time.sleep(0.5)
        any_files = any((found.get(k) or {}).get("files")
                        for k in found)
        verdict["gates"]["midas_hop2"] = ("PASS" if any_files
                                          else "PENDING")
        verdict["midas_hop2"] = found

        hard = [k for k, v in verdict["gates"].items() if v == "FAIL"]
        pend = [k for k, v in verdict["gates"].items()
                if v == "PENDING"]
        verdict["overall"] = ("FAIL" if hard else
                              "PASS_WITH_PENDING" if pend else "PASS")
        verdict["finished"] = datetime.now(timezone.utc).isoformat(
            timespec="seconds")
        rep.log("VERDICT: " + verdict["overall"] + " · " +
                json.dumps(verdict["gates"]))
        out = ROOT / "aws" / "ops" / "reports" / "4905.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(verdict, indent=1, default=str))
        rep.log("report written: aws/ops/reports/4905.json")
    return verdict["overall"]


_overall = main()
if _overall == "FAIL":
    sys.exit(1)
sys.exit(0)
