"""ops_4987 -- Bank of Japan API universe import (Khalid's next).

boj-full v1.1.0 adds the API lane: getMetadata?db=X lists every
series per database; getDataCode pulls full-window values in
40-code batches. Chains are AWS-terminated fleet-wide, so THIS OP
sync-drives to completion.

  P0 shape evidence: getMetadata?db=MD11 + one getDataCode batch
     verbatim heads (parser proof / next-fix fuel)
  G0 settle v1.1.0
  P1 sync drive: invoke {"api_only":1,"budget_s":150} loop up to
     55min or until every discovered db is fully drained
  G2 substance: dbs>=8, series>=1500, one MD11 part parses with
     SURVEY_DATES reaching <=199912
  G3 boj card gains the api counters
"""
import base64
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

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-boj-full"
CAT = "justhodl-provider-catalog"
STATE_KEY = "data/warm/boj-full/_state/state.json"
HUB_KEY = "data/provider-catalog.json"
MARK = "v1.1.1 ops4987"
REL = ("aws/lambdas/justhodl-boj-full/source/lambda_function.py")
ROOTP = Path(__file__).resolve().parents[2]

from botocore.config import Config  # noqa: E402

s3 = boto3.client("s3", region_name=REGION)
# v3: sync-drive invokes run 150-200s server-side; the default
# 60s read timeout killed the run (botocore retry ladder = 403s).
lam = boto3.client(
    "lambda", region_name=REGION,
    config=Config(read_timeout=300, connect_timeout=10,
                  retries={"max_attempts": 0}))


def gj(key, default=None):
    try:
        raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:
        return default


def fetch(url, timeout=45, cap=2_000_000):
    req = urllib.request.Request(
        url, headers={"Accept-Encoding": "identity"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read(cap)


with report("ops_4987_boj_api_universe") as R:
    fails = []
    R.section("P0 shape evidence (runner)")
    try:
        m = fetch("https://www.stat-search.boj.or.jp/api/v1/"
                  "getMetadata?format=json&lang=en&db=MD11")
        R.log("  getMetadata MD11: %dB head=%r" % (
            len(m), m[:220]))
    except Exception as e:
        R.log("  getMetadata err %s" % str(e)[:100])
    try:
        d = fetch("https://www.stat-search.boj.or.jp/api/v1/"
                  "getDataCode?format=json&lang=en&db=MD11"
                  "&startDate=190001&endDate=209912"
                  "&code=DLCLAADBLTTO")
        R.log("  getDataCode full-window: %dB head=%r" % (
            len(d), d[:180]))
    except Exception as e:
        R.log("  getDataCode err %s" % str(e)[:100])

    R.section("G0 settle")
    ok0, t0 = False, time.time()
    while time.time() - t0 < 600:
        try:
            f = lam.get_function(FunctionName=FN)
            req = urllib.request.Request(f["Code"]["Location"])
            with urllib.request.urlopen(req, timeout=90) as r:
                src = zipfile.ZipFile(io.BytesIO(r.read())).read(
                    "lambda_function.py").decode("utf-8",
                                                 "replace")
            if MARK in src:
                ok0 = True
                R.log("  settled (%ds)" % (time.time() - t0))
                break
        except Exception as e:
            R.log("  settle: %s" % str(e)[:80])
        time.sleep(22)
    if not ok0:
        R.log("G0 FAIL")
        sys.exit(1)

    R.section("P0b window ladder (single code)")
    win_ok = None
    for w0, w1, lbl in [("190001", "209912", "full"),
                        ("197001", "198912", "20y"),
                        ("198001", "198912", "10y"),
                        ("198501", "198912", "5y"),
                        ("198801", "198912", "2y")]:
        try:
            d = fetch("https://www.stat-search.boj.or.jp/api/v1/"
                      "getDataCode?format=json&lang=en&db=MD11"
                      "&startDate=%s&endDate=%s"
                      "&code=DLCLAADBLTTO" % (w0, w1),
                      cap=4_000_000)
            hit = b"SURVEY_DATES" in d
            R.log("  %s (%s-%s) -> %dB dates=%s" % (
                lbl, w0, w1, len(d), hit))
            if hit and win_ok is None:
                win_ok = lbl
        except Exception as e:
            R.log("  %s -> %s" % (lbl, str(e)[:70]))
    R.log("  widest working window: %s (engine CHUNK_Y=10)"
          % win_ok)

    R.section("P1 sharded sync drive (6 lanes, 60min)")
    import threading
    st = gj(STATE_KEY) or {}
    dbs_all = sorted(((st.get("api") or {}).get("dbs") or {}))
    if len(dbs_all) < 15:
        try:  # discovery pass only if the map is thin
            lam.invoke(FunctionName=FN,
                       InvocationType="RequestResponse",
                       Payload=json.dumps(
                           {"api_only": 1, "budget_s": 200,
                            "db_filter": []}).encode())
        except Exception as e:
            R.log("  discovery invoke: %s" % str(e)[:90])
        st = gj(STATE_KEY) or {}
        dbs_all = sorted(((st.get("api") or {})
                          .get("dbs") or {}))
    R.log("  dbs=%d %s" % (len(dbs_all), dbs_all))
    shards = [dbs_all[i::6] for i in range(6)]
    t0 = time.time()
    stop = t0 + 60 * 60

    def lane(dblist):
        idle = 0
        while time.time() < stop:
            try:
                r_ = lam.invoke(
                    FunctionName=FN,
                    InvocationType="RequestResponse",
                    Payload=json.dumps(
                        {"api_only": 1, "budget_s": 150,
                         "db_filter": dblist}).encode())
                pl = json.loads(r_["Payload"].read() or b"{}")
                res = pl.get("res") or {}
                if res and all(d >= t for d, t, _ in
                               res.values()):
                    return
                idle = 0
            except Exception:
                idle += 1
                if idle >= 6:
                    return
                time.sleep(8)

    th = [threading.Thread(target=lane, args=(sh,), daemon=True)
          for sh in shards if sh]
    for x in th:
        x.start()
    while time.time() < stop:
        time.sleep(45)
        tot_done = tot_all = tot_parts = 0
        for db in dbs_all:
            ds = gj("data/warm/boj-full/_state/api_%s.json"
                    % db) or {}
            tot_done += ds.get("done", 0)
            tot_all += len(ds.get("codes") or [])
            tot_parts += ds.get("parts", 0)
        R.log("  t+%4ds series %d/%d parts=%d live=%d" % (
            time.time() - t0, tot_done, tot_all, tot_parts,
            sum(1 for x in th if x.is_alive())))
        if tot_all and tot_done >= tot_all:
            break
        if not any(x.is_alive() for x in th):
            break
    tot_done = tot_all = tot_parts = 0
    lagg = []
    for db in dbs_all:
        ds = gj("data/warm/boj-full/_state/api_%s.json"
                % db) or {}
        d_, a_ = ds.get("done", 0), len(ds.get("codes") or [])
        tot_done += d_
        tot_all += a_
        tot_parts += ds.get("parts", 0)
        if d_ < a_:
            lagg.append("%s %d/%d" % (db, d_, a_))
        if ds.get("fail"):
            R.log("    %s fail: %s" % (db, ds["fail"]))
    R.log("  remainder: %s" % (lagg or "none"))
    ok1 = len(dbs_all) >= 8 and tot_all >= 1500 and \
        tot_parts >= 100
    R.log("P1 %s dbs=%d series=%d/%d parts=%d" % (
        "PASS" if ok1 else "FAIL", len(dbs_all), tot_done,
        tot_all, tot_parts))
    if not ok1:
        fails.append("P1")
    if lagg:
        fails.append("REMAINDER(%d dbs)" % len(lagg))

    R.section("G2 substance")
    ok2 = False
    try:
        r_ = s3.list_objects_v2(
            Bucket=B, Prefix="data/warm/boj-full/api/MD11/",
            MaxKeys=2)
        k0 = (r_.get("Contents") or [{}])[0].get("Key")
        js = json.loads(gzip.decompress(s3.get_object(
            Bucket=B, Key=k0)["Body"].read()))
        blob = json.dumps(js)
        import re as _re
        yrs = sorted(set(_re.findall(r'"(19\d\d|20\d\d)', blob)))
        n_series = blob.count("SURVEY_DATES")
        ok2 = n_series >= 10 and bool(yrs) and yrs[0] <= "1999"
        R.log("  %s series=%d yr-span %s..%s" % (
            k0.rsplit("/", 1)[-1], n_series,
            yrs[0] if yrs else None,
            yrs[-1] if yrs else None))
    except Exception as e:
        R.log("  substance err %s" % str(e)[:110])
    R.log("G2 %s" % ("PASS" if ok2 else "FAIL"))
    if not ok2:
        fails.append("G2")

    R.section("G3 card")
    t_mark = datetime.now(timezone.utc).isoformat(
        timespec="seconds")
    try:
        lam.invoke(FunctionName=CAT, InvocationType="Event",
                   Payload=b"{}")
    except Exception:
        pass
    hub, t0 = {}, time.time()
    while time.time() - t0 < 10 * 60:
        time.sleep(30)
        hub = gj(HUB_KEY) or {}
        if (hub.get("as_of") or "") >= t_mark:
            break
    be = next((p for p in hub.get("providers", [])
               if p.get("slug") == "boj"), {}) or {}
    note = be.get("catalog_note") or ""
    R.log("G3 note=%s" % note[:180])
    if "API universe" not in note and "api" not in note.lower():
        fails.append("G3")

    if fails:
        R.log("ops 4987 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(dbs=len(dbs), series=total,
         parts=sum(m.get("parts", 0) for m in dbs.values()))
    R.log("ops 4987 GREEN -- the Bank of Japan series universe "
          "is banked full-window; 24h schedule keeps it fresh")
