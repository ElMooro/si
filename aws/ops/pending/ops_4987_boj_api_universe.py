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
MARK = "v1.1.0 ops4987"
REL = ("aws/lambdas/justhodl-boj-full/source/lambda_function.py")
ROOTP = Path(__file__).resolve().parents[2]

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

    R.section("P1 sync drive")
    t0 = time.time()
    st = {}
    while time.time() - t0 < 55 * 60:
        try:
            resp = lam.invoke(
                FunctionName=FN,
                InvocationType="RequestResponse",
                Payload=json.dumps({"api_only": 1,
                                    "budget_s": 150}).encode())
            body = resp["Payload"].read().decode("utf-8",
                                                 "replace")
            fe = resp.get("FunctionError")
        except Exception as e:
            fe, body = "invoke", str(e)[:120]
        st = gj(STATE_KEY) or {}
        ap = st.get("api") or {}
        dbs = ap.get("dbs") or {}
        total = sum(len(m.get("codes") or [])
                    for m in dbs.values())
        done = sum(m.get("done", 0) for m in dbs.values())
        R.log("  t+%4ds err=%s dbs=%d inv=%d series %d/%d "
              "parts=%d" % (
                  time.time() - t0, fe, len(dbs),
                  len(ap.get("invalid") or {}), done, total,
                  sum(m.get("parts", 0) for m in dbs.values())))
        if fe:
            R.log("    payload: %s" % body[:200])
            fails.append("P1-err")
            break
        if dbs and done >= total and total > 0:
            break
        time.sleep(3)
    ap = (gj(STATE_KEY) or {}).get("api") or {}
    dbs = ap.get("dbs") or {}
    total = sum(len(m.get("codes") or []) for m in dbs.values())
    done = sum(m.get("done", 0) for m in dbs.values())
    for db, why in sorted((ap.get("invalid") or {}).items()):
        R.log("    invalid db %s: %s" % (db, why))
    ok1 = len(dbs) >= 8 and total >= 1500 and done >= total
    R.log("P1 %s dbs=%d series=%d done=%d" % (
        "PASS" if ok1 else "FAIL", len(dbs), total, done))
    if not ok1:
        fails.append("P1")

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
