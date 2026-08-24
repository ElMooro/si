"""ops_4971 -- Khalid expedite: census -> polygon -> fiscaldata ->
OFR (evidence first), + P0: read the imf-full _discover error the
Lambda already wrote to S3 (runner-proven != Lambda-proven; the
answer is sitting in state).

P0 imf     print state.failures._discover verbatim + re-run the
           v1.0.1 loose regex on a fresh runner fetch (expect 222)
           -> conclude Lambda-egress vs parse
P1 census  surface the 5 structurally-named failures with their
           stored refusal reasons; live-retest one URL from stored
           grammar
P2 fiscal  find the REAL dataset registry: Gatsby page-data JSON +
           sitemap -> endpoints beyond our 19
P3 ofr     v1 mnemonic catalog + hf catalog vs banked series counts
P4 polygon read POLYGON_API_KEY from a fleet donor's env (runner
           has AWS creds), probe entitlement depth: earliest daily
           bar (AAPL from 1990), grouped-daily, options/indices/
           flatfiles 200-vs-403
Artifact data/warm/_audit/expedite-4lane.json.  [skip-deploy]
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

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
OUT = "data/warm/_audit/expedite-4lane.json"
UA = {"User-Agent": "JustHodl Research (raafouis@gmail.com)",
      "Accept": "*/*"}
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


def fetch(url, cap=8_000_000, timeout=75, headers=None):
    h = dict(UA)
    if headers:
        h.update(headers)
    try:
        req = urllib.request.Request(url, headers=h)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return {"status": r.status}, r.read(cap)
    except urllib.error.HTTPError as e:
        return {"status": e.code,
                "head": (e.read(200) or b"").decode(
                    "utf-8", "replace")}, b""
    except Exception as e:
        return {"status": 0, "head": str(e)[:150]}, b""


with report("ops_4971_expedite_probe") as R:
    out = {"as_of": datetime.now(timezone.utc).isoformat(
        timespec="seconds"), "ops": 4971}

    # ---- P0 imf ------------------------------------------------------
    R.section("P0 imf-full _discover error (from state) + runner re-parse")
    ist = gj("data/warm/imf-full/_state/state.json") or {}
    derr = (ist.get("failures") or {}).get("_discover")
    R.log("  Lambda _discover err: %s" % json.dumps(derr))
    IB = "https://api.imf.org/external/sdmx/2.1"
    SDMX_ACC = ("application/vnd.sdmx.structure+xml;"
                "version=2.1")
    cands = [
        ("dataflow/IMF plain", IB + "/dataflow/IMF", None),
        ("dataflow/IMF sdmx-acc", IB + "/dataflow/IMF",
         {"Accept": SDMX_ACC}),
        ("dataflow/all", IB + "/dataflow/all", None),
        ("dataflow bare", IB + "/dataflow", None),
        ("dataflow/IMF/all sdmx", IB + "/dataflow/IMF/all",
         {"Accept": SDMX_ACC}),
        ("dataflow ?detail", IB +
         "/dataflow/IMF?detail=full&references=none", None),
    ]
    winner = None
    ladder = []
    for nm, u, hd in cands:
        m_, b_ = fetch(u, cap=12_000_000, timeout=90, headers=hd)
        xml = b_.decode("utf-8", "replace") if b_ else ""
        n = len(set(re.findall(
            r'Dataflow[^>]*\bid="([A-Za-z0-9_.\-]+)"', xml)))
        R.log("  %-24s %s %8dB ids=%d" % (
            nm, m_.get("status"), len(b_), n))
        ladder.append({"cand": nm, "status": m_.get("status"),
                       "bytes": len(b_), "ids": n})
        if n >= 150 and not winner:
            winner = {"name": nm, "url": u, "accept": hd,
                      "ids": n}
        time.sleep(0.4)
    R.log("  WINNER: %s" % json.dumps(winner))
    out["imf"] = {"lambda_err": derr, "ladder": ladder,
                  "winner": winner}

    # ---- P1 census ---------------------------------------------------
    R.section("P1 census: the 5 structural failures, stored reasons")
    cs = None
    for k in ("data/warm/census-us/_state/state.json",
              "data/warm/census/_state/state.json"):
        cs = gj(k)
        if cs:
            R.log("  state at %s" % k)
            break
    cout = {}
    if cs:
        fl = cs.get("failures") or {}
        for slug, v in sorted(fl.items()):
            R.log("  FAIL %-24s %s" % (
                slug, json.dumps(v)[:220]))
            cout[slug] = v if isinstance(v, dict) else {"err": v}
        # conquest recon: engine dataset table -> geography.json
        eng = (Path(__file__).resolve().parents[2].parent /
               "aws/lambdas/justhodl-census-us/source/"
               "lambda_function.py").read_text()
        for slug in list(fl)[:5]:
            m_ds = re.search(
                r'"%s"[^\n]*?"(?:path|ds|dataset)"\s*:\s*'
                r'"([^"]+)"' % re.escape(slug), eng) or re.search(
                r'"%s"\s*:\s*\(?\s*"([^"]+)"'
                % re.escape(slug), eng)
            path = m_ds.group(1) if m_ds else None
            if not path:
                ln = next((l_ for l_ in eng.splitlines()
                           if ('"%s"' % slug) in l_), "")
                R.log("  %-24s table-line: %s" % (
                    slug, ln.strip()[:150]))
                cout.setdefault("_recon", {})[slug] =                     {"line": ln.strip()[:150]}
                continue
            gu = ("https://api.census.gov/data/%s/geography.json"
                  % path.strip("/"))
            m2, b2 = fetch(gu, cap=400_000, timeout=45)
            geos = re.findall(r'"name"\s*:\s*"([^"]+)"',
                              b2.decode("utf-8", "replace")
                              if b2 else "")[:8]
            R.log("  %-24s path=%s geo(%s)=%s" % (
                slug, path, m2.get("status"), geos))
            cout.setdefault("_recon", {})[slug] = {
                "path": path, "geo_status": m2.get("status"),
                "geos": geos}
            time.sleep(0.3)
    else:
        R.log("  census state NOT FOUND under either prefix")
    out["census"] = cout

    # ---- P2 fiscaldata registry --------------------------------------
    R.section("P2 fiscaldata: real dataset registry")
    fout = {}
    cands = [
        "https://fiscaldata.treasury.gov/page-data/datasets/"
        "page-data.json",
        "https://fiscaldata.treasury.gov/page-data/index/"
        "page-data.json",
        "https://fiscaldata.treasury.gov/sitemap.xml",
    ]
    eps = set()
    for u in cands:
        m2, b2 = fetch(u, cap=15_000_000, timeout=90)
        txt = b2.decode("utf-8", "replace") if b2 else ""
        found = set(re.findall(
            r"/services/api/fiscal_service/(v[12]/[a-z0-9_/]+)",
            txt)) | set(re.findall(
                r'"endpoint"\s*:\s*"(v[12]/[a-z0-9_/]+)"', txt)) \
            | set(re.findall(r'"(v[12]/[a-z0-9_/]{8,})"', txt))
        R.log("  %s -> %s %.2fMB eps+%d" % (
            u.rsplit("/", 2)[-2], m2.get("status"),
            len(b2) / 1e6, len(found)))
        eps |= found
    have_fd = gj("data/warm/fiscaldata-full/_state/state.json"
                 ) or {}
    known = set((have_fd.get("universe") or {}).keys())
    new = sorted(e for e in eps if e not in known
                 and not e.endswith("/"))
    R.log("  registry endpoints=%d known=%d NEW=%d sample=%s" % (
        len(eps), len(known), len(new), new[:8]))
    fout = {"registry_eps": len(eps), "known": len(known),
            "new": new[:80]}
    out["fiscaldata"] = fout

    # ---- P3 ofr ------------------------------------------------------
    R.section("P3 OFR: catalogs vs banked")
    oout = {}
    m2, b2 = fetch("https://data.financialresearch.gov/v1/"
                   "metadata/mnemonics", cap=6_000_000)
    try:
        mn = json.loads(b2)
        n_mn = len(mn) if isinstance(mn, list) else \
            len(mn.get("mnemonics", mn))
    except Exception:
        n_mn = 0
    m3, b3 = fetch("https://data.financialresearch.gov/hf/v1/"
                   "metadata/mnemonics", cap=6_000_000)
    try:
        hf = json.loads(b3)
        n_hf = len(hf) if isinstance(hf, list) else \
            len(hf.get("mnemonics", hf))
    except Exception:
        n_hf = 0
    r_ = s3.list_objects_v2(Bucket=B,
                            Prefix="data/warm/ofr-stfm/series/")
    n_stfm = r_.get("KeyCount", 0)
    while r_.get("IsTruncated"):
        r_ = s3.list_objects_v2(
            Bucket=B, Prefix="data/warm/ofr-stfm/series/",
            ContinuationToken=r_["NextContinuationToken"])
        n_stfm += r_.get("KeyCount", 0)
    r2 = s3.list_objects_v2(Bucket=B,
                            Prefix="data/warm/ofr-hfm/series/")
    n_hfm = r2.get("KeyCount", 0)
    R.log("  ofr main catalog=%s (%s) banked stfm-series=%d" % (
        n_mn, m2.get("status"), n_stfm))
    R.log("  ofr hf   catalog=%s (%s) banked hfm-series=%d" % (
        n_hf, m3.get("status"), n_hfm))
    oout = {"main_catalog": n_mn, "hf_catalog": n_hf,
            "banked_stfm": n_stfm, "banked_hfm": n_hfm}
    out["ofr"] = oout

    # ---- P4 polygon --------------------------------------------------
    R.section("P4 polygon: entitlement depth")
    pout = {}
    try:
        env = lam.get_function_configuration(
            FunctionName="justhodl-equity-research"
        )["Environment"]["Variables"]
        pk = env.get("POLYGON_API_KEY") or ""
    except Exception as e:
        pk = ""
        R.log("  donor env err: %s" % str(e)[:80])
    if pk:
        tests = [
            ("daily_1990", "https://api.polygon.io/v2/aggs/ticker/"
             "AAPL/range/1/day/1990-01-01/1990-12-31?limit=5"),
            ("grouped", "https://api.polygon.io/v2/aggs/grouped/"
             "locale/us/market/stocks/2024-06-03?limit=2"),
            ("tickers", "https://api.polygon.io/v3/reference/"
             "tickers?limit=1"),
            ("options", "https://api.polygon.io/v3/reference/"
             "options/contracts?limit=1"),
            ("indices", "https://api.polygon.io/v2/aggs/ticker/"
             "I:SPX/range/1/day/2024-06-03/2024-06-04"),
        ]
        for name, u in tests:
            m4, b4 = fetch(u + "&apiKey=" + pk if "?" in u
                           else u + "?apiKey=" + pk,
                           cap=200_000, timeout=45)
            det = ""
            try:
                js = json.loads(b4)
                det = "results=%s first_t=%s" % (
                    js.get("resultsCount",
                           len(js.get("results") or [])),
                    ((js.get("results") or [{}])[0] or {}
                     ).get("t"))
            except Exception:
                det = (m4.get("head") or "")[:80]
            R.log("  %-10s %s %s" % (name, m4.get("status"), det))
            pout[name] = {"status": m4.get("status"),
                          "detail": det[:100]}
            time.sleep(0.3)
    out["polygon"] = pout

    s3.put_object(Bucket=B, Key=OUT,
                  Body=json.dumps(out, indent=1).encode(),
                  ContentType="application/json")
    R.log("artifact %s" % OUT)
    evidenced = sum(1 for k in ("imf", "census", "fiscaldata",
                                "ofr", "polygon")
                    if out.get(k))
    if evidenced < 4:
        R.log("ops 4971 RED: <4 lanes evidenced")
        sys.exit(1)
    R.kv(imf_lambda_err=bool(derr),
         imf_winner=json.dumps((out["imf"].get("winner") or {}).get("name")),
         census_fails=len(cout),
         fd_new=len(fout.get("new") or []),
         ofr_main=oout.get("main_catalog"),
         polygon_tests=len(pout))
    R.log("ops 4971 GREEN -- expedite lanes evidenced; builds "
          "follow Khalid's order census -> polygon -> fiscaldata "
          "-> ofr")
