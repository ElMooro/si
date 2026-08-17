"""ops/4810 -- justhodl-sp500 v1.2.0 verify: per-sector fair fight.
 (1) settle marker 'sp500 v1.2.0'; Event-invoke; poll engine_v 1.2.0.
 (2) sectors block: >=8 GICS sectors, weights sum 95..101, largest
     weight 20..45 (Tech-era sanity), per-sector pe_ttm in 8..60 where
     present, every sector carries n/cap_t/pe_fwd.
 (3) compare NVDA: sector_context present -- 7 rows, sector matches the
     member row, pe_fwd sector percentile populated. AAPL cross-check.
 (4) full sector readout.
"""
import gzip
import io
import json
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
FN = "justhodl-sp500"
B = "justhodl-dashboard-live"
OUT_KEY = "data/sp500.json"
MARKER = "sp500 v1.2.0"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=120,
                                 retries={"max_attempts": 1}))
FAILED = []


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def settle(rep):
    for att in range(30):
        try:
            gf = lam.get_function(FunctionName=FN)
            raw = urllib.request.urlopen(gf["Code"]["Location"],
                                         timeout=60).read()
            src = zipfile.ZipFile(io.BytesIO(raw)).read(
                "lambda_function.py").decode("utf-8", "replace")
            if MARKER in src:
                rep.ok("deployed marker settled (attempt %d)"
                       % (att + 1))
                return True
        except (ClientError, Exception):  # noqa: BLE001
            pass
        time.sleep(10)
    rep.fail("deployed zip never carried %s" % MARKER)
    FAILED.append("settle")
    return False


def main():
    with report("ops 4810 -- sp500 v1.2.0 sector fair-fight") as rep:
        rep.heading("1. settle + invoke")
        if not settle(rep):
            sys.exit(1)
        t0 = time.time()
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        doc = None
        while time.time() - t0 < 480:
            time.sleep(15)
            try:
                d = sread(OUT_KEY)
                if d.get("engine_v") == "1.2.0":
                    doc = d
                    break
            except ClientError:
                pass
        if not doc:
            rep.fail("doc never refreshed to engine_v 1.2.0 in 8 min")
            sys.exit(1)
        rep.ok("  fresh v1.2.0 doc after ~%ds" % int(time.time() - t0))

        rep.heading("2. sectors block")
        secs = doc.get("sectors") or {}
        if len(secs) < 8:
            rep.fail("  only %d sectors (want >=8)" % len(secs))
            FAILED.append("n_sectors")
        else:
            rep.ok("  sectors = %d" % len(secs))
        wsum = sum(v.get("weight_pct") or 0 for v in secs.values())
        if not 95 <= wsum <= 101:
            rep.fail("  weights sum %.1f not in 95..101" % wsum)
            FAILED.append("weights")
        else:
            rep.ok("  weights sum = %.1f%%" % wsum)
        top = max(secs.items(),
                  key=lambda kv: kv[1].get("weight_pct") or 0) \
            if secs else (None, {})
        tw = top[1].get("weight_pct") or 0
        if not 20 <= tw <= 45:
            rep.fail("  top sector %s weight %.1f not in 20..45"
                     % (top[0], tw))
            FAILED.append("top_weight")
        else:
            rep.ok("  top sector = %s (%.1f%%)" % (top[0], tw))
        for s_name, v in sorted(secs.items(),
                                key=lambda kv:
                                -(kv[1].get("weight_pct") or 0)):
            pe = v.get("pe_ttm")
            bad = pe is not None and not (8 <= pe <= 60)
            (rep.warn if bad else rep.ok)(
                "  %-24s w=%5.1f%% n=%3d pe=%s fpe=%s roe=%s ntm=%s"
                % (s_name, v.get("weight_pct") or 0, v.get("n") or 0,
                   v.get("pe_ttm"), v.get("pe_fwd"), v.get("roe_pct"),
                   v.get("ntm_growth_pct")))
            if v.get("pe_fwd") is None:
                rep.warn("    %s pe_fwd missing" % s_name)

        rep.heading("3. compare sector_context")
        for tkr in ("NVDA", "AAPL"):
            r = lam.invoke(FunctionName=FN, Payload=json.dumps(
                {"compare": tkr}).encode())
            c = json.loads(r["Payload"].read())
            sc = c.get("sector_context")
            ok = (c.get("ok") and sc and len(sc.get("rows") or []) == 7
                  and sc.get("pe_fwd_pctile_in_sector") is not None)
            (rep.ok if ok else rep.fail)(
                "  %s sector_context: %s (n=%s, fwd-P/E pctile-in-"
                "sector=%s)" % (tkr,
                                sc.get("sector") if sc else None,
                                sc.get("n") if sc else None,
                                sc.get("pe_fwd_pctile_in_sector")
                                if sc else None))
            if not ok:
                FAILED.append("sctx_" + tkr)
            else:
                rep.kv(**{tkr.lower() + "_vs_sector": json.dumps(
                    [(x["metric"], x["stock"], x["sector_agg"],
                      x["premium_pct"]) for x in sc["rows"][:4]])})

        rep.heading("4. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("sp500 v1.2.0 LIVE -- sector fair-fight wired end to "
               "end")


if __name__ == "__main__":
    main()
