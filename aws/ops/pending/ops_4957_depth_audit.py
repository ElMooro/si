"""ops_4957 -- provider DEPTH audit: end the scoped-vs-complete lie.

Khalid, 5th time: "small MB tells me you didn't import all their
data." Correct -- several cards are CURATED AGENTS (25 BLS series, 26
World Bank keys) presented with the same face as full-catalog walkers
(FRED 280k, Census 3.9M rows). Census v1.0 had this exact disease.

This ops builds the evidence ledger every follow-up arc drains:
for each suspect provider -- S3 key count, bytes, sampled obs-depth
(min/max dates from real keys) -- classified:
  FULL              believed complete vs source
  SCOPED_BY_DESIGN  entitlement/firehose/extension -- reason stated
  THIN              source holds vastly more -> DRAIN QUEUE, ranked
Artifact: data/warm/_audit/depth-audit.json (consumed by the coming
full-history walkers + a later card-note upgrade). [skip-deploy]
"""
import gzip
import io
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
OUT_KEY = "data/warm/_audit/depth-audit.json"
s3 = boto3.client("s3", region_name=REGION)

# slug -> (prefixes/hot keys, source_universe_note, classification hint)
SUSPECTS = {
 "bls": (["data/warm/bls/", "data/bls-"],
         "download.bls.gov time.series: full CPI/CES/JOLTS/LAUS/PPI "
         "flat files, millions of series since 1913", "THIN"),
 "worldbank": (["data/warm/worldbank/"],
               "api.worldbank.org: ~16k indicators x 260 economies "
               "x 60y", "THIN"),
 "imf": (["data/warm/imf/"],
         "IMF SDMX: IFS/BOP/DOT/GFS full country x indicator "
         "matrices", "THIN"),
 "boe": (["data/warm/boe/"],
         "BoE IADB: thousands of series", "THIN"),
 "snb": (["data/warm/snb/"], "SNB data portal full cube", "THIN"),
 "boj": (["data/warm/boj/"],
         "BOJ stat-search: MD/LA/IR/FM/CO db families, thousands",
         "THIN"),
 "bcb": (["data/warm/bcb/"], "BCB SGS: ~30k series", "THIN"),
 "banxico": (["data/warm/banxico/"], "Banxico SIE: ~30k series",
             "THIN"),
 "dbnomics": (["data/warm/dbnomics/"],
              "aggregator of 80+ providers; fallback lane only",
              "SCOPED_BY_DESIGN"),
 "coinmetrics": (["data/warm/coinmetrics/"],
                 "community API: full daily history, all assets x "
                 "metrics", "THIN"),
 "cboe": (["data/warm/cboe/"],
          "settlement/volume archives years deep", "THIN"),
 "occ": (["data/warm/occ/"], "daily/weekly volume archives", "THIN"),
 "dol-eta": (["data/warm/dol/"],
             "weekly claims since 1987 -- small source; likely FULL",
             "FULL"),
 "nasa-power": (["data/warm/nasa/"],
                "point/agro grids -- scoped ag-weather pulls",
                "SCOPED_BY_DESIGN"),
 "frb-ddp": (["data/warm/frb/", "data/warm/fed-"],
             "DDP packages H15/H8/Z1/G19 full zips", "THIN"),
 "ofr": (["data/warm/ofr-stfm/", "data/warm/ofr/"],
         "STFM v1: full daily history per series since 2014ish",
         "VERIFY"),
 "ofr-hf": (["data/warm/ofr-hf/"],
            "HF monitor: quarterly, shallow by nature", "FULL"),
 "gdelt": (["data/warm/gdelt/"],
           "15-min firehose, TB-scale archive -- pulse only",
           "SCOPED_BY_DESIGN"),
 "polygon": (["data/warm/polygon/"],
             "entitlement-scoped US equities", "SCOPED_BY_DESIGN"),
 "te-feed": (["data/te-feed.json"],
             "single country snapshot endpoint", "SCOPED_BY_DESIGN"),
}
DATE_RX = re.compile(r"(19|20)\d{2}[-/](0[1-9]|1[0-2])")


def sample_dates(key):
    try:
        raw = s3.get_object(Bucket=B, Key=key)["Body"].read(400_000)
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.GzipFile(fileobj=io.BytesIO(raw)).read(400_000)
        txt = raw.decode("utf-8", "replace")
        ds = DATE_RX.findall(txt)
        full = [m.group(0) for m in DATE_RX.finditer(txt)]
        if not full:
            return None, None
        return min(full), max(full)
    except Exception:
        return None, None


with report("ops_4957_depth_audit") as R:
    rows = []
    R.section("provider depth table")
    for slug, (prefs, universe, hint) in SUSPECTS.items():
        tot, nk, samples = 0, 0, []
        for p in prefs:
            if not p.endswith("/"):
                try:
                    h = s3.head_object(Bucket=B, Key=p)
                    tot += h["ContentLength"]
                    nk += 1
                    samples.append(p)
                except Exception:
                    pass
                continue
            tok = None
            while True:
                kw = dict(Bucket=B, Prefix=p, MaxKeys=1000)
                if tok:
                    kw["ContinuationToken"] = tok
                r_ = s3.list_objects_v2(**kw)
                for o in r_.get("Contents", []):
                    tot += o["Size"]
                    nk += 1
                    if len(samples) < 3 and o["Size"] > 500:
                        samples.append(o["Key"])
                if not r_.get("IsTruncated"):
                    break
                tok = r_.get("NextContinuationToken")
        d0, d1 = None, None
        for k in samples[:3]:
            a, b_ = sample_dates(k)
            if a:
                d0 = min(d0 or a, a)
                d1 = max(d1 or b_, b_)
        mb = tot / 1e6
        cls = hint
        if hint == "VERIFY":
            cls = "FULL" if (d0 and d0 <= "2015") else "THIN"
        if hint == "THIN" and mb > 400:
            cls = "FULL"
        rows.append({"slug": slug, "keys": nk, "mb": round(mb, 2),
                     "obs_min": d0, "obs_max": d1, "class": cls,
                     "universe": universe})
        R.log("  %-12s %5d keys %9.2fMB obs=%s..%s  %s" % (
            slug, nk, mb, d0, d1, cls))
    thin = [r for r in rows if r["class"] == "THIN"]
    order = ["bls", "worldbank", "imf", "boe", "coinmetrics", "bcb",
             "banxico", "boj", "snb", "frb-ddp", "cboe", "occ"]
    thin.sort(key=lambda r: order.index(r["slug"])
              if r["slug"] in order else 99)
    R.section("DRAIN QUEUE (ranked)")
    for i, r_ in enumerate(thin, 1):
        R.log("  %2d. %-12s %.2fMB -- %s" % (
            i, r_["slug"], r_["mb"], r_["universe"][:80]))
    s3.put_object(Bucket=B, Key=OUT_KEY, Body=json.dumps({
        "as_of": datetime.now(timezone.utc).isoformat(
            timespec="seconds"),
        "ops": 4957, "rows": rows,
        "drain_queue": [r["slug"] for r in thin]},
        indent=1).encode(), ContentType="application/json")
    ok = len(rows) >= 15 and len(thin) >= 6
    R.log("%s classified=%d thin=%d artifact=%s" % (
        "PASS" if ok else "FAIL", len(rows), len(thin), OUT_KEY))
    R.kv(classified=len(rows), thin=len(thin),
         queue=",".join(r["slug"] for r in thin))
    if not ok:
        R.log("ops 4957 RED")
        sys.exit(1)
    R.log("ops 4957 GREEN -- the ledger every full-history arc "
          "drains; bls-full walker ships in the SAME session")
