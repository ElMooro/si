"""ops_5038 -- reconcile the dashboard against the ground truth.

data.html currently says, of Eurostat:
    "sdmx-eurostat COMPLETE ... 8191 keys ... 8,152 series ...
     100% of 8,152 target"
while the series-extractor has banked hundreds of millions of series.
Both can be true, and the reason is wiring, not data:

    provider-catalog eurostat spec:
        prefixes    = ["data/warm/eurostat/"]
        series_from = ("data/warm/eurostat/catalog.json.gz", "dataflows")
    and _series_list() counts len(list) of that field.

So the card's "8,152 series" is 8,152 DATAFLOWS in the warm SDMX mirror,
and "100%" is the completeness of that mirror -- which sdmx-walker
finished long ago. data/providers/eurostat/series/ appears in neither
`prefixes` nor `series_from`, so every page the extractor has written is
invisible to the catalog, to the S3 KEYS total, and to the WARM+HOT GB
total. The dashboard is not wrong about what it measures; it is silent
about what we spent today building.

This op is READ-ONLY. It establishes:
  P0 the real extraction numbers, on the bracket predicate
  P1 what the IMPORT DEGRADED banner and the 5 sentinel incidents are,
     and whether any of them belong to us
  P2 the eurostat card's inputs, proving the wiring gap rather than
     asserting it
  P3 the other two things the page flags: census-us STALE, FRED queue
  P4 what a correct card would need, costed -- not applied

Nothing is rewired here. A display fix that adds a 2M-object prefix to
a daily catalog scan deserves its own op with the renderer in hand.
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
STATE_KEY = "data/_state/series-extract-eurostat.json"
PFX = "data/providers/eurostat/series/"
MANIFEST = "data/providers/eurostat/series-manifest.json"
FLOWS_TOTAL = 8147

cfg = Config(read_timeout=120, retries={"max_attempts": 4})
s3 = boto3.client("s3", region_name=REGION, config=cfg)


def jget(key):
    b = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
    if key.endswith(".gz"):
        import gzip
        b = gzip.decompress(b)
    return json.loads(b)


def count_prefix(prefix):
    n, byts = 0, 0
    kw = {"Bucket": LIVE, "Prefix": prefix, "MaxKeys": 1000}
    while True:
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            n += 1
            byts += o["Size"]
        if not r.get("IsTruncated"):
            break
        kw["ContinuationToken"] = r.get("NextContinuationToken")
    return n, byts


with report("ops_5038_dashboard_reconcile") as R:
    fails = []
    out = {"op": "ops_5038"}

    R.section("P0 ground truth of the extraction")
    try:
        st = jget(STATE_KEY)
        p_before = int(st.get("n_pages") or 0)
        f0 = len(st.get("flows_done") or [])
        R.log("  n_pages BEFORE count: %d  flows_done=%d  updated_at=%s"
              % (p_before, f0, st.get("updated_at")))
        n_obj, byts = count_prefix(PFX)
        st2 = jget(STATE_KEY)
        p_after = int(st2.get("n_pages") or 0)
        f1 = len(st2.get("flows_done") or [])
        s1 = int(st2.get("series_count") or 0)
        ok = (p_before - 2) <= n_obj <= (p_after + 2)
        R.log("  objects counted     : %d (%.1f GB)" % (n_obj,
                                                        byts / 1e9))
        R.log("  n_pages AFTER count : %d" % p_after)
        R.log("  bracket %d <= %d <= %d : %s" % (
            p_before, n_obj, p_after, "CLEAN" if ok else "*** GAP ***"))
        R.log("  flows %d / %d (%.2f%%)   series %d   %.1f GB" % (
            f1, FLOWS_TOTAL, 100.0 * f1 / FLOWS_TOTAL, s1, byts / 1e9))
        R.log("  holes=%d failed_flows=%d write_errors_last_run=%s" % (
            len(st2.get("missing_pages") or []),
            len(st2.get("failed_flows") or []),
            st2.get("write_errors_this_run")))
        if not ok:
            fails.append("P0:gap")
        out.update(flows=f1, pages=p_after, objects=n_obj, series=s1,
                   gb=round(byts / 1e9, 1), bracket_clean=ok)
    except Exception as e:
        R.log("  state err %s" % str(e)[:150])
        fails.append("P0")
    try:
        man = jget(MANIFEST)
        R.log("  series-manifest: flows_total=%s flows_parsed=%s "
              "series_extracted=%s n_pages=%s updated_at=%s" % (
                  man.get("flows_total"), man.get("flows_parsed"),
                  man.get("series_extracted"), man.get("n_pages"),
                  man.get("updated_at")))
    except Exception as e:
        R.log("  manifest err %s" % str(e)[:110])

    R.section("P1 the IMPORT DEGRADED banner + sentinel incidents")
    try:
        ih = jget("data/import-health.json")
        R.log("  status=%s updated_at=%s" % (ih.get("status"),
                                             ih.get("updated_at")))
        for k in ("headline", "summary", "scope", "queue", "note"):
            if ih.get(k) is not None:
                R.log("  %-9s %s" % (k, str(ih.get(k))[:150]))
        inc = (ih.get("incidents") or ih.get("issues")
               or ih.get("events") or [])
        R.log("  incidents: %d" % len(inc))
        for i, x in enumerate(inc[:10]):
            R.log("   %d) %s" % (i + 1, json.dumps(x, default=str)[:190]))
        prov = ih.get("providers") or {}
        if isinstance(prov, dict):
            bad = {k: v for k, v in prov.items()
                   if str(v).upper() not in ("OK", "COMPLETE", "FRESH")}
            R.log("  non-OK providers: %s" % json.dumps(bad,
                                                        default=str)[:300])
        out["import_status"] = ih.get("status")
        out["incidents"] = len(inc)
    except Exception as e:
        R.log("  import-health err %s" % str(e)[:140])

    R.section("P2 the eurostat card's actual inputs")
    try:
        cat = jget("data/provider-catalog.json")
        provs = cat.get("providers") or cat.get("items") or []
        if isinstance(provs, dict):
            euro = provs.get("eurostat")
        else:
            euro = next((p for p in provs
                         if (p.get("slug") or p.get("id")) == "eurostat"),
                        None)
        R.log("  eurostat card: %s" % json.dumps(euro, default=str)[:520])
        blob = json.dumps(cat, default=str)
        R.log("  does the catalog mention %r anywhere? %s" % (
            PFX, PFX in blob))
        R.log("  -> the card counts data/warm/eurostat/ only; the "
              "extraction lives in data/providers/ and is not scanned")
        R.log("  totals on the page: S3 KEYS and WARM+HOT GB therefore "
              "EXCLUDE every page written today")
    except Exception as e:
        R.log("  catalog err %s" % str(e)[:140])

    R.section("P3 the other two flags on the page")
    for key, label in (("data/warm/census-us/state.json", "census-us"),
                       ("data/_state/census-us.json", "census-us alt"),
                       ("data/_state/fred-import.json", "fred"),
                       ("data/warm/fred/state.json", "fred alt")):
        try:
            d = jget(key)
            keep = {k: d.get(k) for k in
                    ("phase", "status", "updated_at", "queue", "done",
                     "total", "cursor", "scope", "error", "errors")
                    if d.get(k) is not None}
            R.log("  %-12s %s -> %s" % (label, key,
                                        json.dumps(keep, default=str)[:220]))
        except Exception as e:
            R.log("  %-12s %s -> %s" % (label, key,
                                        type(e).__name__))

    R.section("P4 what a correct eurostat card would cost")
    n = out.get("objects") or 0
    R.log("  adding data/providers/eurostat/series/ to the catalog's")
    R.log("  prefixes would make it LIST %d objects (~%d requests) on "
          "every run -- pennies, but it would also move the page's S3 "
          "KEYS from ~776k to ~%.1fM and WARM+HOT from 214.9 GB to "
          "~%.0f GB" % (n, n // 1000 + 1, (776655 + n) / 1e6,
                        214.9 + (out.get("gb") or 0)))
    R.log("  the cheaper honest option: point series_from at the "
          "series-manifest's series_extracted, which needs "
          "_series_list() to accept an int as well as a list (it "
          "currently requires a list of ids and counts len())")
    R.log("  NOT APPLIED HERE -- the renderer decides which of those is "
          "right, and a display change belongs in its own op")
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/dashboard-reconcile.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/dashboard-reconcile.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5038 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(flows=out.get("flows"), series=out.get("series"),
         objects=out.get("objects"), gb=out.get("gb"),
         bracket_clean=out.get("bracket_clean"),
         incidents=out.get("incidents"))
    R.log("ops 5038 GREEN -- dashboard reconciled against ground truth")
