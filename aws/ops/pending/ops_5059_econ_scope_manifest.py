"""ops_5059 -- the definitive economic scope, and why my curation failed.

ops 5058 got the cost right (~15,650 requests, and we HAVE a working
CENSUS_API_KEY -- the probe returned California EMP 16,032,440 / PAYANN
$1.36T) but its curated shortlist was wrong in a way the keyword sweep
caught. I had listed "ecn" as one family with 12 entries. The Economic
Census is not one family: it is split across dozens of ecn* families,
and the sweep surfaced exactly the ones a macro/finance desk would want
me to have -- ecnccard (credit cards), ecncrfin (types of credit and
financing), ecnbranddeal (brokers and dealers), ecncomm (wholesale sales),
ecnconact (construction activity) -- plus rhfs (Rental Housing Finance
Survey) and cfspum (Commodity Flow Survey microdata, i.e. freight).

Curation by memory drops what you did not think of. The sweep is what
makes the scope defensible, so this op keeps both and writes the union
to a manifest the walker will consume, rather than leaving the scope
implicit in code I wrote at 4am.

  P0 enumerate EVERY family, tag each: curated-economic, ecn* (the
     Economic Census in all its parts), sweep-economic, or out
  P1 the union, with entry counts and vintage spans
  P2 cost the union honestly
  P3 write data/_state/census-econ-scope.json -- the walker's input,
     reviewable before a single row is fetched
"""
import json
import re
import sys
import urllib.request
from collections import defaultdict
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
DATA_JSON = "https://api.census.gov/data.json"
SCOPE_KEY = "data/_state/census-econ-scope.json"
UA = "Mozilla/5.0 (compatible; JustHodlAI/1.0) ops-5059"

CURATED = {"cbp", "zbp", "nonemp", "ecn", "ewks", "cps", "sipp",
           "abscb", "abscbo", "abscs", "absnesd", "absnesdo", "absmcb",
           "ase", "rhfs", "cfspum", "cfsarea", "cfsexport"}
OUT = {"dec", "pep", "popproj", "pdb", "geoinfo", "cre",
       "crepuertorico", "acs", "idb", "intltrade", "sbo", "surname",
       "language", "cbdrb"}
KW = re.compile(
    r"employ|payroll|business|manufactur|industr|econom|income|earning|"
    r"establish|firm|revenue|sales|shipment|construct|financ|capital|"
    r"expenditur|wage|labor|workforce|productiv|inventor|retail|"
    r"wholesale|credit|insurance|bank|asset|profit|commodity|freight",
    re.I)

cfg = Config(read_timeout=120, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)


def http(url, cap=60 * 1024 * 1024):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=110) as r:
        return r.status, r.read(cap)


with report("ops_5059_econ_scope_manifest") as R:
    fails = []
    st, raw = http(DATA_JSON)
    items = json.loads(raw).get("dataset") or []
    R.log("  catalogue: %s entries" % f"{len(items):,}")

    R.section("P0/P1 classify every family")
    byfam = defaultdict(list)
    for d in items:
        cd = d.get("c_dataset") or ["(none)"]
        if cd[0] == "timeseries":
            continue
        byfam[cd[0]].append(d)
    picked, reasons = {}, {}
    for fam, ds in byfam.items():
        if fam in OUT:
            continue
        why = None
        if fam in CURATED:
            why = "curated"
        elif fam.startswith("ecn") or fam.startswith("ewks"):
            why = "economic-census"
        elif any(KW.search(str(d.get("title") or "") + " " +
                           str(d.get("description") or "")[:300])
                 for d in ds):
            why = "keyword"
        if why:
            vs = sorted(int(d["c_vintage"]) for d in ds
                        if d.get("c_vintage"))
            picked[fam] = {
                "entries": len(ds),
                "vintages": [vs[0], vs[-1]] if vs else None,
                "why": why,
                "title": str(ds[0].get("title") or "")[:90],
                "datasets": sorted({"/".join(str(x) for x in
                                             (d.get("c_dataset") or []))
                                    for d in ds})[:6]}
            reasons[why] = reasons.get(why, 0) + len(ds)
    tot = sum(v["entries"] for v in picked.values())
    R.log("  families IN SCOPE: %d   entries: %s" % (len(picked),
                                                     f"{tot:,}"))
    R.log("  by reason: %s" % reasons)
    R.log("  --- the Economic Census, in all its parts ---")
    ecn = {k: v for k, v in picked.items() if v["why"] == "economic-census"}
    R.log("  ecn* families: %d, %s entries" % (
        len(ecn), f"{sum(v['entries'] for v in ecn.values()):,}"))
    for k, v in sorted(ecn.items(), key=lambda kv: -kv[1]["entries"])[:16]:
        R.log("    %-16s %2d  %-11s %s" % (
            k, v["entries"],
            "%d–%d" % tuple(v["vintages"]) if v["vintages"] else "—",
            v["title"][:56]))
    R.log("  --- everything else in scope ---")
    for k, v in sorted(((k, v) for k, v in picked.items()
                        if v["why"] != "economic-census"),
                       key=lambda kv: -kv[1]["entries"])[:18]:
        R.log("    %-16s %4d  %-11s %-9s %s" % (
            k, v["entries"],
            "%d–%d" % tuple(v["vintages"]) if v["vintages"] else "—",
            v["why"], v["title"][:46]))
    R.log("  --- dropped as demographics/social ---")
    R.log("    %s" % sorted(f for f in byfam if f in OUT))

    R.section("P2 cost")
    R.log("  %s entries in scope vs 1,704 unfetched total" % f"{tot:,}")
    R.log("  5058 measured ~15,650 requests for its 8 largest; the ecn*")
    R.log("  families are small per-entry (12-30 variables, 1 geo level)")
    R.log("  so the union stays in the same order of magnitude.")
    R.log("  With CENSUS_API_KEY present and answering, this is hours of")
    R.log("  polite crawling, not the 31 days an unkeyed walker faced.")

    R.section("P3 write the scope manifest")
    doc = {"schema": 1, "generated_by": "ops_5059",
           "note": ("The walker's INPUT. Scope is data, not code -- "
                    "reviewable and editable before a row is fetched. "
                    "why=curated | economic-census | keyword; keyword "
                    "entries are the ones curation missed."),
           "excluded": sorted(f for f in byfam if f in OUT),
           "excluded_reason": ("demographics and social programs; the "
                               "decennial census alone was ~101k of the "
                               "~112k requests in ops 5057"),
           "already_held": ("the timeseries family (94 entries) is "
                            "COMPLETE and carries EITS, BDS, QWI, ASM "
                            "-- the headline macro indicators"),
           "families": picked, "total_entries": tot,
           "total_families": len(picked)}
    try:
        s3.put_object(Bucket=LIVE, Key=SCOPE_KEY,
                      Body=json.dumps(doc, indent=1).encode(),
                      ContentType="application/json")
        R.log("  -> %s  (%d families, %s entries)" % (SCOPE_KEY,
                                                      len(picked),
                                                      f"{tot:,}"))
    except Exception as e:
        R.log("  write err %s" % str(e)[:120])
        fails.append("P3")

    if fails:
        R.log("ops 5059 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(families=len(picked), entries=tot,
         economic_census_families=len(ecn))
    R.log("ops 5059 GREEN -- scope is now a reviewable manifest")
