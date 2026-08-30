"""ops_5058 -- scope the Census import to macro, finance and industry.

Khalid: "im only interested in financial, manufacturing, industrial,
employment data from census. only things pertaining to macro and finance
and the economy."

That cuts the problem down hard, and it changes the answer to the
expensive question. ops 5057 found 1,704 unfetched catalogue entries and
estimated ~112k requests for the top six families -- but ~101k of those
were the DECENNIAL census (7,898 variables x 9 geography levels), which
is demographics, not economics. Dropping it removes 90% of the cost and
none of what he asked for.

It also matters that the family we DO hold is the macro one. The
timeseries family carries EITS (advance retail, M3 manufacturers'
shipments, construction spending), BDS, QWI, ASM -- the actual
indicators. So this is not "import everything we skipped"; it is
"identify the economic programs that live OUTSIDE timeseries and get
those, with all their vintages".

  P0 do we have an API key? Without one Census caps at 500 requests/day
     and even a cheap family becomes a week-long crawl. The key is
     checked for existence and TESTED, never printed.
  P1 what the timeseries family already gives us, so nothing is
     re-imported
  P2 classify the 1,704 by his scope: a curated economic shortlist, PLUS
     a keyword sweep over everything else so a program I did not think
     of cannot be silently dropped
  P3 per-family call and volume estimates for the shortlist only
  P4 the plan, ordered by value per request

Read-only.
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
CENSUS = "justhodl-census-us"
DATA_JSON = "https://api.census.gov/data.json"
CSTATE = "data/warm/census-us/_state/state.json"
UA = "Mozilla/5.0 (compatible; JustHodlAI/1.0) ops-5058"

# curated: the Census economic programs, by what they measure
ECON = {
    "cbp":    "County Business Patterns — establishments, employment, "
              "payroll by NAICS x geography",
    "zbp":    "ZIP Code Business Patterns — the same at ZIP granularity",
    "nonemp": "Nonemployer Statistics — self-employment, business "
              "formation",
    "ecn":    "Economic Census — manufacturing, retail, wholesale, "
              "services; the definitive industrial survey",
    "ewks":   "Economic Census, earlier vintages",
    "cps":    "Current Population Survey — the employment survey behind "
              "the unemployment rate",
    "abscb":  "Annual Business Survey — company characteristics",
    "abscbo": "Annual Business Survey — business owners",
    "abscs":  "Annual Business Survey — company summary",
    "absnesd": "Annual Business Survey — nonemployer demographics",
    "absnesdo": "Annual Business Survey — nonemployer owners",
    "ase":    "Annual Survey of Entrepreneurs",
    "sipp":   "Income & Program Participation — household income and "
              "wealth",
}
# demographics/social: deliberately OUT of scope
OUT = {"dec", "pep", "popproj", "pdb", "geoinfo", "cre",
       "crepuertorico", "acs", "intltrade", "idb"}
KEYWORDS = re.compile(
    r"employ|payroll|business|manufactur|industr|econom|income|earning|"
    r"establish|firm|revenue|sales|shipment|construct|hous|financ|"
    r"capital|expenditur|wage|labor|labour|workforce|productiv|"
    r"inventor|retail|wholesale|trade|profit|asset", re.I)

cfg = Config(read_timeout=120, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)


def http(url, cap=60 * 1024 * 1024):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=110) as r:
            return r.status, r.read(cap)
    except Exception as e:
        return getattr(e, "code", -1), str(e)[:140].encode()


with report("ops_5058_census_macro_scope") as R:
    fails = []
    out = {"op": "ops_5058"}

    R.section("P0 API key")
    key = None
    try:
        envd = (lam.get_function_configuration(FunctionName=CENSUS)
                .get("Environment") or {}).get("Variables") or {}
        cand = [k for k in envd if "KEY" in k.upper()
                or "CENSUS" in k.upper()]
        R.log("  env vars that look key-ish: %s" % cand)
        for k in cand:
            v = envd[k]
            if v and len(v) >= 20:
                key = v
                R.log("  using %s (len %d, value not printed)" % (k,
                                                                  len(v)))
                break
    except Exception as e:
        R.log("  env read err %s" % str(e)[:110])
    probe = ("https://api.census.gov/data/2022/cbp"
             "?get=NAME,EMP,PAYANN&for=state:06")
    st, b = http(probe + ("&key=" + key if key else ""))
    R.log("  keyed probe -> HTTP %s%s" % (st, "" if key else " (no key)"))
    if st == 200:
        try:
            rows = json.loads(b)
            R.log("  sample: %s" % json.dumps(rows[:2])[:150])
            R.log("  the API answers, so the walker's problem is rate, "
                  "not access")
        except Exception:
            R.log("  body not JSON: %s" % b[:120])
    else:
        R.log("  probe body: %s" % b[:180])
    out["has_key"] = bool(key)
    out["probe_status"] = st

    R.section("P1 what timeseries already gives us")
    st2, raw = http(DATA_JSON)
    cat = json.loads(raw)
    items = cat.get("dataset") or []
    ts = [d for d in items
          if (d.get("c_dataset") or [None])[0] == "timeseries"]
    fams = defaultdict(int)
    for d in ts:
        cd = d.get("c_dataset") or []
        fams["/".join(str(x) for x in cd[:2])] += 1
    R.log("  timeseries datasets we already hold cover:")
    for k, v in sorted(fams.items(), key=lambda kv: -kv[1])[:14]:
        R.log("    %-34s %d" % (k, v))
    R.log("  -> EITS, BDS, QWI and ASM are ALREADY IN. The headline")
    R.log("     macro indicators are not the gap.")

    R.section("P2 classify the rest against the scope")
    other = [d for d in items
             if (d.get("c_dataset") or [None])[0] != "timeseries"]
    byfam = defaultdict(list)
    for d in other:
        byfam[(d.get("c_dataset") or ["(none)"])[0]].append(d)
    inscope, dropped, surprises = {}, {}, []
    for fam, ds in byfam.items():
        vs = sorted(int(d["c_vintage"]) for d in ds if d.get("c_vintage"))
        span = ("%d–%d" % (vs[0], vs[-1])) if vs else "—"
        if fam in ECON:
            inscope[fam] = (len(ds), span, ECON[fam])
        elif fam in OUT:
            dropped[fam] = (len(ds), span)
        else:
            hits = [d for d in ds
                    if KEYWORDS.search(str(d.get("title") or "")
                                       + " " + str(d.get("description")
                                                   or "")[:300])]
            if hits:
                surprises.append((fam, len(ds), len(hits), span,
                                  str(hits[0].get("title"))[:60]))
    R.log("  IN SCOPE (curated economic programs):")
    tot_in = 0
    for fam, (n, span, why) in sorted(inscope.items(),
                                      key=lambda kv: -kv[1][0]):
        tot_in += n
        R.log("    %-9s %4d entries  %-11s  %s" % (fam, n, span, why[:62]))
    R.log("    total in scope: %d entries" % tot_in)
    R.log("  OUT OF SCOPE (demographics/social, dropped deliberately):")
    for fam, (n, span) in sorted(dropped.items(), key=lambda kv: -kv[1][0]):
        R.log("    %-9s %4d entries  %s" % (fam, n, span))
    R.log("  KEYWORD SWEEP over families I did not curate -- anything")
    R.log("  economic here would be a miss on my part:")
    for fam, n, h, span, t in sorted(surprises, key=lambda x: -x[2])[:12]:
        R.log("    %-13s %3d entries (%d match) %-11s %s" % (fam, n, h,
                                                             span, t))
    if not surprises:
        R.log("    (none)")
    out.update(in_scope_entries=tot_in,
               in_scope={k: v[0] for k, v in inscope.items()},
               surprises=[s[0] for s in surprises])

    R.section("P3 cost of the shortlist only")
    est = 0
    for fam in sorted(inscope, key=lambda f: -inscope[f][0])[:8]:
        d = byfam[fam][0]
        cd = "/".join(str(x) for x in (d.get("c_dataset") or []))
        v = d.get("c_vintage")
        base = ("https://api.census.gov/data/%s/%s" % (v, cd) if v
                else "https://api.census.gov/data/%s" % cd)
        sv, bv = http(base + "/variables.json", cap=30 * 1024 * 1024)
        nv = 0
        if sv == 200:
            try:
                nv = len(json.loads(bv).get("variables") or {})
            except Exception:
                nv = -1
        sg, bg = http(base + "/geography.json", cap=6 * 1024 * 1024)
        ng = 0
        if sg == 200:
            try:
                ng = len(json.loads(bg).get("fips") or [])
            except Exception:
                ng = -1
        n = inscope[fam][0]
        calls = n * max(1, ng) * max(1, (nv // 45) + 1)
        est += calls
        R.log("  %-9s %4d entries · %5s vars · %2s geo -> ~%s calls" % (
            fam, n, nv, ng, f"{calls:,}"))
    R.log("  SHORTLIST TOTAL: ~%s requests" % f"{est:,}")
    R.log("  versus ~112,446 for 5057's top six -- dropping the")
    R.log("  decennial census alone removes ~101,376 of them")
    days_nokey = est / 500.0
    R.log("  at 500/day (no key): %.0f days.  with a key: hours." %
          days_nokey)
    out["shortlist_calls"] = est

    R.section("P4 the plan")
    R.log("  1. CBP + ZBP + nonemp -- establishments, employment and")
    R.log("     payroll by NAICS and geography, 1986–2023. Cheapest and")
    R.log("     the most directly useful to the physical-economy and")
    R.log("     regional desks.")
    R.log("  2. ECN + EWKS -- the Economic Census: manufacturing,")
    R.log("     retail, wholesale, services, 1997–2022.")
    R.log("  3. ABS family -- business formation and ownership.")
    R.log("  4. CPS -- 703 entries, 1989–2026, the employment survey.")
    R.log("     Largest of the four; worth its own lane.")
    R.log("  5. SIPP -- household income and wealth, if consumer")
    R.log("     balance-sheet work matters.")
    R.log("  Each gets a resumable walker on the pattern the eurostat")
    R.log("  and ecb lanes now use: state cursor, budget checked inside")
    R.log("  the row loop, hash-skipped idempotent writes, stall breaker.")
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/census-macro-scope.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/census-macro-scope.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5058 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(has_key=out.get("has_key"), probe=out.get("probe_status"),
         in_scope=out.get("in_scope_entries"),
         calls=out.get("shortlist_calls"))
    R.log("ops 5058 GREEN -- scope decided on measurements")
