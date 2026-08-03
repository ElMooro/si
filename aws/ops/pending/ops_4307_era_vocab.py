"""ops_4307 -- FR2004 era vocabulary probe. The long stitch reached
only 2020 because each seriesbreak era words its descriptions
differently and only SBN2024 matched modern tokens. Print, per break:
row count + sample descriptions for FAIL / REPURCHASE / NET /
SECURITIES / GOVERNMENT so v1.3's per-era token map is built from
evidence. Also test-fetch one OLD-era keyid to confirm the endpoint
serves history that deep."""
import json, sys, urllib.request
from ops_report import report
UA = {"User-Agent": "JustHodl-research/1.0"}

def get(url):
    return urllib.request.urlopen(
        urllib.request.Request(url, headers=UA), timeout=40).read()

with report("4307_era_vocab") as r:
    r.heading("ops 4307 -- what each FR2004 era calls things")
    cat = json.loads(get("https://markets.newyorkfed.org/api/pd/"
                         "list/timeseries.json"))
    rows = (cat.get("pd") or {}).get("timeseries") or []
    by = {}
    for x in rows:
        by.setdefault(x.get("seriesbreak"), []).append(
            (x.get("keyid"), (x.get("description") or "").upper()))
    for sb in ["SBP2001", "SBP2013", "SBN2013", "SBN2015",
               "SBN2022", "SBN2024"]:
        lst = by.get(sb, [])
        r.section("%s -- %d keyids" % (sb, len(lst)))
        for tok in ("FAIL", "REPURCHASE", "NET", "SECURITIES",
                    "GOVERNMENT", "TREASUR"):
            hits = [d for _, d in lst if tok in d][:4]
            r.log("%-11s %d | %s"
                  % (tok, len([1 for _, d in lst if tok in d]),
                     " ||| ".join(h[:58] for h in hits)))
    old = [k for k, d in by.get("SBP2001", [])
           if "FAIL" in d or "REPURCHASE" in d][:1] or \
          [k for k, _ in by.get("SBP2001", [])[:1]]
    if old:
        try:
            doc = json.loads(get(
                "https://markets.newyorkfed.org/api/pd/get/SBP2001/"
                "timeseries/%s.json" % old[0]))
            ts = (doc.get("pd") or {}).get("timeseries") or []
            r.ok("SBP2001 fetch %s -> %d rows, first %s last %s"
                 % (old[0], len(ts),
                    ts[0].get("asofdate") if ts else None,
                    ts[-1].get("asofdate") if ts else None))
        except Exception as e:
            r.warn("SBP2001 fetch: %s" % str(e)[:80])
    if not rows:
        r.fail("catalog empty")
        sys.exit(1)
    r.ok("era vocab probe complete")
