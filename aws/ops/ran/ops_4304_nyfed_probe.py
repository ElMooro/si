"""ops_4304 -- NY Fed PD endpoint-form matrix + OFR mnemonic truth.
The rehypo engine's catalog discovery works (30 keyids) but every
series fetch returns empty. Probe the URL forms against one real
fails keyid + list seriesbreaks; print OFR's actual REPO-* rate
mnemonics. Pure evidence op; the fix ships next."""
import gzip, json, sys, urllib.request
from ops_report import report
UA = {"User-Agent": "JustHodl-research/1.0"}

def get(url, gz=False):
    raw = urllib.request.urlopen(
        urllib.request.Request(url, headers=UA), timeout=40).read()
    if gz or raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return raw

with report("4304_nyfed_probe") as r:
    r.heading("ops 4304 -- endpoint truth for FR2004 + OFR")
    cat = json.loads(get("https://markets.newyorkfed.org/api/pd/"
                         "list/timeseries.json"))
    rows = (cat.get("pd") or {}).get("timeseries") or []
    fails = [x for x in rows
             if "FAIL" in (x.get("description") or "").upper()
             and "TREASUR" in (x.get("description") or "").upper()]
    k = fails[0]["keyid"] if fails else rows[0]["keyid"]
    r.log("probe keyid: %s (%s)" % (k, (fails[0].get("description")
          if fails else "?")[:60]))
    r.log("catalog row keys: %s" % list((fails[0] if fails
                                         else rows[0]).keys()))
    try:
        sb = json.loads(get("https://markets.newyorkfed.org/api/pd/"
                            "list/seriesbreaks.json"))
        breaks = [b.get("seriesbreak") or b.get("label") or b
                  for b in ((sb.get("pd") or {}).get("seriesbreaks")
                            or sb.get("seriesbreaks") or [])]
        r.log("seriesbreaks: %s" % breaks[:12])
    except Exception as e:
        breaks = []
        r.warn("seriesbreaks list: %s" % str(e)[:80])
    forms = [
        ("all", "https://markets.newyorkfed.org/api/pd/get/all/"
                "timeseries/%s.json" % k),
        ("bare", "https://markets.newyorkfed.org/api/pd/get/"
                 "timeseries/%s.json" % k),
    ] + [("sb:%s" % b, "https://markets.newyorkfed.org/api/pd/get/"
         "%s/timeseries/%s.json" % (b, k)) for b in breaks[-3:]]
    for name, url in forms:
        try:
            doc = json.loads(get(url))
            ts = (doc.get("pd") or {}).get("timeseries") or []
            r.log("%-10s -> %d rows %s" % (name, len(ts),
                  ("sample " + json.dumps(ts[-1])[:110]) if ts
                  else json.dumps(doc)[:110]))
        except Exception as e:
            r.log("%-10s -> ERR %s" % (name, str(e)[:80]))
    try:
        m = json.loads(get("https://data.financialresearch.gov/v1/"
                           "metadata/mnemonics", gz=True))
        ms = [str(x if isinstance(x, str) else x.get("mnemonic"))
              for x in (m if isinstance(m, list)
                        else m.get("mnemonics") or [])]
        ar = [x for x in ms if x.startswith("REPO-") and "_AR" in x]
        tv = [x for x in ms if x.startswith("REPO-") and "_TV" in x]
        r.log("OFR AR mnemonics (%d): %s" % (len(ar), ar[:14]))
        r.log("OFR TV mnemonics (%d): %s" % (len(tv), tv[:10]))
    except Exception as e:
        r.warn("OFR metadata: %s" % str(e)[:90])
    if not rows:
        r.fail("catalog empty")
        sys.exit(1)
    r.ok("probe complete")
