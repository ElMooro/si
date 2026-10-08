"""ops 4676 — can ANY of our providers serve ICE BofA history?

Khalid: "check all our data providers to see if we can download them
from them." Target: the 2017-2023 hole on 140 series + the ~46 EM
series still at 2023-only.

Method: probe from inside AWS (open egress) against every provider
whose domain could plausibly carry corporate credit spreads, using
each one's REAL query grammar — plus a handful of external mirrors
that redistribute FRED. Verdict per provider is decided by whether a
dated pre-2020 payload comes back, not by whether the endpoint 200s.

No writes to banked data; the only artifact is the findings doc.
"""
import json
import io
import sys
import time
import zipfile

import boto3
from botocore.config import Config

from ops_report import report

B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=600,
                                 retries={"max_attempts": 1}))

PROBE_FN = """
import json, urllib.request
def lambda_handler(event, context):
    out = []
    for nm, u in event["probes"]:
        try:
            rq = urllib.request.Request(u, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; "
                              "x64) AppleWebKit/537.36 (KHTML, like "
                              "Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Accept": "*/*"})
            r = urllib.request.urlopen(rq, timeout=30)
            b = r.read()
            out.append([nm, "OK", len(b),
                        r.headers.get("Content-Type", "")[:40],
                        b[:700].decode("utf-8", "replace")])
        except Exception as e:
            out.append([nm, "ERR", 0, "", str(e)[:150]])
    return out
"""

# Each probe asks for a KNOWN ICE series with explicit deep history.
PROBES = [
    # ── our own providers, real grammars ──
    ("dbnomics FRED/BAMLH0A0HYM2",
     "https://api.db.nomics.world/v22/series/FRED/BAMLH0A0HYM2"
     "?observations=1"),
    ("dbnomics search BAML",
     "https://api.db.nomics.world/v22/search?q=BAMLH0A0HYM2&limit=5"),
    ("dbnomics provider list",
     "https://api.db.nomics.world/v22/providers"),
    ("fed-board DDP corporate",
     "https://www.federalreserve.gov/datadownload/Output.aspx"
     "?rel=H15&series=bf17364827e38702b42a58cf8eaa3f78&lastobs="
     "&from=01/01/1996&to=12/31/2023&filetype=csv&label=include"
     "&layout=seriescolumn"),
    ("ecb SDMX corporate spread flow",
     "https://data-api.ecb.europa.eu/service/data/FM/"
     "D.U2.EUR.RT.MM.EURIBOR3MD_.HSTA?format=jsondata"
     "&startPeriod=1996-01-01"),
    ("bis stats dataflows",
     "https://stats.bis.org/api/v1/dataflow"),
    ("boe IADB corporate",
     "https://www.bankofengland.co.uk/boeapps/database/_iadb-"
     "FromShowColumns.asp?Travel=NIxAZxSUx&FromSeries=1&ToSeries=50"
     "&DAT=RNG&FD=1&FM=Jan&FY=1996&TD=31&TM=Dec&TY=2023&VFD=Y"
     "&CSVF=TT&C=13T&Filter=N"),
    ("yahoo HYG history (proxy)",
     "https://query1.finance.yahoo.com/v8/finance/chart/HYG"
     "?period1=852076800&period2=1700000000&interval=1d"),
    ("stlouisfed fredgraph.csv direct",
     "https://fred.stlouisfed.org/graph/fredgraph.csv"
     "?id=BAMLH0A0HYM2&cosd=1996-01-01"),
    ("stlouisfed fredgraph BAMLC0A2CAA",
     "https://fred.stlouisfed.org/graph/fredgraph.csv"
     "?id=BAMLC0A2CAA&cosd=1996-01-01"),
    # ── external redistributors worth one shot each ──
    ("nasdaq data link FRED table",
     "https://data.nasdaq.com/api/v3/datasets/FRED/"
     "BAMLH0A0HYM2.csv?start_date=1996-01-01"),
    ("stooq mirror",
     "https://stooq.com/q/d/l/?s=BAMLH0A0HYM2&i=d"),
    ("econdb FRED mirror",
     "https://www.econdb.com/api/series/BAMLH0A0HYM2/?format=json"),
    ("wayback fredgraph 2022",
     "http://archive.org/wayback/available?url=fred.stlouisfed.org/"
     "graph/fredgraph.csv%3Fid%3DBAMLH0A0HYM2&timestamp=20220601"),
]


def verdict(txt, ct):
    """Deep = a dated payload containing a pre-2020 date."""
    import re as _re
    ds = _re.findall(r"(19[89]\d|20[01]\d)-\d{2}-\d{2}", txt)
    return (sorted(ds)[0] if ds else None)


def main():
    with report("4676_provider_ice_sweep") as r:
        r.heading("ops 4676 — every provider probed for ICE history")

        fn = "justhodl-ice-sweep-tmp"
        role = lam.get_function(
            FunctionName="justhodl-ofr-stfm")["Configuration"]["Role"]
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            z.writestr("lambda_function.py", PROBE_FN)
        try:
            lam.delete_function(FunctionName=fn)
            time.sleep(3)
        except Exception:
            pass
        lam.create_function(
            FunctionName=fn, Runtime="python3.12", Role=role,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": buf.getvalue()}, Timeout=280,
            MemorySize=512, Description="ops 4676 temp ICE sweep")
        for _ in range(30):
            if lam.get_function(FunctionName=fn)["Configuration"] \
                    .get("State") == "Active":
                break
            time.sleep(4)
        res = []
        try:
            for i in range(0, len(PROBES), 4):
                resp = lam.invoke(
                    FunctionName=fn, InvocationType="RequestResponse",
                    Payload=json.dumps(
                        {"probes": PROBES[i:i + 4]}).encode())
                res += json.loads(resp["Payload"].read())
        finally:
            try:
                lam.delete_function(FunctionName=fn)
                r.log("  (temp probe deleted)")
            except Exception:
                pass

        r.section("Results — earliest date each source will serve")
        wins = []
        for nm, stt, ln, ct, txt in res:
            if stt != "OK":
                r.log("  %-34s ERR %s" % (nm, txt[:90]))
                continue
            first = verdict(txt, ct)
            flat = txt.replace("\n", " ").replace("\r", " ")[:150]
            if first and first < "2020":
                wins.append((nm, first, ln))
                r.ok("  %-34s %d bytes · EARLIEST %s | %s"
                     % (nm, ln, first, flat))
            else:
                r.log("  %-34s %d bytes · earliest=%s | %s"
                      % (nm, ln, first, flat))

        r.section("Verdict")
        if wins:
            for nm, first, ln in wins:
                r.ok("  USABLE: %s -> back to %s (%d bytes)"
                     % (nm, first, ln))
        else:
            r.log("  No provider in our fleet serves pre-2020 ICE "
                  "history. The 2017-2023 hole is not closable from "
                  "existing rails — TradingView (session-auth, in "
                  "build) and ICE's own trial remain the routes.")
        doc = {"as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                      time.gmtime()),
               "question": "which provider can serve ICE BofA "
                           "pre-2020 history",
               "usable": [{"source": a, "earliest": b, "bytes": c}
                          for a, b, c in wins],
               "probed": [[a, b, c] for a, b, c, _d, _e in res]}
        s3.put_object(Bucket=B, Key="data/ice-source-sweep.json",
                      Body=json.dumps(doc, default=str).encode(),
                      ContentType="application/json",
                      CacheControl="no-cache")
        r.ok("  published data/ice-source-sweep.json")
        if not wins:
            r.fail("no usable provider found — recorded honestly")
            sys.exit(1)
        r.ok("%d usable source(s) found" % len(wins))


if __name__ == "__main__":
    main()
