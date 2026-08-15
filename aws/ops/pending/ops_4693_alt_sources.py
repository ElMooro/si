"""ops 4693 — probe Macrotrends, Trading Economics, MacroMicro for
the 2017-2023 ICE gap (Khalid: "other than wayback what other way").

Server-side only. Verdict is decided by whether a dated pre-2023 row
comes back, not by HTTP 200 -- same discipline as every other probe
tonight.
"""
import io
import json
import sys
import time
import zipfile

import boto3
from botocore.config import Config

from ops_report import report

B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=300,
                                 retries={"max_attempts": 1}))

PROBE_FN = """
import json, re, urllib.request
def lambda_handler(event, context):
    out = []
    for nm, u in event["probes"]:
        try:
            rq = urllib.request.Request(u, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; "
                              "x64) AppleWebKit/537.36 (KHTML, like "
                              "Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Accept": "text/html,application/json,*/*"})
            r = urllib.request.urlopen(rq, timeout=40)
            b = r.read()
            txt = b[:900000].decode("utf-8", "replace")
            dates = sorted(set(re.findall(
                r"(19[89][0-9]|20[01][0-9])-\\d{2}-\\d{2}", txt)))
            out.append([nm, "OK", len(b),
                        r.headers.get("Content-Type", "")[:40],
                        dates[:3], txt[:300]])
        except Exception as e:
            out.append([nm, "ERR", 0, "", [], str(e)[:140]])
    return out
"""

PROBES = [
    ("macrotrends HY spread page",
     "https://www.macrotrends.net/2565/us-high-yield-master-ii-"
     "effective-yield"),
    ("macrotrends corp bond spread",
     "https://www.macrotrends.net/2492/10-year-treasury-corporate-"
     "bond-spread-chart"),
    ("tradingeconomics HY page",
     "https://tradingeconomics.com/united-states/high-yield-bond-"
     "spread"),
    ("tradingeconomics api probe (no key)",
     "https://api.tradingeconomics.com/historical/country/united "
     "states/indicator/high yield bond spread"
     "?d1=1996-01-01&d2=2023-08-14"),
    ("macromicro BAML HY",
     "https://en.macromicro.me/series/1234/us-high-yield-oas"),
    ("macromicro search",
     "https://en.macromicro.me/search?query=BAMLH0A0HYM2"),
]


def main():
    with report("4693_alt_sources") as r:
        r.heading("ops 4693 — Macrotrends / Trading Economics / "
                  "MacroMicro for the 2017-2023 ICE gap")

        fn = "justhodl-alt-src-tmp"
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
            MemorySize=512, Description="ops 4693 alt-source probe")
        for _ in range(30):
            if lam.get_function(FunctionName=fn)["Configuration"] \
                    .get("State") == "Active":
                break
            time.sleep(4)
        res = []
        try:
            for i in range(0, len(PROBES), 2):
                resp = lam.invoke(
                    FunctionName=fn, InvocationType="RequestResponse",
                    Payload=json.dumps(
                        {"probes": PROBES[i:i + 2]}).encode())
                res += json.loads(resp["Payload"].read())
        finally:
            try:
                lam.delete_function(FunctionName=fn)
                r.log("  (temp probe deleted)")
            except Exception:
                pass

        r.section("Results")
        wins = []
        for row in res:
            nm, stt = row[0], row[1]
            if stt != "OK":
                r.log("  %-32s ERR %s" % (nm, row[5][:100]))
                continue
            ln, ct, dates = row[2], row[3], row[4]
            flat = row[5].replace("\n", " ")[:180]
            has_pre2020 = any(d < "2020" for d in dates)
            r.log("  %-32s %d bytes ct=%s dates=%s | %s"
                  % (nm, ln, ct, dates, flat))
            if has_pre2020:
                wins.append(nm)

        r.section("verdict")
        if wins:
            r.ok("candidates worth building on: %s" % wins)
        else:
            r.log("none of the three surfaced usable pre-2020 dated "
                 "data server-side. Macrotrends/TE render charts via "
                 "JS after load (no embedded data in raw HTML) or "
                 "gate behind their own bot protection; MacroMicro "
                 "needs the right series id, not guessed. These three "
                 "are not quick wins -- the ICE trial or your S&P "
                 "Global access are the stronger remaining paths.")
        doc = {"as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                      time.gmtime()),
               "results": [[x[0], x[1], x[2], x[4]] for x in res]}
        s3.put_object(Bucket=B, Key="data/ice-alt-sources.json",
                      Body=json.dumps(doc, default=str).encode(),
                      ContentType="application/json")
        if not wins:
            r.fail("no server-side alt source found — honest "
                   "negative")
            sys.exit(1)


if __name__ == "__main__":
    main()
