"""ops 4677 — two lanes for the 2017-2023 ICE hole.

LANE A (Wayback, strongest): 4676's availability probe confirmed an
archived snapshot of fredgraph.csv?id=BAMLH0A0HYM2 EXISTS (status 200).
If Wayback captured that endpoint before FRED's Apr-2026 truncation,
the capture holds the FULL 1996->capture-date history as FRED itself
served it — which closes the hole AND retroactively validates the 140
GMS_VAAS series we could never splice-check. Query the CDX index for
every BAML capture, pull the best pre-truncation one per test series.

LANE B (investing.com, Khalid's lead): probe their public surfaces for
a comparable credit-spread series with deep history. Honest about bot
protection — a Cloudflare challenge page is reported as a block, not a
success.

Probes only. No banked doc is touched; findings land in the sweep doc.
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
                "Accept": "text/html,application/json,text/csv,*/*",
                "Accept-Language": "en-US,en;q=0.9"})
            r = urllib.request.urlopen(rq, timeout=40)
            b = r.read()
            out.append([nm, "OK", len(b),
                        r.headers.get("Content-Type", "")[:40],
                        b[:900].decode("utf-8", "replace")])
        except Exception as e:
            out.append([nm, "ERR", 0, "", str(e)[:150]])
    return out
"""

CDX = ("http://web.archive.org/cdx/search/cdx?url=fred.stlouisfed.org/"
       "graph/fredgraph.csv*&matchType=prefix&filter=urlkey:.*baml.*"
       "&collapse=urlkey&limit=200&output=json&fl=timestamp,original,"
       "statuscode,length")

PROBES_A = [
    ("wayback CDX BAML captures", CDX),
    ("wayback CDX any fredgraph",
     "http://web.archive.org/cdx/search/cdx?url=fred.stlouisfed.org/"
     "graph/fredgraph.csv*&matchType=prefix&limit=40&output=json"
     "&fl=timestamp,original,statuscode,length"),
    ("wayback avail BAMLC0A2CAA",
     "http://archive.org/wayback/available?url=fred.stlouisfed.org/"
     "graph/fredgraph.csv%3Fid%3DBAMLC0A2CAA&timestamp=20230101"),
]

PROBES_B = [
    ("investing search HY OAS",
     "https://api.investing.com/api/search/v2/search?q=high%20yield"
     "%20option%20adjusted%20spread"),
    ("investing econ indicator page",
     "https://www.investing.com/rates-bonds/"),
    ("investing api instruments",
     "https://api.investing.com/api/financialdata/historical/"
     "1058?start-date=1996-01-01&end-date=2023-08-14&time-frame=Daily"),
]


def main():
    with report("4677_ice_gap_lanes") as r:
        r.heading("ops 4677 — Wayback CDX + investing.com for the "
                  "2017-2023 ICE hole")

        fn = "justhodl-ice-lanes-tmp"
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
            MemorySize=512, Description="ops 4677 temp lanes probe")
        for _ in range(30):
            if lam.get_function(FunctionName=fn)["Configuration"] \
                    .get("State") == "Active":
                break
            time.sleep(4)

        def run(batch):
            got = []
            for i in range(0, len(batch), 3):
                resp = lam.invoke(
                    FunctionName=fn, InvocationType="RequestResponse",
                    Payload=json.dumps(
                        {"probes": batch[i:i + 3]}).encode())
                got += json.loads(resp["Payload"].read())
            return got

        caps = []
        try:
            r.section("LANE A — Wayback CDX index")
            ra = run(PROBES_A)
            for nm, stt, ln, ct, txt in ra:
                if stt != "OK":
                    r.log("  %-30s ERR %s" % (nm, txt[:100]))
                    continue
                r.log("  %-30s %d bytes | %s"
                      % (nm, ln, txt.replace("\n", " ")[:200]))
                if "CDX BAML" in nm:
                    try:
                        rows = json.loads(txt)
                        for row in rows[1:]:
                            caps.append(row)
                    except Exception as e:
                        r.log("      (cdx parse: %s)" % str(e)[:70])
            r.log("  BAML captures indexed: %d" % len(caps))
            for row in caps[:15]:
                r.log("    %s" % (row,))

            r.section("LANE A2 — pull the best pre-truncation capture")
            fetches = []
            best = {}
            for row in caps:
                try:
                    ts, orig, code = row[0], row[1], row[2]
                except Exception:
                    continue
                if str(code) != "200":
                    continue
                sid = ""
                if "id=" in orig:
                    sid = orig.split("id=")[-1].split("&")[0]
                if not sid.upper().startswith("BAML"):
                    continue
                if ts < "20260401":     # pre-truncation
                    if sid not in best or ts > best[sid][0]:
                        best[sid] = (ts, orig)
            r.log("  distinct BAML series with pre-truncation "
                  "captures: %d" % len(best))
            for sid, (ts, orig) in list(best.items())[:12]:
                fetches.append(("wb:%s@%s" % (sid, ts),
                                "http://web.archive.org/web/%sid_/%s"
                                % (ts, orig)))
            if fetches:
                rb = run(fetches[:9])
                for nm, stt, ln, ct, txt in rb:
                    if stt != "OK":
                        r.log("  %-26s ERR %s" % (nm, txt[:90]))
                        continue
                    lines = [x for x in txt.splitlines()
                             if x[:2] in ("19", "20")]
                    r.log("  %-26s %d bytes rows~%d first=%s"
                          % (nm, ln, len(lines),
                             lines[0][:24] if lines else None))
            else:
                r.log("  no pre-truncation BAML captures to fetch")

            r.section("LANE B — investing.com (Khalid's lead)")
            rc = run(PROBES_B)
            for nm, stt, ln, ct, txt in rc:
                flat = txt.replace("\n", " ")[:220]
                if stt != "OK":
                    r.log("  %-30s ERR %s" % (nm, txt[:110]))
                    continue
                blocked = ("cf-browser" in txt or "captcha" in
                           txt.lower() or "Just a moment" in txt)
                r.log("  %-30s %d bytes%s | %s"
                      % (nm, ln, " [BOT-WALL]" if blocked else "",
                         flat))
        finally:
            try:
                lam.delete_function(FunctionName=fn)
                r.log("  (temp probe deleted)")
            except Exception:
                pass

        r.section("verdict")
        try:
            doc = json.loads(s3.get_object(
                Bucket=B,
                Key="data/ice-source-sweep.json")["Body"].read())
        except Exception:
            doc = {}
        doc["wayback"] = {"captures_indexed": len(caps),
                          "series_with_pre_truncation": len(best),
                          "checked_at": time.strftime(
                              "%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        s3.put_object(Bucket=B, Key="data/ice-source-sweep.json",
                      Body=json.dumps(doc, default=str).encode(),
                      ContentType="application/json",
                      CacheControl="no-cache")
        if not caps:
            r.fail("Wayback holds no indexed BAML csv captures — "
                   "lane closed, recorded honestly")
            sys.exit(1)
        r.ok("Wayback indexed %d BAML captures (%d series "
             "pre-truncation) — recovery lane is REAL"
             % (len(caps), len(best)))


if __name__ == "__main__":
    main()
