"""ops 4680 — Wayback ICE recovery, no browser required.

Khalid's research points at archived FRED pages + a Highcharts console
scrape. Same archive, autonomous route: FRED serves every series as a
STATIC text file at fred.stlouisfed.org/data/<ID>.txt (the exact format
the GMS_VAAS archive used — header block then 'YYYY-MM-DD  value'), and
Wayback replays a dated capture directly at
  https://web.archive.org/web/<ts>id_/<url>
That needs no CDX query (which 498'd on us — throttled, not empty) and
no Highcharts. If Wayback holds a 2024/2025 capture of those .txt
pages, it holds FULL inception->capture history as FRED served it
BEFORE the Apr-2026 truncation.

Validation is Khalid's own checksum, enforced as a contract: the
recovered rows must AGREE with our banked FRED values on the
overlapping 2023-08 dates. Disagreement => rejected, not merged.
Merges are union-only; the delete-proof policy keeps them permanent.
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

CORE = ["BAMLH0A0HYM2", "BAMLC0A0CM", "BAMLC0A2CAA", "BAMLC0A3CA",
        "BAMLH0A1HYBB", "BAMLH0A2HYB", "BAMLH0A3HYC",
        "BAMLC0A0CMEY", "BAMLC0A2CAAEY", "BAMLC0A3CAEY",
        "BAMLH0A0HYM2EY", "BAMLH0A1HYBBEY", "BAMLH0A2HYBEY",
        "BAMLH0A3HYCEY", "BAMLCC0A2AATRIV", "BAMLCC0A3ATRIV",
        "BAMLCC0A4BBBTRIV", "BAMLHYH0A1BBTRIV", "BAMLHYH0A2BTRIV",
        "BAMLHYH0A3CMTRIV"]
STAMPS = ["20250601", "20240901", "20240330", "20230901"]

FETCH_FN = """
import json, urllib.request
def lambda_handler(event, context):
    out = []
    for nm, u in event["probes"]:
        try:
            rq = urllib.request.Request(u, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; "
                              "x64) AppleWebKit/537.36 (KHTML, like "
                              "Gecko) Chrome/124.0.0.0 Safari/537.36"})
            r = urllib.request.urlopen(rq, timeout=50)
            b = r.read()
            out.append([nm, "OK", len(b),
                        b[:400000].decode("utf-8", "replace")])
        except Exception as e:
            out.append([nm, "ERR", 0, str(e)[:140]])
    return out
"""


def parse_txt(txt):
    rows = []
    for ln in txt.splitlines():
        if ln[:2] not in ("19", "20"):
            continue
        p = ln.replace(",", " ").split()
        if len(p) >= 2 and p[1] not in (".", ""):
            try:
                float(p[1])
            except ValueError:
                continue
            rows.append({"date": p[0], "value": p[1]})
    return rows


def main():
    with report("4680_wayback_ice") as r:
        r.heading("ops 4680 — Wayback ICE recovery (autonomous)")
        misses = 0

        r.section("1. Locate banked docs")
        want = set(x + ".json" for x in CORE)
        kmap, tok = {}, None
        while True:
            kw = {"Bucket": B, "Prefix": "data/warm/fred-scoped/",
                  "MaxKeys": 1000}
            if tok:
                kw["ContinuationToken"] = tok
            resp = s3.list_objects_v2(**kw)
            for o in resp.get("Contents") or []:
                bn = o["Key"].rsplit("/", 1)[-1]
                if bn in want:
                    kmap[bn[:-5]] = o["Key"]
            if not resp.get("IsTruncated"):
                break
            tok = resp.get("NextContinuationToken")
        r.log("  banked docs found: %d/%d" % (len(kmap), len(CORE)))

        fn = "justhodl-wayback-tmp"
        role = lam.get_function(
            FunctionName="justhodl-ofr-stfm")["Configuration"]["Role"]
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            z.writestr("lambda_function.py", FETCH_FN)
        try:
            lam.delete_function(FunctionName=fn)
            time.sleep(3)
        except Exception:
            pass
        lam.create_function(
            FunctionName=fn, Runtime="python3.12", Role=role,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": buf.getvalue()}, Timeout=300,
            MemorySize=1024, Description="ops 4680 wayback fetch")
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

        try:
            r.section("2. Probe: which capture form works?")
            probe = []
            for ts in STAMPS:
                probe.append(("txt@%s" % ts,
                              "https://web.archive.org/web/%sid_/"
                              "https://fred.stlouisfed.org/data/"
                              "BAMLH0A0HYM2.txt" % ts))
            probe.append(("csv@20250601",
                          "https://web.archive.org/web/20250601id_/"
                          "https://fred.stlouisfed.org/graph/"
                          "fredgraph.csv?id=BAMLH0A0HYM2"))
            best_ts = None
            for nm, stt, ln, txt in run(probe):
                if stt != "OK":
                    r.log("  %-16s ERR %s" % (nm, txt[:100]))
                    continue
                rows = parse_txt(txt)
                r.log("  %-16s %d bytes · rows=%d %s -> %s"
                      % (nm, ln, len(rows),
                         rows[0]["date"] if rows else None,
                         rows[-1]["date"] if rows else None))
                if len(rows) > 4000 and not best_ts:
                    best_ts = nm.split("@")[1]
                    best_kind = nm.split("@")[0]
            if not best_ts:
                misses += 1
                r.fail("  no Wayback capture yielded a deep series — "
                       "lane closed with evidence")
                sys.exit(1)
            r.ok("  WORKING FORM: %s capture @ %s"
                 % (best_kind, best_ts))

            r.section("3. Batch pull + checksum + merge")
            batch = []
            for sid in CORE:
                if best_kind == "txt":
                    u = ("https://web.archive.org/web/%sid_/https://"
                         "fred.stlouisfed.org/data/%s.txt"
                         % (best_ts, sid))
                else:
                    u = ("https://web.archive.org/web/%sid_/https://"
                         "fred.stlouisfed.org/graph/fredgraph.csv"
                         "?id=%s" % (best_ts, sid))
                batch.append((sid, u))
            merged_ok = rejected = empty = 0
            for sid, stt, ln, txt in run(batch):
                key = kmap.get(sid)
                if stt != "OK" or not key:
                    empty += 1
                    r.log("  %-20s %s" % (sid, str(txt)[:90]))
                    continue
                arch = parse_txt(txt)
                if len(arch) < 3000:
                    empty += 1
                    r.log("  %-20s only %d rows — skipped"
                          % (sid, len(arch)))
                    continue
                doc = json.loads(s3.get_object(
                    Bucket=B, Key=key)["Body"].read())
                cur = doc.get("observations") or []
                cmap = dict((o.get("date"), str(o.get("value")))
                            for o in cur)
                ov = [a for a in arch if a["date"] in cmap]
                mm = 0
                for a in ov:
                    try:
                        if abs(float(a["value"])
                               - float(cmap[a["date"]])) > 0.02:
                            mm += 1
                    except Exception:
                        mm += 1
                if not ov:
                    r.log("  %-20s no overlap to checksum — SKIPPED "
                          "(refusing unvalidated merge)" % sid)
                    rejected += 1
                    continue
                if mm > max(2, 0.03 * len(ov)):
                    r.warn("  %-20s CHECKSUM FAIL %d/%d — rejected"
                           % (sid, mm, len(ov)))
                    rejected += 1
                    continue
                keep = [a for a in arch if a["date"] not in cmap]
                new = sorted(keep + cur,
                             key=lambda o: str(o["date"]))
                doc["observations"] = new
                doc["meta"] = dict(doc.get("meta") or {},
                                   recovered_rows=len(keep),
                                   recovered_source="wayback:%s "
                                   "fred/data/%s.txt" % (best_ts, sid),
                                   checksum="%d/%d agree"
                                   % (len(ov) - mm, len(ov)),
                                   history_floor=new[0]["date"])
                doc["meta"].pop("history_gap", None)
                s3.put_object(Bucket=B, Key=key,
                              Body=json.dumps(doc,
                                              default=str).encode(),
                              ContentType="application/json")
                merged_ok += 1
                r.ok("  %-20s +%d rows · checksum %d/%d · %s -> %s "
                     "CONTINUOUS"
                     % (sid, len(keep), len(ov) - mm, len(ov),
                        new[0]["date"], new[-1]["date"]))
        finally:
            try:
                lam.delete_function(FunctionName=fn)
            except Exception:
                pass

        r.section("verdict")
        r.log("  merged=%d rejected=%d empty=%d"
              % (merged_ok, rejected, empty))
        try:
            cov = json.loads(s3.get_object(
                Bucket=B,
                Key="data/repo-coverage.json")["Body"].read())
        except Exception:
            cov = {}
        cov["ice_wayback"] = {"merged": merged_ok,
                              "rejected": rejected,
                              "capture": best_ts,
                              "as_of": time.strftime(
                                  "%Y-%m-%dT%H:%M:%SZ",
                                  time.gmtime())}
        s3.put_object(Bucket=B, Key="data/repo-coverage.json",
                      Body=json.dumps(cov, default=str).encode(),
                      ContentType="application/json")
        if merged_ok < 5:
            r.fail("only %d series closed" % merged_ok)
            sys.exit(1)
        r.ok("%d ICE series now CONTINUOUS inception->today, each "
             "checksum-validated against our own FRED window"
             % merged_ok)


if __name__ == "__main__":
    main()
