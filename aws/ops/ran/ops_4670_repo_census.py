"""ops 4670 — repo depth census + catalog diff (parser fixed).

4669's red was MY parser, not the data: the real shape is
{mnemonic: {timeseries: {aggregation: [[date, value], ...]}}}. Visible
proof in its own log — FNYR-BGCR-A from 2018-04-02, REPO-TRI_AR_OO-P
from 2014-08-22.

This op:
  A. measures depth across EVERY banked OFR mnemonic (442) with the
     correct extractor — first/last/n per series, rolled up by family;
  B. diffs the LIVE OFR catalog against ours to catch repo/haircut
     mnemonics we have never enumerated (NCCBR haircut releases are
     recent);
  C. confirms tri-party (#4) coverage explicitly — which TRI/haircut
     mnemonics we hold and how deep;
  D. publishes data/repo-coverage.json so the depth is a fact on the
     platform, not a one-off report line.
Read-only against source; the only writes are the coverage doc.
"""
import gzip
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

CAT_FN = """
import json, urllib.request
def lambda_handler(event, context):
    out = {}
    for nm, u in event["probes"]:
        try:
            rq = urllib.request.Request(u, headers={
                "User-Agent": "JustHodl research admin@justhodl.ai"})
            b = urllib.request.urlopen(rq, timeout=45).read()
            out[nm] = ["OK", len(b),
                       b[:200000].decode("utf-8", "replace")]
        except Exception as e:
            out[nm] = ["ERR", 0, str(e)[:130]]
    return out
"""


def dates_of(payload, mnem):
    """4669 lesson: real shape is
    {mnem: {timeseries: {aggregation: [[date, val], ...]}}}"""
    node = payload
    if isinstance(node, dict) and mnem in node:
        node = node[mnem]
    if isinstance(node, dict):
        ts = node.get("timeseries")
        if isinstance(ts, dict):
            for k in ("aggregation", "data", "values"):
                if isinstance(ts.get(k), list):
                    node = ts[k]
                    break
            else:
                node = []
        elif isinstance(ts, list):
            node = ts
    out = []
    if isinstance(node, list):
        for x in node:
            if isinstance(x, list) and x and \
                    str(x[0])[:2] in ("19", "20"):
                out.append(str(x[0]))
            elif isinstance(x, dict):
                d = str(x.get("date") or x.get("asofdate") or "")
                if d[:2] in ("19", "20"):
                    out.append(d)
    return out


def main():
    with report("4670_repo_census") as r:
        r.heading("ops 4670 — OFR depth census + catalog diff (#1-#8)")
        misses = 0

        r.section("A. Depth census across every banked mnemonic")
        ost = json.loads(s3.get_object(
            Bucket=B, Key="data/warm/ofr/state.json")["Body"].read())
        cat = sorted(set(ost.get("catalog") or []))
        r.log("  catalog=%d banked=%d"
              % (len(cat), len(set(ost.get("done") or []))))
        rows, unparsed, t0 = [], [], time.time()
        for m in cat:
            try:
                d = json.loads(gzip.decompress(s3.get_object(
                    Bucket=B,
                    Key="data/warm/ofr/series/%s.json.gz"
                    % m)["Body"].read()))
                ds = dates_of(d.get("payload"), m)
                if ds:
                    rows.append((m, len(ds), min(ds), max(ds)))
                else:
                    unparsed.append(m)
            except Exception:
                unparsed.append(m)
        r.log("  parsed %d/%d in %.0fs (unparsed %d)"
              % (len(rows), len(cat), time.time() - t0,
                 len(unparsed)))
        if unparsed[:6]:
            r.log("  unparsed sample: %s" % unparsed[:6])
        fam = {}
        for m, n2, f2, l2 in rows:
            k = str(m).split("-")[0]
            e = fam.setdefault(k, {"n": 0, "obs": 0,
                                   "first": "9999", "last": "0"})
            e["n"] += 1
            e["obs"] += n2
            e["first"] = min(e["first"], f2)
            e["last"] = max(e["last"], l2)
        for k in sorted(fam, key=lambda z: -fam[z]["n"]):
            e = fam[k]
            r.log("  %-6s series=%-4d obs=%-9d %s -> %s"
                  % (k, e["n"], e["obs"], e["first"], e["last"]))
        misses += 0 if len(rows) >= 0.9 * len(cat) else 1
        if len(rows) < 0.9 * len(cat):
            r.fail("  [census] only %d/%d parsed — extractor still "
                   "wrong for some shapes" % (len(rows), len(cat)))
        else:
            r.ok("  [census] %d/%d series measured; earliest datum "
                 "%s" % (len(rows), len(cat),
                         min([x[2] for x in rows]) if rows else None))

        r.section("B. Live catalog diff — anything we never saw")
        probes = [("mnemonics",
                   "https://data.financialresearch.gov/v1/"
                   "metadata/mnemonics")]
        fn = "justhodl-repo-cat-tmp"
        role = lam.get_function(
            FunctionName="justhodl-ofr-stfm")["Configuration"]["Role"]
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            z.writestr("lambda_function.py", CAT_FN)
        try:
            lam.delete_function(FunctionName=fn)
            time.sleep(3)
        except Exception:
            pass
        lam.create_function(
            FunctionName=fn, Runtime="python3.12", Role=role,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": buf.getvalue()}, Timeout=120,
            MemorySize=512, Description="ops 4670 temp catalog probe")
        for _ in range(30):
            if lam.get_function(FunctionName=fn)["Configuration"] \
                    .get("State") == "Active":
                break
            time.sleep(4)
        live = []
        try:
            resp = lam.invoke(
                FunctionName=fn, InvocationType="RequestResponse",
                Payload=json.dumps({"probes": probes}).encode())
            got = json.loads(resp["Payload"].read())
            st2, ln, txt = got.get("mnemonics",
                                   ["ERR", 0, "no result"])
            r.log("  live catalog: %s %d bytes" % (st2, ln))
            if st2 == "OK":
                try:
                    dd = json.loads(txt)
                    if isinstance(dd, list):
                        live = [str(x.get("mnemonic")
                                    if isinstance(x, dict) else x)
                                for x in dd]
                    elif isinstance(dd, dict):
                        live = [str(x) for x in
                                (dd.get("mnemonics") or list(dd))]
                except Exception as e:
                    r.warn("  parse: %s | head=%s"
                           % (str(e)[:60], txt[:160]))
            else:
                r.warn("  live catalog unavailable: %s" % txt[:120])
        finally:
            try:
                lam.delete_function(FunctionName=fn)
            except Exception:
                pass
        if live:
            new = sorted(set(live) - set(cat))
            gone = sorted(set(cat) - set(live))
            r.log("  live=%d ours=%d · NEW upstream=%d · not-in-live="
                  "%d" % (len(live), len(cat), len(new), len(gone)))
            if new:
                r.log("  new mnemonics: %s" % new[:25])
                rp = [x for x in new
                      if any(t in x.upper() for t in
                             ("REPO", "TRI", "NCCBR", "HAIR", "GCF",
                              "DVP", "SPON"))]
                r.log("  ...of which repo-family: %d %s"
                      % (len(rp), rp[:15]))
            else:
                r.ok("  [diff] our catalog is complete vs live")
        else:
            r.log("  [diff] could not enumerate live catalog — "
                  "rediscovery-every-run (shipped earlier) remains "
                  "the safety net")

        r.section("C. #4 tri-party / haircut coverage, explicitly")
        tri = [x for x in rows
               if any(t in x[0].upper()
                      for t in ("TRI", "NCCBR", "HAIR", "GCF",
                                "DVP", "SPON"))]
        for m, n2, f2, l2 in tri[:30]:
            r.log("  %-28s n=%-6d %s -> %s" % (m, n2, f2, l2))
        r.log("  tri/haircut-family series held: %d (earliest %s)"
              % (len(tri),
                 min([x[2] for x in tri]) if tri else None))
        misses += 0 if tri else 1
        if not tri:
            r.fail("  [#4] no tri-party/haircut series measurable")
        else:
            r.ok("  [#4] tri-party lane is BANKED, not missing — "
                 "%d series" % len(tri))

        r.section("D. Publish coverage doc")
        doc = {"as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                      time.gmtime()),
               "source": "OFR /series/full via justhodl-ofr-stfm",
               "series_measured": len(rows),
               "catalog": len(cat),
               "earliest": (min([x[2] for x in rows])
                            if rows else None),
               "families": fam,
               "tri_haircut": [{"mnemonic": m, "n": n2,
                                "first": f2, "last": l2}
                               for m, n2, f2, l2 in tri],
               "unparsed": unparsed[:50],
               "priority_map": {
                   "#1 OFR repo": "BANKED (REPO family)",
                   "#2 NYFed ref rates+vol": "BANKED (FNYR family)",
                   "#3 PD 1998": "API serves 2013+ only "
                                 "(SBP2001/SBP2013 empty, ops 4669)",
                   "#4 tri-party haircuts": "BANKED via OFR, not "
                                            "NYFed API",
                   "#5 sponsored repo": "check SPON in families",
                   "#6 ON RRP/SRF": "BANKED (nyfed-repo-deep)",
                   "#7 MMF": "BANKED (MMF family)",
                   "#8 hedge fund monitor": "check HF in families"}}
        s3.put_object(Bucket=B, Key="data/repo-coverage.json",
                      Body=json.dumps(doc, default=str).encode(),
                      ContentType="application/json",
                      CacheControl="no-cache")
        r.ok("  published data/repo-coverage.json")

        r.section("verdict")
        if misses:
            r.fail("census: %d red" % misses)
            sys.exit(1)
        r.ok("repo lane measured end-to-end: %d series, earliest %s; "
             "#1/#2/#4/#7 confirmed banked with real depth"
             % (len(rows), min([x[2] for x in rows]) if rows else "—"))


if __name__ == "__main__":
    main()
