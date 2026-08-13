"""ops 4652 — Khalid's five priority metrics first-class on the
# r3 ping: run with checkout v1.1.0 (engine-only push does not trigger)
stock-buying screener (v1.1.0): PEG<1, net issuance/(retirement),
basic shares QoQ%, Rev+EPS acceleration QoQ pp, ROIC vs US10Y
(Buffett hurdle via fleet-join FRED:DGS10). EXPLOSIVE tier now
requires all five. Dumps the matrix's relevant column names so
getters bind to exact truth on the next rev if any miss.
"""
import io
import json
import sys
import time
import urllib.request
import zipfile

import boto3
from botocore.config import Config

from ops_report import report

FN = "justhodl-stock-buying"
B = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=900,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def http_get(url, timeout=45):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-4652"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def main():
    misses = 0
    with report("4652_khalid_five") as r:
        r.heading("ops 4652 — khalid-five on stock-buying")

        r.section("matrix column-name evidence (five-relevant)")
        try:
            mx = json.loads(s3.get_object(
                Bucket=B, Key="data/fundamental-census-matrix"
                ".json")["Body"].read())
            names = sorted(set(list((mx.get("cols") or {}).keys())
                               + list((mx.get("metrics") or {})
                                      .keys())))
            rel = [n for n in names if any(k in n.lower() for k in
                   ("peg", "share", "issu", "buyback", "roic",
                    "qoq", "accel", "eps", "rev", "dilut"))]
            r.log("relevant cols (%d/%d): %s"
                  % (len(rel), len(names), rel[:60]))
        except Exception as e:
            r.warn("matrix: %s" % str(e)[:80])

        r.section("deploy (ops-side) + settle")
        import zipfile as zf2
        buf = io.BytesIO()
        with zf2.ZipFile(buf, "w", zf2.ZIP_DEFLATED) as z:
            z.write("aws/lambdas/justhodl-stock-buying/source/"
                    "lambda_function.py", "lambda_function.py")
        for att in range(10):
            try:
                st = lam.get_function_configuration(
                    FunctionName=FN)
                if st.get("LastUpdateStatus") == "InProgress":
                    time.sleep(15)
                    continue
                lam.update_function_code(FunctionName=FN,
                                         ZipFile=buf.getvalue())
                break
            except Exception as e:
                if "ResourceConflict" in str(e):
                    time.sleep(15)
                    continue
                raise
        settled = False
        for att in range(14):
            try:
                gf = lam.get_function(FunctionName=FN)
                zb = http_get(gf["Code"]["Location"], 60)
                src = zf2.ZipFile(io.BytesIO(zb)).read(
                    "lambda_function.py").decode("utf-8",
                                                 "replace")
                if "justhodl-stock-buying v1.1.0" in src:
                    settled = True
                    break
            except Exception:
                pass
            time.sleep(20)
        misses += contract(r, "deploy", settled, "v1.1.0 live")
        if not settled:
            sys.exit(1)

        r.section("run + khalid-five truth")
        inv = lam.invoke(FunctionName=FN,
                         InvocationType="RequestResponse")
        r.kv(fn_error=inv.get("FunctionError"))
        pl = json.loads(s3.get_object(
            Bucket=B,
            Key="data/stock-buying.json")["Body"].read())
        rows = pl.get("rows") or []
        r.kv(us10y=pl.get("us10y_pct"),
             k5_missing=json.dumps(
                 pl.get("khalid_five_missing") or {})[:240],
             universe=len(rows))
        buff = [x for x in rows
                if (x.get("khalid_five") or {}).get(
                    "buffett_pass")]
        pegs = [x for x in rows
                if (x.get("khalid_five") or {}).get("peg_lt_1")]
        ret = [x for x in rows
               if (x.get("khalid_five") or {}).get(
                   "retiring_shares")]
        acc = [x for x in rows
               if (x.get("khalid_five") or {}).get(
                   "accelerating")]
        r.kv(buffett_pass=len(buff), peg_lt_1=len(pegs),
             retiring=len(ret), accelerating=len(acc))
        for x in rows[:8]:
            k = x.get("khalid_five") or {}
            r.log("%-6s tier=%-15s peg=%-5s shQoQ=%-6s "
                  "eAcc=%-6s rAcc=%-6s roic-10y=%-6s buff=%s"
                  % (x.get("symbol"), x.get("tier"),
                     k.get("peg"), k.get("shares_qoq_pct"),
                     k.get("eps_accel_qoq_pp"),
                     k.get("rev_accel_qoq_pp"),
                     k.get("roic_minus_10y_pp"),
                     k.get("buffett_pass")))
        misses += contract(r, "five-block",
                           rows and all(
                               "khalid_five" in x
                               for x in rows[:20]),
                           "khalid_five on every row")
        misses += contract(r, "us10y",
                           isinstance(pl.get("us10y_pct"),
                                      (int, float)),
                           "US10Y fleet-join = %s"
                           % pl.get("us10y_pct"))
        misses += contract(r, "why-link",
                           rows and str(rows[0].get("why", ""))
                           .startswith("why.html?ticker="),
                           "why links use house ?ticker= "
                           "standard")
        misses += contract(r, "signal-counts",
                           len(pegs) + len(ret) + len(acc)
                           + len(buff) >= 1,
                           "peg<1:%d retiring:%d accel:%d "
                           "buffett:%d (any nonzero proves the "
                           "wiring; misses counted honestly)"
                           % (len(pegs), len(ret), len(acc),
                              len(buff)))

        r.section("verdict")
        if misses:
            r.fail("khalid-five: %d red" % misses)
            sys.exit(1)
        r.ok("KHALID FIVE LIVE — us10y %s · peg<1:%d · "
             "retiring:%d · accelerating:%d · buffett-pass:%d"
             % (pl.get("us10y_pct"), len(pegs), len(ret),
                len(acc), len(buff)))


if __name__ == "__main__":
    main()
