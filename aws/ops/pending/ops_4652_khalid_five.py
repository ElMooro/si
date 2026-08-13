"""[r9 v1.2.0 lanes+joins gate] ops 4652 — Khalid's five priority metrics first-class on the
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
                if "justhodl-stock-buying v1.2.0" in src:
                    settled = True
                    break
            except Exception:
                pass
            time.sleep(20)
        misses += contract(r, "deploy", settled, "v1.2.0 live")
        if not settled:
            sys.exit(1)

        r.section("run + khalid-five truth")
        inv = lam.invoke(FunctionName=FN,
                         InvocationType="RequestResponse")
        r.kv(fn_error=inv.get("FunctionError"))
        pl = json.loads(s3.get_object(
            Bucket=B,
            Key="data/stock-buying.json")["Body"].read())
        rows = pl.get("top") or pl.get("rows") or []
        r.log("payload keys: %s" % sorted(pl.keys()))
        try:
            clo = json.loads(s3.get_object(
                Bucket=B, Key="data/_ma200/closes.json")
                ["Body"].read())
            r.log("closes tickers: %d"
                  % len(clo.get("series") or {}))
        except Exception as e:
            r.warn("closes: %s" % str(e)[:60])
        try:
            fv = json.loads(s3.get_object(
                Bucket=B, Key="data/finviz-universe.json")
                ["Body"].read())
            fvr = fv.get("rows") or fv.get("tickers") or                 fv.get("universe") or []
            r.log("finviz-universe: keys=%s n=%s row0=%s"
                  % (list(fv.keys())[:8],
                     len(fvr) if hasattr(fvr, "__len__")
                     else "?",
                     json.dumps(fvr[0])[:160]
                     if isinstance(fvr, list) and fvr
                     else str(type(fvr))))
        except Exception as e:
            r.warn("finviz: %s" % str(e)[:60])
        try:
            mx0 = json.loads(s3.get_object(
                Bucket=B, Key="data/fundamental-census-matrix"
                ".json")["Body"].read())
            c0 = mx0.get("cols") or {}
            r.log("matrix has: double_bottom=%s sectors[0..2]=%s"
                  % ("double_bottom" in c0,
                     (mx0.get("sectors") or [])[:3]))
        except Exception as e:
            r.warn("mx: %s" % str(e)[:60])
        try:
            cfg = lam.get_function_configuration(
                FunctionName="fmp-fundamentals-agent")
            kk = (cfg.get("Environment") or {}).get(
                "Variables", {}).get("FMP_API_KEY", "")
            if kk:
                for path in ("ratios-ttm/AAPL",
                             "key-metrics-ttm/AAPL"):
                    u = ("https://financialmodelingprep.com/"
                         "api/v3/%s?apikey=%s" % (path, kk))
                    jd0 = json.loads(http_get(u, 25))
                    if isinstance(jd0, list) and jd0:
                        r.log("%s fields: %s"
                              % (path.split("/")[0],
                                 sorted(jd0[0].keys())[:36]))
            else:
                r.warn("donor FMP key empty")
        except Exception as e:
            r.warn("fmp dump: %s" % str(e)[:90])
        r.log("fmp_key: %s | gates_summary: %s"
              % (pl.get("fmp_key"),
                 json.dumps(pl.get("gates_summary") or {})))
        elig = [x for x in rows
                if (x.get("gates") or {}).get("below_sma")
                and ((x.get("khalid_five") or {})
                     .get("peg_lt_1"))
                and (x.get("gates") or {}).get("dilution_ok")]
        r.log("fetch-eligible (below_sma+peg<1+dil_ok): %d %s"
              % (len(elig),
                 [e.get("symbol") for e in elig[:8]]))
        und = [x for x in rows
               if (x.get("gates") or {}).get("below_sma")]
        lanes = pl.get("lanes") or {}
        sec_ok = sum(1 for x in rows
                     if (x.get("sector") or "").strip())
        r.kv(lanes=json.dumps(lanes), sectors_nonblank=sec_ok)
        misses += contract(r, "lanes",
                           (lanes.get("broad_below_sma") or 0)
                           >= 40,
                           "broad lane live: %s" % lanes)
        misses += contract(r, "sector-join",
                           sec_ok >= 40,
                           "%d/%d top rows carry sector"
                           % (sec_ok, len(rows)))
        r.log("below_sma rows: %d %s"
              % (len(und), [u.get("symbol") for u in und[:10]]))
        gk = next((k for k in ("gates", "gate_census",
                               "n_gate", "funnel")
                   if isinstance(pl.get(k), dict)), None)
        r.log("funnel key=%s val=%s"
              % (gk, json.dumps(pl.get(gk) or {})[:200]))
        if rows:
            r.log("row keys: %s" % sorted(rows[0].keys()))
            r.log("row sample: %s"
                  % json.dumps(rows[0])[:500])
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
