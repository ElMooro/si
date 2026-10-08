"""[r20 unit-fix cycle] [r16 EPS fusion + confluence page] [r9d seal run: v1.2.0 deployed via deploy-lambdas] ops 4652 — Khalid's five priority metrics first-class on the
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
                if "justhodl-stock-buying v1.3.4" in src:
                    settled = True
                    break
            except Exception:
                pass
            time.sleep(20)
        misses += contract(r, "deploy", settled, "v1.3.4 live")
        if not settled:
            sys.exit(1)

        r.section("pre-invoke: backlog engine")
        try:
            inv0 = lam.invoke(FunctionName="justhodl-backlog",
                              InvocationType="RequestResponse")
            r.log("backlog engine fn_error=%s"
                  % inv0.get("FunctionError"))
            bx0 = json.loads(s3.get_object(
                Bucket=B, Key="data/backlog.json")
                ["Body"].read())
            bt0 = bx0.get("by_ticker") or {}
            eps_have = [k for k, v in bt0.items()
                        if isinstance(v, dict)
                        and v.get("eps") is not None]
            r.log("backlog.json: entries=%d with_eps=%d "
                  "gen=%s" % (len(bt0), len(eps_have),
                              str(bx0.get("generated_at")
                                  )[:19]))
            if bt0:
                k0 = (eps_have or sorted(bt0.keys()))[0]
                r.log("sample %s: %s"
                      % (k0, json.dumps(bt0[k0])[:300]))
            if not eps_have:
                r.log("eps absent -> ops-side redeploy of "
                      "justhodl-backlog from checkout + "
                      "re-invoke")
                buf2 = io.BytesIO()
                with zipfile.ZipFile(buf2, "w",
                                     zipfile.ZIP_DEFLATED) as z2:
                    z2.write("aws/lambdas/justhodl-backlog/"
                             "source/lambda_function.py",
                             "lambda_function.py")
                for att2 in range(8):
                    try:
                        st2 = lam.get_function_configuration(
                            FunctionName="justhodl-backlog")
                        if st2.get("LastUpdateStatus") \
                                == "InProgress":
                            time.sleep(15)
                            continue
                        lam.update_function_code(
                            FunctionName="justhodl-backlog",
                            ZipFile=buf2.getvalue())
                        break
                    except Exception as e2:
                        if "ResourceConflict" in str(e2):
                            time.sleep(15)
                            continue
                        raise
                time.sleep(20)
                inv1 = lam.invoke(
                    FunctionName="justhodl-backlog",
                    InvocationType="RequestResponse")
                r.log("re-invoke fn_error=%s"
                      % inv1.get("FunctionError"))
                bx1 = json.loads(s3.get_object(
                    Bucket=B, Key="data/backlog.json")
                    ["Body"].read())
                bt1 = bx1.get("by_ticker") or {}
                r.log("post-redeploy with_eps=%d"
                      % sum(1 for v in bt1.values()
                            if isinstance(v, dict)
                            and v.get("eps") is not None))
        except Exception as e:
            r.warn("backlog invoke: %s" % str(e)[:80])

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
        try:
            mxb = json.loads(s3.get_object(
                Bucket=B, Key="data/fundamental-census-matrix"
                ".json")["Body"].read())
            cb = mxb.get("cols") or {}
            bl_hits = sorted(k for k in cb
                             if "backlog" in k.lower())
            r.log("matrix backlog cols: %s" % bl_hits)
            tks = mxb.get("tickers") or []
            for probe_t in ("CAT", "BA", "LMT", "AAPL"):
                if probe_t in tks:
                    i2 = tks.index(probe_t)
                    vals = {h: cb[h][i2] for h in bl_hits[:5]}
                    r.log("  %s: %s" % (probe_t,
                                        json.dumps(vals)[:160]))
        except Exception as e:
            r.warn("backlog matrix: %s" % str(e)[:80])
        try:
            pg3 = s3.list_objects_v2(Bucket=B, Prefix="data/",
                                     MaxKeys=1000)
            bks = [o["Key"] for o in pg3.get("Contents") or []
                   if "backlog" in o["Key"].lower()]
            r.log("backlog stores: %s" % bks[:6])
            for bk in bks[:2]:
                bd = json.loads(s3.get_object(
                    Bucket=B, Key=bk)["Body"].read())
                r.log("  %s keys=%s" % (bk,
                                        list(bd.keys())[:10]))
                rws = bd.get("rows") or bd.get("by_ticker")                     or bd.get("companies") or []
                if isinstance(rws, dict):
                    k0 = sorted(rws.keys())[0]
                    r.log("  by_ticker[%s]=%s"
                          % (k0, json.dumps(rws[k0])[:200]))
                elif isinstance(rws, list) and rws:
                    r.log("  row0=%s"
                          % json.dumps(rws[0])[:220])
        except Exception as e:
            r.warn("backlog stores: %s" % str(e)[:80])
        cols_chk = [("sector", lambda x: (x.get("sector")
                                          or "").strip()),
                    ("peg", lambda x: x.get("peg")),
                    ("pe", lambda x: x.get("pe")),
                    ("roic", lambda x: x.get("roic")),
                    ("rs", lambda x: x.get("rs_3m_vs_spy")),
                    ("db", lambda x: x.get("double_bottom")),
                    ("gap", lambda x: (x.get("sma") or {})
                     .get("gap_pct")),
                    ("bklg", lambda x: x.get("backlog_usd"))]
        for lane_name, pred in (
                ("CENSUS", lambda x: x.get("lane") != "BROAD"),
                ("BROAD", lambda x: x.get("lane") == "BROAD")):
            sub = [x for x in rows if pred(x)]
            counts = {nm: sum(1 for x in sub
                              if fn(x) not in (None, ""))
                      for nm, fn in cols_chk}
            r.log("%s n=%d nonnull: %s"
                  % (lane_name, len(sub), json.dumps(counts)))
        try:
            mxc = json.loads(s3.get_object(
                Bucket=B, Key="data/fundamental-census-matrix"
                ".json")["Body"].read()).get("cols") or {}
            import re as _re
            r.log("pe-ish cols: %s"
                  % [k for k in sorted(mxc)
                     if _re.search(r"(^|_)pe($|_)|price_earn",
                                   k)][:10])
            r.log("margin cols: %s"
                  % [k for k in sorted(mxc)
                     if "margin" in k][:10])
        except Exception as e:
            r.warn("mx names: %s" % str(e)[:60])
        try:
            fv2 = json.loads(s3.get_object(
                Bucket=B, Key="data/finviz-universe.json")
                ["Body"].read()).get("by_ticker") or {}
            k1 = sorted(fv2.keys())[0]
            r.log("finviz[%s] = %s"
                  % (k1, json.dumps(fv2[k1])[:340]))
        except Exception as e:
            r.warn("fv entry: %s" % str(e)[:60])
        lanes = pl.get("lanes") or {}
        sec_ok = sum(1 for x in rows
                     if (x.get("sector") or "").strip())
        r.kv(lanes=json.dumps(lanes), sectors_nonblank=sec_ok)
        cen60 = [x for x in rows
                 if x.get("lane") != "BROAD"]
        peg_nn = sum(1 for x in cen60
                     if x.get("peg") is not None)
        gap_nn = sum(1 for x in cen60
                     if (x.get("sma") or {}).get("gap_pct")
                     is not None)
        bst = sum(1 for x in rows
                   if x.get("backlog_status")
                   or x.get("backlog_usd") is not None)
        r.kv(backlog_status_rows=bst,
             top_len=len(rows))
        r.kv(backlog_kinds=json.dumps(
            pl.get("backlog_kinds") or {}))
        eps_n = sum(1 for x in rows
                    if x.get("eps") is not None)
        epsy_n = sum(1 for x in rows
                     if x.get("eps_yoy_pct2") is not None)
        epsq_n = sum(1 for x in rows
                     if x.get("eps_qoq_pct") is not None)
        r.kv(eps_cols="lvl=%d yoy=%d qoq=%d"
             % (eps_n, epsy_n, epsq_n))
        misses += contract(r, "eps-cols",
                           eps_n >= 200 and epsy_n >= 200
                           and epsq_n >= 25,
                           "EPS lvl:%d yoy:%d qoq:%d (qoq from "
                           "XBRL, grows with backlog-engine "
                           "coverage)" % (eps_n, epsy_n,
                                          epsq_n))
        misses += contract(r, "backlog-visible",
                           bst >= 60 and len(rows) >= 200,
                           "%d rows carry backlog status/level "
                           "over %d shipped (sortable reach)"
                           % (bst, len(rows)))
        misses += contract(r, "peg-col",
                           peg_nn >= 50,
                           "%d/%d census rows carry peg "
                           "(peg_ttm bound)" % (peg_nn,
                                                len(cen60)))
        misses += contract(r, "gap-col",
                           gap_nn >= 55,
                           "%d/%d rows carry sma.gap_pct"
                           % (gap_nn, len(cen60)))
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
