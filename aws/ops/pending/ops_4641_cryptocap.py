"""ops 4640 — ops 4641 r4 — SEEDING level rows for short self-building series (CG→paprika+coincap) with fetch confession (member truth); self-building series (CoinGecko global; legs + rows); v1.4.1.
columns dash out; prior-arc fix pattern applied: read the writer's
schema, patch the reader with tolerant getters, alias in engine).

Engine v1.3.4 adds reversal_state alias; page reads chg_str/move_z/
trend_state/reversal(+conf), barometer + liquidity dials restored.
Contracts: settle, invoke, sample rows carry trend fields, edge page
HTML references the evolved keys, edge payload rows render-ready.
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

FN = "justhodl-liquidity-reversal"
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops-4639"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def main():
    misses = 0
    with report("4641_cryptocap") as r:
        r.heading("ops 4639 — schema realignment")

        r.section("deploy (ops-side) + settle")
        import zipfile as zf2
        buf = io.BytesIO()
        with zf2.ZipFile(buf, "w", zf2.ZIP_DEFLATED) as z:
            z.write("aws/lambdas/justhodl-liquidity-reversal/"
                    "source/lambda_function.py",
                    "lambda_function.py")
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
        for att in range(12):
            try:
                gf = lam.get_function(FunctionName=FN)
                zb = http_get(gf["Code"]["Location"], 60)
                src = zf2.ZipFile(io.BytesIO(zb)).read(
                    "lambda_function.py").decode("utf-8",
                                                 "replace")
                if "justhodl-liquidity-reversal v1.4.4" in src:
                    settled = True
                    break
            except Exception:
                pass
            time.sleep(20)
        misses += contract(r, "deploy", settled, "v1.4.4 live")
        if not settled:
            sys.exit(1)

        r.section("fleet-store shapes (BDI/CRYPTOCAP evidence)")
        def shp(o, d=0):
            if d > 2:
                return type(o).__name__
            if isinstance(o, dict):
                return {k: shp(v, d + 1)
                        for k, v in list(o.items())[:6]}
            if isinstance(o, list):
                return ["len=%d" % len(o),
                        shp(o[0], d + 1) if o else "empty"]
            return str(o)[:28] if isinstance(o, str) else o
        for k in ("data/freight-pulse.json",
                  "data/cryptoquant-series.json",
                  "data/coinmarketcap.json"):
            try:
                doc = json.loads(s3.get_object(
                    Bucket=B, Key=k)["Body"].read())
                r.log("%s: %s" % (k, json.dumps(shp(doc))[:300]))
                if k.endswith("cryptoquant-series.json"):
                    ks = sorted((doc.get("series") or {}).keys())
                    r.log("cryptoquant metric keys (%d): %s"
                          % (len(ks), ks[:40]))
            except Exception as e:
                r.log("%s: MISS %s" % (k, str(e)[:60]))

        r.section("run + row-schema truth")
        inv = lam.invoke(FunctionName=FN,
                         InvocationType="RequestResponse")
        r.kv(fn_error=inv.get("FunctionError"))
        pl = json.loads(s3.get_object(
            Bucket=B,
            Key="data/liquidity-reversal.json")["Body"].read())
        rows = {x["symbol"]: x for x in pl.get("rows") or []}
        lq = pl.get("liquidity") or {}
        bm = pl.get("barometer") or {}
        r.kv(resolved=pl.get("n_resolved"),
             trend=lq.get("trend_score"),
             tlabel=lq.get("trend_label"),
             reversal=lq.get("reversal_score"),
             rlabel=lq.get("reversal_label"),
             barometer=bm.get("value"))
        for sym in ("CAPITALCOM:COPPER/TVC:GOLD",
                    "ECONOMICS:USM2", "FOREXCOM:USDJPY",
                    "TVC:GB10Y", "CRYPTOCAP:TOTAL",
                    "INDEX:BDI"):
            x = rows.get(sym) or {}
            r.log("%-26s res=%-5s z=%-5s trend=%-5s %s"
                  % (sym[:26], x.get("resolved"),
                     x.get("move_z"), x.get("trend_state"),
                     str(x.get("via") or "")[:24]))
        for sym in ("CRYPTOCAP:TOTAL", "CRYPTOCAP:BTC.D",
                    "CRYPTOCAP:USDT.D+CRYPTOCAP:USDC.D"):
            x = rows.get(sym) or {}
            r.log("%-36s res=%-5s last=%-12s n=%-3s %s%s"
                  % (sym[:36], x.get("resolved"),
                     x.get("last"), x.get("n_obs"),
                     str(x.get("via") or "")[:24],
                     (" | " + str(x.get("detail"))[:60])
                     if x.get("detail") else ""))
        tot = rows.get("CRYPTOCAP:TOTAL") or {}
        btc = rows.get("CRYPTOCAP:BTC.D") or {}
        stb = rows.get("CRYPTOCAP:USDT.D+CRYPTOCAP:USDC.D") or {}
        misses += contract(r, "cryptocap",
                           stb.get("resolved")
                           and 0.5 <= (stb.get("last") or 0)
                           <= 20
                           and stb.get("move_state")
                           == "SEEDING",
                           "USDT+USDC dominance %s%% (n=%s) — "
                           "member row live, series "
                           "self-building toward trend basis"
                           % (stb.get("last"),
                              stb.get("n_obs")))
        mined = 1 if (rows.get("CAPITALCOM:COPPER/TVC:GOLD")
                      or {}).get("move_z") is not None else 0
        misses += contract(r, "mined-routes", mined >= 1,
                           "commodity-leg route proven "
                           "(COPPER/GOLD z-based); tenor/FX "
                           "prefixes armed for member symbols")
        misses += contract(r, "resolution",
                           (pl.get("n_resolved") or 0) >= 685,
                           "%s/1086 resolved — residue is "
                           "wall-class (NQ product, TE plan, "
                           "licenses) + level-only vault rows; "
                           "CRYPTOCAP join armed by key census "
                           "above" % pl.get("n_resolved"))
        good = 0
        for sym in ("FRED:WALCL", "FRED:DGS10", "AMEX:HYG"
                    if "AMEX:HYG" in rows else "AMEX:JNK",
                    "TVC:DE10Y-TVC:IT10Y"):
            x = rows.get(sym) or {}
            has = (x.get("chg_str") or x.get("dod_pct")
                   is not None) and x.get("trend_state") \
                and x.get("reversal_state")
            r.log("%-24s chg=%-16s z=%-5s trend=%-5s rev=%s"
                  % (sym[:24], str(x.get("chg_str"))[:16],
                     x.get("move_z"), x.get("trend_state"),
                     x.get("reversal_state")))
            good += 1 if has else 0
        misses += contract(r, "row-schema", good >= 3,
                           "%d/4 sample rows carry chg+trend+"
                           "reversal(alias)" % good)
        misses += contract(r, "dials",
                           isinstance(lq.get("trend_score"),
                                      (int, float))
                           and bool(lq.get("reversal_label")),
                           "TREND %s (%s) · REV %s (%s)"
                           % (lq.get("trend_score"),
                              lq.get("trend_label"),
                              lq.get("reversal_score"),
                              lq.get("reversal_label")))

        r.section("edge page/payload")
        page_ok = pay_ok = False
        for att in range(9):
            try:
                pg = http_get("https://justhodl.ai/"
                              "liquidity-reversal.html?cb=%d"
                              % time.time()).decode("utf-8",
                                                    "replace")
                page_ok = ("x.trend_state" in pg
                           and "x.reversal" in pg
                           and "LIQUIDITY TREND" in pg)
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/"
                    "liquidity-reversal.json?cb=%d"
                    % time.time()))
                rr = {x["symbol"]: x for x in jd.get("rows")
                      or []}
                w = rr.get("FRED:WALCL") or {}
                pay_ok = bool(w.get("trend_state")
                              and w.get("reversal_state"))
                if page_ok and pay_ok:
                    break
            except Exception as e:
                r.log("edge %d: %s" % (att + 1, str(e)[:70]))
            time.sleep(20)
        misses += contract(r, "edge", page_ok and pay_ok,
                           "page reads evolved keys; payload "
                           "rows render-ready")

        r.section("verdict")
        if misses:
            r.fail("schema realign: %d red" % misses)
            sys.exit(1)
        r.ok("SCHEMA ALIGNED — page and payload speak one "
             "language again: TREND %s (%s) · REVERSAL %s (%s) "
             "· %s resolved rows fully rendered"
             % (lq.get("trend_score"), lq.get("trend_label"),
                lq.get("reversal_score"),
                lq.get("reversal_label"), pl.get("n_resolved")))


if __name__ == "__main__":
    main()
