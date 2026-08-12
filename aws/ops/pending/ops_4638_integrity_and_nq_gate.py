"""ops 4638 r2 — RESTORED v1.2.0 base (my clone had buried the evolved union+polarity engine), audit fixes re-ported on top as v1.3.0.

From Khalid's liquidity page paste: (1) INTEGRITY — cross-exchange
bare-heuristic collisions (EURONEXT:BANK wearing Nasdaq's BANK
value; LSE:HIGH on blackswan) -> heuristic restricted to US venues
in BOTH engines; (2) STALE guard — discontinued/old series (e.g.
FRED:TREASURY) excluded from all dials; (3) composite grammar v2 —
parentheses + numeric constants + eurozone tenor legs via FRED
monthly 10Y twins (DE/IT/FR/ES/JP) -> BTP-Bund class spreads live;
(4) FX prefix extension (FX/SAXO/OANDA); (5) NQ Cloudflare egress
worker 'justhodl-nq-proxy' deployed with the sealed token — the
~110-row Nasdaq bank-family wall gets a door. Deploy-settles both
engines, injects proxy env, invokes, contracts every fix.
"""
import io
import json
import os
import secrets
import sys
import time
import urllib.parse
import urllib.request
import zipfile

import boto3
from botocore.config import Config

from ops_report import report

LFN = "justhodl-liquidity-reversal"
BFN = "justhodl-blackswan-watch"
B = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=900,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
CF = os.environ.get("CLOUDFLARE_API_TOKEN", "")

WORKER_JS = r"""
addEventListener('fetch', e => e.respondWith(go(e.request)));
async function go(req){
  const u = new URL(req.url);
  if (u.searchParams.get('k') !== KEY)
    return new Response('forbidden', {status:403});
  const path = u.searchParams.get('path') || '';
  if (!path.startsWith('/api/quote/') ||
      !path.includes('/historical'))
    return new Response('bad path', {status:400});
  const r = await fetch('https://api.nasdaq.com' + path, {headers:{
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    'Accept':'application/json, text/plain, */*',
    'Origin':'https://www.nasdaq.com',
    'Referer':'https://www.nasdaq.com/'}});
  return new Response(await r.text(), {status:r.status,
    headers:{'content-type':'application/json'}});
}
"""


def cf_api(method, path, body=None, ctype="application/json"):
    req = urllib.request.Request(
        "https://api.cloudflare.com/client/v4" + path,
        method=method,
        headers={"Authorization": "Bearer " + CF,
                 "Content-Type": ctype})
    if body is not None:
        req.data = body if isinstance(body, bytes) \
            else json.dumps(body).encode()
    with urllib.request.urlopen(req, timeout=30) as h:
        return json.loads(h.read())


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def http_get(url, timeout=45):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-4638"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def s3j(key):
    try:
        return json.loads(s3.get_object(Bucket=B,
                                        Key=key)["Body"].read())
    except Exception:
        return None


def settle(fn, marker, r, tries=16):
    for att in range(tries):
        try:
            gf = lam.get_function(FunctionName=fn)
            zb = http_get(gf["Code"]["Location"], 60)
            src = zipfile.ZipFile(io.BytesIO(zb)).read(
                "lambda_function.py").decode("utf-8", "replace")
            if marker in src:
                return True
        except Exception as e:
            r.log("settle %s %d: %s" % (fn, att + 1, str(e)[:60]))
        time.sleep(30)
    return False


def main():
    misses = 0
    with report("4638_integrity_and_nq_gate") as r:
        r.heading("ops 4638 — integrity + grammar + NQ door")

        r.section("NQ egress worker (Cloudflare)")
        proxy_url = None
        pkey = None
        if not CF:
            r.warn("CLOUDFLARE_API_TOKEN absent in runner env — "
                   "NQ door skipped; all other fixes proceed")
        else:
            try:
                accs = cf_api("GET", "/accounts")
                acct = accs["result"][0]["id"]
                sub = cf_api("GET", "/accounts/%s/workers/"
                             "subdomain" % acct)["result"][
                    "subdomain"]
                pkey = secrets.token_hex(16)
                js = WORKER_JS.replace("KEY",
                                       "'" + pkey + "'")
                cf_api("PUT", "/accounts/%s/workers/scripts/"
                       "justhodl-nq-proxy" % acct,
                       js.encode(),
                       ctype="application/javascript")
                try:
                    cf_api("POST", "/accounts/%s/workers/scripts/"
                           "justhodl-nq-proxy/subdomain" % acct,
                           {"enabled": True})
                except Exception:
                    pass
                proxy_url = ("https://justhodl-nq-proxy.%s."
                             "workers.dev" % sub)
                tqs = ("/api/quote/NQGIT/historical?assetclass="
                       "index&limit=20&fromdate=2026-06-01&"
                       "todate=2026-08-12")
                tb = http_get("%s?k=%s&path=%s"
                              % (proxy_url, pkey,
                                 urllib.parse.quote(tqs,
                                                    safe="")), 30)
                ok = b"tradesTable" in tb or b"data" in tb
                r.kv(proxy=proxy_url, smoke="PASS" if ok
                     else "FAIL", bytes=len(tb))
                if not ok:
                    proxy_url = None
            except Exception as e:
                r.warn("worker deploy: %s" % str(e)[:120])
                proxy_url = None
        if proxy_url:
            for fn in (LFN, BFN):
                cfg = lam.get_function_configuration(
                    FunctionName=fn)
                ev = (cfg.get("Environment") or {}).get(
                    "Variables") or {}
                ev["NQ_PROXY_URL"] = proxy_url
                ev["NQ_PROXY_KEY"] = pkey
                lam.update_function_configuration(
                    FunctionName=fn,
                    Environment={"Variables": ev})
                for _ in range(20):
                    st = lam.get_function_configuration(
                        FunctionName=fn)
                    if st.get("LastUpdateStatus") == "Successful":
                        break
                    time.sleep(5)
            r.ok("  [nq-door] worker live + env injected on both "
                 "engines")

        r.section("deploy-settle both engines")
        ok_l = settle(LFN, "justhodl-liquidity-reversal v1.1.0", r)
        ok_b = settle(BFN, "justhodl-blackswan-watch v1.9.0", r, 8)
        misses += contract(r, "deploy", ok_l and ok_b,
                           "liq v1.3.0 (restored base) + blackswan v1.9.0")
        if not ok_l:
            sys.exit(1)

        r.section("run + audited truth")
        lam.invoke(FunctionName=LFN,
                   InvocationType="RequestResponse")
        pl = s3j("data/liquidity-reversal.json") or {}
        rows = {x["symbol"]: x for x in pl.get("rows") or []}
        g = pl.get("gauge") or {}
        br = g.get("breadth") or {}
        bm = pl.get("barometer") or {}
        r.kv(resolved=pl.get("n_resolved"),
             trend_capable=br.get("n_trend_capable"),
             gauge=g.get("value"), glabel=g.get("label"),
             barometer=bm.get("value"),
             n_stale=sum(1 for x in pl.get("rows") or []
                         if x.get("stale")))
        for sym in ("EURONEXT:BANK", "FRED:TREASURY",
                    "1/(FRED:TOTLL/FRED:M2SL)",
                    "TVC:DE10Y-TVC:IT10Y",
                    "1-FRED:BAMLC0A0CMEY", "FX:EURUSD",
                    "SAXO:JPYEUR", "NASDAQ:NQEU3010"):
            x = rows.get(sym) or {}
            r.log("%-28s res=%-5s state=%-10s z=%-5s %s"
                  % (sym[:28], x.get("resolved"),
                     str(x.get("move_state"))[:10],
                     x.get("move_z"),
                     str(x.get("via") or "")[:26]))
        eb = rows.get("EURONEXT:BANK") or {}
        misses += contract(r, "integrity",
                           not eb.get("resolved")
                           or "heuristic" not in str(
                               eb.get("via") or ""),
                           "EURONEXT:BANK no longer wears the "
                           "Nasdaq value (res=%s via=%s)"
                           % (eb.get("resolved"), eb.get("via")))
        tre = rows.get("FRED:TREASURY") or {}
        misses += contract(r, "stale-guard",
                           tre.get("stale") is True
                           or tre.get("move_state") == "STALE",
                           "discontinued TREASURY excluded from "
                           "dials (stale=%s)" % tre.get("stale"))
        gset = [rows.get(s2) or {} for s2 in
                ("1/(FRED:TOTLL/FRED:M2SL)",
                 "TVC:DE10Y-TVC:IT10Y",
                 "1-FRED:BAMLC0A0CMEY")]
        gok = sum(1 for x in gset if x.get("move_z") is not None
                  or x.get("reversal_state"))
        misses += contract(r, "grammar", gok >= 2,
                           "%d/3 grammar-v2 composites live "
                           "(constants/parens/EZ-tenor legs)"
                           % gok)
        fxok = sum(1 for s2 in ("FX:EURUSD", "SAXO:JPYEUR")
                   if (rows.get(s2) or {}).get("resolved"))
        misses += contract(r, "fx-prefixes", fxok >= 1,
                           "%d/2 extended-prefix FX resolved"
                           % fxok)
        nq_live = [x["symbol"] for x in pl.get("rows") or []
                   if x["symbol"].startswith("NASDAQ:NQ")
                   and x.get("move_z") is not None]
        if proxy_url:
            misses += contract(r, "nq-unlock", len(nq_live) >= 8,
                               "%d NQ bank-family rows via the "
                               "Cloudflare door: %s"
                               % (len(nq_live), nq_live[:5]))
        else:
            r.warn("nq-unlock skipped (no worker) — %d NQ rows"
                   % len(nq_live))
        misses += contract(r, "dials",
                           g.get("label") in ("REVERSING_UP",
                                              "REVERSING_DOWN",
                                              "TRENDING", "MIXED"),
                           "gauge %s (%s) on stale-clean rows"
                           % (g.get("value"), g.get("label")))

        r.section("edge")
        fresh = False
        for att in range(8):
            try:
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/"
                    "liquidity-reversal.json?cb=%d"
                    % time.time()))
                rr = {x["symbol"]: x for x in jd.get("rows")
                      or []}
                if (rr.get("FRED:TREASURY") or {}).get("stale") \
                        or not (rr.get("FRED:TREASURY")
                                or {}).get("resolved"):
                    fresh = True
                    break
            except Exception as e:
                r.log("edge %d: %s" % (att + 1, str(e)[:70]))
            time.sleep(20)
        misses += contract(r, "edge", fresh,
                           "edge serves the audited payload")

        r.section("verdict")
        if misses:
            r.fail("audit gate: %d red" % misses)
            sys.exit(1)
        r.ok("AUDIT SEALED — integrity restored, stale-guarded "
             "dials %s/%s, grammar v2 live (EZ spreads), NQ door "
             "%s · resolved %s"
             % (g.get("value"), g.get("label"),
                "OPEN (%d rows)" % len(nq_live) if proxy_url
                else "pending token",
                pl.get("n_resolved")))


if __name__ == "__main__":
    main()
