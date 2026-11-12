"""ops 6790 - READ-ONLY audit before widening the Inflows & outflows desk further (Khalid 2026-11-12 10:22 ET:
"continue" after the ICI / ETF un-stalls). The four follow-ups named on 2026-11-10: TIC net purchases by asset class,
stablecoin supply history, CryptoQuant exchange-flow histories, Korea / Japan foreign flows. For each, what the fleet
already serves and what the runner can reach - nothing is built here, nothing is written.

1. TIC by asset class: the seven FRED ids the capital-inflows engine already pulls (FORLT{TREAS,AGCY,CORP,EQTY,TOTAL}NET
   99996 + TOTALNET 99990/99991) through the Worker's /series route - HTTP, names, units, first / last, latest value.
2. data/cryptoquant-series.json: every metric's series length and span; which carry exchange flows / reserves.
3. DefiLlama stablecoin history from the runner: /stablecoincharts/all (daily total circulating) + per-coin charts.
4. Korea / Japan from the runner: the KRX data portal endpoint (blocks AWS IPs), JPX's weekly investor-type listing
   and its newest xls, Naver's index trend endpoint; plus what data/history/apac-flows.json holds for korea / japan.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import re
import sys
import time
import urllib.parse

import requests

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
TIC = ["FRED:FORLTTREASNET99996", "FRED:FORLTAGCYNET99996", "FRED:FORLTCORPNET99996", "FRED:FORLTEQTYNET99996", "FRED:FORLTTOTALNET99996", "FRED:FORLTTOTALNET99990", "FRED:FORLTTOTALNET99991",
       "FRED:FORLTTREASNET42609", "FRED:FORLTEQTYNET42609", "FRED:FORLTTREASNET41408", "FRED:FORLTEQTYNET41408", "FRED:FORLTTREASNET13005"]
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36", "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9"}


def fetch(url, timeout=45, **kw):
    t0 = time.time()
    try:
        resp = requests.get(url, timeout=timeout, headers=UA, **kw)
        return resp.status_code, resp.content, time.time() - t0, resp.headers.get("content-type", "")
    except Exception as exc:  # noqa: BLE001
        return type(exc).__name__, b"", time.time() - t0, ""


def main():
    now = datetime.now(timezone.utc)
    facts = {}
    with report("ops_6790_flows_coverage_followups_audit") as r:
        r.heading("ops 6790 - flows desk coverage follow-ups audit %s (read-only)" % now.isoformat(timespec="minutes"))

        r.section("1. TIC net purchases by asset class through /series")
        ok = 0
        for sid in TIC:
            code, body, took, _ = fetch("%s/series?id=%s" % (PROXY, urllib.parse.quote(sid)))
            try:
                p = json.loads(body) if body else {}
            except Exception:  # noqa: BLE001
                p = {}
            obs = p.get("obs") or []
            if obs:
                ok += 1
            r.log("%-28s HTTP %s | %5d obs | %s -> %s | %s | %s | last %s" % (sid, code, len(obs), obs[0][0] if obs else None, obs[-1][0] if obs else None,
                                                                             str(p.get("unit") or p.get("units") or "")[:24], str(p.get("name") or p.get("title") or "")[:70], obs[-1][1] if obs else None))
        facts["tic_resolved"] = "%d/%d" % (ok, len(TIC))

        r.section("2. data/cryptoquant-series.json")
        code, body, took, _ = fetch("%s/data/cryptoquant-series.json?t=%d" % (PROXY, int(time.time())), timeout=60)
        try:
            cq = json.loads(body)
        except Exception:  # noqa: BLE001
            cq = {}
        series = cq.get("series") or {}
        twins = cq.get("twins") or cq.get("twin_series") or {}
        r.log("HTTP %s, %d KB, generated %s, top keys %s, %d series, %d twins" % (code, len(body) // 1024, cq.get("generated_at"), list(cq.keys())[:10], len(series), len(twins)))
        flowish = []
        for name in sorted(series):
            s = series[name]
            d = s.get("d") or []
            tag = "FLOW" if re.search(r"flow|reserve|exchange|whale|stable|inflow|outflow|deposit|withdraw", name) else ""
            if tag:
                flowish.append(name)
            r.log("  %-40s n=%-5d %s -> %s %s" % (name, len(d), d[0] if d else None, d[-1] if d else None, tag))
        for name in sorted(twins)[:40]:
            d = (twins[name] or {}).get("d") or []
            r.log("  twin %-35s n=%-5d %s -> %s" % (name, len(d), d[0] if d else None, d[-1] if d else None))
        facts["cq_series"] = "%d series, %d flow-like: %s" % (len(series), len(flowish), " ".join(flowish)[:300])
        onchain_code, onchain_body, _, _ = fetch("%s/data/cryptoquant-onchain.json?t=%d" % (PROXY, int(time.time())), timeout=60)
        try:
            oc = json.loads(onchain_body)
            mets = oc.get("metrics") or {}
            r.log("cryptoquant-onchain.json: HTTP %s, %d metrics; flow-like: %s" % (onchain_code, len(mets), "; ".join("%s (%s, %s, as_of %s)" % (k, v.get("label"), v.get("unit"), v.get("as_of")) for k, v in sorted(mets.items()) if re.search(r"flow|reserve|exchange|whale|stable", k))[:1200]))
        except Exception as exc:  # noqa: BLE001
            r.warn("cryptoquant-onchain: %s" % type(exc).__name__)

        r.section("3. DefiLlama stablecoin history from the runner")
        for label, url in (("stablecoincharts/all", "https://stablecoins.llama.fi/stablecoincharts/all"), ("stablecoincharts/all?stablecoin=1 (USDT)", "https://stablecoins.llama.fi/stablecoincharts/all?stablecoin=1"),
                           ("stablecoincharts/all?stablecoin=2 (USDC)", "https://stablecoins.llama.fi/stablecoincharts/all?stablecoin=2"), ("stablecoins list", "https://stablecoins.llama.fi/stablecoins?includePrices=true")):
            code, body, took, ctype = fetch(url, timeout=60)
            try:
                p = json.loads(body)
            except Exception:  # noqa: BLE001
                p = None
            if isinstance(p, list) and p and isinstance(p[0], dict) and "date" in p[0]:
                first = datetime.fromtimestamp(int(p[0]["date"]), timezone.utc).date()
                last = datetime.fromtimestamp(int(p[-1]["date"]), timezone.utc).date()
                tot = (p[-1].get("totalCirculating") or {}).get("peggedUSD")
                r.log("%s: HTTP %s %.1fs %d KB | %d days %s -> %s | latest totalCirculating peggedUSD %s | keys %s" % (label, code, took, len(body) // 1024, len(p), first, last, tot, list(p[-1].keys())[:8]))
                facts["defillama_" + label.split("/")[0]] = "%d days %s -> %s" % (len(p), first, last)
            elif isinstance(p, dict):
                assets = p.get("peggedAssets") or []
                r.log("%s: HTTP %s %.1fs %d KB | %d assets | top: %s" % (label, code, took, len(body) // 1024, len(assets), [(a.get("symbol"), a.get("id"), (a.get("circulating") or {}).get("peggedUSD")) for a in assets[:6]]))
            else:
                r.log("%s: HTTP %s %.1fs %d bytes %s" % (label, code, took, len(body), ctype[:40]))

        r.section("4. Korea / Japan from the runner")
        probes = [("KRX data portal (GET)", "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"),
                  ("KRX data portal https", "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"),
                  ("JPX investor-type listing", "https://www.jpx.co.jp/english/markets/statistics-equities/investor-type/index.html"),
                  ("Naver KOSPI index trend", "https://m.stock.naver.com/api/index/KOSPI/trend"),
                  ("Naver domestic investor (foreign net) KOSPI", "https://m.stock.naver.com/api/stock/KOSPI/trend")]
        for label, url in probes:
            code, body, took, ctype = fetch(url, timeout=30)
            text = body.decode("utf-8", "ignore")
            note = ""
            if "jpx" in url and isinstance(code, int) and code == 200:
                links = re.findall(r"[\w./_-]*stock_val[\w./_-]*\.xls[x]?", text, re.I)
                note = " | xls links %d: %s" % (len(links), links[:3])
                if links:
                    xurl = links[0] if links[0].startswith("http") else "https://www.jpx.co.jp" + links[0]
                    xcode, xbody, xtook, xtype = fetch(xurl, timeout=45)
                    note += " | newest xls HTTP %s %d KB %s" % (xcode, len(xbody) // 1024, xtype[:40])
            r.log("%s: HTTP %s %.1fs %d bytes %s%s | %s" % (label, code, took, len(body), ctype[:30], note, re.sub(r"\s+", " ", text[:160])))
        try:
            krx = requests.post("http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd", timeout=30, headers={**UA, "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020203"},
                                data={"bld": "dbms/MDC/STAT/standard/MDCSTAT02203", "locale": "ko_KR", "inqTpCd": "2", "trdVolVal": "2", "askBid": "3", "mktId": "STK", "strtDd": "20261001", "endDd": "20261111", "share": "1", "money": "1", "csvxls_isNo": "false"})
            txt = krx.text
            r.log("KRX investor-type daily (POST MDCSTAT02203): HTTP %s, %d bytes, keys %s, sample %s" % (krx.status_code, len(krx.content), list(krx.json().keys())[:5] if krx.ok and txt.startswith("{") else "-", re.sub(r"\s+", " ", txt[:300])))
            facts["krx_post"] = "HTTP %s %d bytes" % (krx.status_code, len(krx.content))
        except Exception as exc:  # noqa: BLE001
            r.log("KRX POST: %s %s" % (type(exc).__name__, str(exc)[:120]))
            facts["krx_post"] = type(exc).__name__
        code, body, _, _ = fetch("%s/data/history/apac-flows.json?t=%d" % (PROXY, int(time.time())), timeout=60)
        try:
            ap = json.loads(body)
            days = ap.get("days") or {}
            ks = sorted(days)
            last = days[ks[-1]] if ks else {}
            r.log("history/apac-flows.json: HTTP %s, %d days %s -> %s; latest day keys %s; korea-ish %s; japan-ish %s" % (
                code, len(ks), ks[0] if ks else None, ks[-1] if ks else None, list(last.keys())[:20], {k: v for k, v in last.items() if k.startswith("kr") or "korea" in k}, {k: v for k, v in last.items() if k.startswith("jp") or "japan" in k}))
        except Exception as exc:  # noqa: BLE001
            r.warn("history/apac-flows: %s" % type(exc).__name__)
        code, body, _, _ = fetch("%s/data/apac-flows.json?t=%d" % (PROXY, int(time.time())), timeout=60)
        try:
            ap = json.loads(body)
            r.log("apac-flows.json: generated %s | korea %s | japan %s" % (ap.get("generated_at") or ap.get("as_of"), json.dumps(ap.get("korea"))[:400], json.dumps(ap.get("japan"))[:300]))
        except Exception as exc:  # noqa: BLE001
            r.warn("apac-flows: %s" % type(exc).__name__)
        r.kv(**{k: str(v) for k, v in facts.items()})
        r.ok("read-only audit complete")
    sys.exit(0)


if __name__ == "__main__":
    main()
