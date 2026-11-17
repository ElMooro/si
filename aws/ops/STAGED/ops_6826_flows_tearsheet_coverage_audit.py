"""ops 6826 - READ-ONLY audit before the flows tearsheet and a wider Inflows & outflows desk (re-runnable).

Khalid 2026-11-17: "apply the same thing we did to 13F from this picture to inflows & outflows. Also remove all the
charts from inflows & outflows and see what data you can add to it. Only add, don't remove any data - we are looking
to extend coverage."  Audit before building: nothing is deployed, nothing is written but this report.

  1. the desk's catalog as it ships (families, series), every series read the way the page reads it;
  2. discovery through the directory search (/symsearch) for the flow topics the desk does not cover yet;
  3. candidate series verified one by one through /series (HTTP, observations, span, last value, name, unit);
  4. the fleet's feeds: S3 keys under data/ that look like flows, and the shape of the ones with histories
     (ETF creations index, ICI, CryptoQuant, APAC, liquidity, foreign flows, capital inflows, ETF categories);
  5. a pack of the real observations (last rows of every series, gzip + base64) so the page can be built and
     tested offline against live numbers.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import json
import re
import sys
import time
import urllib.parse

import boto3
import requests
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 3, "mode": "standard"}, connect_timeout=10, read_timeout=60)
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36", "Accept": "*/*"}
KEEP = 300   # rows of each series kept in the offline pack

SEARCHES = ["FORTREASPOS", "FORLTTREASNET", "net purchases of corporate equities", "Treasury securities held outright", "mortgage-backed securities held outright", "commercial paper outstanding",
            "securities in bank credit", "commercial and industrial loans all commercial banks", "large time deposits", "central bank liquidity swaps", "custody for foreign official",
            "tri-party repo volume", "SOFR volume", "primary dealer", "MMF Treasury", "money market funds investments", "margin loans", "net issuance", "pension funds net acquisition",
            "exchange-traded funds net issues", "life insurance companies corporate equities", "ECB total assets", "Bank of Japan total assets", "commitments of traders", "stablecoin",
            "revolving consumer credit", "currency in circulation", "discount window primary credit", "federal debt held by the public", "treasury net issuance"]

CANDIDATES = [
    # Fed balance sheet detail (H.4.1)
    "TREAST", "WSHOTSL", "WSHOMCB", "WLCFLPCL", "H41RESPPALDKNWW", "SWPT", "WLRRAL", "RPONTSYD", "WCURCIR", "CURRCIR", "WDTGAL", "WORAL", "WSEFINTL1", "WMTSECL1", "WFCDA", "TOTRESNS", "WLODLL", "RRPONTTLD",
    # banks (H.8)
    "TOTCI", "BUSLOANS", "REALLN", "CONSUMER", "CLSACBW027SBOG", "RELACBW027SBOG", "USGSEC", "OTHSEC", "INVEST", "LTDACBW027SBOG", "ODSACBW027SBOG", "CASACBW027SBOG", "H8B3094NCBA", "DPSLCBW027SBOG",
    "DPSSCBW027SBOG", "DPSFRIW027SBOG", "TOTBKCR", "H8B1058NCBCMG",
    # money and credit
    "M1SL", "WM2NS", "REVOLSL", "NONREVSL", "SLOAS", "MVLOAS", "COMPOUT", "ABCOMP", "FINCP", "NONFINCP", "DTBSPCKFM", "BOGZ1FL663067003Q",
    # Z.1 flows by sector
    "BOGZ1FA573064105Q", "BOGZ1FA543064105Q", "BOGZ1FA513064105Q", "BOGZ1FA663064103Q", "BOGZ1FA223064145Q", "BOGZ1FA103164103Q", "BOGZ1FA793164105Q", "BOGZ1FA153061105Q", "BOGZ1FA713061103Q",
    "BOGZ1FA763061100Q", "BOGZ1FA633061105Q", "BOGZ1FA653061105Q", "BOGZ1FA313161105Q", "BOGZ1FA893064105Q", "BOGZ1FA654090005Q", "BOGZ1FA634090005Q", "BOGZ1FA554090005Q", "BOGZ1FA153064105Q",
    "BOGZ1FA153063005Q", "BOGZ1FA103163003Q", "BOGZ1FA263063005Q", "BOGZ1FA573063005Q", "HNOTSAQ027S", "HNODPAQ027S", "HNOMMFQ027S", "FGTSLQ027S", "CMDEBT", "NCBDBIQ027S", "TODNS", "FGSDODNS", "BCNSDODNS",
    # foreign holdings / TIC, more countries
    "FORTREASPOS29998", "FORTREASPOS36137", "FORTREASPOS33006", "FORTREASPOS31407", "FORTREASPOS12508", "FORTREASPOS46612", "FORTREASPOS42005", "FORTREASPOS46205", "FORTREASPOS43214", "FORTREASPOS35106",
    "FORTREASPOS10405", "FORTREASPOS13501", "FORTREASPOS14109", "FORTREASPOS45209", "FORTREASPOS42307", "FORLTTREASNET13005", "FORLTTREASNET29998", "FORLTTREASNET36137", "FORLTCORPNET13005",
    "FORLTEQTYNET13005", "FORLTEQTYNET29998", "FORLTEQTYNET36137", "FORLTAGCYNET42609", "FORLTAGCYNET41408", "FORLTFORSTKNET99996", "FORLTFORBONDNET99996", "FORLTTOTALNET42609", "FORLTTOTALNET41408",
    # other central banks, the Treasury
    "ECBASSETSW", "JPNASSETS", "MTSDS133FMS", "GFDEBTN", "FYGFDPUN", "FDHBFRBN", "FDHBPIN",
    # OFR short-term funding monitor
    "ofr:MMF-MMF_T_TOT-M", "ofr:MMF-MMF_AG_TOT-M", "ofr:MMF-MMF_TOT-M", "ofr:MMF-MMF_RP_wFR-M", "ofr:MMF-MMF_RP_TOT-M", "ofr:MMF-MMF_CD_TOT-M", "ofr:MMF-MMF_CP_TOT-M", "ofr:REPO-TRI_TV_TOT-P",
    "ofr:REPO-GCF_TV_TOT-P", "ofr:REPO-DVP_TV_TOT-P", "ofr:REPO-TRI_AR_TOT-P", "ofr:REPO-DVP_AR_TOT-P", "ofr:FNYR-SOFR_UV-A", "ofr:FNYR-EFFR_UV-A", "ofr:FNYR-BGCR_UV-A", "ofr:FNYR-TGCR_UV-A",
    "ofr:FNYR-OBFR_UV-A", "ofr:NYPD-PD_RP_TOT-A", "ofr:NYPD-PD_RRP_TOT-A", "ofr:NYPD-PD_AFtD_T-A",
]
FEEDS = ["ici-fund-flows", "ici-flows", "history/apac-flows", "apac-flows", "crypto-etf-flows-history", "crypto-exchange-flows-history", "stablecoin-supply-history", "stablecoin-flow", "liquidity-flow",
         "foreign-flows", "capital-inflows", "etf-true-flows", "cross-asset-flow-state", "etf-flows", "global-flows", "hot-money", "cq-feed", "13f-tickers", "auction-desk", "insider-clusters",
         "polygon-options-flow", "short-volume-research", "repo-master-board", "net-liquidity", "fed-liquidity", "treasury-flows", "tga", "cot", "cot-positioning", "buybacks", "margin-debt", "dark-pool"]
KEY_RE = re.compile(r"flow|cot|position|liquid|repo|tic|issuance|buyback|insider|margin|dealer|auction|stable|fund|capital|money|treasur|deposit|reserve|credit|netliq|creation|hot|etf", re.I)


def fetch(url, timeout=45):
    t0 = time.time()
    try:
        resp = requests.get(url, timeout=timeout, headers=UA)
        return resp.status_code, resp.content, time.time() - t0
    except Exception as exc:  # noqa: BLE001
        return type(exc).__name__, b"", time.time() - t0


def as_json(body):
    try:
        return json.loads(body) if body else None
    except Exception:  # noqa: BLE001
        return None


def series(sid):
    """/series the way the page calls it -> (http, sorted [[date, value]], name, unit)"""
    code, body, _ = fetch("%s/series?id=%s" % (PROXY, urllib.parse.quote(sid)))
    p = as_json(body) or {}
    obs = []
    for row in (p.get("obs") or p.get("bars") or []):
        raw = row[1] if isinstance(row, list) else (row.get("close", row.get("value")) if isinstance(row, dict) else None)
        date = str(row[0] if isinstance(row, list) else (row.get("time") or row.get("date") or ""))[:10]
        try:
            val = float(raw)
        except (TypeError, ValueError):
            continue
        if len(date) == 10 and val == val:
            obs.append([date, val])
    obs.sort()
    return code, obs, str(p.get("name") or p.get("title") or ""), str(p.get("unit") or p.get("units") or ""), str(p.get("frequency") or p.get("freq") or "")


def shape(v, depth=0):
    if isinstance(v, dict):
        if depth >= 2:
            return "{%d keys}" % len(v)
        return "{" + ", ".join("%s: %s" % (k, shape(x, depth + 1)) for k, x in list(v.items())[:14]) + (", ...+%d" % (len(v) - 14) if len(v) > 14 else "") + "}"
    if isinstance(v, list):
        return "[%d x %s]" % (len(v), shape(v[-1], depth + 1) if v else "")
    s = repr(v)
    return s if len(s) < 36 else s[:33] + "..."


def main():
    now = datetime.now(timezone.utc)
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    pack = {"generated_at": now.isoformat(timespec="seconds"), "series": {}, "data": {}}
    with report("ops_6826_flows_tearsheet_coverage_audit") as r:
        r.heading("ops 6826 - flows tearsheet + coverage audit %s (read-only)" % now.isoformat(timespec="minutes"))

        r.section("1. the desk's catalog as shipped")
        src = (ROOT / "chart-pro.html").read_text(encoding="utf-8")
        cat_src = src[src.index("static FLOWS_CATALOG = ["):src.index("static FLOW_FEEDS = {")]
        fams = re.findall(r'\{ cat: "([^"]+)", flag: "[^"]*", items: \[(.*?)\]\}', cat_src, re.S)
        catalog = []
        for name, body in fams:
            items = re.findall(r'\["([^"]+)","([^"]+)","([^"]*)"\]', body)
            catalog.append((name, items))
            r.log("%-70s %3d series" % (name[:70], len(items)))
        total = sum(len(i) for _, i in catalog)
        r.log("**%d families, %d series**" % (len(catalog), total))
        plain = [sym for _, items in catalog for sym, _, _ in items if not sym.startswith("flows:")]
        ok = 0
        stale = []
        for sym in plain:
            sid = sym if ":" in sym else "FRED:" + sym
            code, obs, name, unit, freq = series(sid)
            if obs:
                ok += 1
                pack["series"][sid] = obs[-KEEP:]
                age = (now.date() - datetime.strptime(obs[-1][0], "%Y-%m-%d").date()).days
                if age > 200:
                    stale.append("%s last %s" % (sym, obs[-1][0]))
            else:
                r.log("NOT RESOLVING: %s HTTP %s" % (sym, code))
        r.log("series through /series: %d of %d resolve | last print older than 200 days: %s" % (ok, len(plain), stale or "none"))

        r.section("2. discovery through /symsearch")
        seen_ids = set()
        for q in SEARCHES:
            code, body, took = fetch("%s/symsearch?q=%s&limit=40" % (PROXY, urllib.parse.quote(q)), timeout=40)
            p = as_json(body) or {}
            rows = list((p.get("series_hits") or {}).get("rows") or []) + [x for x in (p.get("rows") or []) if x.get("kind") == "series"]
            uniq = []
            for x in rows:
                if x.get("id") and x["id"] not in seen_ids and x.get("chartable") is not False:
                    seen_ids.add(x["id"])
                    uniq.append(x)
            r.log("**%s** - HTTP %s %.1fs, total %s, %d new series" % (q, code, took, p.get("total"), len(uniq)))
            for x in uniq[:14]:
                r.log("    %-34s %-10s %-16s %s%s" % (str(x.get("id"))[:34], str(x.get("provider") or "")[:10], str(x.get("unit") or x.get("units") or "")[:16], str(x.get("title") or x.get("name") or "")[:95],
                                                    (" | %s..%s" % (x.get("first") or x.get("start") or "", x.get("last") or x.get("end") or "")) if (x.get("last") or x.get("end")) else ""))

        r.section("3. candidates through /series")
        good = []
        for sym in CANDIDATES:
            sid = sym if ":" in sym else "FRED:" + sym
            code, obs, name, unit, freq = series(sid)
            if obs:
                age = (now.date() - datetime.strptime(obs[-1][0], "%Y-%m-%d").date()).days
                gaps = sorted((datetime.strptime(b[0], "%Y-%m-%d") - datetime.strptime(a[0], "%Y-%m-%d")).days for a, b in zip(obs[-9:-1], obs[-8:]))
                spacing = gaps[len(gaps) // 2] if gaps else None
                fresh = spacing is not None and age <= max(10, spacing * 2.6)
                r.log("%s %-26s %5d obs %s -> %s (%sd apart, %sd old) last %-16s %-22s %s" % ("OK   " if fresh else "STALE", sym, len(obs), obs[0][0], obs[-1][0], spacing, age, ("%.6g" % obs[-1][1]), unit[:22], name[:80]))
                if fresh:
                    good.append(sym)
                    pack["series"][sid] = obs[-KEEP:]
            else:
                r.log("---   %-26s HTTP %s no observations" % (sym, code))
        r.log("**fresh candidates %d of %d**: %s" % (len(good), len(CANDIDATES), " ".join(good)))

        r.section("4. the fleet's feeds")
        keys = []
        token = None
        while True:
            kw = {"Bucket": LIVE, "Prefix": "data/", "Delimiter": "/", "MaxKeys": 1000}
            if token:
                kw["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kw)
            keys.extend(resp.get("Contents") or [])
            token = resp.get("NextContinuationToken")
            if not token:
                break
        hits = [k for k in keys if KEY_RE.search(k["Key"])]
        r.log("data/ top-level objects %d, flow-like %d" % (len(keys), len(hits)))
        for k in sorted(hits, key=lambda z: z["Key"]):
            age = (now - k["LastModified"]).total_seconds() / 86400
            r.log("    %-58s %9d B  %5.1f d old" % (k["Key"], k["Size"], age))
        for name in FEEDS:
            try:
                obj = s3.get_object(Bucket=LIVE, Key="data/%s.json" % name)
                body = obj["Body"].read()
                doc = json.loads(gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body)
                r.log("**data/%s.json** %d B, modified %s: %s" % (name, len(body), obj["LastModified"].isoformat(timespec="minutes"), shape(doc)[:1500]))
            except Exception as exc:  # noqa: BLE001
                r.log("data/%s.json: %s" % (name, type(exc).__name__))

        r.section("5. histories the desk can read")
        code, body, _ = fetch("%s/data/etf-creations/index.json?t=%d" % (PROXY, int(time.time())))
        idx = as_json(body) or {}
        funds = idx.get("funds") or idx.get("by_ticker") or idx.get("tickers") or {}
        r.log("etf-creations/index.json HTTP %s: top keys %s; shape %s" % (code, list(idx.keys())[:12], shape(idx)[:700]))
        on_desk = {sym.split("/")[1].split(".")[0] for _, items in catalog for sym, _, _ in items if sym.startswith("flows:etfc/")}
        fund_rows = funds.items() if isinstance(funds, dict) else [((f.get("ticker") or f.get("symbol")), f) for f in funds]
        listed = []
        for t, f in sorted(fund_rows, key=lambda kv: str(kv[0])):
            listed.append(str(t))
            if str(t) not in on_desk:
                r.log("    not on the desk: %-6s %s" % (t, shape(f)[:420]))
        r.log("index lists %d funds; on the desk %d; missing from the index but on the desk: %s" % (len(listed), len(on_desk), sorted(on_desk - set(listed)) or "none"))
        etf_pack = sorted(set(listed))
        for t in etf_pack:
            code, body, _ = fetch("%s/data/etf-creations/%s.json?t=%d" % (PROXY, t, int(time.time()) // 300))
            p = as_json(body)
            if not p or not isinstance(p.get("d"), list):
                r.log("    %s: HTTP %s unreadable" % (t, code))
                continue
            n = len(p["d"])
            ser = p.get("series") or {}
            if t not in on_desk:
                def last_ok(arr):
                    return next((arr[i] for i in range(len(arr) - 1, -1, -1) if arr[i] is not None), None)
                r.log("    %-6s %5d days %s -> %s | last flow %s, 5d %s (non-null %d), 20d %s, net assets %s" % (
                    t, n, p["d"][0] if n else None, p["d"][-1] if n else None, last_ok(ser.get("flow") or []), last_ok(ser.get("flow_5d") or []), sum(1 for v in (ser.get("flow_5d") or []) if v is not None),
                    last_ok(ser.get("flow_20d") or []), last_ok(ser.get("net_assets") or [])))
            pack["data"]["/data/etf-creations/%s.json" % t] = {"ticker": p.get("ticker") or t, "d": p["d"][-KEEP:], "series": {k: (v[-KEEP:] if isinstance(v, list) else v) for k, v in ser.items()},
                                                              "latest": p.get("latest")}
        for path, trim in (("/data/ici-fund-flows.json", None), ("/data/history/apac-flows.json", None), ("/data/crypto-etf-flows-history.json", None), ("/data/crypto-exchange-flows-history.json", None),
                           ("/data/stablecoin-supply-history.json", "series"), ("/data/cryptoquant-series.json", "cq")):
            code, body, _ = fetch("%s%s?t=%d" % (PROXY, path, int(time.time()) // 300), timeout=90)
            p = as_json(body)
            if p is None:
                r.log("%s: HTTP %s unreadable" % (path, code))
                continue
            if trim == "series":
                p["series"] = (p.get("series") or [])[-KEEP * 2:]
            if trim == "cq":
                names = sorted((p.get("series") or {}).keys())
                r.log("cryptoquant-series.json: %d series" % len(names))
                for nm in names:
                    s = p["series"][nm]
                    d, v = s.get("d") or [], s.get("v") or []
                    lastv = next((v[i] for i in range(len(v) - 1, -1, -1) if v[i] is not None), None)
                    r.log("    %-44s n=%-4d %s -> %s last %s" % (nm, len(d), d[0] if d else None, d[-1] if d else None, lastv))
                p["series"] = {nm: {"d": (s.get("d") or [])[-KEEP:], "v": (s.get("v") or [])[-KEEP:]} for nm, s in p["series"].items()}
            else:
                r.log("%s HTTP %s: %s" % (path, code, shape(p)[:900]))
            if path.endswith("ici-fund-flows.json"):
                for k in ("history", "combined_history", "etf_net_issuance_history", "money_market_history"):
                    rows = p.get(k) or []
                    r.log("    ici %s: %d rows, last %s" % (k, len(rows), json.dumps(rows[-1])[:400] if rows else None))
            if path.endswith("history/apac-flows.json"):
                days = p.get("days") or {}
                dk = sorted(days)
                last = days[dk[-1]] if dk else {}
                r.log("    apac days %d %s -> %s; last day: %s" % (len(dk), dk[0] if dk else None, dk[-1] if dk else None, json.dumps(last)[:1400]))
                weeks = p.get("jp_weeks") or {}
                wk = sorted(weeks)
                if wk:
                    mk = (weeks[wk[-1]] or {}).get("markets") or {}
                    r.log("    apac jp_weeks %d %s -> %s; markets %s; prime groups %s" % (len(wk), wk[0], wk[-1], list(mk.keys()), json.dumps(mk.get("prime"))[:700]))
            pack["data"][path] = p

        r.section("6. S&P closes for the market turns + the offline pack")
        code, body, _ = fetch("%s/ohlc?ticker=SPY&span=day&mult=1&days=12000" % PROXY, timeout=90)
        p = as_json(body) or {}
        bars = p.get("bars") or []
        pack["spy"] = [[int(b.get("time") or 0), b.get("close")] for b in bars if b.get("close") is not None]
        r.log("SPY bars %d (HTTP %s)" % (len(pack["spy"]), code))
        blob = base64.b64encode(gzip.compress(json.dumps(pack, separators=(",", ":")).encode("utf-8"), 9)).decode("ascii")
        r.log("pack: %d series, %d data files, %d chars" % (len(pack["series"]), len(pack["data"]), len(blob)))
        r.lines.append("<!-- FLOWS-PACK-GZ-B64-BEGIN")
        r.lines.extend(blob[i:i + 16000] for i in range(0, len(blob), 16000))
        r.lines.append("FLOWS-PACK-GZ-B64-END -->")
        r.kv(families=str(len(catalog)), series=str(total), series_resolving="%d/%d" % (ok, len(plain)), fresh_candidates="%d/%d" % (len(good), len(CANDIDATES)), pack_series=str(len(pack["series"])),
             pack_files=str(len(pack["data"])))
        r.ok("AUDIT: %d families / %d series on the desk; %d of %d candidate series resolve fresh; %d flow-like feeds under data/" % (len(catalog), total, len(good), len(CANDIDATES), len(hits)))


if __name__ == "__main__":
    main()
    sys.exit(0)
