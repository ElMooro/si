"""
justhodl-smart-money-13f — TRACK THE INVESTORS WHO HAVE THE "SECRET"
====================================================================
The most direct way to apply Aschenbrenner's edge is to watch where his (and peer
AGI/AI-infra conviction funds') capital actually goes, then cross-reference it
against the names YOUR system already flags as cheap-for-the-growth. When proven
thematic money is accumulating a bottleneck name that the re-rating radar also
calls mispriced, that is the highest-conviction confluence available.

Pulls each fund's latest Form 13F-HR holdings straight from SEC EDGAR (FREE, no key,
just a declared User-Agent), parses the information table (longs + puts), resolves
tickers, maps each long to its AI-infra layer, and surfaces:
  • LONGS by layer (power / compute / silicon / memory / optics / miners-to-AI / ...)
  • PUTS = "what the smart money is shorting" (the overpriced side — currently the
    mega-cap AI chips). A put that overlaps one of YOUR bullish picks = a contrarian
    warning worth knowing.
  • CONFLUENCE: their longs that also appear cheap in your re-rating radar / ai-infra
    universe — copy-the-thesis candidates.

HONEST LIMITS (stated in output): 13F is stale by up to 45 days, shows only long
equity + listed options (no shorts except puts, no strikes/expiries), and a copycat
sees the book after the fund traded into it. Idea-generation + confirmation, NOT timing.

OUTPUT data/smart-money-13f.json   SCHEDULE weekly Mon 11:00 UTC. Real SEC data, research only.
"""
import json
import re
import time
import urllib.request
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/smart-money-13f.json"
UA = {"User-Agent": "JustHodl Research contact@justhodl.ai"}
s3 = boto3.client("s3", region_name="us-east-1")

# thematic AGI / AI-infrastructure conviction funds (extensible)
FUNDS = [
    {"name": "Situational Awareness LP", "manager": "Leopold Aschenbrenner", "cik": "0002045724"},
]

# issuer-name -> ticker for the known thematic book (13F gives CUSIP+name, not ticker)
NAME_TICKER = {
    "BLOOM ENERGY": "BE", "SANDISK": "SNDK", "MICRON": "MU", "TAIWAN SEMICONDUCTOR": "TSM",
    "INTEL": "INTC", "IREN": "IREN", "APPLIED DIGITAL": "APLD", "CORE SCIENTIFIC": "CORZ",
    "RIOT": "RIOT", "CLEANSPARK": "CLSK", "BITDEER": "BTDR", "HUT 8": "HUT", "HUT8": "HUT",
    "COHERENT": "COHR", "LUMENTUM": "LITE", "EQT": "EQT", "COREWEAVE": "CRWV",
    "NVIDIA": "NVDA", "BROADCOM": "AVGO", "ORACLE": "ORCL", "ADVANCED MICRO": "AMD",
    "ASML": "ASML", "CORNING": "GLW", "SOLARIS": "SEI", "TOWER SEMICONDUCTOR": "TSEM",
    "GALAXY DIGITAL": "GLXY", "CIPHER MINING": "CIFR", "VANECK": "SMH", "NEBIUS": "NBIS",
    "VERTIV": "VRT", "CELESTICA": "CLS", "ASTERA": "ALAB", "CREDO": "CRDO", "PENGUIN": "PESI",
    "POWELL": "POWL", "GE VERNOVA": "GEV", "CONSTELLATION ENERGY": "CEG", "VISTRA": "VST",
    "TALEN": "TLN", "OKLO": "OKLO", "NUSCALE": "SMR",
}


def _get(url, raw=False):
    try:
        r = urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=20).read()
        return r if raw else json.loads(r)
    except Exception as e:
        print(f"[get] {url[:70]}: {e}")
        return None


def resolve_ticker(name):
    u = re.sub(r"[^A-Z0-9 ]", " ", (name or "").upper())
    for key, tk in NAME_TICKER.items():
        if key in u:
            return tk
    return None


def latest_13f(cik):
    """Return (report_date, filing_date, holdings[]) from the latest 13F-HR."""
    subs = _get(f"https://data.sec.gov/submissions/CIK{cik}.json")
    if not subs:
        return None, None, []
    rec = subs.get("filings", {}).get("recent", {})
    forms = rec.get("form", []); accns = rec.get("accessionNumber", [])
    rdates = rec.get("reportDate", []); fdates = rec.get("filingDate", [])
    idx = next((i for i, f in enumerate(forms) if f == "13F-HR"), None)
    if idx is None:
        return None, None, []
    accn = accns[idx]; acc_nodash = accn.replace("-", "")
    cik_int = str(int(cik))
    base = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_nodash}"
    listing = _get(f"{base}/index.json")
    items = (listing or {}).get("directory", {}).get("item", [])
    xmls = [it["name"] for it in items if it.get("name", "").lower().endswith(".xml")
            and "primary_doc" not in it.get("name", "").lower()]
    holdings = []
    for xn in xmls:
        raw = _get(f"{base}/{xn}", raw=True)
        if not raw:
            continue
        txt = raw.decode("utf-8", "ignore")
        if "infoTable" not in txt and "nameOfIssuer" not in txt:
            continue
        txt = re.sub(r"<(/?)[A-Za-z0-9]+:", r"<\1", txt)   # strip namespace prefixes
        for b in re.findall(r"<infoTable>(.*?)</infoTable>", txt, re.S):
            nm = re.search(r"<nameOfIssuer>(.*?)</nameOfIssuer>", b, re.S)
            val = re.search(r"<value>(.*?)</value>", b, re.S)
            sh = re.search(r"<sshPrnamt>(.*?)</sshPrnamt>", b, re.S)
            pc = re.search(r"<putCall>(.*?)</putCall>", b, re.S)
            if not nm:
                continue
            try:
                value = float(re.sub(r"[^0-9.]", "", val.group(1))) if val else 0.0
            except Exception:
                value = 0.0
            holdings.append({
                "issuer": nm.group(1).strip(),
                "ticker": resolve_ticker(nm.group(1)),
                "value": value,
                "shares": (sh.group(1).strip() if sh else None),
                "put_call": (pc.group(1).strip().upper() if pc else None),
            })
        if holdings:
            break
    return (rdates[idx] if idx < len(rdates) else None,
            fdates[idx] if idx < len(fdates) else None, holdings)


def layer_map():
    d = _get("https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/ai-infra-stack.json")
    m = {}
    if isinstance(d, dict):
        for layer in d.get("stack", []):
            for n in layer.get("names", []):
                if n.get("symbol"):
                    m[n["symbol"]] = layer.get("layer")
    return m


def _read_s3(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def lambda_handler(event, context):
    t0 = time.time()
    lm = layer_map()
    # your system's cheap-for-growth names (re-rating radar) for confluence
    rr = _read_s3("data/ai-rerating-radar.json") or {}
    cheap = {}
    for r in ((rr.get("summary", {}) or {}).get("top_setups", []) or []):
        if r.get("symbol"):
            cheap[r["symbol"]] = {"discount_pct": r.get("discount_to_implied_pct"),
                                  "growth_pct": r.get("growth_pct")}

    funds_out = []
    all_long, all_put = {}, {}
    for fund in FUNDS:
        rdate, fdate, holdings = latest_13f(fund["cik"])
        longs = [h for h in holdings if h["put_call"] != "PUT"]
        puts = [h for h in holdings if h["put_call"] == "PUT"]
        longs.sort(key=lambda h: h["value"], reverse=True)
        puts.sort(key=lambda h: h["value"], reverse=True)
        for h in longs:
            if h["ticker"]:
                h["layer"] = lm.get(h["ticker"])
                all_long[h["ticker"]] = all_long.get(h["ticker"], 0) + 1
        for h in puts:
            if h["ticker"]:
                all_put[h["ticker"]] = all_put.get(h["ticker"], 0) + 1
        funds_out.append({
            "fund": fund["name"], "manager": fund["manager"], "cik": fund["cik"],
            "report_date": rdate, "filing_date": fdate,
            "n_longs": len(longs), "n_puts": len(puts),
            "top_longs": longs[:20], "puts": puts[:20],
        })

    # CONFLUENCE: smart-money longs that your radar also calls cheap
    confluence = []
    for tk, n in all_long.items():
        if tk in cheap:
            confluence.append({"ticker": tk, "n_funds_long": n, "layer": lm.get(tk),
                               "your_discount_pct": cheap[tk]["discount_pct"],
                               "your_growth_pct": cheap[tk]["growth_pct"],
                               "note": "smart thematic money is long AND your re-rating radar calls it cheap"})
    confluence.sort(key=lambda x: (x["your_discount_pct"] or 0), reverse=True)

    # by-layer view of the aggregate smart-money long book
    layer_book = {}
    for f in funds_out:
        for h in f["top_longs"]:
            lk = h.get("layer") or "unmapped"
            layer_book.setdefault(lk, [])
            if h["ticker"] and h["ticker"] not in [x["ticker"] for x in layer_book[lk]]:
                layer_book[lk].append({"ticker": h["ticker"], "issuer": h["issuer"]})

    out = {
        "engine": "smart-money-13f", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": "Track the AGI/AI-infra conviction funds that have demonstrated the edge, map their book to "
                  "bottleneck layers, and flag where their longs overlap your own cheap-for-growth names.",
        "funds": funds_out,
        "smart_money_long_by_layer": layer_book,
        "shorting_signal": sorted(all_put.keys()),
        "confluence_cheap_and_backed": confluence,
        "interpretation": "LONGS by layer mirror the buildout bottlenecks; PUTS are the overpriced side they're "
                          "fading (currently mega-cap AI chips). Confluence = your radar + their capital agree.",
        "caveats": "13F is stale up to 45 days, discloses only long equity + listed options (no short stock, no "
                   "option strikes/expiries), and a copycat always sees the book late. Tickers resolved by issuer "
                   "name; unmapped names show issuer only. Idea-generation & confirmation, not entry timing — and "
                   "past positioning is not predictive. Real SEC EDGAR data, research only, not investment advice.",
        "source": "SEC EDGAR Form 13F-HR (free, public)",
        "elapsed_s": round(time.time() - t0, 2),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(),
                  ContentType="application/json")
    nl = sum(f["n_longs"] for f in funds_out)
    print(f"[smart-money-13f] funds={len(funds_out)} longs={nl} confluence={len(confluence)} "
          f"shorting={len(all_put)} {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "funds": len(funds_out),
            "total_longs": nl, "confluence": len(confluence), "n_shorts": len(all_put)})}
