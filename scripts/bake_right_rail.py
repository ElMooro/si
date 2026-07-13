#!/usr/bin/env python3
"""scripts/bake_right_rail.py — Design audit Sec8-C: per-page insight rail.
Computes ONLY real, derivable data at deploy time:
  - feeds: this page's actual data/*.json + cot/*.json refs, with LIVE freshness
  - related: siblings from the audit's own IA taxonomy (given reference data)
  - feedsInto: small curated map of KNOWN documented consumption relationships
  - interpret: page's own non-generic <meta name=description> if present
No section is invented. Empty section -> omitted entirely by the renderer.
Idempotent (skips pages already carrying __jhRail). Two-pass: collect all
unique feed keys first, fetch freshness CONCURRENTLY once, then assemble.
"""
import glob, json, re, sys, time
from concurrent.futures import ThreadPoolExecutor
from email.utils import parsedate_to_datetime

BUCKET = "https://justhodl.ai"

TAXONOMY = {
  "MACRO & LIQUIDITY": ["lce","liquidity","auctions","auction-crisis","treasury-auctions","bonds",
    "global-cycle","macro-data","census","activity-nowcast","consumer-pulse","dealer-survey","ny-fed",
    "ecb","bls","eia","yen-carry","eurodollar","dollar","dxy","repo","crisis"],
  "CROSS-ASSET & POSITIONING": ["desk","cross-asset","flow","options-scanner","gex","positioning",
    "cot-extremes","carry","carry-surface","correlation","analogs","valuations","ath","volatility","vrp",
    "crypto","crypto-opportunities","stablecoin-flow","dex","sentiment","narrative","gdelt","benzinga",
    "apac","onchain","options"],
  "ALPHA & SIGNALS": ["panels","alpha","signals","signal-board","conviction","debate","kill-theses","calls","edge",
    "edge-discovery","screener","chart-patterns","forensic","insider-buys","buyback-scanner",
    "activist-13d","russell-recon","index-recon","breadth-thrust","vix-capitulation","vol-target-unwind",
    "opex-calendar","rv-iv-scanner","pre-pump-radar","retail","sector-emergence","crypto-emergence",
    "ml-predictions","trading-signals","investor","why","compare","stock","master-rank"],
  "RISK & HEDGING": ["risk","risk-desk","risk-regime","regime","regime-map","risk-monitor","risk-radar",
    "firm-risk-board","firm-book","firm-stress","factor-risk","liquidity-capacity","merger-arb-risk",
    "pnl-attribution","stress","global-stress","market-extremes","vol-radar","tail-hedge","hedge-planner",
    "hedge-pnl","anomaly","live-pulse","live","gsi-calibration","horizons-gsi","intelligence","ka"],
  "STRATEGY DESKS": ["merger-arb","pairs-arb","pairs-scanner","spinoff-desk","dividend-growth","rotation",
    "strategist","strategy-portfolio","paper-book","cycle-clock","catalyst","econ-calendar","event-study"],
  "PERSONAL WEALTH": ["compass","wealth-plan","tax-plan","portfolio","portfolio-manager","sizing",
    "risk-sizer","brain"],
  "PLATFORM": ["fred","fmp","ofr","nasdaq-datalink","charts","downloads","notifications","alerts",
    "system","observability","fleet-health","errors","audit","status"],
}
STEM2CAT = {s: cat for cat, stems in TAXONOMY.items() for s in stems}
FEEDS_INTO = {
  "squeeze-risk": [{"label":"Best-Setups Confluence","href":"/best-setups.html"}],
  "options-confluence": [{"label":"Options Hub","href":"/options.html"}],
  "crypto-forecast": [{"label":"Crypto Intel","href":"/crypto-intel.html"},
                       {"label":"Cycle Clock","href":"/cycle-clock.html"},
                       {"label":"Morning Intel","href":"/morning-intelligence.html"}],
  "risk-regime": [{"label":"KA Index","href":"/index.html"}],
  "liquidity-credit-engine": [{"label":"KA Index","href":"/index.html"}],
}
EXCLUDE = {"index.html","screener.html","directory.html","about.html","glossary.html","terms.html",
           "privacy.html","pricing.html","contact.html","api-docs.html"}
GENERIC_META = "institutional market intelligence"


def page_stem(fname):
    return fname[:-5] if fname.endswith(".html") else fname


def category_for(stem):
    if stem in STEM2CAT:
        return STEM2CAT[stem]
    toks = set(stem.split("-"))
    for s, cat in STEM2CAT.items():
        if set(s.split("-")) & toks:
            return cat
    return None


def fetch_age(key, live):
    if not live:
        return None
    import urllib.request
    try:
        r = urllib.request.urlopen(urllib.request.Request(
            f"{BUCKET}/{key}?t={int(time.time())}", headers={"User-Agent": "Mozilla/5.0 jh"}), timeout=12)
        lm = r.headers.get("Last-Modified")
        return (time.time() - parsedate_to_datetime(lm).timestamp()) / 3600 if lm else None
    except Exception:
        return None


def main(build_dir=".", live=True):
    man = json.loads(open(f"{build_dir}/nav-manifest.json", encoding="utf-8").read())
    titles = {p["href"].lstrip("/"): p["title"] for c in man["categories"] for p in c["pages"]}

    plan = {}
    all_keys = set()
    for path in glob.glob(f"{build_dir}/*.html"):
        fname = path.split("/")[-1]
        if fname in EXCLUDE:
            continue
        s = open(path, encoding="utf-8", errors="replace").read()
        if "__jhRail" in s or len(s) < 2000:
            continue
        direct = set(re.findall(r'["\'/](data/[a-z0-9_\-./]+?\.json)', s))
        cot = set(re.findall(r'(cot/[a-z0-9_\-./]+?\.json)', s))
        helper = set("data/" + k for k in re.findall(r"F\('([a-z0-9_\-]+\.json)'\)", s))
        refs = sorted(direct | cot | helper)
        if not refs:
            continue
        plan[path] = {"fname": fname, "text": s, "refs": refs[:6]}
        all_keys.update(refs[:6])

    # ops 3203: one site-wide research chip — top theme pressure + first
    # divergence from data/wl-fusion.json. Real data or nothing.
    research = None
    try:
        import urllib.request
        req = urllib.request.Request(BUCKET + "/data/wl-fusion.json",
                                     headers={"User-Agent": "jh-bake/1.0"})
        fus = json.loads(urllib.request.urlopen(req, timeout=12).read())
        th = fus.get("themes") or {}
        if th:
            top = max(th.items(),
                      key=lambda kv: kv[1].get("pressure_pctile") or 0)
            div = (fus.get("divergences") or [None])[0]
            research = {
                "theme": top[0],
                "pressure": top[1].get("pressure_pctile"),
                "verdict": top[1].get("verdict"),
                "firing": top[1].get("n_firing"),
                "of": top[1].get("n_active"),
                "div": (div.get("text") or div.get("note") or "")[:110]
                       if isinstance(div, dict) else "",
                "href": "/panels.html",
            }
    except Exception:
        research = None

    ages = {}
    with ThreadPoolExecutor(max_workers=16) as ex:
        for k, a in zip(all_keys, ex.map(lambda k: fetch_age(k, live), all_keys)):
            ages[k] = a

    baked = 0
    for path, info in plan.items():
        fname, s, refs = info["fname"], info["text"], info["refs"]
        stem = page_stem(fname)
        cat = category_for(stem)
        related = []
        if cat:
            members = TAXONOMY[cat]
            offset = sum(ord(c) for c in stem) % len(members)
            rotated = members[offset:] + members[:offset]
            for sib in [t for t in rotated if t != stem]:
                for cand_fname, title in titles.items():
                    if page_stem(cand_fname) == sib:
                        related.append({"title": title, "href": "/" + cand_fname})
                        break
                if len(related) == 4:
                    break
        feeds = [{"label": k.split("/")[-1].replace(".json", ""), "h": ages.get(k)} for k in refs]
        fi = FEEDS_INTO.get(stem, [])
        title = titles.get(fname, "")
        m = re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']{20,200})["\']', s, re.I)
        interpret = m.group(1).strip() if m else ""
        if GENERIC_META in interpret.lower():
            interpret = ""
        data = {"title": title, "feeds": feeds, "related": related, "feedsInto": fi, "interpret": interpret}
        if research:
            data["research"] = research
        inject = ("<script>window.__jhRail=" + json.dumps(data, separators=(",", ":")) + ";</script>"
                  '<script src="/jh-right-rail.js" defer></script>')
        s2 = s.replace("</body>", inject + "</body>", 1) if "</body>" in s else s + inject
        open(path, "w", encoding="utf-8").write(s2)
        baked += 1
    print(f"right-rail baked into {baked}/{len(glob.glob(build_dir + '/*.html'))} pages")
    return baked


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else ".", live="--no-live" not in sys.argv)
