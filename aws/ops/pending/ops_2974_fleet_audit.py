#!/usr/bin/env python3
"""ops 2974 -- FLEET AUDIT: every engine, every page, audited in one
machine-readable document (data/fleet-audit.json) + fleet-audit.html.

What it does, concretely:
  A. Repo scan -> live engine registry (name, doc, outs, fred, loc) using
     the exact ops-2876 technique, so numbers match engines.html.
  B. Repo scan -> page->feeds map from every root *.html.
  C. S3 loads: data/engine-wiring.json (wired/orphan), data/engine-trust
     .json (alpha verdicts) -- graceful warns if absent.
  D. Family classification (ordered keyword rules, first match wins);
     unclassified surfaced honestly.
  E. Redundancy detection: engines writing the SAME out key; pages
     serving the same single feed with similar stems; known merge
     clusters verified against files actually present.
  F. GAP MATRIX -- curated institutional add-list (metric, source,
     feasibility, wire-into target honoring no-rebuild) each one
     CROSS-CHECKED against the whole corpus (engine names + outs + fred
     series + page feeds + docs): if evidence exists it is demoted to
     already_covered with the evidence shown. No claiming a gap that
     isn't one.
  G. Engine-specific upgrade notes + umbrella actions + asset-compass
     v1.2 roadmap.
Nothing here is an LLM call -- pure scan + curated matrix + checks.
"""
import glob
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report

AWS_DIR = Path(__file__).resolve().parents[2]
REPO = AWS_DIR.parent
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/fleet-audit.json"

# ── D. family rules (ordered; first match wins) ──
FAMILY_RULES = [
    ("Crypto", r"crypto|btc|eth|solana|onchain|altseason|stablecoin|dex\b|"
               r"cryptoquant|halving|coin|hyperliquid|perps"),
    ("Options & Vol", r"option|gex|dealer-gex|opex|dix\b|skew|iv-crush|"
                      r"vol-|vix|vrp|variance|gamma|straddle"),
    ("Credit", r"credit|cds|high-yield|hyg|oas|spread-internals|lce\b"),
    ("Macro & Rates", r"repo-market|eurodollar|funding|plumbing|liquidity|"
                      r"tga|rrp|reserve|sofr|fed-|fomc|treasury|auction|"
                      r"sovereign|dollar|dxy|fx-|yen|boj|ecb|snb|term-"
                      r"premium|yield|bond|rates|\bust\b|macro|nowcast|"
                      r"inflation|payroll|gdp|bls|bea|census|eia|cycle-"
                      r"clock|global-tide|leverage|inflection|"
                      r"\basset-|compass|cross-asset|carry|cot-|cftc|"
                      r"fedwatch|cb-|construction|consumer|econ-calendar|"
                      r"economy|repo-agent|hiring|labor|margin-lending"),
    ("Risk & Crisis", r"risk|crisis|stress|tail|hedge|defcon|canar|"
                      r"systemic|correlation|divergence|drawdown-guard|"
                      r"switzerland|fragmentation|episode|gsi|horizons|"
                      r"cro-|kill-switch|ciss"),
    ("Global & Flows", r"apac|global|china|india|japan|europe|em-|intl|"
                       r"capital-flow|capital-inflow|flow|ici|etf-flow|"
                       r"rebalance|hot-money|leading-markets|leadlag"),
    ("Commodities & Metals", r"gold|silver|metal|miner|copper|uranium|"
                             r"oil\b|natgas|commodit|agri|energy"),
    ("Equity Signals", r"insider|13f|13d|institutional|smart-money|"
                       r"dark-pool|short|squeeze|buyback|earnings|eps|"
                       r"estimate|analyst|pead|momentum|value|quality|"
                       r"factor|sector|industry|rotation|breadth|dividend|"
                       r"magic|beneish|forensic|resilience|ignition|"
                       r"pattern|pairs|merger|spinoff|russell|ipo|equity|"
                       r"stock|screen|scanner|bagger|upside|catch-up|"
                       r"dislocat|boom|bottleneck|scarcity|chokepoint|"
                       r"supply|patent|lobby|political|conviction|"
                       r"compound|confluence|attention|pump|ath\b|"
                       r"highs|targets|theme|opportun|nobrainer|starmine|"
                       r"predictab|ma200|ma-|trend|tape|internals|"
                       r"whisper|blackout|activist|ark\b|finviz|"
                       r"heatmap|market-map|groups"),
    ("Research & LLM", r"brain|ask|strategist|kill-theses|llm|digest|"
                       r"research|why\b|dossier|narrative|gdelt|sentiment|"
                       r"news|buzz|investor-lens|technical-overlay|"
                       r"debate|critique|devils|ai-|chatgpt|chat-api|"
                       r"vision|synthesis|brief|report|intel|secretary|"
                       r"mirror|future|investor-agents|interpreter"),
    ("Portfolio & Exec", r"alloc|portfolio|sizing|weights|hedge-planner|"
                         r"pnl|firm-book|journal|watchlist|paper-book|"
                         r"position|wealth|tax|track-record|desk-returns|"
                         r"best-ideas|best-setups|coffee-can|khalid|ka-metrics"),
    ("Meta & Infra", r"registry|compiler|monitor|health|observ|deploy|"
                     r"proxy|auth|user|telegram|sentinel|alert|history|"
                     r"cache|router|cost|freshness|synthetic|streaming|"
                     r"fanout|public-api|feed-|backup|cleanup|kb\b|"
                     r"skill|calibrat|scorecard|accuracy|proof|"
                     r"signal-|meta-labeler|backtest|edge-|trust|"
                     r"genealogy|halflife|orthogonality|replay|"
                     r"interpretation|engine-|conflicts|ab-test|"
                     r"anomaly|api-keys|infra|data-collector|snapshot|"
                     r"coordinator|escalation|email|feedback|failure|"
                     r"validator|dep-graph|processor|collector|admin|"
                     r"agent-api|-agent$|-api$|chart-data|charts-agent|"
                     r"predictor|analyzer|dr-"),
    ("Equity Signals",
     r"asymmetric|apex|backlog|beta-|capex|capital-return|capitulation|"
     r"cascade|catalyst|cef|consensus|convergence|convexity|early-mov|"
     r"esi\b|etf-constituents|eva|event-study|filings|finnhub|"
     r"forced-selling|forward|fundamentals|gap-fill|hunter|laggard|"
     r"alpha-|behavior|accumulation|rerating|divcut|bloomberg|"
     r"implied-prob|magnitude|analog|market-|intraday|pulse|massive|"
     r"lockup|inventory|technical-analysis|predictor|whisper"),
]


def classify(name):
    n = name.replace("justhodl-", "")
    for fam, rx in FAMILY_RULES:
        if re.search(rx, n):
            return fam
    return "Unclassified"


# ── F. curated GAP MATRIX (tokens are cross-checked against corpus) ──
GAP_MATRIX = [
 dict(id="M1", family="Credit", name="SLOOS bank lending standards",
      tokens=["DRTSCILM", "DRTSCIS", "sloos"], source="FRED (quarterly)",
      feasibility="FEASIBLE_NOW", wire_into="lce + risk-regime funding block",
      why="Banks tighten 2-3 quarters before HY spreads widen and defaults "
          "rise -- the best slow-moving credit lead the fleet lacks."),
 dict(id="M2", family="Macro & Rates", name="Atlanta Fed GDPNow official "
      "nowcast", tokens=["gdpnow"], source="Atlanta Fed public CSV",
      feasibility="FEASIBLE_NOW", wire_into="activity-nowcast (overlay + "
      "divergence alert vs internal growth pulse)",
      why="Free official nowcast; divergence vs the internal proxy is "
          "itself a signal."),
 dict(id="M3", family="Macro & Rates", name="5y5y forward inflation + real "
      "5y5y", tokens=["T5YIFR"], source="FRED T5YIFR + DFII5/DFII10",
      feasibility="FEASIBLE_NOW", wire_into="asset-compass macro block + "
      "fomc", why="The anchoring measure the Fed actually watches; one "
      "series read."),
 dict(id="M4", family="Macro & Rates", name="QRA bill-share issuance mix",
      tokens=["bill_share", "qra", "bill share"],
      source="TreasuryDirect API + TBAC docs", feasibility="MEDIUM",
      wire_into="treasury-noise + repo-market",
      why="Bill-heavy issuance drains RRP not reserves; coupon-heavy hits "
          "duration -- changes the liquidity read of the same deficit."),
 dict(id="M5", family="Credit", name="CCC/BB quality-spread ratio",
      tokens=["BAMLH0A3HYC", "BAMLH0A1HYBB"], source="FRED",
      feasibility="FEASIBLE_NOW", wire_into="risk-regime + asset-compass "
      "credit_context", why="Credit internals: CCC blowing out while BB "
      "sleeps marks the cycle turn before index OAS moves."),
 dict(id="M6", family="Credit", name="BBB share / fallen-angel pressure",
      tokens=["BAMLC0A4CBBB"], source="FRED BBB vs BB OAS gap",
      feasibility="FEASIBLE_NOW", wire_into="new Credit Desk umbrella",
      why="Downgrade wave risk: the IG->HY cliff is where forced selling "
          "lives."),
 dict(id="M7", family="Credit", name="HY primary-issuance window monitor",
      tokens=["sifma", "primary issuance"], source="SIFMA weekly public "
      "stats", feasibility="MEDIUM", wire_into="lce",
      why="Issuance windows shut before spreads gap -- the market says no "
          "to new paper first."),
 dict(id="M8", family="Options & Vol", name="VVIX vol-of-vol + VVIX/VIX "
      "ratio", tokens=["VVIX"], source="Yahoo ^VVIX",
      feasibility="FEASIBLE_NOW", wire_into="vol-radar + tail-hedge",
      why="Hedging-cost regime: when vol-of-vol is bid, tail protection "
          "is expensive and dealers are short convexity."),
 dict(id="M9", family="Options & Vol", name="Implied correlation (COR3M)",
      tokens=["COR3M", "implied correlation"], source="Yahoo ^COR3M",
      feasibility="MEDIUM", wire_into="options-confluence",
      why="Dispersion regime: low implied corr = single-stock market; "
          "spikes mark macro takeover."),
 dict(id="M10", family="Options & Vol", name="0DTE gamma share",
      tokens=["0dte"], source="Polygon same-day-expiry chain OI",
      feasibility="MEDIUM", wire_into="dealer-gex",
      why="Intraday pin/acceleration risk now dominated by 0DTE -- "
          "gamma maps ignoring it mis-state dealer positioning."),
 dict(id="M11", family="Equity Signals", name="Estimate-revision breadth "
      "index (market level)", tokens=["revision breadth",
      "revision_breadth"], source="derive from existing estimate-"
      "revisions engine", feasibility="FEASIBLE_NOW",
      wire_into="equity-confluence + morning-intel",
      why="% of coverage with positive 3m EPS revisions leads the EPS "
          "cycle; the engine has the rows, nobody aggregates them."),
 dict(id="M12", family="Equity Signals", name="NAAIM manager exposure",
      tokens=["naaim"], source="NAAIM weekly public number",
      feasibility="MEDIUM", wire_into="sentiment-extreme composite",
      why="Active-manager beta positioning -- pairs with AAII (already "
          "ingested) for a two-sided sentiment extreme."),
 dict(id="M13", family="Risk & Crisis", name="Stock-bond rolling-"
      "correlation regime flag", tokens=["stock_bond", "spy_tlt_corr"],
      source="computable from existing daily bars",
      feasibility="FEASIBLE_NOW", wire_into="risk-regime + asset-compass",
      why="The 60/40 switch: positive stock-bond corr means duration no "
          "longer hedges equities -- changes every hedge ratio downstream."),
 dict(id="M14", family="Global & Flows", name="Global USD-M2 impulse",
      tokens=["global m2", "global_m2"], source="FRED national M2s, "
      "FX-converted", feasibility="FEASIBLE_NOW",
      wire_into="global-tide + cycle-clock",
      why="The liquidity tide behind every risk asset; YoY impulse leads "
          "BTC and QQQ at cycle scale."),
 dict(id="M15", family="Commodities & Metals", name="Gold-miner margin "
      "proxy (AISC vs gold)", tokens=["aisc", "miner margin"],
      source="FMP fundamentals vs GLD", feasibility="FEASIBLE_NOW",
      wire_into="metals-miners",
      why="Margin expansion, not gold price, drives miner re-ratings -- "
          "GDX beta already measured, margins close the loop."),
 dict(id="M16", family="Risk & Crisis", name="OFR Financial Stress Index "
      "auto-pull", tokens=["ofr-fsi", "ofr_fsi", "financial stress index"],
      source="OFR public CSV", feasibility="FEASIBLE_NOW",
      wire_into="systemic-stress + ofr page (currently feedless)",
      why="Official daily systemic-stress benchmark to sanity-check the "
          "in-house composite."),
 dict(id="M17", family="Crypto", name="Spot-ETF daily flows (IBIT/FBTC)",
      tokens=["ibit", "fbtc"], source="issuer pages / Farside",
      feasibility="MEDIUM", wire_into="crypto-liquidity + altseason",
      why="The marginal BTC buyer since 2024 is the ETF complex; flows "
          "are the cleanest demand read."),
 dict(id="M18", family="Crypto", name="Cross-exchange perp funding "
      "aggregate", tokens=["funding_rate", "perp funding"],
      source="Binance/Bybit public endpoints", feasibility="MEDIUM",
      wire_into="crypto-risk", why="Leverage temperature; extreme funding "
      "marks local tops/bottoms better than price."),
 dict(id="M19", family="Global & Flows", name="EM FX carry basket",
      tokens=["em_fx_carry", "em carry"], source="Polygon FX (engine "
      "already streams 19 pairs)", feasibility="FEASIBLE_NOW",
      wire_into="dollar-radar (14th canary)",
      why="Carry appetite is the purest global risk thermometer; the "
          "pairs are already fetched."),
 dict(id="M20", family="Macro & Rates", name="Baltic Dry / freight pulse",
      tokens=["baltic", "bdi"], source="Yahoo ^BDI (fragile)",
      feasibility="MEDIUM", wire_into="activity-nowcast",
      why="Physical-economy pulse uncorrelated with survey data."),
 dict(id="M21", family="Macro & Rates", name="Daily inflation proxy "
      "(Truflation-style)", tokens=["truflation"],
      source="Truflation free endpoint", feasibility="MEDIUM",
      wire_into="fomc + fed-speak",
      why="Daily CPI read between monthly prints; divergence vs Cleveland "
          "nowcast is tradeable around CPI days."),
 dict(id="M22", family="Credit", name="Muni/Treasury ratio",
      tokens=["muni_ratio", "muni ratio"], source="internal (MUB yield "
      "vs DGS10 -- both already fetched)", feasibility="FEASIBLE_NOW",
      wire_into="asset-compass credit sleeve v1.2",
      why="The muni cheapness gauge every allocator quotes; two numbers "
          "already in the pipeline."),
]

UMBRELLAS = [
 dict(id="U1", name="Insider Desk", action="GROUP",
      targets=["insider", "insiders", "insider-buys", "insider-clusters",
               "insider-drawdown"],
      note="5 pages, 4 feeds -- one hub with modules; keep engines."),
 dict(id="U2", name="Pairs Desk", action="MERGE",
      targets=["pairs", "pairs-scanner", "pairs-arb"],
      note="pairs and pairs-scanner serve the SAME feed "
           "(data/pairs-scanner.json) -- merge; fold pairs-arb in."),
 dict(id="U3", name="Treasury Desk", action="GROUP",
      targets=["auctions", "treasury-auctions", "auction-crisis",
               "macro-frontrun"],
      note="Four auction surfaces; one desk with tabs."),
 dict(id="U4", name="Signal Intelligence", action="GROUP",
      targets=["proof", "accuracy", "scorecard", "signal-scorecard",
               "signal-replay", "signal-halflife", "signal-genealogy",
               "signal-orthogonality", "calibration-fleet",
               "gsi-calibration", "track-public"],
      note="11 meta-quality pages -> one Signal Intelligence hub; this "
           "is the platform's proof layer and deserves one front door."),
 dict(id="U5", name="System Status", action="MERGE",
      targets=["system", "system-health", "health", "status", "uptime"],
      note="FIVE status pages -> keep status.html, 301 the rest."),
 dict(id="U6", name="Charts", action="MERGE",
      targets=["chart-pro", "chart-macro", "charts"],
      note="Three chart surfaces, one keeper (chart-pro)."),
 dict(id="U7", name="Duplicate feeds", action="MERGE",
      targets=["carry vs carry-surface", "bottleneck vs bottleneck-boom",
               "download vs downloads"],
      note="Each pair serves identical feeds/purpose -- keep one, 301 "
           "the other."),
 dict(id="U8", name="Credit Desk (NEW)", action="CREATE",
      targets=["cds-monitor (page currently feedless)",
               "lce credit block", "asset-compass credit sleeve",
               "M5/M6 spread internals when added"],
      note="Credit is the thinnest family relative to hedge-fund "
           "standard -- one credit-desk.html unifying it changes that."),
 dict(id="U9", name="Orphan adoption", action="GROUP",
      targets=["97 orphan-fresh engines (standing item)"],
      note="Group into EXISTING desks per doctrine -- audit table marks "
           "each orphan's family so adoption is a mapping exercise."),
]

ENGINE_NOTES = {
 "justhodl-dealer-gex": "Add dollar-gamma per 1% move and gamma-flip "
                        "distance as stored history series (trend of "
                        "positioning, not just snapshot).",
 "justhodl-correlation-breaks": "Emit an explicit SPY/TLT rolling-corr "
                        "regime flag (M13) -- downstream hedge ratios "
                        "need the state, not just break events.",
 "justhodl-master-allocator": "Consume asset-compass v1.1 ER vector + "
                        "corr column as inputs -- the compass->allocator "
                        "bridge is the highest-value fusion available.",
 "justhodl-strategist": "Ingest data/fleet-audit.json monthly for "
                        "self-aware fleet commentary (which desks are "
                        "stale/thin).",
 "justhodl-resilience-radar": "Cross-wire PEAD persistence stat -- "
                        "resilient names with positive drift post-print "
                        "are the cleanest subset.",
 "justhodl-sentiment-extremes": "AAII feed already exists in the corpus "
                        "-- fold it in; add NAAIM (M12) when sourced.",
 "justhodl-altseason": "Add SOL leg from asset-compass (now priced "
                        "daily) as third confirmation.",
 "justhodl-eurodollar-plumbing": "Cross-currency basis remains "
                        "NEEDS_SOURCE (BIS quarterly only) -- document "
                        "the honest gap in-page.",
 "justhodl-asset-compass": "v1.2 roadmap below -- forecast ledger is "
                        "the credibility unlock.",
}

COMPASS_V12 = [
 "Forecast ledger: log the monthly ER vector, grade 12m later through "
 "the existing edge-accuracy machinery -- compass predictions get "
 "PROVEN/NEGATIVE verdicts like every other signal.",
 "Full 31x31 correlation matrix + cluster map (currently only vs SPY).",
 "Deterministic scenario table: +/-100bp, recession, inflation shock -- "
 "computed from published durations and betas, no simulation hand-waving.",
 "Regime-conditional ER: join risk-regime state, show ER given regime.",
 "Allocator bridge: feed master-allocator the ER vector + corr matrix.",
 "FX-hedged variants for EWJ/FXI/INDA ERs.",
 "Muni tax-equivalent yield toggle (M22).",
]


def scan_engines():
    reg = {}
    for f in glob.glob(str(REPO / "aws/lambdas/*/source/lambda_function.py")):
        eng = f.split("/")[-3]
        try:
            src = open(f, encoding="utf-8", errors="ignore").read()
        except Exception:
            continue
        reg[eng] = _reg_row(src)
    if len(reg) >= 100:
        return reg
    # sparse-checkout fallback: read blobs straight from the git tree so
    # the audit is identical regardless of which files are materialized
    import subprocess
    ls = subprocess.run(
        ["git", "-C", str(REPO), "ls-tree", "-r", "origin/main",
         "--name-only", "aws/lambdas"],
        capture_output=True, text=True, timeout=120).stdout.splitlines()
    paths = [p for p in ls if p.endswith("/source/lambda_function.py")
             and "_archived" not in p]
    proc = subprocess.Popen(
        ["git", "-C", str(REPO), "cat-file", "--batch"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    for p in paths:
        proc.stdin.write(("origin/main:%s\n" % p).encode())
    proc.stdin.close()
    out = proc.stdout.read()
    proc.wait(timeout=300)
    i = 0
    for p in paths:
        nl = out.index(b"\n", i)
        hdr = out[i:nl].decode(errors="ignore").split()
        if len(hdr) < 3 or hdr[1] != "blob":
            i = nl + 1
            continue
        size = int(hdr[2])
        src = out[nl + 1:nl + 1 + size].decode("utf-8", errors="ignore")
        i = nl + 1 + size + 1
        reg[p.split("/")[2]] = _reg_row(src)
    return reg


def _reg_row(src):
    m = re.match(r'\s*(?:"""|\'\'\')(.{20,900}?)(?:"""|\'\'\')', src, re.S)
    doc = re.sub(r"\s+", " ", m.group(1)).strip()[:280] if m else ""
    fred = sorted({t for t in re.findall(r'"([A-Z][A-Z0-9]{3,17})"', src)
                   if any(ch.isdigit() for ch in t)})[:40]
    outs = sorted(set(re.findall(r'data/[a-z0-9._-]+\.json', src)))[:12]
    return {"doc": doc, "fred": fred, "outs": outs,
            "loc": src.count("\n")}


def scan_pages():
    pages = {}
    for f in glob.glob(str(REPO / "*.html")):
        stem = Path(f).stem
        try:
            h = open(f, encoding="utf-8", errors="ignore").read()
        except Exception:
            continue
        t = re.search(r"<title>(.*?)</title>", h, re.S)
        title = re.sub(r"\s+", " ", t.group(1)).strip()[:90] if t else stem
        feeds = sorted(set(re.findall(r'data/[a-z0-9._-]+\.json', h)))
        pages[stem] = {"title": title, "feeds": feeds}
    return pages


def build_corpus(reg, pages):
    toks = set()
    for e, r in reg.items():
        toks.add(e.lower())
        toks.update(x.lower() for x in r["fred"])
        toks.update(o.lower() for o in r["outs"])
        toks.update(re.findall(r"[a-z0-9_-]{3,}", r["doc"].lower()))
    for p, v in pages.items():
        toks.add(p.lower())
        toks.update(f.lower() for f in v["feeds"])
        toks.update(re.findall(r"[a-z0-9_-]{3,}", v["title"].lower()))
    blob = " ".join(sorted(toks))
    return toks, blob


def crosscheck(item, toks, blob):
    for t in item["tokens"]:
        tl = t.lower()
        if tl in toks or tl.replace(" ", "-") in blob \
                or tl.replace(" ", "_") in blob or tl in blob:
            return "already_covered", t
    return "gap", None


def main():
    fails, warns = [], []
    hl = {}
    with report("2974_fleet_audit") as rep:
        import boto3
        s3 = boto3.client("s3", region_name="us-east-1")

        rep.section("A/B. Repo scans")
        reg = scan_engines()
        pages = scan_pages()
        rep.kv(engines=len(reg), pages=len(pages))
        if len(reg) < 500:
            fail_msg = "engine scan suspiciously small: %d" % len(reg)
            fails.append(fail_msg)
            rep.fail(fail_msg)
        if len(pages) < 300:
            fails.append("page scan suspiciously small: %d" % len(pages))
            rep.fail(fails[-1])

        rep.section("C. S3 context loads")
        wiring, trust = {}, {}
        try:
            wiring = json.loads(s3.get_object(
                Bucket=BUCKET, Key="data/engine-wiring.json")
                ["Body"].read())
            rep.kv(wired=wiring.get("wired"),
                   orphaned=wiring.get("orphaned"))
        except Exception as e:
            warns.append("engine-wiring.json unavailable: %s" % str(e)[:70])
        try:
            trust = json.loads(s3.get_object(
                Bucket=BUCKET, Key="data/engine-trust.json")
                ["Body"].read())
        except Exception as e:
            warns.append("engine-trust.json unavailable: %s" % str(e)[:70])
        tmap = {}
        for row in (trust.get("engines") or trust.get("rows") or []):
            nm = row.get("engine") or row.get("name")
            if nm:
                tmap[nm] = row.get("alpha_verdict") or row.get("verdict") \
                    or row.get("status")
        orphan_names = {o.get("name") for o in
                        (wiring.get("orphan_detail") or [])}

        rep.section("D. Classification")
        fam_count = defaultdict(int)
        engines_out = {}
        feed_writers = defaultdict(list)
        for name, r in sorted(reg.items()):
            fam = classify(name)
            fam_count[fam] += 1
            for o in r["outs"]:
                feed_writers[o].append(name)
            engines_out[name] = {
                "family": fam, "outs": r["outs"], "loc": r["loc"],
                "fred_n": len(r["fred"]),
                "status": ("ORPHAN" if name.replace("justhodl-", "")
                           in orphan_names or name in orphan_names
                           else ("NO_OUTS" if not r["outs"] else "WIRED")),
                "trust": tmap.get(name) or tmap.get(
                    name.replace("justhodl-", "")),
                "note": ENGINE_NOTES.get(name),
            }
        rep.kv(**{k.replace(" ", "_").replace("&", "and"): v
                  for k, v in sorted(fam_count.items())})
        uncls = fam_count.get("Unclassified", 0)
        if uncls > len(reg) * 0.12:
            warns.append("high unclassified count: %d" % uncls)

        rep.section("E. Redundancy")
        dup_feeds = {k: v for k, v in feed_writers.items() if len(v) > 1}
        page_feed_map = defaultdict(list)
        for p, v in pages.items():
            if len(v["feeds"]) == 1:
                page_feed_map[v["feeds"][0]].append(p)
        page_dups = {k: v for k, v in page_feed_map.items()
                     if len(v) > 1}
        rep.kv(engine_shared_outs=len(dup_feeds),
               single_feed_page_collisions=len(page_dups))
        umb = []
        for u in UMBRELLAS:
            tgts = [t.split(" ")[0] for t in u["targets"]]
            present = [t for t in tgts if t in pages or
                       ("justhodl-" + t) in reg or t in reg]
            umb.append(dict(u, verified_present=present,
                            missing=[t for t in tgts
                                     if t not in present]))

        rep.section("F. Gap matrix cross-check")
        toks, blob = build_corpus(reg, pages)
        gaps, covered = [], []
        for item in GAP_MATRIX:
            status, ev = crosscheck(item, toks, blob)
            row = dict(item)
            row["status"] = status
            if ev:
                row["evidence"] = ev
                covered.append(row)
            else:
                gaps.append(row)
        rep.kv(gaps=len(gaps), already_covered=len(covered),
               covered_ids=[c["id"] for c in covered])
        hl["gaps_n"] = len(gaps)
        hl["covered_n"] = len(covered)
        hl["covered_ids"] = [c["id"] + ":" + (c.get("evidence") or "")
                             for c in covered]
        feasible_now = [g["id"] for g in gaps
                        if g["feasibility"] == "FEASIBLE_NOW"]
        hl["feasible_now"] = feasible_now

        rep.section("G. Assemble + upload")
        fam_summary = []
        for fam in sorted(fam_count):
            rows = [n for n, e in engines_out.items()
                    if e["family"] == fam]
            fam_summary.append({
                "family": fam, "engines": len(rows),
                "orphans": sum(1 for n in rows
                               if engines_out[n]["status"] == "ORPHAN"),
                "no_outs": sum(1 for n in rows
                               if engines_out[n]["status"] == "NO_OUTS"),
                "with_trust_verdict": sum(1 for n in rows
                                          if engines_out[n]["trust"]),
                "gap_items": [g["id"] for g in gaps
                              if g["family"] == fam],
            })
        out = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "ops": 2974,
            "totals": {"engines": len(reg), "pages": len(pages),
                       "families": len(fam_count),
                       "wired_per_wiring_doc": wiring.get("wired"),
                       "orphaned_per_wiring_doc": wiring.get("orphaned"),
                       "gaps": len(gaps),
                       "gaps_feasible_now": len(feasible_now),
                       "already_covered": len(covered)},
            "families": fam_summary,
            "engines": engines_out,
            "gap_matrix": {"gaps": gaps, "already_covered": covered},
            "umbrella_actions": umb,
            "engine_shared_outs": {k: v for k, v in
                                   sorted(dup_feeds.items())[:40]},
            "page_single_feed_collisions": page_dups,
            "compass_v12_roadmap": COMPASS_V12,
            "method": "repo scan (ops-2876 technique) + engine-wiring + "
                      "engine-trust + curated matrix cross-checked "
                      "against the full corpus; zero LLM.",
        }
        body = json.dumps(out, ensure_ascii=False).encode()
        s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=body,
                      ContentType="application/json",
                      CacheControl="public, max-age=900")
        rep.kv(uploaded_kb=round(len(body) / 1024, 1))
        hl["engines"] = len(reg)
        hl["pages"] = len(pages)
        hl["families"] = {k: v for k, v in sorted(fam_count.items())}

        if not fails:
            rep.ok("fleet audit live: %d engines / %d pages / %d families; "
                   "%d genuine gaps (%d feasible now), %d claims demoted "
                   "to already-covered by evidence"
                   % (len(reg), len(pages), len(fam_count), len(gaps),
                      len(feasible_now), len(covered)))
        _write(rep, fails, warns, hl)


def _write(rep, fails, warns, hl):
    out = {"ops": 2974, "fails": fails, "warns": warns,
           "verdict": "PASS" if not fails else "FAIL",
           "ts": datetime.now(timezone.utc).isoformat()}
    out.update(hl)
    rp = AWS_DIR / "ops" / "reports" / "2974.json"
    rp.parent.mkdir(parents=True, exist_ok=True)
    rp.write_text(json.dumps(out, indent=1))
    rep.log("FAILS=%d WARNS=%d" % (len(fails), len(warns)))
    if fails:
        sys.exit(1)


main()
sys.exit(0)
