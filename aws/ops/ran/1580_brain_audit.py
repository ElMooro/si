# ops 1580 — BRAIN GAP AUDIT: what the brain knows that the engines don't measure.
# Reads data/brain.json, scans notes vs a metric lexicon (pre-annotated with deployed
# coverage), discovers novel capitalized terms, and emits a ranked gap report.
import json, re, boto3
from collections import Counter
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
out = {"ops": 1580}

raw = json.loads(s3.get_object(Bucket=B, Key="data/brain.json")["Body"].read())
out["brain_top_keys"] = sorted(raw.keys())[:12] if isinstance(raw, dict) else type(raw).__name__

def harvest_texts(node, acc, depth=0):
    if depth > 6: return
    if isinstance(node, str):
        if len(node) > 30: acc.append(node)
    elif isinstance(node, dict):
        t = node.get("text") or node.get("note") or node.get("content") or node.get("body")
        if isinstance(t, str) and len(t) > 20:
            acc.append(t)
        else:
            for v in node.values(): harvest_texts(v, acc, depth+1)
    elif isinstance(node, list):
        for v in node: harvest_texts(v, acc, depth+1)
texts = []
harvest_texts(raw, texts)
out["n_notes"] = len(texts)
out["sample_note"] = (texts[0][:220] if texts else None)
corpus = "\n".join(texts)
low = corpus.lower()

# term, [regex variants], implemented_where (None = GAP), candidate_free_source
LEX = [
 ("MOVE index", [r"\bmove index\b", r"\bmove\b(?=.{0,30}(vol|index|bond))"], None, "no free feed; proxy via bond-vol-regime exists"),
 ("ACM term premium", [r"term premium", r"\bacm\b"], None, "NY Fed ACM CSV (free)"),
 ("Swap spreads", [r"swap spread"], None, "FRED SOFR swap probe / OFR"),
 ("Cross-currency basis", [r"cross.currency basis", r"\bxccy\b", r"basis swap"], None, "paid; note-only"),
 ("EFFR-SOFR spread", [r"effr", r"fed funds.{0,20}sofr"], None, "FRED EFFR & SOFR (free)"),
 ("CP spread", [r"commercial paper", r"\bcp spread", r"cp.bill"], None, "FRED DCPF3M vs DTB3"),
 ("Primary dealer positions/fails", [r"primary dealer", r"dealer (positions|fails|inventor)"], None, "NY Fed PD weekly (free)"),
 ("MMF assets", [r"money market fund", r"\bmmf\b"], None, "FRED MMMFFAQ027S + ICI"),
 ("Bill share of issuance", [r"bill share", r"t.bill issuance", r"\bqra\b", r"coupon issuance"], None, "Treasury MSPD monthly (free)"),
 ("SOMA runoff", [r"\bsoma\b", r"mbs runoff", r"balance sheet runoff"], None, "NY Fed SOMA CSV (free)"),
 ("Fed custody (foreign)", [r"custody", r"foreign official.{0,20}(holdings|treasur)"], None, "FRED H.4.1 probe"),
 ("TIC flows", [r"\btic\b", r"foreign (buying|selling).{0,20}treasur"], None, "Treasury TIC (free)"),
 ("Sahm rule (US)", [r"sahm"], None, "FRED SAHMREALTIME"),
 ("SLOOS (US)", [r"sloos", r"senior loan officer"], None, "FRED DRTSCILM etc"),
 ("Jobless claims", [r"jobless claims", r"initial claims", r"\bicsa\b", r"continuing claims"], None, "FRED ICSA/CCSA"),
 ("JOLTS quits", [r"\bjolts\b", r"quits rate"], None, "FRED JTSQUR"),
 ("ISM/PMI (US)", [r"\bism\b", r"\bpmi\b(?!.{0,10}eu)"], None, "regional Fed composite (free proxy)"),
 ("GSCPI supply chain", [r"gscpi", r"supply chain pressure"], None, "NY Fed GSCPI JSON (free)"),
 ("Copper/gold", [r"copper.{0,6}gold", r"dr\.? copper"], None, "FRED PCOPPUSDM/GOLDPM"),
 ("Margin debt", [r"margin debt", r"finra margin"], None, "FINRA monthly stats (free)"),
 ("AAII sentiment", [r"\baaii\b"], None, "AAII csv (free-ish)"),
 ("NAAIM exposure", [r"naaim"], None, "NAAIM weekly (free)"),
 ("SKEW index", [r"\bskew index\b", r"cboe skew"], None, "CBOE feed dead; vol-surface skew exists"),
 ("Gamma exposure/GEX", [r"\bgex\b", r"gamma exposure", r"dealer gamma"], None, "paid chains; opex-gamma-pin exists"),
 ("0DTE", [r"0.?dte"], None, "paid"),
 ("Buffett indicator", [r"buffett indicator", r"market cap.{0,12}gdp"], None, "FRED Z.1 NCBEILQ027S/GDP"),
 ("Equity risk premium", [r"equity risk premium", r"\berp\b"], None, "computable: E/P - real 10y"),
 ("Near-term fwd spread", [r"near.term forward", r"18.?m(onth)? forward"], None, "Fed series probe"),
 ("Real rates / TIPS", [r"real (rates|yield)", r"\btips\b", r"dfii"], None, "FRED DFII10"),
 ("5y5y inflation (US)", [r"5y5y(?!.{0,20}(ecb|eu|euro))", r"t5yifr"], None, "FRED T5YIFR"),
 ("Bank reserves", [r"bank reserves", r"wresbal", r"reserve balances"], None, "FRED WRESBAL"),
 ("Repo fails", [r"\bfails\b(?=.{0,40}(repo|deliver|treasur))"], None, "NY Fed PD fails (free)"),
 ("Delinquencies", [r"delinquen"], None, "FRED DRCCLACBS etc"),
 ("C&I loans", [r"c&i", r"commercial and industrial loans", r"busloans"], None, "FRED BUSLOANS"),
 ("HY default rate", [r"default rate"], None, "paid (Moody's); HY OAS exists"),
 ("CDS / CDX / iTraxx", [r"\bcds\b", r"\bcdx\b", r"itraxx"], None, "paid; HY OAS proxy exists"),
 ("EMBI / EM spreads", [r"\bembi\b", r"em spread"], None, "FRED BAMLEMHB? probe"),
 ("Baltic Dry", [r"baltic dry", r"\bbdi\b"], None, "no free; note-only"),
 ("Lumber", [r"\blumber\b"], None, "FRED WPU081 (PPI proxy)"),
 ("Semis ratio (SOX)", [r"\bsox\b", r"semis?(?=.{0,20}(lead|ratio|spy))"], None, "compute SMH/SPY via Polygon"),
 ("ETF/fund flows", [r"fund flows", r"etf flows", r"\bici\b"], None, "ICI weekly (scrape)"),
 ("Insider (US agg)", [r"insider (buy|sell)"], "ignition P2 + insider engine", None),
 ("Buybacks", [r"buyback"], "buyback-yield engine", None),
 ("Put/call", [r"put.?call"], "sentiment-extreme composite v2 (CBOE dead)", None),
 ("VIX term/skew", [r"vix term", r"backwardation", r"\bvvix\b"], "vol-surface v3", None),
 ("COT/CFTC", [r"\bcot\b", r"cftc"], "cftc agent + R7 deep-view", None),
 ("HY OAS", [r"hy oas", r"high.yield spread"], "deep base splice + kb state", None),
 ("2s10s curve", [r"2s10s", r"yield curve inver"], "kb/yield-curve feed", None),
 ("NFCI", [r"nfci", r"financial conditions"], "alert-backtester", None),
 ("TGA/RRP/net liquidity", [r"\btga\b", r"\brrp\b", r"net liquidity", r"reverse repo"], "liquidity-inflection", None),
 ("SOFR tail", [r"sofr"], "crisis-canaries C1", None),
 ("Discount window", [r"discount window"], "crisis-canaries C3", None),
 ("H.8 deposits", [r"\bh\.?8\b", r"deposit (outflow|flight)"], "crisis-canaries C4", None),
 ("Auction tails", [r"auction (tail|stress)", r"bid.cover"], "auction-crisis + slope", None),
 ("Payroll revisions", [r"revision"], "ALFRED nowcast C6", None),
 ("China credit", [r"china credit", r"\btsf\b"], "liq-inflection pillar", None),
 ("Stablecoins", [r"stablecoin"], "stablecoin-flow + accel", None),
 ("TARGET2", [r"target2"], "EU radar #3", None),
 ("CISS", [r"\bciss\b"], "EU radar core", None),
 ("Swap lines", [r"swap line"], "eurodollar composite (SWPT)", None),
 ("DXY/dollar", [r"\bdxy\b", r"dollar index"], "dollar-radar feed", None),
 ("FTD", [r"fails.to.deliver", r"\bftd\b"], "ignition P3", None),
 ("Dark pool", [r"dark pool"], "ignition P4 (parked: FINRA key)", None),
 ("Short interest", [r"short interest", r"days to cover"], None, "FINRA key (parked) / FMP probe"),
 ("13F clusters", [r"13f"], "ignition P5 (parked: FMP tier)", None),
 ("Term premium", [r"term premium"], None, "NY Fed ACM (dup-check)"),
 ("M2 (US)", [r"\bm2\b"], None, "FRED M2SL"),
 ("Eurodollar/offshore USD", [r"eurodollar", r"offshore dollar"], "ESI master composite", None),
 ("Gold", [r"\bgold\b"], "tide/crypto-intel partial", None),
 ("Breadth (equity)", [r"breadth", r"advance.decline", r"\ba/?d line\b"], None, "compute from Polygon universe"),
 ("New highs-lows", [r"new (52.?week )?(highs|lows)"], None, "compute from Polygon"),
 ("McClellan", [r"mcclellan"], None, "compute from A/D"),
]
hits = []
for name, pats, impl, src in LEX:
    c = 0; ex = []
    for p in pats:
        for m in re.finditer(p, low):
            c += 1
            if len(ex) < 2:
                s_ = max(0, m.start()-60); e_ = min(len(corpus), m.end()+90)
                ex.append(corpus[s_:e_].replace("\n", " ")[:150])
    if c:
        hits.append({"metric": name, "note_hits": c, "implemented": impl,
                     "source_candidate": src, "excerpts": ex})
hits.sort(key=lambda x: -x["note_hits"])
out["gaps"] = [h for h in hits if not h["implemented"]][:40]
out["covered"] = [{"metric": h["metric"], "hits": h["note_hits"], "where": h["implemented"]}
                  for h in hits if h["implemented"]][:30]

# novel-term discovery: frequent Capitalized/ALLCAPS tokens not in lexicon
lexwords = set(w.lower() for n,_,_,_ in LEX for w in re.findall(r"[a-z]+", n.lower()))
STOP = set("""the and for with from this that have will been into over under after
above about their there which would could should very more most than then when
what where each both fed ecb usd eur spx vix btc note notes market markets rate
rates index data daily weekly monthly year years week month price level levels
high low signal stress crisis risk macro equity bond bonds credit yield yields
inflation growth gdp china europe japan treasury treasuries""".split())
toks = Counter()
for m in re.finditer(r"\b([A-Z][A-Za-z0-9&/\.-]{2,18}(?: [A-Z][A-Za-z0-9&/\.-]{2,14}){0,2})\b", corpus):
    t = m.group(1)
    tl = t.lower()
    if tl in STOP or all(w in lexwords or w in STOP for w in re.findall(r"[a-z]+", tl)):
        continue
    toks[t] += 1
out["novel_terms_top40"] = toks.most_common(40)
open("aws/ops/reports/1580_brain_audit.json","w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({"n_notes": out["n_notes"], "n_gaps": len(out["gaps"]),
  "top_gaps": [(g["metric"], g["note_hits"]) for g in out["gaps"][:18]],
  "top_covered": [(c["metric"], c["hits"]) for c in out["covered"][:8]]}, default=str)[:900])
