"""justhodl-brain-compiler — THE BRAIN → FLEET COMPILER (monthly)

Closes the loop Khalid asked for: "whatever I put into the brain is how my system
interprets everything." Every month this engine:

  1. Reads every note in the brain (data/brain.json — the operator's playbook).
  2. Extracts TESTABLE / ACTIONABLE claims from the prose:
       relationship claims  ("X predicts / precedes / leads / front-runs Y")
       threshold rules      ("when X above/below N ...")
       watch instructions   ("watch / monitor X")
  3. Routes each claim against the FLEET REGISTRY (data/engine-registry.json —
     a repo-scan of all ~380 engines: docstrings, FRED series, output feeds)
     and grades coverage:
       COVERED  — an engine already implements the concept
       PARTIAL  — an engine touches it but weakly
       GAP      — nothing in the fleet covers it → goes on the BUILD QUEUE
  4. Publishes data/brain-compiler.json: the routing table, the gaps, and a
     build queue with suggested engine specs.

The build queue is consumed by the operator's Claude sessions (which is exactly
how hqm_reversal_lead and qe_precursor_30y were wired into liquidity-inflection),
and is designed so an LLM leg can auto-draft engine specs once LLM credits are
restored. Deterministic, real data only.

OUTPUT data/brain-compiler.json    SCHEDULE monthly cron(0 6 1 * ? *)
"""
import json
import re
from collections import defaultdict
from datetime import datetime, timezone

import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/brain-compiler.json"

# concept lexicon: brain-prose pattern -> tokens to look for in engine registry
CONCEPTS = {
    "reverse repo / RRP":        (r"\brrp\b|reverse repo", ["RRPONTSYD", "rrp", "reverse repo"]),
    "TGA":                       (r"\btga\b|treasury general account", ["WTREGEN", "tga"]),
    "Fed balance sheet":         (r"balance sheet|\bwalcl\b|\bqt\b|quantitative tighten", ["WALCL", "balance sheet", "qt"]),
    "QE":                        (r"\bqe\b|quantitative eas", ["qe", "quantitative"]),
    "SOFR / repo plumbing":      (r"\bsofr\b|repo (rate|spike|market)", ["SOFR", "sofr", "repo"]),
    "30y long bond":             (r"30.?y(ea)?r|long bond|\bdgs30\b", ["DGS30", "30y", "long bond"]),
    "HQM quality credit":        (r"\bhqm\b|high.?quality market", ["HQMCB", "hqm"]),
    "yield curve":               (r"yield curve|inversion|invert|steepen", ["T10Y2Y", "curve", "steepen", "inversion"]),
    "HY / junk credit":          (r"high.?yield|junk|\bhy\b (spread|oas)|ccc", ["BAMLH0A0HYM2", "high yield", "hy_", "ccc"]),
    "bill auctions":             (r"bill auction|4.?w(ee)?k|8.?w(ee)?k|bid.to.cover|auction tail|dealer allot", ["auction", "bill", "bid_to_cover"]),
    "market breadth":            (r"breadth|advance.decline|small.?cap", ["breadth", "IWM", "advance"]),
    "copper":                    (r"copper|dr\.? copper", ["copper", "PCOPPUSDM", "HG=F"]),
    "jobless claims":            (r"jobless|initial claims|continuing claims", ["IC4WSA", "claims"]),
    "volatility / VIX":          (r"\bvix\b|volatility spike|vol regime", ["VIX", "volatility", "vol"]),
    "dollar / DXY":              (r"\bdxy\b|dollar (strength|index|milkshake|wrecking)", ["DXY", "dollar"]),
    "gold":                      (r"\bgold\b", ["gold", "GLD", "GC=F"]),
    "bitcoin / crypto":          (r"bitcoin|\bbtc\b|crypto liquidity", ["bitcoin", "BTC", "crypto"]),
    "Sahm rule":                 (r"sahm", ["sahm"]),
    "M2 / money supply":         (r"\bm2\b|money supply", ["M2", "money supply"]),
    "PMI / ISM":                 (r"\bpmi\b|\bism\b", ["CFNAI", "pmi", "ism"]),
    "bank reserves":             (r"bank reserves|reserve balances", ["reserves", "WRESBAL"]),
    "MMF / money market funds":  (r"money market fund|\bmmf\b", ["mmf", "money market"]),
    "SLOOS / lending standards": (r"sloos|lending standards|loan officer", ["DRTSCILM", "sloos", "lending"]),
    "TIC / foreign flows":       (r"\btic\b|foreign (buy|sell|custody|holdings)", ["tic", "foreign", "custody"]),
    "swap lines":                (r"swap line", ["SWPT", "swap line", "swap_line"]),
    "term premium":              (r"term premium", ["term premium", "THREEFYTP"]),
    "credit spreads":            (r"credit spread", ["spread", "credit"]),
    "housing / permits":         (r"housing|building permit|mortgage", ["PERMIT", "housing", "mortgage"]),
    "oil / energy":              (r"\boil\b|crude|wti|brent", ["oil", "crude", "WTI", "DCOILWTICO"]),
    "china liquidity":           (r"china (liquidity|credit|pboc)|\bpboc\b", ["china", "pboc"]),
    "ECB / euro":                (r"\becb\b|euro (area|zone)", ["ecb", "euro"]),
    "japan / BOJ / yen":         (r"\bboj\b|\byen\b|\bjpy\b|japan (rates|yield)", ["boj", "yen", "jpy", "japan"]),
    "buybacks":                  (r"buyback", ["buyback"]),
    "positioning / COT":         (r"\bcot\b|positioning|net (long|short)s?\b", ["cot", "positioning"]),
    "GEX / gamma":               (r"\bgex\b|gamma", ["gex", "gamma"]),
    "seasonality":               (r"seasonal", ["seasonal"]),
    "stablecoins":               (r"stablecoin|usdt|usdc", ["stablecoin", "usdt", "usdc"]),
}

REL = re.compile(r"predict|precede|lead(s|ing)? |front.?run|always |every time|historic(ally)?|"
                 r"before (a|the) (crash|dump|crisis|recession|rally|bottom|top)|causes|forces|"
                 r"signals? (a|the)|tends? to|→|precursor|foreshadow", re.I)
THRESH = re.compile(r"(above|below|over|under|exceed|greater than|less than|>=|<=|>|<)\s*[\d.]+", re.I)
WATCH = re.compile(r"^\s*(watch|monitor|track|alert on|keep an eye)", re.I)
JUNKY = re.compile(r"^\s*(#|//|\{|\[|import |def |SELECT |http)", re.I)


def gj(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def sentences(text):
    return [s.strip() for s in re.split(r"(?<=[.!?])\s+|\n+", text) if 25 <= len(s.strip()) <= 400]


def concepts_in(s):
    low = s.lower()
    return [name for name, (pat, _) in CONCEPTS.items() if re.search(pat, low)]


def build_index(registry):
    """Precompute per-engine match structures ONCE (658 engines x 321 claims was
    O(n*m*keys) with per-pair regex -> timed out; this makes matching set-based)."""
    idx = []
    for eng, meta in registry.items():
        if eng == "justhodl-brain-compiler":
            continue                                   # never route claims to ourselves (lexicon strings != coverage)
        fred = tuple(x.upper() for x in meta.get("fred", []))
        kfull = tuple(meta.get("keys", []))
        parts = set()
        for k in kfull:
            parts.update(p for p in k.split("_") if len(p) >= 2)
        blob = (eng + " " + meta.get("doc", "") + " " + " ".join(meta.get("outs", []))).lower()
        idx.append((eng, fred, parts, kfull, blob))
    return idx


def match_engines(concept_names, index):
    """Score engines against the claim's concepts: FRED prefix (HQMCB->HQMCB10YR),
    semantic key parts (sahm -> sahm_rule), full snake_case keys (swap_line ->
    swap_line_usage), then doc/name substring."""
    want = set()
    for c in concept_names:
        want.update(t.lower() for t in CONCEPTS[c][1])
    scores = defaultdict(int)
    for eng, fred, parts, kfull, blob in index:
        for t in want:
            T = t.upper()
            if any(f.startswith(T) for f in fred):
                scores[eng] += 2                      # series-level match is strong
            elif t in parts:
                scores[eng] += 2                      # engine has a field named after the concept
            elif "_" in t and any(t in k for k in kfull):
                scores[eng] += 2                      # snake_case token inside a full key
            elif len(t) >= 4 and t in blob:
                scores[eng] += 1
    ranked = sorted(scores.items(), key=lambda kv: -kv[1])[:4]
    return [{"engine": e, "score": sc} for e, sc in ranked if sc > 0]


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    brain = gj("data/brain.json") or {}
    # ── merge TradingView notes from the mirror (additive, idempotent) ──
    try:
        tv_mirror = gj("data/tradingview-notes.json") or {}
        tv_notes  = tv_mirror.get("notes") or []
        brain_ids = {n.get("id") for n in (brain.get("notes") or [])}
        added_tv  = 0
        for tn in tv_notes:
            if not isinstance(tn, dict): continue
            text = str(tn.get("text") or "").strip()
            if len(text) < 20: continue
            sym  = str(tn.get("symbol") or "UNTAGGED").upper()
            nid  = tn.get("id") or ("tv-" + __import__("hashlib").sha1(
                       (sym + "|" + text[:120]).encode()).hexdigest()[:16])
            if nid in brain_ids: continue
            body = "[TV:%s] %s" % (sym, text)
            if not brain.get("notes"): brain["notes"] = []
            brain["notes"].append({"id": nid, "cat": "thesis",
                "text": body, "pinned": False,
                "created": tn.get("created"), "_tv_symbol": sym})
            brain_ids.add(nid); added_tv += 1
        if added_tv: print("[tv-merge] +%d TV notes merged into brain" % added_tv)
    except Exception as _tv_e:
        print("[tv-merge] %s" % _tv_e)
    notes = brain.get("notes") or (brain if isinstance(brain, list) else [])
    registry = (gj("data/engine-registry.json") or {}).get("engines", {})
    claims, seen = [], set()
    for n in notes:
        if not isinstance(n, dict):
            continue
        txt = n.get("text") or ""
        if len(txt) < 40 or JUNKY.search(txt):
            continue
        for s in sentences(txt):
            cs = concepts_in(s)
            if not cs:
                continue
            if not (REL.search(s) or THRESH.search(s) or WATCH.search(s)):
                continue
            key = s[:70].lower()
            if key in seen:
                continue
            seen.add(key)
            claims.append({"note_id": n.get("id"), "cat": n.get("cat"),
                           "pinned": bool(n.get("pinned")), "claim": s[:300], "concepts": cs})
            if len(claims) >= 800:
                break
        if len(claims) >= 800:
            break
    # route every claim (index precomputed once — see build_index)
    index = build_index(registry)
    gap_by_concept = defaultdict(list)
    n_cov = n_part = n_gap = 0
    for c in claims:
        eng = match_engines(c["concepts"], index)
        best = eng[0]["score"] if eng else 0
        if best >= 2:
            c["status"] = "COVERED"; n_cov += 1
        elif best == 1:
            c["status"] = "PARTIAL"; n_part += 1
        else:
            c["status"] = "GAP"; n_gap += 1
            for cn in c["concepts"]:
                gap_by_concept[cn].append(c["claim"][:160])
        c["engines"] = eng
    build_queue = [{"concept": cn,
                    "suggested_engine": "justhodl-" + re.sub(r"[^a-z0-9]+", "-", cn.lower()).strip("-"),
                    "n_claims": len(v), "sample_claims": v[:3],
                    "note": "No engine in the fleet registry covers this concept — candidate for a "
                            "new engine or a new block inside the closest existing engine."}
                   for cn, v in sorted(gap_by_concept.items(), key=lambda kv: -len(kv[1]))]
    claims.sort(key=lambda c: ({"GAP": 0, "PARTIAL": 1, "COVERED": 2}[c["status"]], not c["pinned"]))
    out = {"engine": "justhodl-brain-compiler", "version": "1.0",
           "generated_at": now.isoformat(),
           "summary": {"n_notes": len(notes), "n_claims": len(claims),
                       "covered": n_cov, "partial": n_part, "gaps": n_gap,
                       "n_registry_engines": len(registry),
                       "coverage_pct": round(100.0 * n_cov / len(claims), 1) if claims else None,
                       "headline": "%d testable claims compiled from %d brain notes: %d covered by the fleet, "
                                   "%d partial, %d gaps queued for build." % (len(claims), len(notes), n_cov, n_part, n_gap)},
           "build_queue": build_queue,
           "claims": claims[:400],
           "how_this_closes_the_loop": ("Monthly, every brain note is parsed for testable claims and routed "
                                        "against the live fleet registry. COVERED = the system already interprets "
                                        "that note. GAPs land on the build queue, which the operator's Claude "
                                        "sessions consume to extend the nearest engine or create a new one — and "
                                        "an LLM leg can auto-draft specs once LLM credits are restored."),
           "note": "Deterministic compile of the operator's brain. Real data only — not advice."}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, ensure_ascii=False, default=str).encode("utf-8"),
                  ContentType="application/json; charset=utf-8", CacheControl="max-age=3600")
    return {"ok": True, "claims": len(claims), "covered": n_cov, "gaps": n_gap}
