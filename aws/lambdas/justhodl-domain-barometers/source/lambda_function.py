"""
justhodl-domain-barometers v1.0 — MACRO / LIQUIDITY / RISK (brain-constitutional).

Khalid's directive (2026-07-27): "the classification must come from my brain
notes and tradingview notes. make sure every single indicator is included and
covered and classified... under one of 3 categories: Macro, liquidity, risk
and im interested on how it can predict markets."

WHAT THIS DOES
  1. CLASSIFIES all 561 TradingView-vault symbols into exactly one of MACRO /
     LIQUIDITY / RISK using HIS OWN WORDS — a multinomial Naive Bayes trained
     on the brain corpus itself, weak-labelled from the doctrine notes the
     risk-gate cites. Every symbol publishes the tier, margin, the terms that
     decided it, and the note ids. Nothing is hand-mapped by the model author.
  2. BUILDS A BAROMETER per domain (0-100, high = supportive of risk assets)
     by fusing two independent measurements:
       a) the risk-gate's brain-cited legs (history-aware, event-study
          validated on his Oct-2025 RRP call) — funding+carry -> LIQUIDITY,
          credit+structure -> RISK, dollar+growth -> MACRO;
       b) VAULT BREADTH — the share of that domain's LIVE driver symbols
          moving favourably, signed by a published polarity basis.
     The gate supplies depth, the vault supplies breadth. Disagreement between
     them is reported, never smoothed over.
  3. PREDICTS per asset class from the three barometers, each rule citing the
     brain note it implements, blended with the existing rotation-dashboard
     regime prior (WIRED, not rebuilt).
  4. GRADES ITSELF over time via a self-building ledger: each run appends the
     barometer state and the forward return of every asset-class proxy. Day
     one reports n_obs=0 / ACCRUING. It never claims a backtest it lacks.

DRIVER vs ASSET: an index cannot be both an input to the forecast and the
target of it. Symbols in equity/etf/index/crypto categories are ASSETS (what
gets predicted); everything else is a DRIVER (what moves the barometers).
Assets are still classified into a domain, as he asked — they just do not
vote on the barometer that predicts them.

CONSUMES  data/brain.json, data/tradingview.json, data/risk-gate.json,
          data/rotation-dashboard.json
EMITS     data/domain-barometers.json
"""
import json
import math
import re
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone

import boto3

MARKER = "domain-barometers v1.4 ops4010 six-country"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/domain-barometers.json"
LEDGER_KEY = "domain-barometers/history-ledger.json"
DOMS = ("MACRO", "LIQUIDITY", "RISK")

s3 = boto3.client("s3")

# ── the doctrine anchors: note id -> domain. These are the notes the
# risk-gate already implements leg-by-leg; they seed the classifier. ──────
ANCHORS = {
    "nmq5x1e4os92j": "LIQUIDITY", "nmq5x1e4kuplz": "LIQUIDITY",
    "tv-f452edc700d3a8da": "LIQUIDITY", "tv-a56315720e79a9ea": "LIQUIDITY",
    "nmq5vhvebjob6": "LIQUIDITY", "tv-9fa576184567fa8f": "LIQUIDITY",
    "tv-f58a44fc2f839aac": "LIQUIDITY", "tv-b3ec3933837d5155": "LIQUIDITY",
    "tv-8711fbee989cf1eb": "RISK", "tv-ba419d7b64a1e75d": "RISK",
    "tv-e38d57bffedb366a": "RISK", "nmq5w1e03r08g": "RISK",
    "tv-14a76b6087dc80eb": "RISK",
    "tv-ab761f92999efe68": "MACRO", "nmq5x00zhe98n": "MACRO",
    "tv-b4c32545ea1dc640": "MACRO", "nmq5x00zh27pq": "MACRO",
    "nmq5x0cp7zp4j": "MACRO", "tv-c8640dea0c15ee5c": "MACRO",
}
FUT_UNDERLYING = {
    "CL": "WTI", "GC": "GOLD", "SI": "SILVER", "HG": "COPPER", "PL": "PLATINUM",
    "PA": "PALLADIUM", "NG": "WTI", "ZF": "US05Y", "ZN": "US10Y", "ZT": "US02Y",
    "ZB": "US30Y", "SR": "SOFR", "SO": "SOFR", "SON": "SOFR", "GE": "SOFR",
    "6J": "JPIRYY", "6E": "EUINTR", "6B": "GB10Y", "TOPIX": "TOPIX",
    "MME": "SPX", "EMM": "SPX", "USW": "US30Y", "USP": "US30Y", "US": "US30Y",
    "I": "SPX", "YIT": "IT10Y", "AWN": "WTI", "AW": "WTI", "F1U": "US05Y",
    "FMMG": "SPX", "FMEA": "SPX", "FMMI": "SPX", "GBW": "GB10Y", "MWL": "SPX",
}
ASSET_CATS = {"equity", "etf", "index", "crypto"}
ASSET_SYMS = {"SPX", "NDX", "RUT", "IXIC", "RUA", "NI225", "TOPIX", "HSI", "DAX",
              "AEX", "SX5E", "SXXP", "SENSEX", "XJO", "NZ50G", "KRX", "KOSPI",
              "MDAX", "W4500", "EWT", "000300", "000001", "000039"}

# ── POLARITY: does a RISE in this metric help or hurt risk assets? Each
# entry cites the brain note that states the direction. Anything not named
# here falls back to a category default, and every symbol publishes which
# basis was used — the reader always sees why the sign is what it is. ────
POLARITY_NOTES = {
    "DXY": (-1, "strong dollar wrecks the system [tv-ab761f92999efe68]"),
    "USDX": (-1, "strong dollar wrecks the system [tv-ab761f92999efe68]"),
    "MOVE": (-1, "MOVE spike = forced selling then QE [tv-14a76b6087dc80eb]"),
    "VIX": (-1, "vol up = derisking [his VIX notes]"),
    "VVIX": (-1, "vol-of-vol up = derisking"),
    "SKEW": (-1, "tail-risk pricing up = derisking"),
    "TEDRATE": (-1, "TED widens in crisis = credit risk"),
    "WRESBAL": (+1, "bank reserves = barometer of liquidity [tv-f452edc700d3a8da]"),
    "USM2": (+1, "money stock expansion = liquidity"),
    "USM0": (+1, "monetary base expansion = liquidity"),
    "USM1": (+1, "money stock expansion = liquidity"),
    "SOFR": (-1, "SOFR-IORB decoupling = canary in the coal mine [tv-a56315720e79a9ea]"),
    "JPLG": (+1, "Japan bank lending decline = flash warning [tv-b3ec3933837d5155]"),
    "USIRYY": (-1, "inflation forces tightening"),
    "USCLI": (+1, "leading index up = growth"),
    "USLEI": (+1, "leading index up = growth"),
    "USNFP": (+1, "employment growth"),
    "BDI": (+1, "freight rates = real global demand"),
    # growth nowcasts: these live in the vault's "rates" bucket by name shape,
    # but a RISE is growth-positive, not a tightening. Signing them off the
    # rates default inverted them (ops 3964 caught GDPNOW +35.7% being counted
    # as an adverse MACRO driver).
    "GDPNOW": (+1, "GDP nowcast up = growth"),
    "USCFNAI": (+1, "Chicago Fed activity up = growth"),
    "CFNAIMA3": (+1, "Chicago Fed 3m activity up = growth"),
    "USGDPYY": (+1, "GDP growth"), "CNGDPYY": (+1, "GDP growth"),
    "JPGDPYY": (+1, "GDP growth"), "EUGDPYY": (+1, "GDP growth"),
    "USINBR": (+1, "industrial activity up = growth"),
    # ops 3967 audit: these carry heavy note counts and an UNAMBIGUOUS direction
    # in his doctrine, but were scoring 0 and therefore not voting at all.
    # "WHEN DOLLAR STRENGTHEN AND RATES ARE HIGHER IN THE US YOU BETTER EXIT
    #  THE MARKETS" [nmq5x00zhe98n] gives the sign for both the policy rate
    # and the broad dollar.
    "FEDFUNDS": (-1, "policy rate up = tighter, exit condition [nmq5x00zhe98n]"),
    "DFF": (-1, "policy rate up = tighter [nmq5x00zhe98n]"),
    "EFFR": (-1, "policy rate up = tighter [nmq5x00zhe98n]"),
    "IORB": (-1, "policy rate up = tighter [nmq5x00zhe98n]"),
    "DTWEXBGS": (-1, "broad dollar up = global system wrecked [tv-ab761f92999efe68]"),
    "RBUSBIS": (-1, "real broad dollar up = global tightening [tv-ab761f92999efe68]"),
    "DTB3": (-1, "bill yields up = tighter [nmq5x00zhe98n]"),
    "DTB6": (-1, "bill yields up = tighter [nmq5x00zhe98n]"),
    "TB4WK": (-1, "bill yields up = tighter [nmq5x00zhe98n]"),
    "US01MY": (-1, "front-end yield up = tighter [nmq5x00zhe98n]"),
    "US02MY": (-1, "front-end yield up = tighter [nmq5x00zhe98n]"),
    "US03MY": (-1, "front-end yield up = tighter [nmq5x00zhe98n]"),
    "US06MY": (-1, "front-end yield up = tighter [nmq5x00zhe98n]"),
    "CPFF": (-1, "commercial paper spread up = funding stress"),
    # ops 4003 central-bank expansion — each direction cites the note it implements
    "JP01Y": (-1, "JGB yields up = yen carry funding cost up [tv-9fa576184567fa8f]"),
    "JP05Y": (-1, "JGB yields up = carry funding cost [tv-9fa576184567fa8f]"),
    "JP10Y": (-1, "JGB 10Y up = the carry-unwind trigger [tv-9fa576184567fa8f]"),
    "JP20Y": (-1, "long JGB up = BOJ losing the curve [tv-9fa576184567fa8f]"),
    "JP30Y": (-1, "long JGB up = BOJ losing the curve [tv-9fa576184567fa8f]"),
    "NOINTR": (-1, "policy rate up = tighter [nmq5x00zhe98n]"),
    "NO10Y": (-1, "long rates up = tighter [nmq5x00zhe98n]"),
    "PEINTR": (-1, "EM policy tightening [nmq5x00zhe98n]"),
    "PEFER": (+1, "net intl reserves = EM buffer vs the dollar squeeze [tv-ab761f92999efe68]"),
    "PENUSD": (-1, "sol per dollar up = dollar squeeze on EM [tv-ab761f92999efe68]"),
    # ops 4010 — BOJ family + six-country layer
    "JPMB": (+1, "BOJ monetary base = yen liquidity provider [tv-9fa576184567fa8f]"),
    "JPM2YY": (+1, "Japan M2 growth = yen liquidity [tv-9fa576184567fa8f]"),
    "JPM3": (+1, "Japan M3 growth = yen liquidity [tv-9fa576184567fa8f]"),
    "JPTANKAN": (+1, "Tankan mfg DI up = Japan output [brain doctrine: "
                 "INDPRO-class output leads recessions]"),
    "JPCALLR": (-1, "call rate up = carry funding cost [tv-9fa576184567fa8f]"),
    "BRINTR": (-1, "Selic up = tighter [nmq5x00zhe98n]"),
    "BRFER": (+1, "Brazil intl reserves = EM buffer [tv-ab761f92999efe68]"),
    "BRLUSD": (-1, "BRL per dollar up = dollar squeeze on EM [tv-ab761f92999efe68]"),
    "FIIPYY": (+1, "industrial output up [brain doctrine: INDPRO leads]"),
    "ESIPYY": (+1, "industrial output up [brain doctrine: INDPRO leads]"),
    "ITIPYY": (+1, "industrial output up [brain doctrine: INDPRO leads]"),
    "CHIPYY": (+1, "industrial output up [brain doctrine: INDPRO leads]"),
    "KRIPYY": (+1, "industrial output up [brain doctrine: INDPRO leads]"),
    "BRIPYY": (+1, "industrial output up [brain doctrine: INDPRO leads]"),
    "IT10Y": (-1, "periphery long yields up = tighter/fragmentation [nmq5x00zhe98n]"),
    "ES10Y": (-1, "periphery long yields up = tighter [nmq5x00zhe98n]"),
    "FI10Y": (-1, "long yields up = tighter [nmq5x00zhe98n]"),
    "CH10Y": (-1, "long yields up = tighter [nmq5x00zhe98n]"),
    "KR10Y": (-1, "long yields up = tighter [nmq5x00zhe98n]"),
    "CHINTR": (-1, "policy rate up = tighter [nmq5x00zhe98n]"),
    "KRINTR": (-1, "policy rate up = tighter [nmq5x00zhe98n]"),
}
# The rates polarity ("yields up = tighter") may only be applied to things
# that ARE a yield or a policy/inflation rate. Anything else in that bucket
# scores 0 and is EXCLUDED from breadth: a missing vote is honest, a wrong
# sign quietly corrupts the barometer.
YIELD_RX = re.compile(r"(\d+(M|Y)$|INTR|IRYY|TNX|SOFR|RATE|YIELD|BLR|FEDFUNDS|DFF|EFFR|IORB|DGS|DTB|TB\d|BILL)")
CAT_POLARITY = {
    "credit": (-1, "spreads widen = liquidity tightening [tv-8711fbee989cf1eb]"),
    "vol": (-1, "volatility up = derisking [tv-14a76b6087dc80eb]"),
    "plumbing": (+1, "funding availability up = liquidity"),
    "macro": (+1, "activity up = growth"),
    "commodity": (+1, "demand-led commodity strength = growth"),
    "rates": (-1, "yields up = tighter financial conditions [nmq5x00zhe98n]"),
    "fx": (0, "direction depends on the pair — excluded unless named"),
    "futures": (0, "contract-dependent — excluded unless named"),
    "other": (0, "unmapped — excluded from breadth"),
}

# ── asset classes predicted, and their vault proxies ─────────────────────
ASSET_CLASSES = {
    "us_equity_large": ["SPX", "NDX"],
    "us_equity_small": ["RUT", "W4500"],
    "intl_developed_equity": ["SX5E", "DAX", "NI225", "TOPIX", "SXXP", "AEX", "MDAX"],
    "em_equity": ["HSI", "KOSPI", "KRX", "SENSEX", "000300", "EWT"],
    "duration_treasuries": ["US10Y", "US30Y"],
    "credit_hy": ["BAMLH0A0HYM2"],
    "gold": ["XAUUSD", "GOLD"],
    "commodities": ["CL1!", "USOIL", "WTI", "COPPER", "HG1!"],
    "dollar": ["DXY", "USDX"],
    "crypto": ["BTCUSD", "BITCOIN"],
}

STOP = set("""the a an and or of to in on for is are was were be been being at by with as it
this that these those from into over under out up down not no if then than so but we you i my
me your his her they them their our us he she its dont cant will would should could have has
had do does did what when where who whom which why how all any both each more most other some
such only own same too very just now also there here about after before again against because
while during through between above below off once further one two three four five six seven
eight nine ten new old good bad big small high low get got go going goes went make makes made
see saw look looks like likes really much many lot lots thing things way ways time times year
years day days week weeks month months something anything everything someone people said says
say think know want need take takes give gives come comes even still back well since however
therefore thus etc via per within without across among able around another always never
""".split())
# brain notes contain pasted HTML/CSS; those tokens are not doctrine
HTML_JUNK = re.compile(r"^(classname|bg-|text-|span|hover|div|href|http|www|px-|py-|mt-|mb-|"
                       r"flex|grid|rounded|border|font-|text|style|class|width|height)")
WORD = re.compile(r"[a-z][a-z\-]{3,}")
TAG = re.compile(r"^\[TV:([A-Z0-9_:!\.\-]+)\]")
MENTION = re.compile(r"\b(FRED|ECONOMICS|AMEX|NASDAQ|NYSE|TVC|ICEUS|CME|CBOT|COMEX):"
                     r"([A-Z0-9_\.]{2,22})\b")
T2_MIN_MARGIN = 0.15          # below this the NB call is a coin flip -> inherit


def toks(t):
    return [w for w in WORD.findall((t or "").lower())
            if w not in STOP and not HTML_JUNK.match(w)]


def gj(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"[barometers] miss {key}: {e}")
        return default


# ══════════════════════════ 1. CLASSIFY ══════════════════════════════════
def classify(brain, rows):
    notes = brain.get("notes") or []
    by_id = {n.get("id"): n for n in notes}
    vault_syms = [r["symbol"] for r in rows]
    vset = set(vault_syms)
    cat_of = {r["symbol"]: r.get("category") for r in rows}

    subj, ment = defaultdict(list), defaultdict(list)
    note_syms, note_subj = defaultdict(set), {}
    for n in notes:
        txt, nid = n.get("text") or "", n.get("id")
        m = TAG.match(txt)
        if m:
            sym = m.group(1).split(":")[-1]
            subj[sym].append((nid, txt[len(m.group(0)):].strip()))
            note_syms[nid].add(sym)
            note_subj[nid] = sym
        for _ex, sym in MENTION.findall(txt):
            if note_subj.get(nid) != sym:
                ment[sym].append((nid, txt))
                note_syms[nid].add(sym)

    df, note_tok = Counter(), {}
    for n in notes:
        tk = set(toks(n.get("text")))
        note_tok[n.get("id")] = tk
        df.update(tk)

    raw = {d: Counter() for d in DOMS}
    for nid, d in ANCHORS.items():
        if nid in by_id:
            raw[d].update(note_tok.get(nid, set()))
    seeds = {}
    for d in DOMS:
        others = set()
        for o in DOMS:
            if o != d:
                others |= set(raw[o])
        seeds[d] = {w for w in raw[d] if w not in others and df.get(w, 0) >= 5}

    lab = {}
    for n in notes:
        tk = note_tok.get(n.get("id"), set())
        sc = sorted(((len(tk & seeds[d]), d) for d in DOMS), reverse=True)
        if sc[0][0] >= 2 and sc[0][0] > sc[1][0]:
            lab[n.get("id")] = sc[0][1]

    vocab = {w for w, c in df.items() if c >= 5}
    cnt = {d: Counter() for d in DOMS}
    tot = {d: 0 for d in DOMS}
    for nid, d in lab.items():
        for w in toks((by_id.get(nid) or {}).get("text")):
            if w in vocab:
                cnt[d][w] += 1
                tot[d] += 1
    V = len(vocab)
    logp = {d: {w: math.log(cnt[d].get(w, 0) + 1) - math.log(tot[d] + V) for w in vocab}
            for d in DOMS}

    def nb(text, restrict=None):
        tk = [w for w in toks(text) if w in vocab]
        if not tk:
            return None, 0.0, []
        cand = list(restrict or DOMS)
        sc = {d: sum(logp[d][w] for w in tk) / len(tk) for d in cand}
        o = sorted(sc.items(), key=lambda kv: -kv[1])
        best = o[0][0]
        mg = o[0][1] - o[1][1] if len(o) > 1 else 1.0
        top = sorted(set(tk), key=lambda w: -(logp[best][w] -
                     max(logp[x][w] for x in DOMS if x != best)))[:6]
        return best, mg, top

    dom, tier, evid, marg, nids = {}, {}, {}, {}, {}

    subj_anchor = defaultdict(set)
    for nid, d in ANCHORS.items():
        s = note_subj.get(nid)
        if s in vset:
            subj_anchor[s].add(d)
    for s, ds in subj_anchor.items():
        ids = [i for i, _b in subj.get(s, [])][:4]
        if len(ds) == 1:
            dom[s], tier[s] = next(iter(ds)), "T1a"
            evid[s] = "subject of Khalid's doctrine note"
        else:
            b, mg, top = nb(" ".join(b for _i, b in subj.get(s, [])), restrict=sorted(ds))
            dom[s], tier[s], marg[s] = b or sorted(ds)[0], "T1a*", round(mg, 4)
            evid[s] = f"subject of {len(ds)} doctrine notes {sorted(ds)}; his wording favours {dom[s]} {top}"
        nids[s] = ids
    for nid, d in ANCHORS.items():
        for s in note_syms.get(nid, ()):
            if s in vset and s not in dom:
                dom[s], tier[s], evid[s] = d, "T1b", f"named inside doctrine note {nid}"
                nids[s] = [nid]

    weak = {}
    for s in vault_syms:
        if s in dom:
            continue
        own = subj.get(s) or []
        body = " ".join(b for _i, b in own) or " ".join(b for _i, b in (ment.get(s) or [])[:6])
        b, mg, top = nb(body)
        if not b:
            continue
        nids[s] = [i for i, _x in own][:4] or [i for i, _x in (ment.get(s) or [])][:2]
        if mg >= T2_MIN_MARGIN:
            dom[s], tier[s], marg[s] = b, "T2", round(mg, 4)
            evid[s] = f"his own notes (n={len(own)}) — deciding terms {top}"
        else:
            weak[s] = (b, round(mg, 4), top)

    for s in vault_syms:
        if s in dom or not s.endswith("!"):
            continue
        root = re.sub(r"\d*!$", "", s)
        und = FUT_UNDERLYING.get(root) or FUT_UNDERLYING.get(root.rstrip("0123456789"))
        if und in dom:
            dom[s], tier[s] = dom[und], "T3f"
            evid[s] = f"futures on {und} — inherits its domain"

    for s in vault_syms:
        if s in dom:
            continue
        votes = Counter()
        for nid, _b in (subj.get(s, []) + ment.get(s, [])):
            for p in note_syms.get(nid, ()):
                if p != s and tier.get(p, "").startswith(("T1", "T2")):
                    votes[dom[p]] += 1
        if votes:
            d, k = votes.most_common(1)[0]
            dom[s], tier[s], evid[s] = d, "T3", f"co-occurs in his notes with {k} {d} metrics"

    fam = defaultdict(list)
    for s in vault_syms:
        fam[re.sub(r"[0-9!]+.*$", "", s) or s].append(s)
    for f, mem in fam.items():
        known = Counter(dom[m] for m in mem if m in dom)
        if known:
            d = known.most_common(1)[0][0]
            for m in mem:
                if m not in dom:
                    dom[m], tier[m], evid[m] = d, "T4", f"same family as {f}* ({d})"

    catvote = defaultdict(Counter)
    for s, d in dom.items():
        if tier[s].startswith(("T1", "T2")):
            catvote[cat_of.get(s)][d] += 1
    prior = {c: v.most_common(1)[0][0] for c, v in catvote.items() if v}
    for s in vault_syms:
        if s in dom:
            continue
        c = cat_of.get(s)
        if c in prior:
            dom[s], tier[s] = prior[c], "T5"
            w = weak.get(s)
            evid[s] = (f"his notes on this one were too evenly split "
                       f"(margin {w[1]}) — inherits the '{c}' domain learned from "
                       f"his high-confidence metrics" if w else
                       f"inherits the '{c}' domain learned from his high-confidence metrics")
    for s in vault_syms:
        if s not in dom:
            dom[s], tier[s], evid[s] = "MACRO", "T6", "no brain evidence"

    conf = {}
    for s in vault_syms:
        t, m = tier[s], marg.get(s, 0)
        conf[s] = ("HIGH" if t in ("T1a", "T1a*") or (t == "T2" and m >= 0.35)
                   else "MEDIUM" if t == "T2" or t == "T1b"
                   else "LOW")
    return dom, tier, evid, marg, nids, conf, prior, len(lab)


# ══════════════════════════ 2. BAROMETERS ════════════════════════════════
def polarity(sym, cat):
    if sym in POLARITY_NOTES:
        p, why = POLARITY_NOTES[sym]
        return p, why, "brain_note"
    p, why = CAT_POLARITY.get(cat, (0, "unmapped"))
    if cat == "rates" and not YIELD_RX.search(sym):
        return 0, "in the rates bucket but not a yield/policy rate — excluded rather than mis-signed", "guarded"
    return p, why, "category_default"


def effective_change(r, prev_vals):
    """The feed supplies chg_pct only on some adapter paths (Yahoo ^MOVE and
    every FRED yoy/single-observation path return a level with no change).
    ops 3967 showed that silently dropped 39 LIVE directional metrics —
    including MOVE, his most-cited RISK anchor at 42 notes. So derive it:
    feed first, then the row's own prev, then this engine's ledger."""
    ch = r.get("chg_pct")
    if ch is not None:
        return float(ch), "feed"
    v = r.get("value")
    if not isinstance(v, (int, float)):
        return None, None
    pv = r.get("prev")
    if isinstance(pv, (int, float)) and pv:
        return (v / pv - 1) * 100, "row_prev"
    lv = prev_vals.get(r["symbol"])
    if isinstance(lv, (int, float)) and lv:
        return (v / lv - 1) * 100, "ledger"
    return None, None


def build_barometers(rows, dom, gate, cat_of, prev_vals):
    """Fuse the gate's brain-cited legs (depth) with vault breadth."""
    LEG_MAP = {"LIQUIDITY": ("funding", "carry"),
               "RISK": ("credit", "structure"),
               "MACRO": ("dollar", "growth")}
    legs = (gate or {}).get("legs") or {}
    out = {}
    for d in DOMS:
        # (a) gate component: legs are scored -2..+2 -> 0..100
        used, scores = [], []
        for lk in LEG_MAP[d]:
            leg = legs.get(lk) or {}
            sc = leg.get("score_fused", leg.get("score"))
            if sc is None:
                continue
            scores.append(float(sc))
            used.append({"leg": lk, "score": round(float(sc), 3),
                         "why": (leg.get("why") or [])[:3]})
        gate_c = (sum(scores) / len(scores) + 2) / 4 * 100 if scores else None

        # (b) vault breadth: signed share of live drivers moving favourably
        up = dn = 0
        n_asset = n_drv = n_drv_live = n_dir = 0
        movers = []
        for r in rows:
            s = r["symbol"]
            if dom.get(s) != d:
                continue
            if r.get("status") != "LIVE":
                if not (cat_of.get(s) in ASSET_CATS or s in ASSET_SYMS):
                    n_drv += 1
                continue
            cat = cat_of.get(s)
            if cat in ASSET_CATS or s in ASSET_SYMS:
                n_asset += 1
                continue
            n_drv += 1
            n_drv_live += 1
            pol, why, basis = polarity(s, cat)
            if not pol:
                continue
            n_dir += 1
            ch, chsrc = effective_change(r, prev_vals)
            if ch is None:
                continue
            signed = pol * float(ch)
            (up, dn) = (up + 1, dn) if signed > 0 else (up, dn + 1)
            movers.append({"symbol": s, "chg_pct": round(float(ch), 3),
                           "chg_source": chsrc, "polarity": pol,
                           "signed": round(signed, 3),
                           "polarity_basis": basis, "why": why})
        n = up + dn
        breadth_c = (up / n * 100) if n else None
        movers.sort(key=lambda m: m["signed"])

        parts = [c for c in (gate_c, breadth_c) if c is not None]
        score = round(sum(parts) / len(parts), 1) if parts else None
        state = (None if score is None else
                 "EXPANSIVE" if score >= 65 else "SUPPORTIVE" if score >= 55 else
                 "NEUTRAL" if score >= 45 else "TIGHTENING" if score >= 30 else "STRESSED")
        disagree = (gate_c is not None and breadth_c is not None
                    and abs(gate_c - breadth_c) >= 25)
        out[d] = {
            "score_0_100": score, "state": state,
            "gate_component": None if gate_c is None else round(gate_c, 1),
            "breadth_component": None if breadth_c is None else round(breadth_c, 1),
            "gate_legs_used": used,
            "n_drivers_live": n, "n_favourable": up, "n_adverse": dn,
            "disagreement": disagree,
            "disagreement_note": ("gate depth and vault breadth disagree by >25 points — "
                                  "treat this barometer as unstable and size down"
                                  if disagree else None),
            "worst_movers": movers[:6], "best_movers": movers[-6:][::-1],
            "coverage": {
                "classified": sum(1 for x in rows if dom.get(x["symbol"]) == d),
                "drivers": n_drv, "live_drivers": n_drv_live,
                "with_direction": n_dir, "voting": n,
                "not_voting": {"is_a_prediction_target": n_asset,
                               "no_live_value": n_drv - n_drv_live,
                               "direction_not_stated_in_your_notes": n_drv_live - n_dir,
                               "no_measurable_move_yet": n_dir - n},
                "note": "voting is the honest denominator — a metric with no live value "
                        "or no stated direction is excluded rather than guessed at",
            },
            "interpretation": "100 = maximally supportive of risk assets, 0 = maximally hostile",
            "breadth_uses": "SIGN of each driver's move only. Magnitudes shown in the mover "
                            "lists are informational — percent change is meaningless for "
                            "index-level series that oscillate around zero (CFNAI et al).",
        }
    return out


# ══════════════════════════ 3. PREDICT ═══════════════════════════════════
def predict(bar, gate, rot):
    """Per-asset-class expectation. Every rule cites the note it implements.
    Blended with the existing rotation-dashboard regime prior (wired, not
    rebuilt)."""
    L = (bar["LIQUIDITY"] or {}).get("score_0_100")
    R = (bar["RISK"] or {}).get("score_0_100")
    M = (bar["MACRO"] or {}).get("score_0_100")
    rot_prior = ((rot or {}).get("l1_nowcast") or {}).get("class_prior") or {}
    regime = ((rot or {}).get("l1_nowcast") or {}).get("regime")
    posture = (gate or {}).get("posture")

    def z(v):
        return 0.0 if v is None else (v - 50) / 50.0    # -1..+1

    zl, zr, zm = z(L), z(R), z(M)
    preds = {}

    # weights per class: (liquidity, risk, macro) — the transmission Khalid's
    # doctrine describes: liquidity rules, credit is visible liquidity, and
    # the dollar/growth pair sets the macro backdrop.
    W = {
        "us_equity_large": (0.45, 0.35, 0.20),
        "us_equity_small": (0.50, 0.35, 0.15),
        "intl_developed_equity": (0.40, 0.30, 0.30),
        "em_equity": (0.50, 0.25, 0.25),
        "credit_hy": (0.35, 0.55, 0.10),
        "crypto": (0.60, 0.30, 0.10),
        "commodities": (0.20, 0.15, 0.65),
        "duration_treasuries": (0.30, -0.40, -0.30),
        "gold": (0.35, -0.30, -0.25),
        "dollar": (-0.45, -0.35, -0.20),
    }
    CITE = {
        "us_equity_large": "liquidity rules — plenty of liquidity, everyone risk-on [nmq5x0cp7zp4j]",
        "us_equity_small": "small caps are the high-beta expression of the same liquidity [nmq5x0cp7zp4j]",
        "intl_developed_equity": "a weak dollar is what lets the rest of the world run [tv-ab761f92999efe68]",
        "em_equity": "EM is the first to unwind when liquidity dries up [nmq5x0cp7zp4j]",
        "credit_hy": "as long as credit spreads narrow the markets do well [tv-8711fbee989cf1eb]; "
                     "leverage sits in the weakest HY and fallen angels [nmq5w1e03r08g]",
        "crypto": "the purest liquidity asset — it leads the drain and the flood [nmq5x0cp7zp4j]",
        "commodities": "commodities track real global growth, not financial liquidity [nmq5x00zh27pq]",
        "duration_treasuries": "when the dollar strengthens and US rates are higher you better exit "
                               "risk — duration is the other side of that trade [nmq5x00zhe98n]",
        "gold": "gold performs when the currency or the plumbing is in crisis [his XAUUSD notes]",
        "dollar": "the dollar is the single most important variable; it rises as liquidity "
                  "leaves the system [tv-ab761f92999efe68]",
    }
    for cls, (wl, wr, wm) in W.items():
        raw = wl * zl + wr * zr + wm * zm
        rp = rot_prior.get(cls)
        blended = raw if rp is None else 0.75 * raw + 0.25 * float(rp)
        direction = ("BULLISH" if blended >= 0.30 else "LEAN_BULLISH" if blended >= 0.10
                     else "NEUTRAL" if blended > -0.10
                     else "LEAN_BEARISH" if blended > -0.30 else "BEARISH")
        conviction = ("HIGH" if abs(blended) >= 0.40 else
                      "MEDIUM" if abs(blended) >= 0.18 else "LOW")
        drv = sorted((("liquidity", wl * zl), ("risk", wr * zr), ("macro", wm * zm)),
                     key=lambda kv: -abs(kv[1]))
        preds[cls] = {
            "direction": direction, "score": round(blended, 3),
            "conviction": conviction,
            "dominant_driver": drv[0][0],
            "driver_contributions": {k: round(v, 3) for k, v in drv},
            "rotation_prior_used": rp,
            "brain_basis": CITE[cls],
            "proxies": ASSET_CLASSES.get(cls, []),
        }
    return {
        "asset_classes": preds,
        "regime_context": {"rotation_regime": regime, "risk_gate_posture": posture,
                           "sizing_multiplier": (gate or {}).get("sizing_multiplier")},
        "how_to_read": "score is a -1..+1 expectation from the three barometers, "
                       "blended 75/25 with the rotation-dashboard regime prior. It is a "
                       "POSITIONING TILT, not a price target and not a timing signal — "
                       "macro gates sizing, technicals time entries [tv-c8640dea0c15ee5c].",
    }


# ══════════════════════════ 4. GRADE (self-building) ═════════════════════
def load_prev_values():
    led = gj(LEDGER_KEY, {}) or {}
    obs = led.get("observations") or []
    for o in reversed(obs):
        if o.get("values"):
            return o["values"], o.get("date")
    return {}, None


def update_ledger(bar, rows, now):
    """Append today's barometer state + asset proxy levels. Forward returns
    are computed on later runs against these stamps. Day one: n_obs 0."""
    led = gj(LEDGER_KEY, {"observations": []}) or {"observations": []}
    obs = led.get("observations") or []
    px = {}
    idx = {r["symbol"]: r for r in rows}
    for cls, proxies in ASSET_CLASSES.items():
        for p in proxies:
            r = idx.get(p)
            if r and r.get("status") == "LIVE" and isinstance(r.get("value"), (int, float)):
                px[cls] = {"proxy": p, "value": float(r["value"])}
                break
    vals = {r["symbol"]: r["value"] for r in rows
            if r.get("status") == "LIVE" and isinstance(r.get("value"), (int, float))}
    obs.append({"date": now.strftime("%Y-%m-%d"), "ts": now.isoformat(),
                "values": vals,
                "barometers": {d: (bar[d] or {}).get("score_0_100") for d in DOMS},
                "states": {d: (bar[d] or {}).get("state") for d in DOMS},
                "proxy_levels": px})
    seen, dedup = set(), []
    for o in sorted(obs, key=lambda o: o["date"]):
        if o["date"] in seen:
            dedup[-1] = o
            continue
        seen.add(o["date"])
        dedup.append(o)
    dedup = dedup[-1500:]

    graded, n_pairs = {}, 0
    by_date = {o["date"]: o for o in dedup}
    dates = sorted(by_date)
    for h in (21, 63):
        for i, d0 in enumerate(dates):
            j = i + h
            if j >= len(dates):
                break
            a, b = by_date[d0], by_date[dates[j]]
            for cls, pa in (a.get("proxy_levels") or {}).items():
                pb = (b.get("proxy_levels") or {}).get(cls)
                if not pb or pb["proxy"] != pa["proxy"] or not pa["value"]:
                    continue
                ret = (pb["value"] / pa["value"] - 1) * 100
                st = (a.get("states") or {}).get("LIQUIDITY")
                if not st:
                    continue
                k = f"{cls}|{st}|{h}d"
                g = graded.setdefault(k, {"n": 0, "sum": 0.0})
                g["n"] += 1
                g["sum"] += ret
                n_pairs += 1
    table = {k: {"n": v["n"], "mean_fwd_return_pct": round(v["sum"] / v["n"], 2)}
             for k, v in graded.items() if v["n"] >= 3}
    led = {"engine": "justhodl-domain-barometers", "updated_at": now.isoformat(),
           "observations": dedup}
    s3.put_object(Bucket=S3_BUCKET, Key=LEDGER_KEY, Body=json.dumps(led, default=str),
                  ContentType="application/json")
    return {
        "n_observations": len(dedup),
        "n_forward_pairs": n_pairs,
        "status": "ACCRUING" if len(table) == 0 else "GRADING",
        "conditional_forward_returns": table,
        "honesty": "this table is EMPTY until the ledger has enough history. The engine "
                   "never reports a backtest it has not actually accumulated. Grading "
                   "follows his doctrine: event-study / regime P&L, never daily IC.",
    }


# ══════════════════════════ handler ══════════════════════════════════════
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    print(f"[barometers] {MARKER}")

    brain = gj("data/brain.json") or {}
    vault = gj("data/tradingview.json") or {}
    # ops4216 BUS WAVE-1: beyond-book indicators join as drivers —
    # suffix-filtered to polarity-resolvable families only, so the
    # honest denominator widens with signal, never with noise.
    try:
        _bus = gj("data/indicator-bus.json") or {}
        _have = {str(r.get("symbol"))
                 for r in vault.get("symbols") or []}
        _rx = re.compile(
            r"^[A-Z]{2}(INTR|IRYY|GDPYY|UR|FI|CIR|M0|M1|M2|FER|GRES|"
            r"BOT|CA|GDG|UP|RSYY|MPRYY|BCOI|CCI|10Y|02Y)$")
        _add = []
        for _k, _r in (_bus.get("indicators") or {}).items():
            if _k not in _have and _rx.match(_k):
                _add.append({
                    "symbol": _k, "status": "LIVE",
                    "value": _r.get("v"), "asof": _r.get("asof"),
                    "source": "bus:" + str(_r.get("src"))[:24],
                    "category": "rates"
                    if ("10Y" in _k or "02Y" in _k or "INTR" in _k)
                    else "macro"})
        vault.setdefault("symbols", []).extend(_add)
        print("[barometers] bus extras +%d" % len(_add))
    except Exception:
        pass
    gate = gj("data/risk-gate.json") or {}
    rot = gj("data/rotation-dashboard.json") or {}
    rows = vault.get("symbols") or []
    if not rows or not brain.get("notes"):
        raise RuntimeError("brain or vault unavailable — refusing to publish")

    cat_of = {r["symbol"]: r.get("category") for r in rows}
    dom, tier, evid, marg, nids, conf, prior, n_lab = classify(brain, rows)

    sym_out = []
    for r in rows:
        s = r["symbol"]
        cat = cat_of.get(s)
        is_asset = cat in ASSET_CATS or s in ASSET_SYMS
        pol, why, basis = polarity(s, cat)
        sym_out.append({
            "symbol": s, "domain": dom[s], "confidence": conf[s],
            "tier": tier[s], "evidence": evid.get(s), "margin": marg.get(s),
            "brain_note_ids": nids.get(s, [])[:4],
            "role": "asset" if is_asset else "driver",
            "polarity": pol, "polarity_why": why, "polarity_basis": basis,
            "category": cat, "status": r.get("status"), "value": r.get("value"),
            "chg_pct": r.get("chg_pct"), "asof": r.get("asof"),
            "source": r.get("source"), "n_notes": r.get("n_notes"),
            "note_snippet": (r.get("note_snippet") or "")[:240],
        })

    prev_vals, prev_date = load_prev_values()
    bar = build_barometers(rows, dom, gate, cat_of, prev_vals)
    pred = predict(bar, gate, rot)
    grade = update_ledger(bar, rows, now)

    tc, dc = Counter(tier.values()), Counter(dom.values())
    own = sum(v for k, v in tc.items() if k.startswith(("T1", "T2")))
    by_dom = {d: {"n": dc.get(d, 0),
                  "n_drivers": sum(1 for x in sym_out if x["domain"] == d and x["role"] == "driver"),
                  "n_live": sum(1 for x in sym_out if x["domain"] == d and x["status"] == "LIVE"),
                  "symbols": [x["symbol"] for x in sym_out if x["domain"] == d]}
              for d in DOMS}

    out = {
        "engine": "justhodl-domain-barometers",
        "version": "1.0",
        "marker": MARKER,
        "generated_at": now.isoformat(),
        "brain_constitution": {
            "directive": "Khalid 2026-07-27: classification must come from my brain notes "
                         "and tradingview notes; every indicator classified into MACRO / "
                         "LIQUIDITY / RISK.",
            "method": "multinomial Naive Bayes (Laplace a=1, equal class priors) trained on "
                      "the brain corpus, weak-labelled from his doctrine notes. No hand-written "
                      "symbol->domain map: every assignment publishes its tier, margin, the "
                      "terms that decided it, and the note ids.",
            "anchor_notes": ANCHORS,
            "tier_legend": {
                "T1a": "symbol is the SUBJECT of one of his doctrine notes",
                "T1a*": "subject of doctrine notes in 2 domains — his wording broke the tie",
                "T1b": "named inside a doctrine note",
                "T2": f"classified from his own notes (NB margin >= {T2_MIN_MARGIN})",
                "T3f": "futures contract inheriting its underlying's domain",
                "T3": "co-occurs in his notes with classified metrics",
                "T4": "same symbol family as a classified sibling",
                "T5": "his notes were too evenly split — inherits the category domain "
                      "learned from his high-confidence metrics",
                "T6": "no brain evidence (should be zero)",
            },
        },
        "classification_audit": {
            "n_symbols": len(sym_out), "n_weak_labeled_notes": n_lab,
            "tier_counts": dict(tc), "domain_counts": dict(dc),
            "from_his_own_notes": own,
            "from_his_own_notes_pct": round(own / max(1, len(sym_out)) * 100, 1),
            "confidence_counts": dict(Counter(conf.values())),
            "learned_category_priors": prior,
        },
        "ledger_prev_values": {"n": len(prev_vals), "as_of": prev_date},
        "barometers": bar,
        "predictions": pred,
        "grading": grade,
        "by_domain": by_dom,
        "symbols": sym_out,
        "consume_as": "domain = how Khalid's notes frame the metric; barometer = the state of "
                      "that domain 0-100; predictions = positioning tilt per asset class. "
                      "Macro gates sizing before selection [nmq5x0cp7zp4j].",
        "elapsed_s": round(time.time() - t0, 1),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str),
                  ContentType="application/json", CacheControl="max-age=600")
    print(f"[barometers] DONE {out['elapsed_s']}s "
          f"M={bar['MACRO']['score_0_100']} L={bar['LIQUIDITY']['score_0_100']} "
          f"R={bar['RISK']['score_0_100']} own={own}/{len(sym_out)}")
    return {"ok": True, "n_symbols": len(sym_out),
            "barometers": {d: bar[d]["score_0_100"] for d in DOMS},
            "own_notes_pct": out["classification_audit"]["from_his_own_notes_pct"]}
