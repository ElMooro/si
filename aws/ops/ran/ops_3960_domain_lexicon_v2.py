"""
ops_3960 — PROBE 2 (still no engine code): fix the three defects ops 3959 found.

3959 proved the DATA is there — 530/561 vault symbols carry rich prose in
Khalid's own notes, 31 thin, ZERO bare. So the classification can and must
come from his notes (T1/T2), not from a category fallback. It landed 466 on
T5 only because the lexicon was broken.

DEFECT 1 — LEXICON SELECTED RARE WORDS. Log-odds lift over 19 short anchor
  notes surfaced "bueno", "tonight", "jargon", "swoops", and his typos
  ("alwyas", "aggresively"). Doctrine vocabulary is the opposite: words that
  are BOTH in his anchor notes AND frequent across the 12,122-note corpus.
  FIX: seed = in-anchor AND df>=5. Then weak-label every note by seed hits
  and EXPAND the lexicon to terms whose usage skews >=2x to one domain
  (df>=8). Bootstrapped from his writing, denoised by frequency.

DEFECT 2 — ANCHOR BY MENTION, NOT SUBJECT. DXY resolved LIQUIDITY off the
  yen-carry note that merely mentions it, while his dedicated [TV:DXY] note
  ("you can't get your global macro view right if you get your dollar view
  wrong") is MACRO. FIX: T1a = symbol is the SUBJECT of the anchor note
  (the [TV:SYM] tag) outranks T1b = merely mentioned.

DEFECT 3 — FUTURES UNCLASSIFIED. 25 symbols (CL1!, HG1!, GC2!, 6J2!, ZF1!,
  SR32!...) failed every tier: the family regex strips digits but leaves "!",
  and the futures category had no seeds. FIX: join each futures symbol to its
  UNDERLYING spot symbol already in the vault (CL1!->WTI, HG1!->COPPER,
  GC2!->GOLD, 6J2!->JPY, ZF1!->US05Y, SR32!->SOFR) and inherit the
  underlying's brain-derived domain.

Also tags every symbol is_driver (an indicator that MOVES a barometer) vs
is_asset (a thing the barometers PREDICT) — an index cannot be both an input
to the forecast and the target of it.

Gates: 100% coverage AND T1+T2 (his own words) >= 60% of symbols.
"""
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

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

# futures root -> underlying spot symbol that already lives in the vault
FUT_UNDERLYING = {
    "CL": "WTI", "GC": "GOLD", "SI": "SILVER", "HG": "COPPER", "PL": "PLATINUM",
    "PA": "PALLADIUM", "NG": "WTI", "ZF": "US05Y", "ZN": "US10Y", "ZT": "US02Y",
    "ZB": "US30Y", "SR": "SOFR", "SO": "SOFR", "SON": "SOFR", "GE": "SOFR",
    "6J": "JPIRYY", "6E": "EUINTR", "6B": "GB10Y", "TOPIX": "TOPIX",
    "MME": "SPX", "EMM": "SPX", "USW": "US30Y", "USP": "US30Y", "US": "US30Y",
    "I": "SPX", "YIT": "IT10Y", "AWN": "WTI", "AW": "WTI", "F1U": "US05Y",
    "FMMG": "SPX", "FMEA": "SPX", "FMMI": "SPX", "GBW": "GB10Y", "MWL": "SPX",
    "SR3": "SOFR", "SO3": "SOFR",
}

ASSET_CATS = {"equity", "etf", "index", "crypto"}
ASSET_SYMS = {"SPX", "NDX", "RUT", "IXIC", "RUA", "NI225", "TOPIX", "HSI", "DAX",
              "AEX", "SX5E", "SXXP", "SENSEX", "XJO", "NZ50G", "KRX", "KOSPI",
              "MDAX", "W4500", "EWT", "000300", "000001", "000039"}

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
therefore thus etc via per within without across among able around another
""".split())
WORD = re.compile(r"[a-z][a-z\-]{3,}")
TAG = re.compile(r"^\[TV:([A-Z0-9_:!\.\-]+)\]")
MENTION = re.compile(r"\b(FRED|ECONOMICS|AMEX|NASDAQ|NYSE|TVC|ICEUS|CME|CBOT|COMEX):"
                     r"([A-Z0-9_\.]{2,22})\b")


def toks(t):
    return [w for w in WORD.findall((t or "").lower()) if w not in STOP]


def main():
    with report("3960_domain_lexicon_v2") as rep:
        rep.heading("ops 3960 — PROBE 2: brain lexicon v2 + subject priority + futures join")
        checks = []

        brain = json.loads(s3.get_object(Bucket=BUCKET, Key="data/brain.json")["Body"].read())
        vault = json.loads(s3.get_object(Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
        notes = brain.get("notes") or []
        rows = vault.get("symbols") or []
        vault_syms = [r["symbol"] for r in rows]
        vset = set(vault_syms)
        cat_of = {r["symbol"]: r.get("category") for r in rows}
        by_id = {n.get("id"): n for n in notes}

        # per-symbol notes, split subject (own [TV:] tag) vs mention
        subj_notes = defaultdict(list)
        ment_notes = defaultdict(list)
        note_syms = defaultdict(set)
        note_subj = {}
        for n in notes:
            txt, nid = n.get("text") or "", n.get("id")
            m = TAG.match(txt)
            if m:
                sym = m.group(1).split(":")[-1]
                body = txt[len(m.group(0)):].strip()
                subj_notes[sym].append((nid, body))
                note_syms[nid].add(sym)
                note_subj[nid] = sym
            for _ex, sym in MENTION.findall(txt):
                if note_subj.get(nid) != sym:
                    ment_notes[sym].append((nid, txt))
                    note_syms[nid].add(sym)

        # ── corpus df ────────────────────────────────────────────────────
        rep.section("A. lexicon v2 — in-anchor AND frequent, then weak-label expansion")
        df = Counter()
        note_tok = {}
        for n in notes:
            tk = set(toks(n.get("text")))
            note_tok[n.get("id")] = tk
            df.update(tk)

        seeds = {}
        for dom in ("MACRO", "LIQUIDITY", "RISK"):
            c = Counter()
            for nid, d in ANCHORS.items():
                if d == dom and nid in by_id:
                    c.update(note_tok.get(nid, set()))
            seeds[dom] = {w for w in c if df.get(w, 0) >= 5}
        # drop terms shared by all three domains (non-discriminative)
        shared = seeds["MACRO"] & seeds["LIQUIDITY"] & seeds["RISK"]
        for d in seeds:
            seeds[d] -= shared
        for d in seeds:
            rep.log(f"  seed {d} (n={len(seeds[d])}): {sorted(seeds[d])[:26]}")
        checks.append(("seeds found for 3 domains", all(len(seeds[d]) >= 5 for d in seeds)))

        # weak-label every note by seed hits, then expand
        lab = {}
        for n in notes:
            tk = note_tok.get(n.get("id"), set())
            sc = {d: len(tk & seeds[d]) for d in seeds}
            top = sorted(sc.items(), key=lambda kv: -kv[1])
            if top[0][1] >= 2 and top[0][1] > top[1][1]:
                lab[n.get("id")] = top[0][0]
        labn = Counter(lab.values())
        rep.kv(weak_labeled_notes=len(lab), **{f"wl_{k}": v for k, v in labn.items()})

        dom_df = {d: Counter() for d in seeds}
        for nid, d in lab.items():
            dom_df[d].update(note_tok.get(nid, set()))
        lex = {d: dict() for d in seeds}
        for w, n_all in df.items():
            if n_all < 8:
                continue
            vals = {d: dom_df[d].get(w, 0) / max(1, labn.get(d, 1)) for d in seeds}
            order = sorted(vals.items(), key=lambda kv: -kv[1])
            if order[0][1] > 0 and order[0][1] >= 2.0 * max(order[1][1], 1e-9):
                lex[order[0][0]][w] = round(order[0][1] / max(order[1][1], 1e-9), 2)
        for d in lex:
            top = sorted(lex[d].items(), key=lambda kv: -df[kv[0]])[:30]
            rep.log(f"  LEX {d} (n={len(lex[d])}): " + ", ".join(w for w, _s in top))
        checks.append(("expanded lexicon non-trivial",
                       all(len(lex[d]) >= 20 for d in lex)))

        # ── classification ladder ────────────────────────────────────────
        rep.section("B. ladder")
        dom, tier, evid = {}, {}, {}

        for nid, d in ANCHORS.items():                      # T1a subject
            s = note_subj.get(nid)
            if s in vset and s not in dom:
                dom[s], tier[s], evid[s] = d, "T1a", f"subject of anchor {nid}"
        for nid, d in ANCHORS.items():                      # T1b mention
            for s in note_syms.get(nid, ()):
                if s in vset and s not in dom:
                    dom[s], tier[s], evid[s] = d, "T1b", f"named in anchor {nid}"

        for s in vault_syms:                                # T2 own notes
            if s in dom:
                continue
            own = subj_notes.get(s) or []
            body = " ".join(b for _i, b in own) or \
                   " ".join(b for _i, b in (ment_notes.get(s) or [])[:6])
            if not body.strip():
                continue
            tk = toks(body)
            sc = {d: sum(lex[d].get(w, 0) for w in tk) for d in lex}
            order = sorted(sc.items(), key=lambda kv: -kv[1])
            if order[0][1] >= 3 and order[0][1] >= 1.35 * max(order[1][1], 1e-9):
                hit = sorted({w for w in tk if w in lex[order[0][0]]},
                             key=lambda w: -lex[order[0][0]][w])[:6]
                dom[s], tier[s] = order[0][0], "T2"
                evid[s] = (f"own notes {[i for i, _b in own][:2]} "
                           f"score={order[0][1]:.0f}v{order[1][1]:.0f} terms={hit}")

        for root, und in FUT_UNDERLYING.items():            # T3f futures->underlying
            pass
        for s in vault_syms:
            if s in dom or not s.endswith("!"):
                continue
            root = re.sub(r"\d*!$", "", s)
            und = FUT_UNDERLYING.get(root) or FUT_UNDERLYING.get(root.rstrip("0123456789"))
            if und and und in dom:
                dom[s], tier[s] = dom[und], "T3f"
                evid[s] = f"futures on {und} → inherits {dom[und]}"

        for s in vault_syms:                                # T3 co-occurrence
            if s in dom:
                continue
            votes = Counter()
            for nid, _b in (subj_notes.get(s, []) + ment_notes.get(s, [])):
                for peer in note_syms.get(nid, ()):
                    if peer != s and tier.get(peer, "").startswith(("T1", "T2")):
                        votes[dom[peer]] += 1
            if votes:
                d, k = votes.most_common(1)[0]
                dom[s], tier[s], evid[s] = d, "T3", f"co-occurs with {k} {d} symbols"

        fam = defaultdict(list)                             # T4 family
        for s in vault_syms:
            fam[re.sub(r"[0-9!]+.*$", "", s) or s].append(s)
        for f, mem in fam.items():
            known = Counter(dom[m] for m in mem if m in dom)
            if not known:
                continue
            d = known.most_common(1)[0][0]
            for m in mem:
                if m not in dom:
                    dom[m], tier[m] = d, "T4"
                    evid[m] = f"family '{f}' majority {d}"

        catvote = defaultdict(Counter)                      # T5 learned prior
        for s, d in dom.items():
            if tier[s].startswith(("T1", "T2")):
                catvote[cat_of.get(s)][d] += 1
        prior = {c: v.most_common(1)[0][0] for c, v in catvote.items() if v}
        rep.log(f"  learned category priors (T1/T2 only): {json.dumps(prior, sort_keys=True)}")
        for s in vault_syms:
            if s not in dom and cat_of.get(s) in prior:
                dom[s], tier[s] = prior[cat_of[s]], "T5"
                evid[s] = f"category '{cat_of[s]}' prior {prior[cat_of[s]]} {dict(catvote[cat_of[s]])}"

        glob = Counter(dom.values()).most_common(1)[0][0] if dom else "MACRO"
        for s in vault_syms:                                # T6 backstop
            if s not in dom:
                dom[s], tier[s] = glob, "T6"
                evid[s] = f"no brain evidence — global majority {glob}"

        tc = Counter(tier.values())
        dc = Counter(dom.values())
        own_words = sum(v for k, v in tc.items() if k.startswith(("T1", "T2")))
        rep.kv(**{k: tc.get(k, 0) for k in ("T1a", "T1b", "T2", "T3f", "T3", "T4", "T5", "T6")})
        rep.kv(MACRO=dc.get("MACRO", 0), LIQUIDITY=dc.get("LIQUIDITY", 0),
               RISK=dc.get("RISK", 0), classified=len(dom), total=len(vault_syms),
               from_his_own_words=own_words,
               own_words_pct=round(own_words / max(1, len(vault_syms)) * 100, 1))

        rep.section("C. driver vs asset split (no self-prediction)")
        drivers = [s for s in vault_syms
                   if cat_of.get(s) not in ASSET_CATS and s not in ASSET_SYMS]
        rep.kv(drivers=len(drivers), assets=len(vault_syms) - len(drivers))
        dd = Counter(dom[s] for s in drivers)
        rep.kv(driver_MACRO=dd.get("MACRO", 0), driver_LIQUIDITY=dd.get("LIQUIDITY", 0),
               driver_RISK=dd.get("RISK", 0))

        rep.section("D. worked examples")
        for s in ["RRPONTSYD", "SOFR", "WRESBAL", "JPLG", "MOVE", "VIX", "VVIX", "SKEW",
                  "DXY", "USDX", "US10Y", "US02Y", "TEDRATE", "XAUUSD", "GOLD", "CL1!",
                  "HG1!", "GC2!", "SR32!", "ZF1!", "SPX", "USM2", "USCLI", "USIRYY",
                  "BDI", "USNFP", "USFER", "USLEI", "USINTR", "CN10Y"]:
            if s in dom:
                rep.log(f"  {s:10s} → {dom[s]:9s} [{tier[s]:3s}] {evid.get(s,'')[:140]}")

        checks.append(("100% classified", len(dom) == len(vault_syms)))
        checks.append(("no T6 backstop needed", tc.get("T6", 0) == 0))
        checks.append(("majority from his own notes (T1/T2 >= 60%)",
                       own_words >= 0.60 * len(vault_syms)))
        checks.append(("all 3 domains have drivers",
                       all(dd.get(d, 0) >= 5 for d in ("MACRO", "LIQUIDITY", "RISK"))))

        failed = [l for l, ok in checks if not ok]
        for l, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — {len(dom)}/{len(vault_syms)} classified, "
               f"{own_words} ({round(own_words/len(vault_syms)*100,1)}%) from his own notes; "
               f"MACRO {dc['MACRO']} LIQUIDITY {dc['LIQUIDITY']} RISK {dc['RISK']}")


if __name__ == "__main__":
    main()
