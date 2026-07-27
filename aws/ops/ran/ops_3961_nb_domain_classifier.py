"""
ops_3961 — PROBE 3 (still no engine code): replace the ratio hack with a
properly normalised classifier.

3960 reached 100% coverage and 87.7% from Khalid's own notes, but the
evidence proved the scoring was unsound:

  * term weight = ratio with a 1e-9 floor -> scores like 280,320,850 and
    "top terms" that were noise ("knee", "chaneel", "imperial", "frontier").
  * MACRO lexicon grew to 988 terms vs RISK 321, so bigger-lexicon-wins:
    every ambiguous category prior resolved MACRO (rates, fx, vol, futures,
    other) and the split came out MACRO 336 / LIQUIDITY 141 / RISK 84.
  * consequences that contradict his own doctrine: VIX -> MACRO and SR32!
    (SOFR futures) -> MACRO, while his MOVE note grounds vol as RISK and his
    SOFR/repo notes ground funding as LIQUIDITY.
  * DXY resolved by dict order because it is the subject of TWO anchor notes
    (the yen-carry one and the dollar one) — an ordering artifact, not
    evidence.

FIXES
  1. SEEDS SHARPENED: a seed term must appear in EXACTLY ONE domain's anchor
     notes (pairwise-exclusive, not just triple-shared removal) and df>=5.
  2. MULTINOMIAL NAIVE BAYES with Laplace alpha=1 trained on the weak-labeled
     notes, EQUAL CLASS PRIORS (so corpus imbalance cannot decide), scoring
     log P(terms|domain). Bounded, frequency-aware, and the top contributing
     terms are real doctrine words instead of rare noise.
  3. ANCHOR CONFLICTS resolved by NB score across the conflicting domains
     instead of dict order.
  4. DIAGNOSTIC: dump the raw note text Khalid wrote for the contested
     symbols (VIX, VVIX, SKEW, SR32!, DXY, XAUUSD) so the label can be
     checked against his actual words rather than assumed.

Gates: 100% coverage, zero backstop, T1/T2 >= 60%, and every domain must
hold a real share of drivers (no single-domain collapse).
"""
import json
import math
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
WORD = re.compile(r"[a-z][a-z\-]{3,}")
TAG = re.compile(r"^\[TV:([A-Z0-9_:!\.\-]+)\]")
MENTION = re.compile(r"\b(FRED|ECONOMICS|AMEX|NASDAQ|NYSE|TVC|ICEUS|CME|CBOT|COMEX):"
                     r"([A-Z0-9_\.]{2,22})\b")
DOMS = ("MACRO", "LIQUIDITY", "RISK")


def toks(t):
    return [w for w in WORD.findall((t or "").lower()) if w not in STOP]


def main():
    with report("3961_nb_domain_classifier") as rep:
        rep.heading("ops 3961 — PROBE 3: Naive Bayes domain classifier from the brain")
        checks = []

        brain = json.loads(s3.get_object(Bucket=BUCKET, Key="data/brain.json")["Body"].read())
        vault = json.loads(s3.get_object(Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
        notes = brain.get("notes") or []
        rows = vault.get("symbols") or []
        vault_syms = [r["symbol"] for r in rows]
        vset, cat_of = set(vault_syms), {r["symbol"]: r.get("category") for r in rows}
        by_id = {n.get("id"): n for n in notes}

        subj_notes, ment_notes = defaultdict(list), defaultdict(list)
        note_syms, note_subj = defaultdict(set), {}
        for n in notes:
            txt, nid = n.get("text") or "", n.get("id")
            m = TAG.match(txt)
            if m:
                sym = m.group(1).split(":")[-1]
                subj_notes[sym].append((nid, txt[len(m.group(0)):].strip()))
                note_syms[nid].add(sym)
                note_subj[nid] = sym
            for _ex, sym in MENTION.findall(txt):
                if note_subj.get(nid) != sym:
                    ment_notes[sym].append((nid, txt))
                    note_syms[nid].add(sym)

        rep.section("A. sharpened seeds (exclusive to one domain, df>=5)")
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
            rep.log(f"  {d} (n={len(seeds[d])}): {sorted(seeds[d])[:30]}")
        checks.append(("exclusive seeds for all domains",
                       all(len(seeds[d]) >= 5 for d in DOMS)))

        rep.section("B. weak labels + multinomial NB (Laplace a=1, equal priors)")
        lab = {}
        for n in notes:
            tk = note_tok.get(n.get("id"), set())
            sc = sorted(((len(tk & seeds[d]), d) for d in DOMS), reverse=True)
            if sc[0][0] >= 2 and sc[0][0] > sc[1][0]:
                lab[n.get("id")] = sc[0][1]
        labn = Counter(lab.values())
        rep.kv(weak_labeled=len(lab), **{f"wl_{d}": labn.get(d, 0) for d in DOMS})
        checks.append(("all domains weak-labeled", all(labn.get(d, 0) >= 100 for d in DOMS)))

        vocab = {w for w, c in df.items() if c >= 5}
        cnt = {d: Counter() for d in DOMS}
        tot = {d: 0 for d in DOMS}
        for nid, d in lab.items():
            for w in toks((by_id.get(nid) or {}).get("text")):
                if w in vocab:
                    cnt[d][w] += 1
                    tot[d] += 1
        V = len(vocab)
        logp = {d: {} for d in DOMS}
        for d in DOMS:
            den = math.log(tot[d] + V)
            for w in vocab:
                logp[d][w] = math.log(cnt[d].get(w, 0) + 1) - den
        for d in DOMS:
            disc = sorted(vocab, key=lambda w: -(logp[d][w] - max(logp[o][w] for o in DOMS if o != d)))
            rep.log(f"  NB top {d}: " + ", ".join(disc[:26]))

        def nb(text, restrict=None):
            tk = [w for w in toks(text) if w in vocab]
            if not tk:
                return None, 0.0, []
            cand = restrict or DOMS
            sc = {d: sum(logp[d][w] for w in tk) / len(tk) for d in cand}
            o = sorted(sc.items(), key=lambda kv: -kv[1])
            best = o[0][0]
            margin = o[0][1] - o[1][1] if len(o) > 1 else 1.0
            top = sorted(set(tk), key=lambda w: -(logp[best][w] -
                         max(logp[x][w] for x in DOMS if x != best)))[:6]
            return best, margin, top

        rep.section("C. ladder")
        dom, tier, evid, marg = {}, {}, {}, {}
        subj_anchor = defaultdict(set)
        for nid, d in ANCHORS.items():
            s = note_subj.get(nid)
            if s in vset:
                subj_anchor[s].add(d)
        for s, ds in subj_anchor.items():
            if len(ds) == 1:
                dom[s], tier[s] = next(iter(ds)), "T1a"
                evid[s] = "subject of his anchor note"
            else:
                body = " ".join(b for _i, b in subj_notes.get(s, []))
                b2, mg, top = nb(body, restrict=sorted(ds))
                dom[s], tier[s], marg[s] = b2 or sorted(ds)[0], "T1a*", round(mg, 4)
                evid[s] = f"subject of {len(ds)} anchors {sorted(ds)} → NB picked {dom[s]} {top}"
        for nid, d in ANCHORS.items():
            for s in note_syms.get(nid, ()):
                if s in vset and s not in dom:
                    dom[s], tier[s], evid[s] = d, "T1b", f"named in anchor {nid}"

        for s in vault_syms:
            if s in dom:
                continue
            own = subj_notes.get(s) or []
            body = " ".join(b for _i, b in own) or \
                   " ".join(b for _i, b in (ment_notes.get(s) or [])[:6])
            b2, mg, top = nb(body)
            if b2 and mg >= 0.02:
                dom[s], tier[s], marg[s] = b2, "T2", round(mg, 4)
                evid[s] = f"own notes n={len(own)} margin={mg:.3f} terms={top}"

        for s in vault_syms:
            if s in dom or not s.endswith("!"):
                continue
            root = re.sub(r"\d*!$", "", s)
            und = FUT_UNDERLYING.get(root) or FUT_UNDERLYING.get(root.rstrip("0123456789"))
            if und in dom:
                dom[s], tier[s] = dom[und], "T3f"
                evid[s] = f"futures on {und} → {dom[und]}"

        for s in vault_syms:
            if s in dom:
                continue
            votes = Counter()
            for nid, _b in (subj_notes.get(s, []) + ment_notes.get(s, [])):
                for p in note_syms.get(nid, ()):
                    if p != s and tier.get(p, "").startswith(("T1", "T2")):
                        votes[dom[p]] += 1
            if votes:
                d, k = votes.most_common(1)[0]
                dom[s], tier[s], evid[s] = d, "T3", f"co-occurs with {k} {d} symbols"

        fam = defaultdict(list)
        for s in vault_syms:
            fam[re.sub(r"[0-9!]+.*$", "", s) or s].append(s)
        for f, mem in fam.items():
            known = Counter(dom[m] for m in mem if m in dom)
            if known:
                d = known.most_common(1)[0][0]
                for m in mem:
                    if m not in dom:
                        dom[m], tier[m], evid[m] = d, "T4", f"family '{f}' majority {d}"

        catvote = defaultdict(Counter)
        for s, d in dom.items():
            if tier[s].startswith(("T1", "T2")):
                catvote[cat_of.get(s)][d] += 1
        prior = {c: v.most_common(1)[0][0] for c, v in catvote.items() if v}
        rep.log(f"  learned category priors: {json.dumps(prior, sort_keys=True)}")
        for s in vault_syms:
            if s not in dom and cat_of.get(s) in prior:
                dom[s], tier[s] = prior[cat_of[s]], "T5"
                evid[s] = f"category '{cat_of[s]}' prior {dict(catvote[cat_of[s]])}"
        for s in vault_syms:
            if s not in dom:
                dom[s], tier[s], evid[s] = "MACRO", "T6", "no evidence"

        tc, dc = Counter(tier.values()), Counter(dom.values())
        own = sum(v for k, v in tc.items() if k.startswith(("T1", "T2")))
        rep.kv(**{k: tc.get(k, 0) for k in ("T1a", "T1a*", "T1b", "T2", "T3f", "T3", "T4", "T5", "T6")})
        rep.kv(**{d: dc.get(d, 0) for d in DOMS}, total=len(vault_syms),
               own_words=own, own_pct=round(own / len(vault_syms) * 100, 1))

        drivers = [s for s in vault_syms if cat_of.get(s) not in ASSET_CATS
                   and s not in ASSET_SYMS]
        dd = Counter(dom[s] for s in drivers)
        rep.kv(drivers=len(drivers), **{f"driver_{d}": dd.get(d, 0) for d in DOMS})

        rep.section("D. contested symbols — his ACTUAL note text")
        for s in ("VIX", "VVIX", "SKEW", "SR32!", "DXY", "XAUUSD", "TEDRATE"):
            own_n = subj_notes.get(s) or []
            txt = " ".join(b for _i, b in own_n)[:300].replace("\n", " ")
            rep.log(f"  {s} → {dom.get(s)} [{tier.get(s)}] margin={marg.get(s)}")
            rep.log(f"      notes(n={len(own_n)}): {txt}")

        rep.section("E. worked examples")
        for s in ["RRPONTSYD", "SOFR", "WRESBAL", "JPLG", "MOVE", "US10Y", "US02Y",
                  "GOLD", "CL1!", "HG1!", "SPX", "USM2", "USCLI", "USIRYY", "BDI",
                  "USNFP", "USLEI", "CN10Y", "USINTR", "USM0"]:
            if s in dom:
                rep.log(f"  {s:10s} → {dom[s]:9s} [{tier[s]:4s}] {evid.get(s,'')[:130]}")

        checks.append(("100% classified", len(dom) == len(vault_syms)))
        checks.append(("no backstop", tc.get("T6", 0) == 0))
        checks.append(("T1/T2 >= 60%", own >= 0.60 * len(vault_syms)))
        checks.append(("no domain collapse (each >= 8% of drivers)",
                       all(dd.get(d, 0) >= 0.08 * len(drivers) for d in DOMS)))
        failed = [l for l, ok in checks if not ok]
        for l, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — {len(dom)}/{len(vault_syms)}; own-notes {own} ({round(own/len(vault_syms)*100,1)}%); "
               f"MACRO {dc['MACRO']} LIQUIDITY {dc['LIQUIDITY']} RISK {dc['RISK']}")


if __name__ == "__main__":
    main()
