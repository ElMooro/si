"""
ops_3959 — PROBE ONLY (writes no engine code, no S3 artifacts).

Khalid's directive: the MACRO / LIQUIDITY / RISK classification of every
TradingView vault symbol must come from HIS OWN brain notes + TV notes —
not from my judgement of what a ticker means — and EVERY indicator must end
up classified (no unclassified bucket).

This probe establishes, empirically, whether that is reachable and with what
evidence quality, before a single line of engine code is written:

  A. corpus     — brain size, TV-tagged notes, vault symbols, and how much
                  REAL note body text exists per symbol (a bare "[TV:SYM]"
                  tag with no prose cannot classify anything).
  B. anchors    — dump the full text of the doctrine notes the risk-gate
                  already cites, per leg. These are ground-truth domain
                  seeds written by Khalid.
  C. lexicon    — MINE the vocabulary from those anchor notes themselves
                  (distinctive terms per domain, log-odds vs the rest of the
                  corpus). The lexicon is derived from his writing, not
                  invented by me.
  D. ladder     — dry-run the 5-tier assignment (T1 anchor / T2 own-note
                  terms / T3 co-occurrence / T4 family / T5 learned category
                  prior) and report coverage + margin + worked examples with
                  matched terms and note ids.

Gate: brain + vault load, and the ladder must reach 100% of vault symbols.
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

# The note ids the risk-gate cites, with the leg each one grounds.
# leg -> domain: funding/carry = LIQUIDITY, credit/structure = RISK,
# dollar/growth = MACRO  (this mapping is the gate's own framework)
ANCHORS = {
    "nmq5x1e4os92j": ("funding", "LIQUIDITY"),
    "nmq5x1e4kuplz": ("funding", "LIQUIDITY"),
    "tv-f452edc700d3a8da": ("funding", "LIQUIDITY"),
    "tv-a56315720e79a9ea": ("funding", "LIQUIDITY"),
    "nmq5vhvebjob6": ("funding", "LIQUIDITY"),
    "tv-9fa576184567fa8f": ("carry", "LIQUIDITY"),
    "tv-f58a44fc2f839aac": ("carry", "LIQUIDITY"),
    "tv-b3ec3933837d5155": ("carry", "LIQUIDITY"),
    "tv-8711fbee989cf1eb": ("credit", "RISK"),
    "tv-ba419d7b64a1e75d": ("credit", "RISK"),
    "tv-e38d57bffedb366a": ("credit", "RISK"),
    "nmq5w1e03r08g": ("credit", "RISK"),
    "tv-14a76b6087dc80eb": ("structure", "RISK"),
    "tv-ab761f92999efe68": ("dollar", "MACRO"),
    "nmq5x00zhe98n": ("dollar", "MACRO"),
    "tv-b4c32545ea1dc640": ("dollar", "MACRO"),
    "nmq5x00zh27pq": ("growth", "MACRO"),
    "nmq5x0cp7zp4j": ("hierarchy", "MACRO"),
    "tv-c8640dea0c15ee5c": ("hierarchy", "MACRO"),
}

STOP = set("""the a an and or of to in on for is are was were be been being at by with as it
this that these those from into over under out up down not no if then than so but we you i my
me your his her they them their our us he she its it's dont don't can cant can't will would
should could have has had do does did what when where who whom which why how all any both each
more most other some such only own same too very just now also there here about after before
again against because while during through between above below off once further
one two three four five six seven eight nine ten new old good bad big small high low get got
go going goes went make makes made see saw look looks like likes really much many lot lots
thing things way ways time times year years day days week weeks month months
""".split())

WORD = re.compile(r"[a-z][a-z\-']{2,}")
TAG = re.compile(r"^\[TV:([A-Z0-9_:!\.\-]+)\]")
MENTION = re.compile(r"\b(FRED|ECONOMICS|AMEX|NASDAQ|NYSE|TVC|ICEUS|CME|CBOT|COMEX):"
                     r"([A-Z0-9_\.]{2,22})\b")


def toks(text):
    return [w for w in WORD.findall((text or "").lower()) if w not in STOP]


def main():
    with report("3959_brain_domain_probe") as rep:
        rep.heading("ops 3959 — PROBE: brain-sourced MACRO/LIQUIDITY/RISK classification")
        rep.log("no engine code written; no S3 artifact written. evidence only.")
        checks = []

        # ── A. corpus ────────────────────────────────────────────────────
        rep.section("A. corpus")
        brain = json.loads(s3.get_object(Bucket=BUCKET, Key="data/brain.json")["Body"].read())
        vault = json.loads(s3.get_object(Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
        notes = brain.get("notes") or []
        rows = vault.get("symbols") or []
        checks.append(("brain loaded", len(notes) > 1000))
        checks.append(("vault loaded", len(rows) > 100))
        rep.kv(brain_notes=len(notes), vault_symbols=len(rows),
               vault_live=vault.get("n_live"), vault_version=vault.get("version"))

        by_id = {n.get("id"): n for n in notes}
        # full note map per symbol: tagged notes + prefixed mentions anywhere
        sym_notes = defaultdict(list)          # sym -> [(note_id, body)]
        note_syms = defaultdict(set)           # note_id -> {syms}
        for n in notes:
            txt = n.get("text") or ""
            nid = n.get("id")
            m = TAG.match(txt)
            if m:
                sym = m.group(1).split(":")[-1]
                body = txt[len(m.group(0)):].strip()
                sym_notes[sym].append((nid, body))
                note_syms[nid].add(sym)
            for _ex, sym in MENTION.findall(txt):
                if not (m and sym == m.group(1).split(":")[-1]):
                    sym_notes[sym].append((nid, txt))
                    note_syms[nid].add(sym)

        vault_syms = [r["symbol"] for r in rows]
        vset = set(vault_syms)
        prose = {s: sum(len(b) for _i, b in sym_notes.get(s, [])) for s in vault_syms}
        n_rich = sum(1 for s in vault_syms if prose[s] >= 120)
        n_thin = sum(1 for s in vault_syms if 0 < prose[s] < 120)
        n_bare = sum(1 for s in vault_syms if prose[s] == 0)
        rep.kv(symbols_with_rich_prose_120ch=n_rich, symbols_thin_prose=n_thin,
               symbols_no_prose=n_bare)
        rep.log("  → bare-tag symbols cannot self-classify; they need T3/T4/T5 inheritance.")

        # ── B. anchor notes (ground truth, Khalid's words) ───────────────
        rep.section("B. doctrine anchor notes — full text")
        found = 0
        anchor_text = defaultdict(list)        # domain -> [text]
        for nid, (leg, dom) in ANCHORS.items():
            n = by_id.get(nid)
            if not n:
                rep.log(f"  MISSING {nid} ({leg}/{dom})")
                continue
            found += 1
            t = (n.get("text") or "").strip()
            anchor_text[dom].append(t)
            rep.log(f"  [{dom}/{leg}] {nid}\n      {t[:420]}")
        checks.append(("majority of anchor notes resolve", found >= len(ANCHORS) * 0.6))
        rep.kv(anchors_found=found, anchors_expected=len(ANCHORS))

        # ── C. mine the lexicon FROM his anchor notes (log-odds) ─────────
        rep.section("C. lexicon mined from Khalid's own notes (log-odds vs corpus)")
        corpus = Counter()
        for n in notes:
            corpus.update(set(toks(n.get("text"))))
        N = max(1, len(notes))
        lex = {}
        for dom, texts in anchor_text.items():
            c = Counter()
            for t in texts:
                c.update(set(toks(t)))
            scored = []
            for w, k in c.items():
                if len(w) < 4:
                    continue
                base = corpus.get(w, 0) / N
                lift = (k / max(1, len(texts))) / max(base, 1.0 / N)
                if lift > 3:
                    scored.append((round(lift, 1), w, k))
            scored.sort(reverse=True)
            lex[dom] = [w for _l, w, _k in scored[:40]]
            rep.log(f"  {dom}: " + ", ".join(f"{w}({l})" for l, w, _k in scored[:22]))
        checks.append(("lexicon mined for all 3 domains",
                       all(len(lex.get(d, [])) >= 5 for d in ("MACRO", "LIQUIDITY", "RISK"))))

        # ── D. dry-run the 5-tier ladder ─────────────────────────────────
        rep.section("D. classification ladder dry-run")
        dom_of = {}
        tier_of = {}
        evid = {}

        # T1: symbol named in an anchor note
        for nid, (_leg, dom) in ANCHORS.items():
            for s in note_syms.get(nid, ()):
                if s in vset and s not in dom_of:
                    dom_of[s], tier_of[s] = dom, "T1"
                    evid[s] = f"anchor {nid}"
        t1 = sum(1 for s in dom_of if tier_of[s] == "T1")

        # T2: score the symbol's own notes against the mined lexicon
        for s in vault_syms:
            if s in dom_of or prose[s] == 0:
                continue
            body = " ".join(b for _i, b in sym_notes[s])
            tk = set(toks(body))
            sc = {d: len(tk & set(ws)) for d, ws in lex.items()}
            top = sorted(sc.items(), key=lambda kv: -kv[1])
            if top and top[0][1] >= 2 and top[0][1] > (top[1][1] if len(top) > 1 else 0):
                dom_of[s], tier_of[s] = top[0][0], "T2"
                hits = sorted(tk & set(lex[top[0][0]]))[:6]
                evid[s] = f"own notes {[i for i, _b in sym_notes[s]][:2]} terms={hits}"
        t2 = sum(1 for s in dom_of if tier_of[s] == "T2")

        # T3: co-occurrence with already-classified symbols in the same note
        for s in vault_syms:
            if s in dom_of:
                continue
            votes = Counter()
            for nid, _b in sym_notes.get(s, ()):
                for peer in note_syms.get(nid, ()):
                    if peer != s and peer in dom_of and tier_of[peer] in ("T1", "T2"):
                        votes[dom_of[peer]] += 1
            if votes:
                d, k = votes.most_common(1)[0]
                dom_of[s], tier_of[s] = d, "T3"
                evid[s] = f"co-occurs with {k} {d} symbols"
        t3 = sum(1 for s in dom_of if tier_of[s] == "T3")

        # T4: family inheritance (alpha prefix / numeric-suffix families)
        fam = defaultdict(list)
        for s in vault_syms:
            f = re.sub(r"[0-9]+.*$", "", s) or s
            fam[f].append(s)
        for f, members in fam.items():
            known = Counter(dom_of[m] for m in members if m in dom_of)
            if not known:
                continue
            d = known.most_common(1)[0][0]
            for m in members:
                if m not in dom_of:
                    dom_of[m], tier_of[m] = d, "T4"
                    evid[m] = f"family '{f}' majority {d} (n={sum(known.values())})"
        t4 = sum(1 for s in dom_of if tier_of[s] == "T4")

        # T5: learned category prior (majority domain of T1-T4 in same vault category)
        cat_of = {r["symbol"]: r.get("category") for r in rows}
        catvote = defaultdict(Counter)
        for s, d in dom_of.items():
            catvote[cat_of.get(s)][d] += 1
        cat_prior = {c: v.most_common(1)[0][0] for c, v in catvote.items() if v}
        rep.log(f"  learned category priors: {json.dumps(cat_prior, sort_keys=True)}")
        for s in vault_syms:
            if s not in dom_of:
                c = cat_of.get(s)
                if c in cat_prior:
                    dom_of[s], tier_of[s] = cat_prior[c], "T5"
                    evid[s] = f"category '{c}' learned prior {cat_prior[c]} ({dict(catvote[c])})"
        t5 = sum(1 for s in dom_of if tier_of[s] == "T5")

        unresolved = [s for s in vault_syms if s not in dom_of]
        counts = Counter(dom_of.values())
        rep.kv(T1_anchor=t1, T2_own_notes=t2, T3_cooccurrence=t3,
               T4_family=t4, T5_category_prior=t5,
               classified=len(dom_of), total=len(vault_syms),
               unresolved=len(unresolved))
        rep.kv(MACRO=counts.get("MACRO", 0), LIQUIDITY=counts.get("LIQUIDITY", 0),
               RISK=counts.get("RISK", 0))
        if unresolved:
            rep.log(f"  UNRESOLVED sample: {unresolved[:25]}")

        rep.section("E. worked examples (evidence per symbol)")
        for s in ["RRPONTSYD", "SOFR", "JPLG", "MOVE", "VIX", "DXY", "USDX", "US10Y",
                  "US02Y", "TEDRATE", "XAUUSD", "CL1!", "SPX", "USM2", "USCLI",
                  "USIRYY", "BDI", "HG1!", "USNFP", "USFER"]:
            if s in dom_of:
                rep.log(f"  {s:10s} → {dom_of[s]:9s} [{tier_of[s]}] {evid.get(s, '')[:150]}")
            elif s in vset:
                rep.log(f"  {s:10s} → UNRESOLVED")

        # coverage of the LIVE (valued) subset matters most for barometers
        live = [r["symbol"] for r in rows if r.get("status") == "LIVE"]
        live_cls = sum(1 for s in live if s in dom_of)
        rep.kv(live_symbols=len(live), live_classified=live_cls)
        checks.append(("100% of vault symbols classified", not unresolved))
        checks.append(("all three domains populated",
                       all(counts.get(d, 0) > 0 for d in ("MACRO", "LIQUIDITY", "RISK"))))

        failed = [l for l, ok in checks if not ok]
        for l, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — ladder reaches {len(dom_of)}/{len(vault_syms)} symbols "
               f"(T1 {t1} / T2 {t2} / T3 {t3} / T4 {t4} / T5 {t5}); "
               f"MACRO {counts.get('MACRO')} LIQUIDITY {counts.get('LIQUIDITY')} "
               f"RISK {counts.get('RISK')}")


if __name__ == "__main__":
    main()
