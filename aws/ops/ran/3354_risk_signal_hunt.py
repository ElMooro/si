"""ops 3354 — chase down dispersed risk signals. THREE tasks:
  1. Mine data/brain.json for RISK-related operator notes (crisis/stress/tail/contagion/
     carry/sovereign/liquidity vocabulary) — extract the actual claims so we implement
     what's really in the brain, not assumptions.
  2. Diagnose why data/sovereign-stress.json is missing (NoSuchKey) — is the engine
     writing to a different key? Find its real output.
  3. Confirm the global-tide liquidity-impulse fields + range so we can build a proper
     signed→stress transform for the overlay.
Read-only audit.
"""
import json
import re
import boto3
from collections import Counter
from ops_report import report

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def gj(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode())
    except Exception as e:
        return {"__err__": f"{type(e).__name__}"}


RISK_VOCAB = [
    "risk", "crisis", "crash", "stress", "tail", "contagion", "carry", "unwind",
    "sovereign", "btp", "bund", "spread", "liquidity", "drawdown", "recession",
    "volatility", "vix", "hedge", "defensive", "credit", "default", "fragil",
    "systemic", "meltdown", "blowup", "deleverage", "margin", "funding", "repo",
]


with report("3354_risk_signal_hunt") as r:
    # ── 1. BRAIN RISK NOTES ──
    r.section("1. Brain notes — risk-related")
    brain = gj("data/brain.json")
    notes = (brain.get("notes") or []) if isinstance(brain, dict) else []
    r.log(f"brain total notes: {len(notes)}")
    if brain.get("__err__"):
        r.log(f"  brain.json: {brain['__err__']}")
    risk_notes = []
    cat_counter = Counter()
    for n in notes:
        txt = (n.get("text") or n.get("note") or "").lower()
        cat = n.get("cat") or n.get("category") or "?"
        if any(w in txt for w in RISK_VOCAB):
            risk_notes.append({"cat": cat, "text": (n.get("text") or n.get("note") or "")[:280]})
            cat_counter[cat] += 1
    r.log(f"RISK-related notes: {len(risk_notes)} (of {len(notes)})")
    r.log(f"by category: {dict(cat_counter)}")
    # which risk CONCEPTS appear, and how often — this tells us what to implement
    concept_hits = Counter()
    for rn in risk_notes:
        t = rn["text"].lower()
        for w in RISK_VOCAB:
            if w in t:
                concept_hits[w] += 1
    r.log(f"risk concept frequency: {dict(concept_hits.most_common())}")
    # dump a sample of the actual risk notes so we can read the operator's thinking
    r.log("── sample risk notes (first 25) ──")
    for rn in risk_notes[:25]:
        r.log(f"  [{rn['cat']}] {rn['text']}")

    # notes-themes clustering (macro/untagged notes by theme)
    r.section("1b. notes-themes clusters (macro)")
    themes = gj("data/notes-themes.json")
    if isinstance(themes, dict) and not themes.get("__err__"):
        tk = themes.get("themes") or themes.get("clusters") or themes
        if isinstance(tk, dict):
            for name, v in list(tk.items())[:20]:
                cnt = v.get("n") if isinstance(v, dict) else (len(v) if isinstance(v, list) else "?")
                r.log(f"  theme '{name}': {cnt}")
        elif isinstance(tk, list):
            for t in tk[:20]:
                r.log(f"  {t.get('theme') or t.get('name')}: {t.get('n') or t.get('count')}")
    else:
        r.log(f"  notes-themes: {themes.get('__err__', 'unexpected shape')}")

    # ── 2. SOVEREIGN-STRESS missing output ──
    r.section("2. sovereign-stress — find real output")
    for k in ["data/sovereign-stress.json", "data/sovereign_stress.json",
              "data/sovereign-fiscal.json", "data/sovereign.json"]:
        o = gj(k)
        if o.get("__err__"):
            r.log(f"  {k}: {o['__err__']}")
        else:
            keys = list(o.keys())[:12] if isinstance(o, dict) else type(o).__name__
            r.log(f"  ✓ {k}: {keys}")

    # ── 3. GLOBAL-TIDE transform ──
    r.section("3. global-tide — liquidity impulse fields + range")
    gt = gj("data/global-tide.json")
    if not gt.get("__err__"):
        gli = gt.get("gli") or {}
        r.log(f"  gli: {gli}")
        r.log(f"  risk block: {gt.get('risk')}")
        for src in ("fed", "ecb", "boj", "china"):
            b = gt.get(src) or {}
            if isinstance(b, dict):
                r.log(f"  {src}: injection_score={b.get('injection_score')} keys={list(b.keys())[:6]}")
    else:
        r.log(f"  global-tide: {gt['__err__']}")
