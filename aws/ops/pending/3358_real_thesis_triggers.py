"""ops 3358 — refined thesis-note mining. The first pass was polluted by ~1,559 identical
synthetic seed notes ("Real macro note number N: when the dollar strengthens..."). Filter
those out and surface the operator's GENUINE risk theses with concrete tradeable content:
real tickers, real price levels, bps spreads, %-thresholds, or named indicators. Read-only.
"""
import json
import re
from collections import Counter
import boto3
from ops_report import report

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def gj(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode())
    except Exception as e:
        return {"__err__": type(e).__name__}


# synthetic seed patterns to EXCLUDE
SEED = re.compile(r"real macro note number\s*\d+|^\s*test note|lorem ipsum|placeholder", re.I)
RISK = ["risk", "crisis", "stress", "tail", "contagion", "carry", "unwind", "sovereign",
        "spread", "liquidity", "drawdown", "recession", "vix", "hedge", "credit",
        "repo", "rrp", "funding", "hyg", "lqd", "crash", "sell-off", "selloff", "defensive",
        "yield", "curve", "inversion", "dxy", "move", "cds"]
# concrete tradeable markers
TICKER = re.compile(r"\b([A-Z]{2,5})\b|\$[A-Z]{1,5}|TV:[A-Z:]+|AMEX:|NASDAQ:|CBOE:")
LEVEL = re.compile(r"\$?\d[\d,]*\.?\d*\s*(%|bps?|bp\b)|above \$?\d|below \$?\d|"
                   r"cross(es|ed)?\s+\d|breaks?\s+\$?\d|\d+\s*(day|week|month|dma|ma)\b", re.I)


with report("3358_real_thesis_triggers") as r:
    brain = gj("data/brain.json")
    notes = (brain.get("notes") or []) if isinstance(brain, dict) else []
    r.log(f"brain total: {len(notes)}")

    # dedup by normalized text, drop seeds
    seen = set()
    real_risk = []
    seed_count = 0
    for n in notes:
        cat = (n.get("cat") or n.get("category") or "").lower()
        txt = (n.get("text") or n.get("note") or "").strip()
        if not txt:
            continue
        if SEED.search(txt):
            seed_count += 1
            continue
        low = txt.lower()
        if not any(w in low for w in RISK):
            continue
        norm = re.sub(r"\s+", " ", low)[:120]
        if norm in seen:
            continue
        seen.add(norm)
        has_level = bool(LEVEL.search(txt))
        has_ticker = bool(re.search(r"\$[A-Z]{1,5}|TV:|AMEX:|NASDAQ:|CBOE:|\b[A-Z]{3,5}\b", txt))
        real_risk.append({"cat": cat, "level": has_level, "ticker": has_ticker,
                          "text": txt[:400]})

    r.log(f"synthetic seed notes excluded: {seed_count}")
    r.log(f"unique real risk notes: {len(real_risk)}")
    with_level = [n for n in real_risk if n["level"]]
    r.log(f"  → with concrete LEVELS/thresholds: {len(with_level)}")
    r.log(f"  → thesis-category: {sum(1 for n in real_risk if n['cat']=='thesis')}")

    r.section("Real risk theses WITH concrete tradeable levels")
    for n in with_level[:35]:
        tag = f"[{n['cat']}]"
        r.log(f"  {tag} {n['text']}")

    r.section("Other substantive risk theses (no explicit level, deduped)")
    no_level = [n for n in real_risk if not n["level"]]
    for n in no_level[:15]:
        r.log(f"  [{n['cat']}] {n['text'][:220]}")
