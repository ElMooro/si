"""
justhodl-best-ideas  the platform's master house view.

The platform runs a dozen independent opportunity engines  bagger DNA,
earnings drift, estimate revisions, price breakouts, deep value,
fundamental quality, buyback cannibals, insider clusters, sector catch-up.
Each is a single lens. This engine asks the question that actually matters
to a retail investor: which names does the WHOLE research stack agree on?

The institutional idea is multi-factor confluence. Two engines in the same
family (two earnings engines, say) agreeing is barely independent
confirmation. A name confirmed across ORTHOGONAL families  growth AND
value AND capital-return AND insider buying  is genuinely rare, and that
agreement is what professional allocators pay up for. So the score is
driven first by how many independent FACTOR FAMILIES confirm a name,
then by raw engine count, then by how strongly each engine flags it.

This is NOT the Boom Board. The Boom Board hunts growth/breakout names
that could run hard. The Best Ideas board is style-agnostic: a boring,
cheap, cash-generative compounder with insider buying can top this list
and never appear on the Boom Board. It is the platform's single ranked
answer to "what should I actually look at right now."

INPUT   the S3 JSON output of 12 opportunity engines + screener/data.json
OUTPUT  data/conviction-stack.json        SCHEDULE daily 14:45 UTC
Pure synthesis  no external API. Real data only. Research, not advice.
"""
import json
import time
from collections import defaultdict
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/best-ideas.json"

# id, human label, S3 key, list path, symbol field, score field, family,
# the plain-English phrase, optional cap on how many top-by-score to keep
SPECS = [
    ("bagger", "Bagger Engine", "data/bagger-engine.json", ["top_100"],
     "symbol", "bagger_score", "GROWTH",
     "carries multibagger growth DNA", None),
    ("revaccel", "Revenue Acceleration", "data/revenue-acceleration.json",
     ["all_qualifying"], "symbol", "score", "GROWTH",
     "revenue growth is accelerating quarter over quarter", None),
    ("earnpead", "Earnings PEAD", "data/earnings-pead.json",
     ["all_qualifying"], "symbol", "score", "EARNINGS",
     "has a serial earnings-beat streak", 70),
    ("peadsig", "PEAD Signals", "data/pead-signals.json",
     ["all_qualifying"], "symbol", "score", "EARNINGS",
     "post-earnings drift is running in its favour", 70),
    ("epsrev", "EPS Revision Velocity", "data/eps-revision-velocity.json",
     ["all_qualifying"], "symbol", "score", "EARNINGS",
     "analysts are revising earnings estimates upward", 80),
    ("momentum", "Momentum Breakout", "data/momentum-breakout.json",
     ["all_qualifying"], "symbol", "score", "MOMENTUM",
     "is in a confirmed technical price breakout", 70),
    ("deepvalue", "Deep Value", "data/deep-value.json",
     ["all_qualifying"], "symbol", "score", "VALUE",
     "trades at a deep-value discount to intrinsic worth", None),
    ("asym", "Asymmetric Scorer", "data/asymmetric-scorer.json",
     ["top_setups"], "symbol", "composite_score", "VALUE",
     "screens as an asymmetric risk/reward setup", None),
    ("quality", "Fundamentals X-Ray", "data/fundamentals.json",
     ["companies"], "ticker", None, "QUALITY",
     "has pristine fundamentals (DCF, Altman-Z, Piotroski)", None),
    ("cannibal", "Capital-Return Cannibal", "data/capital-return.json",
     ["cannibals"], "symbol", "cannibal_score", "CAPITAL",
     "management is shrinking the share count with FCF-funded buybacks",
     None),
    ("insider", "Insider Cluster", "data/insider-aggregate.json",
     ["notable_cluster_buys"], "symbol", None, "SMART_MONEY",
     "a cluster of company insiders are buying their own stock", None),
    ("catchup", "Beta-Laggard Catch-Up", "data/beta-laggards.json",
     ["top_candidates"], "symbol", "catch_up_score", "ROTATION",
     "is set up to catch up to leaders in a working sector", None),
]

FAMILY_LABEL = {
    "GROWTH": "Growth", "EARNINGS": "Earnings momentum",
    "MOMENTUM": "Price momentum", "VALUE": "Value",
    "QUALITY": "Fundamental quality", "CAPITAL": "Capital allocation",
    "SMART_MONEY": "Insider / smart money", "ROTATION": "Sector rotation",
}


def num(v):
    try:
        f = float(v)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def dig(obj, path):
    cur = obj
    for p in path:
        if isinstance(cur, dict):
            cur = cur.get(p)
        else:
            return None
    return cur


def harvest(spec):
    """Return {SYMBOL: strength 0-1} for one engine."""
    eid, label, key, path, symf, scoref, fam, phrase, cap = spec
    try:
        obj = json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception as e:
        return {}, f"unreadable: {str(e)[:80]}"
    lst = dig(obj, path)
    if not isinstance(lst, list) or not lst:
        return {}, "empty"

    rows = []
    for it in lst:
        if not isinstance(it, dict):
            continue
        sym = (it.get(symf) or it.get("symbol") or it.get("ticker") or "")
        sym = str(sym).upper().strip()
        if not sym:
            continue
        sc = num(it.get(scoref)) if scoref else None
        if sc is None:
            # score-less engines: derive a strength proxy
            sc = (num(it.get("piotroski")) or num(it.get("n_buyers"))
                  or num(it.get("score")) or 1.0)
        rows.append((sym, sc))

    rows.sort(key=lambda x: x[1], reverse=True)
    if cap:
        rows = rows[:cap]
    if not rows:
        return {}, "empty"
    # normalise to 0-1 by rank within this engine (cross-engine comparable)
    n = len(rows)
    out = {}
    for i, (sym, _) in enumerate(rows):
        out[sym] = round(1.0 - (i / n) * 0.7, 4)  # 1.0 .. 0.3
    return out, f"{n} qualified"


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    # screener reference data (price / target / sector / cap / name)
    ref = {}
    try:
        sc = json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key="screener/data.json")["Body"].read())
        rows = sc.get("stocks")
        if not isinstance(rows, list):
            bs = sc.get("by_symbol") or {}
            rows = list(bs.values()) if isinstance(bs, dict) else []
        for r in rows:
            if isinstance(r, dict) and r.get("symbol"):
                sy = r["symbol"].upper()
                ref[sy] = {
                    "name": r.get("name") or sy,
                    "sector": r.get("sector"),
                    "market_cap": num(r.get("marketCap")),
                    "price": num(r.get("price")),
                    "target": num(r.get("priceTargetMedian"))
                    or num(r.get("priceTargetMean")),
                }
    except Exception as e:
        print(f"[conviction] screener ref read failed: {e}")

    # harvest every engine
    flags = defaultdict(list)   # SYMBOL -> [(eid,label,fam,phrase,strength)]
    coverage = {}
    for spec in SPECS:
        eid, label, key, path, symf, scoref, fam, phrase, cap = spec
        got, status = harvest(spec)
        coverage[eid] = {"label": label, "family": fam, "status": status,
                         "n": len(got)}
        for sym, strength in got.items():
            flags[sym].append((eid, label, fam, phrase, strength))

    # build the stack  one entry per symbol confirmed by >= 2 families
    stack = []
    for sym, hits in flags.items():
        fams = sorted({h[2] for h in hits})
        if len(fams) < 2:
            continue
        engines_hit = len(hits)
        avg_strength = sum(h[4] for h in hits) / engines_hit
        fam_c = min(len(fams), 5) / 5.0
        eng_c = min(engines_hit, 7) / 7.0
        score = round(100 * (0.55 * fam_c + 0.25 * eng_c
                             + 0.20 * avg_strength), 1)

        tier = ("CONVICTION TITAN" if len(fams) >= 4
                else "HIGH CONVICTION" if len(fams) == 3
                else "CONFIRMED")

        r = ref.get(sym, {})
        price = r.get("price")
        target = r.get("target")
        upside = (round((target - price) / price * 100, 1)
                  if price and target and price > 0 else None)

        # signals, strongest family first
        sigs = sorted(hits, key=lambda h: h[4], reverse=True)
        signals = [{"engine": h[1], "family": h[2],
                    "family_label": FAMILY_LABEL.get(h[2], h[2]),
                    "note": h[3], "strength": round(h[4], 2)}
                   for h in sigs]

        # plain-English why  one representative phrase per family
        by_fam = {}
        for h in sigs:
            by_fam.setdefault(h[2], h[3])
        fam_names = [FAMILY_LABEL.get(f, f) for f in fams]
        clause = "; ".join(
            f"{FAMILY_LABEL.get(f, f).lower()} ({by_fam[f]})"
            for f in sorted(by_fam, key=lambda f: -max(
                h[4] for h in sigs if h[2] == f)))
        why = (
            f"{r.get('name', sym)} is confirmed by {engines_hit} "
            f"independent engine{'s' if engines_hit != 1 else ''} across "
            f"{len(fams)} orthogonal factor families "
            f"({', '.join(fam_names)}). In plain terms: {clause}. When "
            "signals this different all point the same way, the noise "
            "from any one of them cancels out  that agreement is the "
            "platform's highest-conviction read."
            + (f" Analyst target ${target:.2f} ({upside:+.0f}%)."
               if target and upside is not None else ""))

        stack.append({
            "symbol": sym, "name": r.get("name", sym),
            "sector": r.get("sector"), "market_cap": r.get("market_cap"),
            "price": price, "conviction_score": score,
            "conviction_tier": tier,
            "families_hit": len(fams), "engines_hit": engines_hit,
            "families": fam_names,
            "price_target": target, "upside_pct": upside,
            "signals": signals, "why": why,
            "risk": ("Multi-factor confluence cancels single-signal noise "
                     "but not market or macro risk. Crowded high-conviction "
                     "names can still de-rate together in a broad "
                     "drawdown  size positions accordingly."),
        })

    stack.sort(key=lambda x: (x["families_hit"], x["conviction_score"]),
               reverse=True)
    titans = [s for s in stack if s["conviction_tier"] == "CONVICTION TITAN"]
    high = [s for s in stack if s["conviction_tier"] == "HIGH CONVICTION"]

    out = {
        "schema_version": "1.0",
        "method": "cross_engine_factor_confluence",
        "generated_at": now.isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "ok": len(stack) >= 1,
        "headline": (
            f"{len(stack)} names carry multi-engine conviction  "
            f"{len(titans)} TITANS confirmed across 4+ factor families, "
            f"{len(high)} HIGH-CONVICTION across 3."),
        "how_to_read": (
            "Every name here is flagged by at least two of the platform's "
            "twelve opportunity engines, in two DIFFERENT factor families. "
            "More families = more independent confirmation = higher "
            "conviction. CONVICTION TITANS (4+ families) are the rarest "
            "and strongest reads. This list is style-agnostic: growth, "
            "value, quality and capital-return ideas all compete on the "
            "same board. It answers one question  what does the whole "
            "research stack agree on right now."),
        "n_total": len(stack),
        "n_titans": len(titans),
        "n_high_conviction": len(high),
        "stack": stack[:80],
        "titans": titans[:25],
        "high_conviction": high[:30],
        "family_legend": FAMILY_LABEL,
        "engine_coverage": coverage,
        "disclaimer": ("Confluence improves the signal-to-noise ratio; it "
                       "is not a guarantee. Research and education only  "
                       "not investment advice."),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=3600")
    print(f"[best-ideas] {len(stack)} names | {len(titans)} titans | "
          f"{len(high)} high | {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": out["ok"], "n_total": len(stack), "titans": len(titans),
        "high": len(high)})}
