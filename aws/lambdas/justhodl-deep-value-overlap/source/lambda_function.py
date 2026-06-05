"""justhodl-deep-value-overlap — the "Deep Value + Catalyst Overlap" master board.

The audit's #2 (Massive impact, low effort): you have 80% of the inputs, they're
just not joined. This intersects them into ONE ranked board:

  CHEAP on multiple valuation lenses (EV/Sales, P/S, P/B, EV/EBITDA, FCF yield,
    dislocation cheapness)
  + CATALYST layer (insider cluster, capital-flow accumulation, short-squeeze,
    estimate revisions, sector flow tailwind, backlog accelerating, gov/FDA)
  + INFLECTION (Rule-of-40, margin, FCF, expected-growth turn)
  - RISK FILTER (Altman-Z distress, leverage)

Output: a ranked table where each name shows WHICH lenses + catalysts fired.

Reads (all existing): opportunities, dislocations, capital-flow, finra-short,
  insider-clusters, best-setups, backlog, catalyst-calendar.
OUTPUT: data/deep-value-overlap.json
SCHEDULE: daily 17:00 UTC (after the upstream engines refresh).
"""
import json, time
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/deep-value-overlap.json"
s3 = boto3.client("s3", region_name=REGION)


def read_json(key, default=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception: return default


def sf(v):
    try:
        f = float(v); return f if f == f else None
    except Exception: return None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    opp = read_json("data/opportunities.json") or {}
    disl = read_json("data/dislocations.json") or {}
    cf = read_json("data/capital-flow.json") or {}
    finra = read_json("data/finra-short.json") or {}
    insider = read_json("data/insider-clusters.json") or {}
    best = read_json("data/best-setups.json") or {}
    backlog = read_json("data/backlog.json") or {}
    catalysts = read_json("data/catalyst-calendar.json") or {}

    # Index everything by ticker
    opp_i = {r.get("ticker"): r for r in (opp.get("all") or [])}
    disl_i = {}
    for r in ([*(disl.get("buy_the_laggard") or []), *(disl.get("top_dislocations") or [])]):
        disl_i.setdefault(r.get("ticker"), r)
    cf_acc = {r.get("ticker"): r for r in (cf.get("accumulating") or [])}
    cf_dist = {r.get("ticker") for r in (cf.get("distributing") or [])}
    squeeze = {r.get("ticker"): r for r in (finra.get("squeeze_candidates") or [])}
    ins = {}
    for c in (insider.get("clusters") or insider.get("items") or insider.get("top_clusters") or []):
        ins[c.get("ticker")] = c
    bl = backlog.get("by_ticker") or {}
    setups_i = {s.get("ticker"): s for s in (best.get("top_setups") or [])}
    cat_tk = {}
    for e in (catalysts.get("events") or []):
        if e.get("ticker") and e.get("type") in ("FDA", "GOV_CONTRACT", "EARNINGS"):
            cat_tk.setdefault(e.get("ticker"), []).append(e.get("type"))

    # Candidate universe = anything appearing in opportunities (has full fundamentals)
    rows = []
    for tk, o in opp_i.items():
        gi = o.get("growth_intel") or {}
        # ── Valuation lenses (count how many say "cheap") ──
        lenses = []
        if (o.get("scores") or {}).get("value", 0) >= 60: lenses.append("value-score cheap")
        d = disl_i.get(tk)
        if d and (d.get("cheapness") or 0) >= 0.6: lenses.append("EV/Sales cheap vs cohort")
        if gi.get("pe_vs_industry_pct") is not None and gi["pe_vs_industry_pct"] < -15: lenses.append("P/E < industry")
        if gi.get("peg_forward") is not None and gi["peg_forward"] < 1.0: lenses.append("PEG < 1")
        if o.get("fcf_yield") is not None and sf(o.get("fcf_yield")) and sf(o["fcf_yield"]) > 5: lenses.append("FCF yield > 5%")
        n_cheap = len(lenses)

        # ── Catalyst layer ──
        catalysts_hit = []
        if tk in ins: catalysts_hit.append("insider cluster")
        if tk in cf_acc: catalysts_hit.append("institutions accumulating")
        if tk in squeeze: catalysts_hit.append("short-squeeze setup")
        rev = (o.get("estimate_revision") or {}).get("direction")
        if rev == "UP": catalysts_hit.append("estimates revised up")
        if d and d.get("cheap_and_inflecting"): catalysts_hit.append("cheap & inflecting")
        b = bl.get(tk)
        if b and (b.get("demand_accelerating") or b.get("deferred_accelerating")): catalysts_hit.append("backlog accelerating")
        for ct in cat_tk.get(tk, []):
            catalysts_hit.append({"FDA": "FDA catalyst", "GOV_CONTRACT": "gov contract", "EARNINGS": "earnings soon"}.get(ct, ct))
        n_cat = len(catalysts_hit)

        # ── Inflection ──
        infl = []
        if o.get("compounder_score") and o["compounder_score"] >= 70: infl.append("durable compounder")
        if gi.get("rule_of_40") is not None and gi["rule_of_40"] >= 40: infl.append("Rule-of-40+")
        if gi.get("expected_company_growth_pct") and gi["expected_company_growth_pct"] > 15: infl.append("growth accelerating")
        n_infl = len(infl)

        # ── Risk filter ──
        altman = sf(o.get("altman_z") or o.get("altmanZ"))
        de = sf(o.get("debt_to_equity") or o.get("debtToEquity"))
        distress = (altman is not None and altman < 1.8) or (de is not None and de > 3)
        if tk in cf_dist:
            distress = True  # institutions actively selling = avoid

        # ── Need cheap + at least one catalyst; skip pure-rich or no-catalyst ──
        if n_cheap < 1 or n_cat < 1:
            continue

        # Score: cheapness × catalyst breadth × inflection, risk-gated
        score = (n_cheap * 14 + n_cat * 12 + n_infl * 10)
        if distress:
            score *= 0.55
        score = round(min(100, score), 1)

        rows.append({
            "ticker": tk, "name": o.get("company") or o.get("name"),
            "sector": o.get("sector"), "cap_bucket": o.get("cap_bucket"),
            "overlap_score": score,
            "n_value_lenses": n_cheap, "value_lenses": lenses,
            "n_catalysts": n_cat, "catalysts": catalysts_hit,
            "n_inflection": n_infl, "inflection": infl,
            "distress_flag": distress,
            "altman_z": altman,
            "verdict": o.get("verdict"),
            "conviction": (setups_i.get(tk) or {}).get("conviction"),
            "ev_sales": (d or {}).get("ev_sales"),
            "expected_growth_pct": gi.get("expected_company_growth_pct"),
        })

    rows.sort(key=lambda r: -r["overlap_score"])
    # the prize: cheap on >=2 lenses + >=2 catalysts + inflection + safe
    prime = [r for r in rows if r["n_value_lenses"] >= 2 and r["n_catalysts"] >= 2
             and r["n_inflection"] >= 1 and not r["distress_flag"]][:30]

    out = {
        "engine": "deep-value-overlap", "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1),
        "n_scored": len(rows),
        "prime_setups": prime,
        "board": rows[:120],
        "method": ("Joins value lenses (value score, EV/Sales dislocation, P/E vs "
                   "industry, forward PEG, FCF yield) with catalysts (insider, "
                   "capital flow, short-squeeze, estimate revisions, backlog "
                   "acceleration, FDA/gov/earnings) and inflection (compounder, "
                   "Rule-of-40, growth), risk-gated by Altman-Z & leverage. "
                   "Score = cheapness x catalyst breadth x inflection."),
        "legend": {"prime_setups": "cheap on >=2 lenses + >=2 catalysts + inflection + not distressed"},
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[overlap] DONE {round(time.time()-t0,1)}s — {len(rows)} scored, {len(prime)} prime")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "scored": len(rows),
                                                     "prime": len(prime),
                                                     "top": [r["ticker"] for r in prime[:5]]})}
