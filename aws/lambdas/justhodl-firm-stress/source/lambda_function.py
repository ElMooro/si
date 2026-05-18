"""
justhodl-firm-stress -- the firm Stress Desk.

Every institutional trading desk runs a scenario book: a battery of named
shocks (historical replays + hypothetical macro moves) re-priced through the
firm's *actual* positions, plus a reverse stress test that asks "what move
would it take to lose X%". This engine is that desk.

It does NOT invent per-name assumptions. It re-uses the firm's own factor
model: the Factor Risk Model (justhodl-factor-risk) already regresses every
name onto six tradable factors and caches the loadings. The Stress Desk reads
that cache, applies each scenario as a six-factor shock vector, and propagates
it through the loadings to a per-name shocked return -- exactly how a
hedge-fund risk desk re-prices a book.

PIPELINE
  data/firm-book.json             -- the 266-name consolidated book (net_pct,
                                     desk attribution, sector)
  data/factor-loadings-cache.json -- per-name six-factor betas (Factor Risk
                                     Model's warm cache)
  data/factor-risk.json           -- firm vol / VaR context (optional)
            |
            v
  for each scenario S (15 named shocks):
      per name i:  shocked_return_i = sum_f  beta[i][f] * shock_S[f]
      firm P&L %   = sum_i  net_pct_i * shocked_return_i
      attribute the loss across desks, sectors, and worst names
            |
            v
  reverse stress: scale a unit risk-off vector until the firm loss hits
                  -15% and -25%; report the implied factor move
            |
            v
  data/firm-stress.json

Names missing from the loadings cache fall back to a sector-average loading,
then a book-average loading -- so the desk produces a full-book number from
day one and sharpens as the Factor Risk Model's cache warms.

Schedule: cron(0 3 * * ? *) -- 03:00 UTC, after the Factor Risk Model (02:30)
so the loadings cache is fresh.
"""
import json
import time
from datetime import datetime, timezone

import boto3

S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/firm-stress.json"
FIRM_KEY = "data/firm-book.json"
CACHE_KEY = "data/factor-loadings-cache.json"
FACTOR_RISK_KEY = "data/factor-risk.json"
SCHEMA = "1.0"

FACTOR_NAMES = ["MKT", "SIZE", "VALUE", "MOM", "QUALITY", "LOWVOL"]

# firm loss limits the Stress Desk polices the worst scenario against
LOSS_LIMIT_SOFT = -12.0     # AMBER at or below
LOSS_LIMIT_HARD = -20.0     # RED at or below

# ---------------------------------------------------------------------------
# Scenario battery. Each shock is a CUMULATIVE episode move in the same
# six-factor spread space the Factor Risk Model regresses on (MKT = total SPY
# return; the five style factors are long/short spread returns). These are
# stylised historical analogues a desk recalibrates -- they are well grounded
# in the published factor record of each episode.
# ---------------------------------------------------------------------------
SCENARIOS = [
    # ---- historical replays ----
    {"name": "2008 Global Financial Crisis (Sep-Nov 2008)", "type": "historical",
     "shock": {"MKT": -0.30, "SIZE": -0.08, "VALUE": -0.05,
               "MOM": 0.05, "QUALITY": 0.06, "LOWVOL": 0.08}},
    {"name": "2020 COVID Crash (Feb-Mar 2020)", "type": "historical",
     "shock": {"MKT": -0.34, "SIZE": -0.12, "VALUE": -0.10,
               "MOM": 0.04, "QUALITY": 0.05, "LOWVOL": 0.07}},
    {"name": "2022 Rate Shock (H1 2022)", "type": "historical",
     "shock": {"MKT": -0.20, "SIZE": -0.05, "VALUE": 0.08,
               "MOM": -0.06, "QUALITY": 0.02, "LOWVOL": 0.04}},
    {"name": "Q4 2018 Selloff", "type": "historical",
     "shock": {"MKT": -0.14, "SIZE": -0.06, "VALUE": -0.03,
               "MOM": -0.04, "QUALITY": 0.03, "LOWVOL": 0.05}},
    {"name": "Aug 2007 Quant / Momentum Crash", "type": "historical",
     "shock": {"MKT": -0.02, "SIZE": -0.01, "VALUE": 0.04,
               "MOM": -0.12, "QUALITY": -0.03, "LOWVOL": 0.01}},
    {"name": "Feb 2018 Volmageddon", "type": "historical",
     "shock": {"MKT": -0.09, "SIZE": -0.03, "VALUE": -0.01,
               "MOM": -0.02, "QUALITY": 0.02, "LOWVOL": 0.03}},
    {"name": "Mar 2023 Regional Bank Crisis", "type": "historical",
     "shock": {"MKT": -0.05, "SIZE": -0.09, "VALUE": -0.07,
               "MOM": 0.02, "QUALITY": 0.05, "LOWVOL": 0.04}},
    {"name": "Aug 2015 China Devaluation", "type": "historical",
     "shock": {"MKT": -0.11, "SIZE": -0.05, "VALUE": -0.03,
               "MOM": -0.02, "QUALITY": 0.03, "LOWVOL": 0.04}},
    {"name": "2013 Taper Tantrum", "type": "historical",
     "shock": {"MKT": -0.06, "SIZE": -0.03, "VALUE": 0.02,
               "MOM": -0.03, "QUALITY": 0.01, "LOWVOL": -0.02}},
    # ---- hypothetical macro shocks ----
    {"name": "Equity -20% Broad Selloff", "type": "hypothetical",
     "shock": {"MKT": -0.20, "SIZE": -0.06, "VALUE": -0.02,
               "MOM": 0.00, "QUALITY": 0.04, "LOWVOL": 0.05}},
    {"name": "Rates +150bp Shock", "type": "hypothetical",
     "shock": {"MKT": -0.08, "SIZE": -0.04, "VALUE": 0.07,
               "MOM": -0.05, "QUALITY": 0.00, "LOWVOL": -0.03}},
    {"name": "Stagflation Scenario", "type": "hypothetical",
     "shock": {"MKT": -0.15, "SIZE": -0.06, "VALUE": 0.05,
               "MOM": -0.04, "QUALITY": 0.02, "LOWVOL": 0.00}},
    {"name": "Momentum Unwind / Crowding Break", "type": "hypothetical",
     "shock": {"MKT": -0.04, "SIZE": 0.02, "VALUE": 0.06,
               "MOM": -0.15, "QUALITY": -0.04, "LOWVOL": 0.01}},
    {"name": "Risk-Off Flight to Quality", "type": "hypothetical",
     "shock": {"MKT": -0.12, "SIZE": -0.05, "VALUE": -0.03,
               "MOM": 0.03, "QUALITY": 0.06, "LOWVOL": 0.06}},
    {"name": "Soft-Landing Melt-Up (positive control)", "type": "control",
     "shock": {"MKT": 0.12, "SIZE": 0.05, "VALUE": 0.02,
               "MOM": 0.04, "QUALITY": -0.02, "LOWVOL": -0.03}},
]

# unit vectors used for the reverse stress test (a -10% risk-off tape and a
# +10% melt-up tape) -- the desk scales whichever one the book loses on.
UNIT_RISK_OFF = {"MKT": -0.10, "SIZE": -0.04, "VALUE": -0.02,
                 "MOM": 0.02, "QUALITY": 0.03, "LOWVOL": 0.04}
UNIT_MELT_UP = {"MKT": 0.10, "SIZE": 0.04, "VALUE": 0.02,
                "MOM": -0.02, "QUALITY": -0.03, "LOWVOL": -0.04}

s3 = boto3.client("s3", region_name="us-east-1")


# ---- small utilities ------------------------------------------------------
def get_json(key):
    try:
        raw = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        return json.loads(raw)
    except Exception:
        return None


def put_json(key, obj):
    s3.put_object(Bucket=S3_BUCKET, Key=key,
                  Body=json.dumps(obj, default=str).encode("utf-8"),
                  ContentType="application/json", CacheControl="no-cache")


def shocked_return(betas, shock):
    """Predicted total return of a name under a six-factor shock vector."""
    return sum(betas.get(f, 0.0) * shock.get(f, 0.0) for f in FACTOR_NAMES)


# ---------------------------------------------------------------------------
def lambda_handler(event, context):
    t0 = time.time()

    firm = get_json(FIRM_KEY)
    if not firm or not firm.get("equity_book"):
        return {"statusCode": 200,
                "body": json.dumps({"ok": False, "error": "no firm-book"})}
    book = firm["equity_book"]
    fb_asof = firm.get("generated_at", "")

    cache = get_json(CACHE_KEY) or {}
    loadings = cache.get("loadings", {}) if isinstance(cache, dict) else {}

    frisk = get_json(FACTOR_RISK_KEY) or {}
    frisk_firm = frisk.get("firm", {}) if isinstance(frisk, dict) else {}

    # ---- 1) sector-average and book-average fallback loadings -------------
    sector_acc, book_acc = {}, {f: 0.0 for f in FACTOR_NAMES}
    n_book_load = 0
    for sym, e in loadings.items():
        b = e.get("betas") if isinstance(e, dict) else None
        if not b:
            continue
        n_book_load += 1
        for f in FACTOR_NAMES:
            book_acc[f] += b.get(f, 0.0)
    book_avg = ({f: book_acc[f] / n_book_load for f in FACTOR_NAMES}
                if n_book_load else
                {f: (1.0 if f == "MKT" else 0.0) for f in FACTOR_NAMES})

    # sector averages are computed from names that DO have direct loadings
    sym_sector = {}
    for r in book:
        sym_sector[r["symbol"]] = r.get("sector") or "Unknown"
    for sym, e in loadings.items():
        b = e.get("betas") if isinstance(e, dict) else None
        if not b:
            continue
        sec = sym_sector.get(sym, "Unknown")
        a = sector_acc.setdefault(sec, {"sum": {f: 0.0 for f in FACTOR_NAMES},
                                        "n": 0})
        for f in FACTOR_NAMES:
            a["sum"][f] += b.get(f, 0.0)
        a["n"] += 1
    sector_avg = {sec: {f: a["sum"][f] / a["n"] for f in FACTOR_NAMES}
                  for sec, a in sector_acc.items() if a["n"] > 0}

    # ---- 2) resolve a loading vector for every name in the book -----------
    names = []          # [{symbol,name,sector,net_pct,desks,betas,src}]
    n_direct = n_proxy = n_bookfallback = 0
    for r in book:
        sym = r["symbol"]
        net = float(r.get("net_pct") or 0.0)
        if net == 0.0:
            continue
        sec = r.get("sector") or "Unknown"
        e = loadings.get(sym)
        betas = e.get("betas") if isinstance(e, dict) else None
        if betas:
            src = "direct"
            n_direct += 1
        elif sec in sector_avg:
            betas = sector_avg[sec]
            src = "sector"
            n_proxy += 1
        else:
            betas = book_avg
            src = "book"
            n_bookfallback += 1
        names.append({
            "symbol": sym, "name": r.get("name") or sym, "sector": sec,
            "net_pct": net, "side": r.get("side") or "",
            "desks": r.get("desks") or {}, "betas": betas, "src": src,
        })

    # ---- 3) re-price the book through every scenario ----------------------
    scen_rows = []
    for sc in SCENARIOS:
        shock = sc["shock"]
        firm_pnl = 0.0
        desk_pnl, sector_pnl = {}, {}
        contribs = []
        for nm in names:
            ret = shocked_return(nm["betas"], shock)        # fractional
            # firm P&L in % of book = net_pct * fractional return
            pnl = nm["net_pct"] * ret
            firm_pnl += pnl
            sector_pnl[nm["sector"]] = sector_pnl.get(nm["sector"], 0.0) + pnl
            # split the name's P&L across its desks by weight share
            dks = nm["desks"]
            tot_w = sum(abs(float(v)) for v in dks.values()) if dks else 0.0
            if tot_w > 0:
                for dname, w in dks.items():
                    share = abs(float(w)) / tot_w
                    desk_pnl[dname] = desk_pnl.get(dname, 0.0) + pnl * share
            else:
                desk_pnl["Unattributed"] = (
                    desk_pnl.get("Unattributed", 0.0) + pnl)
            contribs.append({
                "symbol": nm["symbol"], "name": nm["name"],
                "sector": nm["sector"], "side": nm["side"],
                "net_pct": round(nm["net_pct"], 3),
                "ret_pct": round(ret * 100.0, 2),
                "pnl_pct": round(pnl, 3),
            })
        contribs.sort(key=lambda x: x["pnl_pct"])
        desk_rows = sorted(
            [{"desk": d, "pnl_pct": round(v, 3)} for d, v in desk_pnl.items()],
            key=lambda x: x["pnl_pct"])
        sector_rows = sorted(
            [{"sector": s, "pnl_pct": round(v, 3)}
             for s, v in sector_pnl.items()],
            key=lambda x: x["pnl_pct"])
        scen_rows.append({
            "scenario": sc["name"], "type": sc["type"], "shock": shock,
            "book_pnl_pct": round(firm_pnl, 2),
            "desk_pnl": desk_rows,
            "sector_pnl": sector_rows,
            "top_losers": contribs[:8],
            "top_gainers": list(reversed(contribs[-8:])),
        })
    scen_rows.sort(key=lambda x: x["book_pnl_pct"])

    # ---- 4) reverse stress test -------------------------------------------
    def firm_pnl_for(shock):
        return sum(nm["net_pct"] * shocked_return(nm["betas"], shock)
                   for nm in names)

    pnl_off = firm_pnl_for(UNIT_RISK_OFF)
    pnl_up = firm_pnl_for(UNIT_MELT_UP)
    # pick the unit vector the book actually loses money on
    if pnl_off <= pnl_up:
        unit_vec, unit_pnl, unit_label = UNIT_RISK_OFF, pnl_off, "risk-off (-10% tape)"
    else:
        unit_vec, unit_pnl, unit_label = UNIT_MELT_UP, pnl_up, "melt-up (+10% tape)"

    def reverse_target(target_loss):
        # firm P&L scales linearly with the shock multiplier
        if unit_pnl >= -1e-9:
            return {"reachable": False,
                    "note": ("the book does not lose money on the %s axis -- "
                             "it is hedged against this direction" % unit_label)}
        k = target_loss / unit_pnl     # unit_pnl < 0, target_loss < 0 -> k > 0
        implied = {f: round(unit_vec[f] * k, 4) for f in FACTOR_NAMES}
        mkt = implied["MKT"] * 100.0
        return {
            "reachable": True,
            "multiplier": round(k, 2),
            "implied_shock": implied,
            "interpretation": (
                "a %s move roughly %.1fx the size of a -10%% tape -- "
                "equivalent to a market leg of about %.0f%%"
                % (unit_label.split(" ")[0], k, mkt)),
        }

    reverse = {
        "unit_vector_used": unit_label,
        "unit_pnl_pct": round(unit_pnl, 3),
        "to_minus_15pct": reverse_target(-15.0),
        "to_minus_25pct": reverse_target(-25.0),
    }

    # ---- 5) posture + headline --------------------------------------------
    worst = scen_rows[0] if scen_rows else None
    worst_loss = worst["book_pnl_pct"] if worst else 0.0
    if worst_loss <= LOSS_LIMIT_HARD:
        posture = "RED"
    elif worst_loss <= LOSS_LIMIT_SOFT:
        posture = "AMBER"
    else:
        posture = "GREEN"

    losses = [s["book_pnl_pct"] for s in scen_rows]
    losses_sorted = sorted(losses)
    median_pnl = losses_sorted[len(losses_sorted) // 2] if losses_sorted else 0.0
    n_negative = sum(1 for x in losses if x < 0)

    headline = (
        "Worst modelled scenario: %s at %.1f%% of book. %d of %d scenarios "
        "lose money. Firm stress posture %s (soft limit %.0f%%, hard %.0f%%)."
        % (worst["scenario"].split(" (")[0] if worst else "-", worst_loss,
           n_negative, len(scen_rows), posture,
           LOSS_LIMIT_SOFT, LOSS_LIMIT_HARD))

    out = {
        "schema": SCHEMA,
        "engine": "justhodl-firm-stress",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "build_seconds": round(time.time() - t0, 2),
        "firm_book_asof": fb_asof,
        "posture": posture,
        "headline": headline,
        "loss_limits": {"soft_pct": LOSS_LIMIT_SOFT, "hard_pct": LOSS_LIMIT_HARD},
        "summary": {
            "n_names_modelled": len(names),
            "n_direct_loadings": n_direct,
            "n_sector_proxy": n_proxy,
            "n_book_fallback": n_bookfallback,
            "firm_gross_pct": firm.get("firm", {}).get("gross_exposure_pct"),
            "firm_net_pct": firm.get("firm", {}).get("net_exposure_pct"),
            "annual_vol_pct": frisk_firm.get("annual_vol_pct"),
            "var_99_1d_pct": frisk_firm.get("var_99_1d_pct"),
            "worst_scenario": worst["scenario"] if worst else None,
            "worst_loss_pct": worst_loss,
            "median_scenario_pnl_pct": round(median_pnl, 2),
            "n_scenarios": len(scen_rows),
            "n_losing_scenarios": n_negative,
        },
        "scenarios": scen_rows,
        "reverse_stress": reverse,
        "method": (
            "Each named scenario is a cumulative six-factor shock vector "
            "(market plus five style spreads). Every firm-book name is "
            "re-priced as the dot product of its factor loadings -- taken "
            "from the Factor Risk Model's cache -- with the shock vector; "
            "firm P&L is the net-weighted sum. Names without a direct "
            "regression use a sector-average loading, then a book-average "
            "loading. The reverse stress test scales a unit risk-off vector "
            "until the firm loss reaches the target."),
        "disclaimer": (
            "Scenario-analytics model for the platform's model firm book. "
            "Shock vectors are stylised historical analogues, not forecasts. "
            "Not investment advice."),
    }
    put_json(OUT_KEY, out)
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "posture": posture, "worst": worst_loss,
        "n_names": len(names), "n_scenarios": len(scen_rows)})}


if __name__ == "__main__":
    print(lambda_handler({}, None))
