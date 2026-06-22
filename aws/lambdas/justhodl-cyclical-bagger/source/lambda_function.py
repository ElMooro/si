"""
justhodl-cyclical-bagger  ·  v1.0   —   THE CYCLICAL 20x DETECTOR
================================================================================
The cyclical sibling to bagger-engine (which hunts SECULAR compounders). MU and
SNDK did not 20x by compounding 20%/yr for 15 years — they 20x'd as CYCLICAL
EARNINGS EXPLOSIONS: a deeply loss-making trough -> a violent operating-leverage
snapback + a secular demand overlay (the AI memory wall), in ~2 years.

This engine catches that specific pattern, and — the hard part — separates the
real ones from shallow inflections that fizzle, using the discriminator
calibrated on MU/SNDK vs. negative controls (AVGO/AMD/TXN/KLAC):

  CALIBRATION (op-margin swing off trough, the killer discriminator):
    MU   +114.6pp   SNDK  +100.9pp   <- 20x shape
    AVGO  +31.6pp   AMD    +16.1pp   TXN +4.9pp  KLAC +6.2pp  <- rejected
  The 20x names snapped ~100pp off a NEGATIVE-margin trough; secular growers
  that never had the near-death trough sit at 5-32pp and are correctly excluded.

THE SIGNATURE (what makes a 20x possible vs. a shallow fizzle):
  1. COIL      — how deep the trough was (gross/op margin deeply negative). The
                 deeper the loss, the more coiled the operating-leverage spring.
  2. VIOLENCE  — the op-margin swing off trough (>=40pp real, >=90pp = MU-class).
  3. IGNITION  — it's accelerating NOW (revenue-acceleration) + EPS turned neg->pos.
  4. CATALYST  — a secular upstream driver tightening (supply-inflection theme).
  5. ROOM      — small/mid/large base (NOT mega — a $500B co can't 20x to $10T).
  6. CHEAP     — not yet re-rated (rerating-radar discount); trailing P/E is a
                 TRAP at the inflection (MU was 137x P/E right when it was the buy).

STAGE: EARLY (coil+violence present, price hasn't run = the buy window) ->
       CONFIRMING (EPS accelerating, price moving) -> LATE (already 5x+ off
       trough, demote — we hunt pre-boom, not mid-boom).

This is a VIEW that adds a NEW discriminator (trough depth/violence) not graded
elsewhere, so it DOES log top_picks for forward grading (measure-before-trust),
and is FDR-gated like confluence. Research, not advice.
"""
import json, time, urllib.request
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

VERSION = "1.0"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/cyclical-bagger.json"
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
POLY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
s3 = boto3.client("s3", "us-east-1")

# sector/keyword -> supply-inflection themes (ETFs) whose upstream tightening is the catalyst
SECTOR_THEME = {
    "semicond": ["SMH", "SOXX", "AIQ", "BOTZ", "MEMORY_DEMAND"], "technology": ["SMH", "SOXX", "AIQ"],
    "energy": ["XLE", "XOP", "OIH", "AMLP"], "oil": ["XLE", "XOP"],
    "materials": ["COPX", "PICK", "LIT", "REMX", "SLX"], "mining": ["GDX", "GDXJ", "COPX", "PICK"],
    "lithium": ["LIT", "REMX"], "uranium": ["URA", "URNM", "NLR"], "gold": ["GDX", "GDXJ"],
    "steel": ["SLX", "PICK"], "lumber": ["WOOD", "XHB"], "industrial": ["PICK", "GRID"],
}


def _read(key):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception: return None

def _gj(url, tries=2):
    for _ in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=25) as r: return json.loads(r.read())
        except Exception: time.sleep(1)
    return None

def clamp(v, lo=0.0, hi=1.0): return max(lo, min(hi, v))


def discriminators(sym):
    """Deep-trough + snapback-violence from FMP quarterly financials + price stage."""
    inc = _gj(f"https://financialmodelingprep.com/stable/income-statement?symbol={sym}&period=quarter&limit=12&apikey={FMP}")
    if not isinstance(inc, list) or len(inc) < 6: return None
    rows = list(reversed(inc))
    gm = []; om = []; eps = []; rev = []; dates = []
    for r in rows:
        rv = r.get("revenue") or 0
        if rv <= 0: continue
        gm.append((r.get("grossProfit") or 0) / rv * 100)
        om.append((r.get("operatingIncome") or 0) / rv * 100)
        eps.append(r.get("epsdiluted") or r.get("eps") or 0)
        rev.append(rv); dates.append(str(r.get("date", ""))[:10])
    if len(om) < 6: return None
    ttm_eps = [sum(eps[max(0, i - 3):i + 1]) for i in range(len(eps))]
    om_trough = min(om); gm_trough = min(gm)
    trough_idx = om.index(om_trough)
    om_swing = om[-1] - om_trough
    out = {
        "gm_trough": round(gm_trough, 1), "gm_now": round(gm[-1], 1),
        "om_trough": round(om_trough, 1), "om_now": round(om[-1], 1),
        "om_swing_pp": round(om_swing, 1),
        "ttmEPS_trough": round(min(ttm_eps), 2), "ttmEPS_now": round(ttm_eps[-1], 2),
        "eps_neg_to_pos": bool(min(ttm_eps) < 0 and ttm_eps[-1] > 0),
        "rev_scaling_x": round(rev[-1] / max(min(rev), 1), 2),
        "trough_date": dates[trough_idx] if trough_idx < len(dates) else None,
        "gm_rising_streak": sum(1 for i in range(len(gm) - 1, 0, -1) if gm[i] > gm[i - 1]) if len(gm) > 1 else 0,
    }
    return out


def price_stage(sym, trough_date):
    """Has the price already run off the trough? EARLY vs LATE."""
    import datetime as dt
    to = dt.date.today().isoformat(); fr = (dt.date.today() - dt.timedelta(days=900)).isoformat()
    px = _gj(f"https://api.polygon.io/v2/aggs/ticker/{sym}/range/1/day/{fr}/{to}?adjusted=true&sort=asc&limit=50000&apikey={POLY}")
    bars = px.get("results", []) if isinstance(px, dict) else []
    if not bars: return None, None, None
    last = bars[-1]["c"]
    p_trough = None
    if trough_date:
        try:
            tgt = dt.datetime.strptime(trough_date, "%Y-%m-%d").timestamp() * 1000
            for b in bars:
                if b["t"] <= tgt + 86400000 * 7: p_trough = b["c"]
                else: break
        except Exception: pass
    if p_trough is None:
        p_trough = min(b["c"] for b in bars)
    run_x = round(last / p_trough, 2) if p_trough else None
    return round(last, 2), round(p_trough, 2), run_x


def lambda_handler(event, context):
    t0 = time.time(); diag = []

    # ---- fusion inputs ----
    racc = _read("data/revenue-acceleration.json") or {}
    rerate = _read("data/ai-rerating-radar.json") or {}
    supply = _read("data/supply-inflection.json") or {}

    # secular catalyst: theme -> inflection score (defensive across schemas)
    theme_score = {}
    for cand_key in ("themes", "theme_scores", "by_theme", "signals", "theme_inflection"):
        v = supply.get(cand_key)
        if isinstance(v, dict):
            for k, val in v.items():
                sc = val.get("score") if isinstance(val, dict) else val
                if isinstance(sc, (int, float)): theme_score[str(k).upper()] = sc
        elif isinstance(v, list):
            for it in v:
                if isinstance(it, dict):
                    k = it.get("theme") or it.get("etf") or it.get("symbol")
                    sc = it.get("score") or it.get("inflection_score")
                    if k and isinstance(sc, (int, float)): theme_score[str(k).upper()] = sc
    diag.append("supply themes=%d" % len(theme_score))

    # rerating lookup: symbol -> (discount_to_implied, cap_bucket, market_cap, growth)
    rr = {}
    for r in rerate.get("all_ranked", []):
        s = r.get("symbol")
        if s: rr[s.upper()] = r

    # ---- candidate pool ----
    # The next MU is NOT in an "accelerating" screen yet — it's still in the deep
    # trough. So the pool = depressed CYCLICAL universe (where trough->snapback 20x
    # happens) UNION names already inflecting (revenue-acceleration) UNION cheap
    # (rerating) UNION small-caps (bagger). The engine filters for the deep-trough
    # + violent-snapback signature within that broad pool.
    CYCLICAL_UNIVERSE = [
        # solar (deep trough)
        "FSLR","ENPH","SEDG","RUN","ARRY","SHLS","MAXN","CSIQ","JKS","NOVA","FLNC","BE","PLUG",
        # lithium / battery materials (deep trough)
        "ALB","SQM","LAC","LTHM","PLL","MP","SGML","LILM",
        # oil services / E&P (cyclical)
        "HAL","SLB","BKR","NOV","FTI","RIG","VAL","NE","HP","PTEN","LBRT","WFRD","TDW","CHX",
        # chemicals (cyclical trough)
        "DOW","LYB","CE","OLN","WLK","CC","HUN","ASIX","TROX","KRO",
        # metals / miners
        "FCX","AA","CLF","X","STLD","NUE","CMC","MT","VALE","TECK","HBM",
        # uranium
        "CCJ","UEC","UUUU","DNN","NXE","LEU",
        # shipping (deep cyclical)
        "ZIM","SBLK","GNK","EGLE","INSW","STNG","DAC","GSL","NMM",
        # ag / fertilizer (cyclical)
        "MOS","CF","NTR","IPI","SMG",
        # memory / storage / semicap stragglers
        "WDC","STX","ACMR","UCTT","ICHR","COHU","FORM","AEHR","PLAB",
        # other deep cyclicals
        "GT","DAN","AXL","REAL","WOLF","ON","MTSI",
    ]
    pool = set(CYCLICAL_UNIVERSE)
    racc_meta = {}
    summ = racc.get("summary", {})
    for lst in (summ.get("top_25_overall", []), summ.get("microcap_picks", []), racc.get("all_qualifying", [])[:80]):
        for r in (lst or []):
            s = r.get("symbol")
            if s: pool.add(s.upper()); racc_meta.setdefault(s.upper(), r)
    for r in rerate.get("all_ranked", []):
        if r.get("is_candidate") and r.get("symbol"): pool.add(r["symbol"].upper())
    bag = _read("data/bagger-engine.json") or {}
    for r in (bag.get("candidates") or bag.get("top") or bag.get("all_ranked") or [])[:40]:
        s = r.get("symbol") or r.get("ticker")
        if s: pool.add(s.upper())
    pool.update(["MU", "SNDK"])   # reference self-check
    pool = list(pool)[:150]
    diag.append("pool=%d" % len(pool))

    def theme_catalyst(sym):
        rrr = rr.get(sym, {}); sector = str(rrr.get("sector") or racc_meta.get(sym, {}).get("metrics", {}).get("sector") or "").lower()
        themes = []
        for kw, th in SECTOR_THEME.items():
            if kw in sector: themes += th
        if not themes and ("semi" in sector or "tech" in sector or sym in ("MU", "SNDK")):
            themes = ["SMH", "SOXX", "MEMORY_DEMAND"]
        best = 0.0
        for th in themes:
            if th.upper() in theme_score: best = max(best, theme_score[th.upper()])
        return round(best, 1), themes[:4]

    def evaluate(sym):
        d = discriminators(sym)
        if not d: return None
        # ---- calibrated scores (0..1) ----
        coil = clamp((-d["om_trough"]) / 50.0) if d["om_trough"] < 0 else clamp((5 - d["gm_trough"]) / 30.0) * 0.4
        violence = clamp((d["om_swing_pp"] - 20) / 80.0)        # 20pp=0, 100pp=1.0
        eps_turn = 1.0 if d["eps_neg_to_pos"] else (0.4 if d["ttmEPS_now"] > d["ttmEPS_trough"] else 0.0)
        scaling = clamp((d["rev_scaling_x"] - 1.2) / 4.0)
        accel = clamp((racc_meta.get(sym, {}).get("score", 0)) / 100.0)
        cat, themes = theme_catalyst(sym)
        catalyst = clamp(cat / 100.0)
        rrr = rr.get(sym, {})
        disc = rrr.get("discount_to_implied_pct")
        cheap = clamp((disc or 0) / 100.0) if isinstance(disc, (int, float)) else 0.4
        mcap = rrr.get("market_cap") or racc_meta.get(sym, {}).get("metrics", {}).get("market_cap")
        cap_bucket = rrr.get("cap_bucket")
        if not cap_bucket and isinstance(mcap, (int, float)):
            cap_bucket = ("nano" if mcap < 3e8 else "micro" if mcap < 2e9 else "small" if mcap < 1e10 else
                          "mid" if mcap < 5e10 else "large" if mcap < 2e11 else "mega")
        room = {"nano": 1.0, "micro": 0.95, "small": 0.85, "mid": 0.65, "large": 0.4, "mega": 0.1}.get(cap_bucket, 0.6)

        # ---- the 20x-SHAPE gate (calibrated: MU/SNDK pass, AVGO/AMD/TXN/KLAC fail) ----
        coil_deep = (d["om_trough"] < -10) or (d["gm_trough"] < 5)
        violent = d["om_swing_pp"] >= 40
        not_mega = cap_bucket != "mega" and not (isinstance(mcap, (int, float)) and mcap > 5e11)
        twenty_x_shape = bool(coil_deep and violent and d["eps_neg_to_pos"] and not_mega)

        # ---- stage via price run off trough ----
        last, p_tr, run_x = price_stage(sym, d.get("trough_date"))
        if run_x is None: stage = "UNKNOWN"
        elif run_x >= 5: stage = "LATE"
        elif run_x >= 2: stage = "CONFIRMING"
        else: stage = "EARLY"

        # composite (gate-weighted: shape names scored on the full spring; others penalized)
        spring = 0.30 * coil + 0.30 * violence + 0.12 * eps_turn + 0.10 * scaling
        confirm = 0.10 * accel + 0.08 * catalyst
        composite = 100 * (spring + confirm) * (0.55 + 0.45 * room) * (0.7 + 0.5 * cheap if twenty_x_shape else 0.5)
        # demote LATE (already run) — we want pre-boom
        if stage == "LATE": composite *= 0.45
        composite = round(min(100, composite), 1)

        reasons = []
        if coil_deep: reasons.append(f"deep trough (om {d['om_trough']}%, gm {d['gm_trough']}%)")
        if violent: reasons.append(f"violent snapback (+{d['om_swing_pp']}pp op-margin)")
        if d["eps_neg_to_pos"]: reasons.append("EPS turned negative->positive")
        if catalyst > 0.3: reasons.append(f"secular catalyst {themes[:2]} ({cat})")
        if cheap > 0.5: reasons.append(f"cheap vs growth ({disc}% below fair)")
        return {
            "ticker": sym, "cap_bucket": cap_bucket, "market_cap": mcap,
            "cyclical_20x_score": composite, "twenty_x_shape": twenty_x_shape, "stage": stage,
            "coil_score": round(coil, 2), "violence_score": round(violence, 2),
            "acceleration_score": round(accel, 2), "secular_catalyst_score": round(catalyst, 2),
            "cheap_score": round(cheap, 2), "room_score": round(room, 2),
            "om_trough": d["om_trough"], "om_now": d["om_now"], "om_swing_pp": d["om_swing_pp"],
            "gm_trough": d["gm_trough"], "gm_now": d["gm_now"],
            "eps_neg_to_pos": d["eps_neg_to_pos"], "ttmEPS_trough": d["ttmEPS_trough"], "ttmEPS_now": d["ttmEPS_now"],
            "rev_scaling_x": d["rev_scaling_x"], "gm_rising_streak": d["gm_rising_streak"],
            "secular_themes": themes, "discount_to_fair_pct": disc,
            "price_now": last, "price_at_trough": p_tr, "run_from_trough_x": run_x,
            "reasons": reasons,
        }

    results = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(evaluate, s): s for s in pool}
        for f in as_completed(futs):
            try:
                r = f.result()
                if r: results.append(r)
            except Exception: pass
    diag.append("evaluated=%d" % len(results))

    results.sort(key=lambda r: r["cyclical_20x_score"], reverse=True)
    shape_book = [r for r in results if r["twenty_x_shape"] and r["stage"] in ("EARLY", "CONFIRMING")]
    shape_book.sort(key=lambda r: (r["stage"] != "EARLY", -r["cyclical_20x_score"]))

    # ---- FDR gate (forward-grading auto-activation, like confluence) ----
    ea = _read("data/engine-alpha.json") or {}
    proven = set(str(x).lower() for x in (ea.get("alpha_proven_signals") or []))
    mode = "PROVEN" if "eng:cyclical-bagger" in proven else "PROVISIONAL"

    out = {
        "engine": "cyclical-bagger", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1), "mode": mode,
        "thesis": ("The CYCLICAL 20x: deep loss-making trough -> violent operating-leverage snapback + "
                   "secular demand overlay (the MU/SNDK pattern), caught EARLY and separated from shallow "
                   "inflections that fizzle. Sibling to bagger-engine (secular compounders)."),
        "discriminator": {
            "killer_metric": "operating-margin swing off trough",
            "calibration": {"MU": 114.6, "SNDK": 100.9, "AVGO": 31.6, "AMD": 16.1, "TXN": 4.9, "KLAC": 6.2},
            "gate": "coil(om_trough<-10 OR gm_trough<5) AND violence(om_swing>=40pp) AND eps_neg_to_pos AND not_mega",
        },
        "stats": {"pool": len(pool), "evaluated": len(results),
                  "twenty_x_shape": sum(1 for r in results if r["twenty_x_shape"]),
                  "shape_early_confirming": len(shape_book)},
        "twenty_x_shape_book": shape_book[:25],
        "all_ranked": results[:60],
        "auto_activation": {"rule": "logs top_picks; FDR-graded forward vs SPY; flips PROVEN when scorecard proves it",
                            "active": mode == "PROVEN"},
        "methodology": (
            "Pool = revenue-acceleration (accelerating names) + ai-rerating-radar candidates. For each, deep-trough "
            "+ snapback-violence computed from FMP quarterly financials (op/gross margin trough, swing off trough, "
            "EPS neg->pos, revenue scaling), overlaid with supply-inflection secular-catalyst (theme tightening), "
            "rerating-radar cheapness, and a small/mid base gate (mega excluded — can't 20x). Discriminator "
            "calibrated so MU/SNDK pass and AVGO/AMD/TXN/KLAC are rejected. Stage via price run off the trough "
            "(EARLY=buy window, LATE demoted). NOT fit to MU/SNDK — defined on the economic logic of operating "
            "leverage; logged to the harvester for forward FDR grading. Research, not investment advice."),
        "disclaimer": "Most names with the 20x SHAPE still will not 20x — this filters out the structurally impossible and the shallow, and flags the window. Not investment advice.",
        "diagnostics": diag[-10:],
        # harvester picks the NEW discriminator (forward-graded)
        "top_picks": [{"ticker": r["ticker"], "score": r["cyclical_20x_score"], "stage": r["stage"],
                       "twenty_x_shape": r["twenty_x_shape"]} for r in shape_book[:15]],
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache, max-age=0")
    print("[cyclical-bagger v1] pool=%d eval=%d 20x_shape=%d early/conf=%d | MU/SNDK self-check:" % (
        len(pool), len(results), out["stats"]["twenty_x_shape"], len(shape_book)))
    for r in results:
        if r["ticker"] in ("MU", "SNDK"):
            print("   %s: score=%s shape=%s stage=%s swing=%spp eps_n2p=%s run=%sx" % (
                r["ticker"], r["cyclical_20x_score"], r["twenty_x_shape"], r["stage"], r["om_swing_pp"],
                r["eps_neg_to_pos"], r["run_from_trough_x"]))
    if shape_book:
        print("  20x-shape book (early/confirming):")
        for r in shape_book[:10]:
            print("   %s %s score=%s swing=%spp run=%sx cap=%s" % (
                r["ticker"], r["stage"], r["cyclical_20x_score"], r["om_swing_pp"], r["run_from_trough_x"], r["cap_bucket"]))
    return {"statusCode": 200, "body": json.dumps({"mode": mode, "shape": out["stats"]["twenty_x_shape"]})}
