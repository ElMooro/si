"""
justhodl-risk-gate v1.0 — THE MASTER RISK GATE (BRAIN-CONSTITUTIONAL).

Khalid's directive (2026-07-26): "brain is how i want my system to think —
all decisions and analysis should be based on brain." This engine is the
top of the decision hierarchy: ONE authoritative posture (RISK_ON / NEUTRAL
/ RISK_OFF / SEVERE) computed BEFORE any asset-class or stock selection,
with every rule implementing a specific brain note (IDs cited inline and in
the output's brain_constitution block).

THE SIX LEGS (each = a brain framework):
  1 FUNDING/PLUMBING  — RRP level + drain BOTH directions (note nmq5x1e4os92j;
      Oct-2025 call = drain to ~zero below pre-COVID — the direction the
      canary-grid missed), reserves composite he specified (nmq5x1e4kuplz:
      TOTRESNS+BOGMBBM+CASACBW027SBOG; tv-f452edc700d3a8da WRESBAL barometer),
      SOFR-IORB decoupling (tv-a56315720e79a9ea "canary in the coal mine").
  2 CREDIT            — "THE MOST IMPORTANT THING FOR ALL MARKETS IS
      LIQUIDITY... as long as credit spreads narrow the markets are gonna do
      well" (tv-8711fbee989cf1eb CCC); BBB cliff-edge cascade
      (tv-ba419d7b64a1e75d); Euro HY OAS spike = dollar shortage
      (tv-e38d57bffedb366a); leverage lives in weak HY + fallen angels
      (nmq5w1e03r08g).
  3 DOLLAR            — "You can't get your global macro view right if you
      get your dollar view wrong — the single most important variable"
      (tv-ab761f92999efe68); "when dollar strengthens and rates are higher
      YOU BETTER EXIT" (nmq5x00zhe98n); consistent 10Y decline = 2007/2020
      crisis pattern (tv-b4c32545ea1dc640).
  4 CARRY/EURODOLLAR  — yen = second liquidity provider (tv-9fa576184567fa8f);
      carry unwind margin-call mechanism (tv-f58a44fc2f839aac); consumes the
      existing justhodl-yen-carry composite live.
  5 GLOBAL GROWTH     — INDPRO "predicts recessions years ahead... HUGE
      consideration to be invested or risk off" (nmq5x00zh27pq).
  6 MARKET STRUCTURE  — MOVE/vol margin-call cascade (tv-14a76b6087dc80eb);
      VIX as the tradable proxy here.

HIERARCHY DOCTRINE (nmq5x0cp7zp4j "LIQUIDITY RULES", tv-c8640dea0c15ee5c
"Macro is the king that rules; technicals only time entries"): this gate's
sizing_multiplier is meant to be consumed by position-sizer-v2, sizing-engine,
opportunity-engine, best-setups, master-ranker as a MULTIPLIER on exposure —
macro gates sizing BEFORE selection.

GRADING AMENDMENT (the reason this engine exists): macro/risk signals are
graded by EVENT STUDY — did the gate flip to RISK_OFF before real drawdowns
(including replaying Sep-Nov 2025, his RRP call) — NEVER by daily IC, which
structurally cannot see rare-event regime value. The replay is possible
because posture is a PURE FUNCTION of trailing series values (no lookahead).

Output: data/risk-gate.json
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/risk-gate.json"
MARKER = "risk-gate v2.3 BRAIN-CONSTITUTIONAL FLEET-FUSED"

s3 = boto3.client("s3")

# Every FRED series this gate consumes, with the brain note that demands it.
SERIES = {
    "RRPONTSYD":       "nmq5x1e4os92j",       # reverse repo — BOTH directions
    "WRESBAL":         "tv-f452edc700d3a8da", # reserves barometer
    "TOTRESNS":        "nmq5x1e4kuplz",       # his explicit composite leg 1
    "BOGMBBM":         "nmq5x1e4kuplz",       # his explicit composite leg 2
    "CASACBW027SBOG":  "nmq5x1e4kuplz",       # his explicit composite leg 3
    "SOFR":            "tv-a56315720e79a9ea", # SOFR decoupling canary
    "IORB":            "tv-a56315720e79a9ea",
    "BAMLH0A3HYC":     "tv-8711fbee989cf1eb", # CCC — liquidity doctrine
    "BAMLC0A4CBBB":    "tv-ba419d7b64a1e75d", # BBB cliff-edge
    "BAMLH0A0HYM2":    "nmq5w1e03r08g",       # broad HY
    "BAMLHE00EHYIOAS": "tv-e38d57bffedb366a", # Euro HY OAS dollar shortage
    "DTWEXBGS":        "tv-ab761f92999efe68", # broad dollar
    "DGS10":           "nmq5x00zhe98n",       # 10Y (exit rule + crisis decline)
    "DEXJPUS":         "tv-f58a44fc2f839aac", # yen (carry unwind)
    "INDPRO":          "nmq5x00zh27pq",       # recession predictor
    "VIXCLS":          "tv-14a76b6087dc80eb", # vol cascade proxy
    "DCPN3M":          "nmrdt9tk992wt",       # 3M CP (CP-FFR spread, fuse-list)
    "DFF":             "nmrdt9tk992wt",       # fed funds for the CP spread
    "RIFSPPNA2P2D90NB": "nmq5x1qrkm0cn",     # A2/P2 spread (graceful if ID wrong)
    "SP500":           "event-study-benchmark",
}


def fred(series_id, start="2018-06-01"):
    url = (f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}"
           f"&api_key={FRED_KEY}&file_type=json&observation_start={start}")
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-RiskGate/1.0"})
            with urllib.request.urlopen(req, timeout=25) as r:
                obs = json.loads(r.read()).get("observations", [])
            out = {}
            for o in obs:
                v = o.get("value")
                if v not in (None, "", "."):
                    out[o["date"]] = float(v)
            return out
        except Exception as e:
            if attempt == 2:
                print(f"[risk-gate] FRED {series_id} failed: {str(e)[:80]}")
                return {}
            time.sleep(1 + attempt)
    return {}


def build_calendar(all_series):
    days = set()
    for s in all_series.values():
        days.update(s.keys())
    return sorted(days)


def ffill_on(calendar, series):
    out, last = {}, None
    for d in calendar:
        if d in series:
            last = series[d]
        out[d] = last
    return out


def _pct(cur, prev):
    if cur is None or prev is None or prev == 0:
        return None
    return (cur / prev - 1) * 100.0


def _win(vals_by_day, calendar, i, back):
    j = i - back
    if j < 0:
        return None
    return vals_by_day.get(calendar[j])


def compute_posture(F, calendar, i):
    """PURE function of trailing values at day index i — replayable, no
    lookahead. Returns (posture, composite, legs dict)."""
    d = calendar[i]
    legs = {}

    # ---- LEG 1: FUNDING / PLUMBING (weight .25) ----------------------------
    rrp = F["RRPONTSYD"].get(d)
    rrp_1y = _win(F["RRPONTSYD"], calendar, i, 252)
    reserves = F["WRESBAL"].get(d)
    res_13w = _pct(reserves, _win(F["WRESBAL"], calendar, i, 65))
    sofr, iorb = F["SOFR"].get(d), F["IORB"].get(d)
    sofr_iorb_bp = (sofr - iorb) * 100 if (sofr is not None and iorb is not None) else None

    funding = 0.0
    notes1 = []
    if rrp is not None:
        if rrp < 25:  # ~zero, below pre-COVID range — Khalid's Oct-2025 condition
            if res_13w is not None and res_13w < 0:
                funding -= 2.0
                notes1.append(f"RRP buffer EXHAUSTED ({rrp:.0f}B, near zero, below pre-COVID) "
                              f"while reserves drain {res_13w:.1f}%/13w [nmq5x1e4os92j]")
            else:
                funding -= 1.0
                notes1.append(f"RRP near zero ({rrp:.0f}B) — no buffer left; QT now drains "
                              f"reserves directly [nmq5x1e4os92j]")
        elif rrp_1y is not None and rrp_1y > 0 and (rrp / rrp_1y) > 2.5 and rrp > 400:
            funding -= 1.0
            notes1.append(f"RRP SPIKE {rrp:.0f}B (cash parking at the Fed, 2019-style) "
                          f"[canary-grid dir=rise, kept]")
    if res_13w is not None:
        if res_13w < -6:
            funding -= 1.0
            notes1.append(f"Reserves draining fast {res_13w:.1f}%/13w [tv-f452edc700d3a8da]")
        elif res_13w > 2:
            funding += 0.5
            notes1.append(f"Reserves building {res_13w:.1f}%/13w = liquidity injection")
    if sofr_iorb_bp is not None:
        if sofr_iorb_bp > 15:
            funding -= 1.0
            notes1.append(f"SOFR-IORB +{sofr_iorb_bp:.0f}bp — funding decoupling "
                          f"'canary in the coal mine' [tv-a56315720e79a9ea]")
        elif sofr_iorb_bp > 5:
            funding -= 0.5
            notes1.append(f"SOFR-IORB +{sofr_iorb_bp:.0f}bp elevated [tv-a56315720e79a9ea]")
    cp, ff = F["DCPN3M"].get(d), F["DFF"].get(d)
    cp_ffr_bp = (cp - ff) * 100 if (cp is not None and ff is not None) else None
    if cp_ffr_bp is not None and cp_ffr_bp > 40:
        funding -= 0.5
        notes1.append(f"CP-FFR spread +{cp_ffr_bp:.0f}bp — credit conditions tightening "
                      f"in the funding market [nmrdt9tk992wt]")
    a2p2 = F.get("RIFSPPNA2P2D90NB", {}).get(d)
    if a2p2 is not None and a2p2 > 0.5:
        funding -= 0.4
        notes1.append(f"A2/P2 spread {a2p2:.2f}% — low-grade paper stress [nmq5x1qrkm0cn]")
    legs["funding"] = {"score": max(-2.0, min(2.0, funding)), "why": notes1,
                       "cp_ffr_bp": cp_ffr_bp,
                       "rrp_bn": rrp, "reserves_13w_pct": res_13w,
                       "sofr_iorb_bp": sofr_iorb_bp}

    # ---- LEG 2: CREDIT (weight .25) ---------------------------------------
    ccc = F["BAMLH0A3HYC"].get(d)
    ccc_21 = _pct(ccc, _win(F["BAMLH0A3HYC"], calendar, i, 21))
    hy_21 = _pct(F["BAMLH0A0HYM2"].get(d), _win(F["BAMLH0A0HYM2"], calendar, i, 21))
    euro_21 = _pct(F["BAMLHE00EHYIOAS"].get(d), _win(F["BAMLHE00EHYIOAS"], calendar, i, 21))
    credit = 0.0
    notes2 = []
    if ccc_21 is not None:
        if ccc_21 < -3:
            credit += 1.5
            notes2.append(f"CCC narrowing {ccc_21:.1f}%/21d — 'as long as spreads narrow, "
                          f"markets do well' [tv-8711fbee989cf1eb]")
        elif ccc_21 > 20:
            credit -= 2.0
            notes2.append(f"CCC widening {ccc_21:.1f}%/21d — leverage cracking "
                          f"[nmq5w1e03r08g]")
        elif ccc_21 > 8:
            credit -= 1.0
            notes2.append(f"CCC widening {ccc_21:.1f}%/21d [tv-8711fbee989cf1eb]")
    if hy_21 is not None and hy_21 > 12:
        credit -= 0.5
        notes2.append(f"Broad HY widening {hy_21:.1f}%/21d")
    if euro_21 is not None and euro_21 > 15:
        credit -= 1.0
        notes2.append(f"Euro HY OAS spiking {euro_21:.1f}%/21d = dollar shortage "
                      f"[tv-e38d57bffedb366a]")
    legs["credit"] = {"score": max(-2.0, min(2.0, credit)), "why": notes2,
                      "ccc_level": ccc, "ccc_21d_pct": ccc_21,
                      "euro_hy_21d_pct": euro_21}

    # ---- LEG 3: DOLLAR (weight .20) ---------------------------------------
    dxy_63 = _pct(F["DTWEXBGS"].get(d), _win(F["DTWEXBGS"], calendar, i, 63))
    dgs10 = F["DGS10"].get(d)
    dgs10_63bp = ((dgs10 - _win(F["DGS10"], calendar, i, 63)) * 100
                  if (dgs10 is not None and _win(F["DGS10"], calendar, i, 63) is not None) else None)
    dgs10_126bp = ((dgs10 - _win(F["DGS10"], calendar, i, 126)) * 100
                   if (dgs10 is not None and _win(F["DGS10"], calendar, i, 126) is not None) else None)
    dollar = 0.0
    notes3 = []
    if dxy_63 is not None and dgs10_63bp is not None:
        if dxy_63 > 2 and dgs10_63bp > 30:
            dollar -= 2.0
            notes3.append(f"Dollar +{dxy_63:.1f}%/63d AND 10Y +{dgs10_63bp:.0f}bp — "
                          f"'YOU BETTER EXIT THE MARKETS' [nmq5x00zhe98n]")
        elif dxy_63 > 2:
            dollar -= 1.0
            notes3.append(f"Dollar rising fast +{dxy_63:.1f}%/63d — global liquidity drain "
                          f"[tv-ab761f92999efe68]")
        elif dxy_63 < -1.5:
            dollar += 1.0
            notes3.append(f"Dollar weakening {dxy_63:.1f}%/63d = global liquidity easing")
    if dgs10_126bp is not None and dgs10_126bp < -60 and (dxy_63 or 0) > 0:
        dollar -= 1.0
        notes3.append(f"10Y persistent decline {dgs10_126bp:.0f}bp/126d with firm dollar — "
                      f"2007/2020 flight-to-safety pattern [tv-b4c32545ea1dc640]")
    legs["dollar"] = {"score": max(-2.0, min(2.0, dollar)), "why": notes3,
                      "dxy_63d_pct": dxy_63, "dgs10_63d_bp": dgs10_63bp}

    # ---- LEG 4: CARRY / EURODOLLAR (weight .10) ---------------------------
    jpy = F["DEXJPUS"].get(d)  # JPY per USD; falling = yen strengthening
    jpy_21 = _pct(jpy, _win(F["DEXJPUS"], calendar, i, 21))
    carry = 0.0
    notes4 = []
    if jpy_21 is not None:
        if jpy_21 < -4:
            carry -= 2.0
            notes4.append(f"Yen strengthening {jpy_21:.1f}%/21d — carry-unwind margin-call "
                          f"cascade [tv-f58a44fc2f839aac]")
        elif jpy_21 < -2:
            carry -= 1.0
            notes4.append(f"Yen firming {jpy_21:.1f}%/21d — carry stress building")
        elif jpy_21 > 1.5:
            carry += 0.5
            notes4.append("Yen weakening — carry funding easy [tv-9fa576184567fa8f]")
    legs["carry"] = {"score": max(-2.0, min(2.0, carry)), "why": notes4,
                     "usdjpy_21d_pct": jpy_21}

    # ---- LEG 5: GLOBAL GROWTH (weight .10) --------------------------------
    ind = F["INDPRO"].get(d)
    ind_yoy = _pct(ind, _win(F["INDPRO"], calendar, i, 252))
    growth = 0.0
    notes5 = []
    if ind_yoy is not None:
        if ind_yoy < -2:
            growth -= 2.0
            notes5.append(f"INDPRO {ind_yoy:.1f}% YoY contracting hard — 'predicts "
                          f"recessions years ahead' [nmq5x00zh27pq]")
        elif ind_yoy < 0:
            growth -= 1.0
            notes5.append(f"INDPRO {ind_yoy:.1f}% YoY negative [nmq5x00zh27pq]")
        elif ind_yoy > 1:
            growth += 1.0
            notes5.append(f"INDPRO +{ind_yoy:.1f}% YoY expanding")
    legs["growth"] = {"score": max(-2.0, min(2.0, growth)), "why": notes5,
                      "indpro_yoy_pct": ind_yoy}

    # ---- LEG 6: MARKET STRUCTURE (weight .10) -----------------------------
    vix = F["VIXCLS"].get(d)
    structure = 0.0
    notes6 = []
    if vix is not None:
        if vix > 30:
            structure -= 2.0
            notes6.append(f"VIX {vix:.0f} — forced-deleveraging zone (MOVE cascade "
                          f"mechanism) [tv-14a76b6087dc80eb]")
        elif vix > 22:
            structure -= 1.0
            notes6.append(f"VIX {vix:.0f} elevated")
        elif vix < 16:
            structure += 1.0
            notes6.append(f"VIX {vix:.0f} calm")
    legs["structure"] = {"score": max(-2.0, min(2.0, structure)), "why": notes6,
                         "vix": vix}

    W = {"funding": .25, "credit": .25, "dollar": .20, "carry": .10,
         "growth": .10, "structure": .10}
    # ops 4316 — collateral leg (advisory; weights untouched): the
    # treasury-rehypo desk's composite enters as a recorded leg and
    # adjusts the gate only at STRAINED/SEIZING, fully disclosed.
    try:
        _rh = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/treasury-rehypo.json"
        )["Body"].read())
        _c, _b = _rh.get("composite"), _rh.get("band")
        _sc = max(-2.0, min(2.0, -((_c or 50) - 50) / 12.5))
        legs["collateral"] = {
            "score": round(_sc, 2), "advisory": True,
            "why": ["treasury-rehypo composite %s (%s): fails/"
                    "velocity/specialness/funding/RRP proxy stack "
                    "(ops 4302-4308)" % (_c, _b)],
            "cite": "ops4302/rehypo"}
    except Exception as _e:
        legs["collateral"] = {"score": None, "advisory": True,
                              "why": ["rehypo unreadable: %s"
                                      % str(_e)[:60]]}
    composite = sum(legs[k]["score"] * W[k] for k in W)
    _b = (legs.get("collateral") or {}).get("why", [""])[0]
    _band = _rh.get("band") if "_rh" in dir() else None
    if _band == "STRAINED":
        composite -= 0.15
        legs["collateral"]["applied"] = "-0.15 (STRAINED)"
    elif _band == "SEIZING":
        composite -= 0.35
        legs["collateral"]["applied"] = "-0.35 (SEIZING)"

    # Posture bands + the plumbing override (nmq5vhvebjob6: never touch stocks
    # when plumbing is shaky — a broken funding leg confirmed by credit is
    # SEVERE regardless of the other legs' average).
    if legs["funding"]["score"] <= -2 and legs["credit"]["score"] <= -1:
        posture = "SEVERE"
    elif composite >= 0.35:
        posture = "RISK_ON"
    elif composite > -0.35:
        posture = "NEUTRAL"
    elif composite > -0.95:
        posture = "RISK_OFF"
    else:
        posture = "SEVERE"
    return posture, round(composite, 3), legs


SIZING = {"RISK_ON": 1.0, "NEUTRAL": 0.75, "RISK_OFF": 0.45, "SEVERE": 0.20}


def event_study(F, calendar, postures):
    """Grade the gate the way the brain demands (event-study, not daily IC):
    find flips INTO RISK_OFF-or-worse, measure SP500 forward returns after
    each flip vs the unconditional baseline, and report the Sep-Nov 2025
    window specifically (Khalid's October RRP call)."""
    sp = F["SP500"]
    flips = []
    bad = {"RISK_OFF", "SEVERE"}
    for i in range(1, len(calendar)):
        if postures[i] in bad and postures[i - 1] not in bad:
            flips.append(i)

    def fwd(i, n):
        d0, dn = calendar[i], (calendar[i + n] if i + n < len(calendar) else None)
        if dn is None or sp.get(d0) is None or sp.get(dn) is None:
            return None
        return round((sp[dn] / sp[d0] - 1) * 100, 2)

    flip_rows = []
    for i in flips:
        flip_rows.append({"date": calendar[i], "posture": postures[i],
                          "spx_fwd_21d_pct": fwd(i, 21), "spx_fwd_63d_pct": fwd(i, 63)})

    # unconditional baseline over the replay window
    base21 = [fwd(i, 21) for i in range(0, len(calendar) - 22, 5)]
    base21 = [x for x in base21 if x is not None]
    baseline_21d = round(sum(base21) / len(base21), 2) if base21 else None

    # October 2025 window — his call
    oct_win = [i for i, d in enumerate(calendar) if "2025-09-15" <= d <= "2025-11-15"]
    oct_postures = {}
    oct_rrp_min = None
    for i in oct_win:
        oct_postures[postures[i]] = oct_postures.get(postures[i], 0) + 1
        r = F["RRPONTSYD"].get(calendar[i])
        if r is not None:
            oct_rrp_min = r if oct_rrp_min is None else min(oct_rrp_min, r)

    # time-in-posture + avoided-drawdown framing (regime P&L, per the brain)
    in_bad = [i for i in range(len(calendar)) if postures[i] in bad]
    bad_fwd21 = [fwd(i, 21) for i in in_bad[::5]]
    bad_fwd21 = [x for x in bad_fwd21 if x is not None]
    avg_fwd21_while_bad = round(sum(bad_fwd21) / len(bad_fwd21), 2) if bad_fwd21 else None

    return {
        "methodology": "event-study + regime P&L per brain doctrine — NEVER daily IC "
                       "(rare-event regime signals are invisible to daily grading)",
        "n_flips_to_risk_off_or_worse": len(flips),
        "flips": flip_rows[-12:],
        "spx_baseline_fwd_21d_pct": baseline_21d,
        "avg_spx_fwd_21d_while_risk_off_pct": avg_fwd21_while_bad,
        "gate_adds_value_if": "avg fwd return while RISK_OFF < baseline (drawdown avoided)",
        "october_2025_replay": {
            "window": "2025-09-15 .. 2025-11-15",
            "posture_day_counts": oct_postures,
            "rrp_min_in_window_bn": oct_rrp_min,
            "khalid_call": "his other system flipped risk-off on RRP drain to ~zero "
                           "(below pre-COVID) and stayed risk-off",
        },
    }




# ── FLEET LAYER (v2.0, Khalid-approved list 2026-07-26) ─────────────────────
# Consumes existing engines' LIVE outputs as per-leg adjustments. Applied to
# the LIVE posture only — the FRED replay stays pure so the event study keeps
# its integrity (fleet feeds lack deep history; grading them by replay would
# be fake). Per-leg fleet adjustment clamped to ±0.75 so no single feed and
# no leg's fleet inputs can dominate the FRED base. Missing/stale = 0 + an
# honest status, never a synthesized value.
FLEET_STALE_H = 72.0

def _feed(key):
    try:
        o = s3.get_object(Bucket=S3_BUCKET, Key=key)
        age = (datetime.now(timezone.utc) - o["LastModified"]).total_seconds() / 3600
        return json.loads(o["Body"].read()), round(age, 1)
    except Exception:
        return None, None

def _fi(name, key, value, adj, note, age):
    status = "MISSING" if value is None else ("STALE" if (age or 0) > FLEET_STALE_H else "OK")
    return {"input": name, "feed": key, "value": value,
            "score_adj": adj if status == "OK" else 0.0,
            "note": note, "age_h": age, "status": status}

def fleet_adjust(legs):
    """Returns {leg: [fleet_input dicts]}; mutates nothing."""
    out = {k: [] for k in legs}

    # LEG 1 FUNDING — fails, dealer stress, 10Y auction, SOMA/TGA + xcc basis
    pd_doc, a = _feed("data/nyfed-primary-dealer.json")
    v = (pd_doc or {}).get("net_treasury_total_b")
    adj = (-0.3 if (isinstance(v, (int, float)) and v < -50) else 0.0)
    out["funding"].append(_fi("dealer_net_treasury_b", "nyfed-primary-dealer", v, adj,
        "dealer net treasury positioning; deep short = stressed intermediation "
        "[tv-a8157acb4435ffe6]; corp_net_bonds_b not exported in live artifact (producer todo)", a))
    ofr, a = _feed("data/ofr-stfm.json")
    v = (ofr or {}).get("fails_cross")
    adj = (-0.4 if (isinstance(v, (int, float)) and v > 1.5) else 0.0)
    out["funding"].append(_fi("fails_cross_z", "ofr-stfm", v, adj,
        "fails = collateral scramble [nmq5x0cp7zp4j]; fails_cross not in live artifact yet — producer-side export needed, stays MISSING honestly", a))
    ag, a = _feed("data/auction-grades.json")
    g10 = None
    for r in ((ag or {}).get("graded_auctions") or []):
        if "10" in json.dumps({k: r.get(k) for k in r if not isinstance(r.get(k), dict)}):
            g10 = r.get("overall_grade"); break
    adj = (-0.4 if g10 in ("D", "F", "D-", "D+") else (0.2 if g10 in ("A", "A+") else 0.0))
    out["funding"].append(_fi("auction_10y_grade", "auction-grades", g10, adj,
        "the 10-year auction is THE one to watch [nmq5x0cp8023c]", a))
    cp_doc, a = _feed("data/crisis-plumbing.json")
    v = ((cp_doc or {}).get("composite") or {}).get("composite_stress_score")
    adj = (-0.4 if (isinstance(v, (int, float)) and v > 60) else
           (0.2 if (isinstance(v, (int, float)) and v < 20) else 0.0))
    out["funding"].append(_fi("plumbing_composite", "crisis-plumbing", v, adj,
        "SOMA/TGA/repo composite [nmq5x1e4pod5k]", a))
    xd = (cp_doc or {}).get("xcc_basis_proxy") or {}
    zs = []
    sigs = []
    for sub in ("rate_diff_jpy_3m", "rate_diff_eur_3m", "obfr_iorb_spread"):
        d_ = xd.get(sub) or {}
        z_ = d_.get("z_score_1y")
        if isinstance(z_, (int, float)): zs.append(z_)
        if d_.get("signal"): sigs.append(f"{sub}:{d_['signal']}")
    worst_z = min(zs) if zs else None
    non_normal = any("NORMAL" not in s_ for s_ in sigs) if sigs else False
    v = {"worst_z_1y": worst_z, "signals": sigs} if zs or sigs else None
    adj = (-0.4 if (non_normal or (worst_z is not None and worst_z <= -2)) else 0.0)
    out["funding"].append(_fi("xcc_basis_signals", "crisis-plumbing", v, adj,
        "cross-currency basis proxies (JPY/EUR rate-diff z + OBFR-IORB) — ranked #1 "
        "leading funding signal [nmq5x1qrmghwy]", a))

    # LEG 2 CREDIT — 5-lens composite + IG z-scores
    cc, a = _feed("data/credit-composite.json")
    v = (cc or {}).get("composite")
    adj = (-0.5 if (isinstance(v, (int, float)) and v > 60) else
           (-0.25 if (isinstance(v, (int, float)) and v > 40) else
            (0.25 if (isinstance(v, (int, float)) and v < 20) else 0.0)))
    out["credit"].append(_fi("credit_composite_0_100", "credit-composite", v, adj,
        "5-lens incl %-banks-tightening which follows CCC [nmq8lde8rj3g8]", a))
    cs, a = _feed("data/credit-stress.json")
    zs = [(m or {}).get("z_score_60d") for m in ((cs or {}).get("metrics") or {}).values()]
    zs = [z for z in zs if isinstance(z, (int, float))]
    v = round(sum(zs) / len(zs), 2) if zs else None
    adj = (-0.4 if (v is not None and v > 1.5) else (0.2 if (v is not None and v < -0.5) else 0.0))
    out["credit"].append(_fi("credit_z60_mean", "credit-stress", v, adj,
        "mean spread z across the IG/HY ladder — widening cascade [tv-ba419d7b64a1e75d]", a))
    tf2, a = _feed("data/etf-true-flows.json")
    hyg = (((tf2 or {}).get("by_etf") or {}).get("HYG") or {}).get("net_flow_20d_usd")
    v = round(hyg / 1e9, 2) if isinstance(hyg, (int, float)) else None
    adj = (-0.3 if (v is not None and v < -1.0) else (0.15 if (v is not None and v > 1.0) else 0.0))
    out["credit"].append(_fi("hyg_net_flow_20d_bn", "etf-true-flows", v, adj,
        "HY fund outflows = credit risk-off; HYG/LQD in the fuse-list [nmrdt9tk992wt]", a))

    # LEG 3 DOLLAR/GLOBAL — BTP-Bund/fragmentation + BIS cross-border
    ef, a = _feed("data/euro-fragmentation.json")
    v = ((ef or {}).get("fragmentation") or {}).get("widest_spread_bp") or (ef or {}).get("widest_spread_bp")
    adj = (-0.4 if (isinstance(v, (int, float)) and v > 150) else
           (-0.2 if (isinstance(v, (int, float)) and v > 100) else 0.0))
    out["dollar"].append(_fi("btp_bund_widest_bp", "euro-fragmentation", v, adj,
        "BTP-Bund / fragmentation — fuse-list [nmrdt9tk992wt]", a))
    bis, a = _feed("data/bis-crossborder.json")
    ys = [(r or {}).get("yoy_pct") for r in ((bis or {}).get("by_counterparty") or [])]
    ys = sorted([y for y in ys if isinstance(y, (int, float))])
    v = ys[len(ys)//2] if ys else None
    adj = (-0.3 if (v is not None and v < 0) else (0.2 if (v is not None and v > 8) else 0.0))
    out["dollar"].append(_fi("bis_crossborder_yoy_median", "bis-crossborder", v, adj,
        "eurodollar loan growth = global liquidity creation [nmq5vzr2aozu3]", a))
    ci, a = _feed("data/ciss-stress.json")
    v = (ci or {}).get("ea_regime")
    adj = (-0.3 if (isinstance(v, str) and any(t in v.upper() for t in ("HIGH", "STRESS", "SEVERE", "ELEV"))) else 0.0)
    out["dollar"].append(_fi("ecb_ciss_regime", "ciss-stress", v, adj,
        "euro-area systemic stress regime; sovereign board (data/sovereign-stress.json, "
        "no composite field) stays deferred", a))

    # LEG 4 CARRY/EURODOLLAR — yen-carry scored, plumbing board, China credit
    yc, a = _feed("data/yen-carry.json")
    v = (yc or {}).get("unwind_risk_score")
    adj = (-0.6 if (isinstance(v, (int, float)) and v > 70) else
           (-0.3 if (isinstance(v, (int, float)) and v > 50) else
            (0.2 if (isinstance(v, (int, float)) and v < 25) else 0.0)))
    out["carry"].append(_fi("yen_unwind_risk_0_100", "yen-carry", v, adj,
        "5-factor carry unwind — margin-call cascade [tv-f58a44fc2f839aac]", a))
    ep, a = _feed("data/eurodollar-plumbing.json")
    v = (ep or {}).get("stress_score")
    adj = (-0.5 if (isinstance(v, (int, float)) and v > 60) else
           (0.25 if (isinstance(v, (int, float)) and v < 20) else 0.0))
    out["carry"].append(_fi("eurodollar_stress_0_100", "eurodollar-plumbing", v, adj,
        "the 7-layer offshore-USD transmission board [nmq5x1e4kdcoz]", a))
    cl, a = _feed("data/china-liquidity.json")
    v = ((cl or {}).get("money") or {}).get("m1_yoy_pct")
    adj = (-0.3 if (isinstance(v, (int, float)) and v < 0) else
           (0.2 if (isinstance(v, (int, float)) and v > 8) else 0.0))
    out["carry"].append(_fi("china_m1_yoy_pct", "china-liquidity", v, adj,
        "CNY liquidity/devaluation risk channel [nmq5x0cpig3hx]", a))
    tvj, a = _feed("data/tradingview.json")
    jrow = next((r_ for r_ in ((tvj or {}).get("symbols") or [])
                 if r_.get("symbol") == "JPLG"), {})
    v = jrow.get("value")
    pv = jrow.get("prev")
    adj = 0.0
    if isinstance(v, (int, float)):
        if v < 0:
            adj = -0.4
        elif (isinstance(pv, (int, float)) and v < pv - 0.3) or v < 1.0:
            adj = -0.2
    out["carry"].append(_fi("jplg_loan_growth_yoy", "tradingview-vault(BOJ)", v, adj,
        "Japan bank lending YoY from the BOJ API — 'JPLG decline = FLASH WARNING' "
        "[tv-b3ec3933837d5155]; contraction -0.4, sharp decel -0.2", a))

    # ops4003 jp10y-carry: JGB 10Y from the vault (MOF official curve). Rising
    # long JGB yields raise the funding cost of every yen-carry position —
    # the unwind trigger [tv-9fa576184567fa8f]. Level >2.5% or a >4% jump vs
    # the stored prev shaves the carry leg; absence is neutral, never guessed.
    tvv, a = _feed("data/tradingview.json")
    _row = next((r for r in ((tvv or {}).get("symbols") or [])
                 if r.get("symbol") == "JP10Y"), None)
    v = (_row or {}).get("value")
    if isinstance(v, (int, float)):
        pv = (_row or {}).get("prev")
        adj = 0.0
        if v >= 2.5:
            adj = -0.15
        if isinstance(pv, (int, float)) and pv and (v / pv - 1) > 0.04:
            adj = min(adj, 0.0) - 0.1
        out["carry"].append(_fi("jgb10y_carry_cost", "tradingview-vault(MOF)", v, adj,
            "JGB 10Y par yield — yen carry funding cost; >2.5% -0.15, "
            ">4% jump -0.1 [tv-9fa576184567fa8f]", a))

    # LEG 5 GLOBAL GROWTH — Taiwan/Korea exports, freight, air cargo, ports
    al, a = _feed("data/asia-leads.json")
    v = ((al or {}).get("taiwan_exports") or {}).get("yoy_pct")
    adj = (-0.4 if (isinstance(v, (int, float)) and v < 0) else
           (0.3 if (isinstance(v, (int, float)) and v > 15) else 0.0))
    out["growth"].append(_fi("taiwan_exports_yoy", "asia-leads", v, adj,
        "Taiwan exports YoY — fuse-list [nmrdt9tk992wt]", a))
    fp, a = _feed("data/freight-pulse.json")
    ys = [((fp or {}).get("series") or {}).get(k, {}).get("yoy_pct")
          for k in ("tsi_freight", "cass_shipments", "truck_tonnage", "rail_carloads")]
    ys = sorted([y for y in ys if isinstance(y, (int, float))])
    v = ys[len(ys)//2] if ys else None
    adj = (-0.3 if (v is not None and v < -3) else (0.2 if (v is not None and v > 3) else 0.0))
    out["growth"].append(_fi("freight_yoy_median", "freight-pulse", v, adj,
        "real-economy nowcast", a))
    ac, a = _feed("data/air-cargo.json")
    v = (ac or {}).get("yoy_pct")
    adj = (-0.2 if (isinstance(v, (int, float)) and v < -5) else
           (0.1 if (isinstance(v, (int, float)) and v > 5) else 0.0))
    out["growth"].append(_fi("air_cargo_yoy", "air-cargo", v, adj, "air freight nowcast", a))
    pw, a = _feed("data/portwatch.json")
    ys = [(r or {}).get("yoy_pct") for r in ((pw or {}).get("chokepoints") or [])]
    ys = sorted([y for y in ys if isinstance(y, (int, float))])
    v = ys[len(ys)//2] if ys else None
    adj = (-0.3 if (v is not None and v < -15) else 0.0)
    out["growth"].append(_fi("portwatch_chokepoint_yoy_median", "portwatch", v, adj,
        "median guards against single disrupted-chokepoint noise", a))

    # LEG 6 STRUCTURE — bond-vol board, vol-migration spill, ETF flows, CFTC ctx
    bv, a = _feed("data/bond-vol.json")
    v = (bv or {}).get("composite_z_score")
    fr = ((bv or {}).get("funding_plumbing") or {}).get("regime")
    adj = (-0.5 if (isinstance(v, (int, float)) and v > 1.5) else 0.0)
    adj += (-0.2 if fr == "TIGHTENING" else 0.0)
    out["structure"].append(_fi("bond_vol_z_plus_funding", "bond-vol",
        {"z": v, "funding_regime": fr} if v is not None else None, adj,
        "real MOVE-proxy + funding regime — vol->forced-selling cascade [tv-14a76b6087dc80eb]", a))
    fv, a = _feed("data/fifx-vol-history.json")
    rows = (fv or {}).get("rows") or []
    v = (rows[-1] or {}).get("spill") if rows else None
    adj = (-0.4 if (isinstance(v, (int, float)) and v > 2) else 0.0)
    out["structure"].append(_fi("vol_migration_spill_z", "fifx-vol-history", v, adj,
        "cross-asset vol spillover; breadth 100% in 2008 AND 2020", a))
    tf, a = _feed("data/etf-true-flows.json")
    tot = 0.0; seen = False
    for side in ("inflows", "outflows"):
        for r in ((tf or {}).get(side) or []):
            fl = (r or {}).get("net_flow_20d_usd")
            if isinstance(fl, (int, float)):
                tot += fl; seen = True
    v = round(tot / 1e9, 2) if seen else None
    adj = (-0.3 if (v is not None and v < -10) else (0.2 if (v is not None and v > 10) else 0.0))
    out["structure"].append(_fi("etf_net_flow_20d_usd_bn", "etf-true-flows", v, adj,
        "capital risk-on/off direction feeds signals [Khalid 2026-06-09]", a))
    cf, a = _feed("data/cftc-deep-view.json")
    rows = (cf or {}).get("all_contract_analyses") or []
    v = len(rows) if rows else None
    tv, a = _feed("data/tradingview.json")
    tvi = {r_.get("symbol"): r_ for r_ in ((tv or {}).get("symbols") or [])}
    mv = (tvi.get("MOVE") or {}).get("value")
    adj = (-0.4 if (isinstance(mv, (int, float)) and mv > 130) else
           (-0.2 if (isinstance(mv, (int, float)) and mv > 110) else 0.0))
    out["structure"].append(_fi("move_index", "tradingview-vault", mv, adj,
        "REAL MOVE via the vault — margin-call->forced-selling->QE mechanism "
        "[tv-14a76b6087dc80eb]", a))
    cl_front = (tvi.get("CL1!") or {}).get("value")
    spot = (tvi.get("WTI") or {}).get("value") or (tvi.get("USOIL") or {}).get("value")
    bw = (round((cl_front / spot - 1) * 100, 2)
          if isinstance(cl_front, (int, float)) and isinstance(spot, (int, float)) and spot
          else None)
    adj = (-0.4 if (bw is not None and bw < -1.0) else 0.0)
    out["structure"].append(_fi("oil_backwardation_front_vs_spot_pct", "tradingview-vault",
        bw, adj,
        "front-month vs spot proxy — 'oil backwardation preceded every crisis' "
        "[nmrdt9tk992wt]; full-curve version still needs a real strip source", a))
    out["structure"].append(_fi("cftc_dealer_positioning", "cftc-deep-view", v, 0.0,
        "context only until n>=26 weekly reports [rotation-arc COT rule]; "
        "excessive longs = vulnerable setup [nmq5x0b27is35]", a))
    return out

def read_feed(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[risk-gate] {MARKER}")

    F = {}
    for sid in SERIES:
        F[sid] = fred(sid)
        print(f"[risk-gate] {sid}: {len(F[sid])} obs")
    if sum(1 for s in F.values() if s) < 12:
        raise RuntimeError("too few FRED series resolved — refusing to publish a fake gate")

    calendar = build_calendar({k: v for k, v in F.items() if k != "SP500"})
    # forward-fill every series onto the union calendar (weekly/monthly legs)
    for sid in F:
        F[sid] = ffill_on(calendar, F[sid])

    # replay from 2023-01-01 (baselines need the 2018+ tail)
    start_i = next((i for i, d in enumerate(calendar) if d >= "2023-01-01"), 0)
    postures, composites = [None] * len(calendar), [None] * len(calendar)
    for i in range(start_i, len(calendar)):
        p, c, _ = compute_posture(F, calendar, i)
        postures[i], composites[i] = p, c

    # live reading = last day, with full leg detail
    li = len(calendar) - 1
    replay_posture, replay_comp, live_legs = compute_posture(F, calendar, li)

    # v2.0: fuse the approved fleet feeds into the LIVE posture (replay stays
    # FRED-pure). Per-leg fleet adjustment clamped to +/-0.75.
    fleet_in = fleet_adjust(live_legs)
    W = {"funding": .25, "credit": .25, "dollar": .20, "carry": .10,
         "growth": .10, "structure": .10}
    live_comp = 0.0
    for k in W:
        fa = max(-0.75, min(0.75, sum(x["score_adj"] for x in fleet_in.get(k, []))))
        live_legs[k]["fleet_adj"] = round(fa, 3)
        live_legs[k]["fleet_inputs"] = fleet_in.get(k, [])
        live_legs[k]["score_fused"] = round(max(-2.0, min(2.0, live_legs[k]["score"] + fa)), 3)
        live_comp += live_legs[k]["score_fused"] * W[k]
    live_comp = round(live_comp, 3)
    if live_legs["funding"]["score_fused"] <= -2 and live_legs["credit"]["score_fused"] <= -1:
        live_posture = "SEVERE"
    elif live_comp >= 0.35: live_posture = "RISK_ON"
    elif live_comp > -0.35: live_posture = "NEUTRAL"
    elif live_comp > -0.95: live_posture = "RISK_OFF"
    else: live_posture = "SEVERE"

    # existing-fleet context (consumed, never duplicated) — live only
    yen = read_feed("data/yen-carry.json")
    crisis = read_feed("data/crisis-composite.json")
    fleet_context = {
        "yen_carry_composite": (yen or {}).get("composite") or (yen or {}).get("headline"),
        "crisis_composite": (crisis or {}).get("composite") or (crisis or {}).get("headline"),
    }

    es = event_study(F, calendar, postures)

    # recent posture timeline (last 90 days, thinned)
    timeline = [{"date": calendar[i], "posture": postures[i], "composite": composites[i]}
                for i in range(max(start_i, li - 90), li + 1, 3)]

    out = {
        "engine": "justhodl-risk-gate",
        "version": "1.0",
        "marker": MARKER,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "brain_constitution": {
            "directive": "Khalid 2026-07-26: brain is how the system thinks; all "
                         "decisions/analysis based on brain. Every rule above cites "
                         "the note ID it implements.",
            "series_to_note": SERIES,
            "hierarchy": "macro gates SIZING before selection [nmq5x0cp7zp4j, "
                         "tv-c8640dea0c15ee5c]; never touch stocks when plumbing "
                         "is shaky [nmq5vhvebjob6]",
        },
        "posture": live_posture,
        "composite": live_comp,
        "replay_posture_fred_only": replay_posture,
        "replay_composite_fred_only": replay_comp,
        "sizing_multiplier": SIZING[live_posture],
        "legs": live_legs,
        "fleet_context": fleet_context,
        "event_study": es,
        "recent_timeline": timeline,
        "consume_as": "multiply position size / conviction by sizing_multiplier; "
                      "RISK_OFF tightens verdict thresholds; SEVERE = distressed-"
                      "buyer posture only (cash/gold/quality per pinned seed1)",
        "elapsed_s": round(time.time() - t0, 1),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str),
                  ContentType="application/json", CacheControl="max-age=600")
    print(f"[risk-gate] DONE {out['elapsed_s']}s posture={live_posture} "
          f"comp={live_comp} flips={es['n_flips_to_risk_off_or_worse']}")
    return {"ok": True, "posture": live_posture, "composite": live_comp,
            "sizing_multiplier": out["sizing_multiplier"],
            "n_flips": es["n_flips_to_risk_off_or_worse"]}
