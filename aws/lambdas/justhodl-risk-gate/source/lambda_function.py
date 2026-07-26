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
MARKER = "risk-gate v1.0 BRAIN-CONSTITUTIONAL"

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
    legs["funding"] = {"score": max(-2.0, min(2.0, funding)), "why": notes1,
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
    composite = sum(legs[k]["score"] * W[k] for k in W)

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
    live_posture, live_comp, live_legs = compute_posture(F, calendar, li)

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
