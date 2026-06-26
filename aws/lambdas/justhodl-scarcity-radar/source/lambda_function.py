"""
justhodl-scarcity-radar  ·  v1.0  —  THE NEXT-SHORTAGE RADAR
================================================================================
The Micron pattern: a supply shortage in an inelastic, oligopoly market meets a
demand inflection, the underlying price breaks out, and the few suppliers re-rate
violently. The alpha is NOT the shortage everyone sees — it's the vertical whose
LEADING INDICATORS are inflecting while the equity is still cheap, un-moved, and
ignored. This synthesizer fuses the eight engines that each see one facet into one
ranked board on two axes:

  SCARCITY (is a real shortage building?)
    = vertical tightness (supply-inflection)         — the input is inflecting
    + company capture   (bottleneck-boom)            — revenue accelerating, inventory drawing
    + pricing power     (chokepoint criticality)     — oligopoly that can't be routed around
    + spot momentum     (commodity-curves)

  STEALTH (is no one looking yet?)
    = cheap             (low P/S, discount-to-fair)
    + un-moved          (supply-chain laggard / low recent perf)
    + quiet narrative   (narrative-vs-tape quiet accumulation — smart money buying, no buzz)
    + early theme phase (themes-detected: EMERGING/ACCELERATING, not EXTENDED)

  HEADLINE = high SCARCITY × high STEALTH  →  tightening hard, pricing power, cheap, silent.
  Plus a per-vertical tightness ranking so you see WHICH shortage is building.

Top names are scorecard-graded on forward excess-vs-SPY — measure-before-trust.
OUTPUT: data/scarcity-radar.json     SCHEDULE: daily
"""
import json, time, boto3, math
from datetime import datetime, timezone
from decimal import Decimal

S3 = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/scarcity-radar.json"
VERSION = "1.0.0"

# industry/sector keyword -> the supply-inflection / themes-detected theme ETF
IND_THEME = [
    (("semiconduct", "memory", "electronic compon", "semicap"), "SMH"),
    (("uranium", "nuclear"),                                     "URA"),
    (("copper",),                                                "COPX"),
    (("lithium", "battery"),                                     "LIT"),
    (("rare earth",),                                            "REMX"),
    (("steel", "iron", "aluminum", "metals & mining", "mining"), "PICK"),
    (("gold", "silver", "precious"),                             "GDX"),
    (("oil", "gas", "petroleum", "drilling", "refin"),           "XLE"),
    (("power", "utilit", "electric", "grid", "transmission"),    "GRID"),
    (("aerospace", "defense"),                                   "ITA"),
    (("homebuild", "lumber", "building products", "construction"), "XHB"),
    (("biotech", "pharmaceutic", "drug manufact"),               "XBI"),
    (("reit", "real estate"),                                    "XLRE"),
]


def _read(k):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
    except Exception:
        return None


def clamp(v, lo=0.0, hi=100.0):
    return max(lo, min(hi, v))


def theme_for(industry, sector):
    s = ((industry or "") + " " + (sector or "")).lower()
    for kws, etf in IND_THEME:
        if any(k in s for k in kws):
            return etf
    return None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    si = _read("data/supply-inflection.json") or {}
    bb = _read("data/bottleneck-boom.json") or {}
    cp = _read("data/chokepoint.json") or {}
    scg = _read("data/supply-chain-graph.json") or {}
    nvt = _read("data/narrative-vs-tape.json") or {}
    td = _read("data/themes-detected.json") or {}

    by_theme = si.get("by_theme") or {}              # etf -> {composite_inflection_score, ...}
    theme_phase = {t.get("etf"): t.get("phase") for t in (td.get("themes") or [])}
    phase_stealth = {"DORMANT": 60, "EMERGING": 100, "ACCELERATING": 75, "EXTENDED": 15, None: 50}

    def tightness(etf):
        return float((by_theme.get(etf) or {}).get("composite_inflection_score") or 0)

    # ── assemble candidate names with whatever each engine knows ──
    names = {}  # ticker -> dict

    def touch(tk, name=None, industry=None, sector=None):
        d = names.setdefault(tk, {"ticker": tk, "name": name, "industry": industry, "sector": sector,
                                   "engines": [], "capture": 0, "pricing_power": 0, "cheap": 0,
                                   "quiet": 0, "un_moved": 0, "vertical": None})
        if name and not d.get("name"):
            d["name"] = name
        if industry and not d.get("industry"):
            d["industry"] = industry
        if sector and not d.get("sector"):
            d["sector"] = sector
        return d

    # 1. bottleneck-boom -> capture + cheapness + inventory draw
    for r in (bb.get("ranks") or []):
        tk = r.get("ticker")
        if not tk:
            continue
        d = touch(tk, r.get("name"), r.get("industry"), r.get("sector"))
        d["engines"].append("bottleneck-boom")
        zsum = sum(r.get(z, 0) or 0 for z in ("z_rev_growth_yoy", "z_rev_accel_pp", "z_rev_to_mcap_pct"))
        cap = r.get("boom_score") or r.get("score")
        d["capture"] = max(d["capture"], float(cap) if isinstance(cap, (int, float)) else clamp(zsum / 9 * 100))
        ps = r.get("ps_ttm")
        if isinstance(ps, (int, float)) and ps > 0:
            d["cheap"] = max(d["cheap"], clamp((4.0 - ps) / 4.0 * 100))   # ps<4 = cheap-ish
        if (r.get("inv_turnover") or 0) > 8:
            d["capture"] = min(100, d["capture"] + 8)                     # inventory drawing fast

    # 2. chokepoint -> pricing power; cheap_chokepoint_book = pricing power + cheap (ideal)
    crit = {}
    for L in ("all_chokepoints", "discovered_chokepoint_book", "hidden_chokepoint_book"):
        for r in (cp.get(L) or []):
            if r.get("ticker"):
                crit[r["ticker"]] = max(crit.get(r["ticker"], 0), float(r.get("criticality") or 0))
    cheap_choke = {r.get("ticker") for r in (cp.get("cheap_chokepoint_book") or []) if r.get("ticker")}
    for tk, c in crit.items():
        d = touch(tk)
        if "chokepoint" not in d["engines"]:
            d["engines"].append("chokepoint")
        d["pricing_power"] = max(d["pricing_power"], c)
    for tk in cheap_choke:
        d = touch(tk)
        d["cheap"] = max(d["cheap"], 80)          # an oligopoly trading cheap is the prize

    # 3. supply-chain laggards -> un-moved supplier to a booming hub
    for r in (scg.get("supply_chain_laggards") or []):
        tk = r.get("ticker")
        if not tk:
            continue
        d = touch(tk)
        d["engines"].append("supply-chain-laggard")
        gap = r.get("lag_gap_pct") or 0
        d["un_moved"] = max(d["un_moved"], clamp(abs(gap) * 2))        # bigger lag vs booming customer
        d["supplies_to"] = r.get("supplies_to")

    # 4. narrative-vs-tape quiet accumulation -> nobody looking + smart money buying
    for r in (nvt.get("quiet_accumulation") or []):
        tk = r.get("ticker")
        if not tk:
            continue
        d = touch(tk)
        d["engines"].append("quiet-accumulation")
        d["quiet"] = max(d["quiet"], 85)
        d["smart_money"] = r.get("tape")

    # ── score every candidate on scarcity × stealth ──
    board = []
    for tk, d in names.items():
        etf = theme_for(d.get("industry"), d.get("sector"))
        d["vertical"] = etf
        vt = tightness(etf) if etf else 0
        phase = theme_phase.get(etf)
        d["vertical_tightness"] = round(vt, 1)
        d["theme_phase"] = phase

        scarcity = clamp(0.42 * vt + 0.33 * d["capture"] + 0.25 * d["pricing_power"])
        # un-moved if we have a lag signal OR an extended-phase penalty isn't applied
        stealth = clamp(0.34 * d["cheap"] + 0.28 * d["quiet"] + 0.20 * d["un_moved"]
                        + 0.18 * phase_stealth.get(phase, 50))
        composite = round(math.sqrt(max(0, scarcity) * max(0, stealth)), 1)   # BOTH must be high

        d["scarcity"] = round(scarcity, 1)
        d["stealth"] = round(stealth, 1)
        d["composite"] = composite
        d["engines"] = sorted(set(d["engines"]))
        board.append(d)

    board.sort(key=lambda x: -x["composite"])
    # the headline: real shortage building AND still ignored
    headline = [b for b in board if b["scarcity"] >= 35 and b["stealth"] >= 45][:25]

    # ── per-vertical tightness ranking (which shortage is building) ──
    sig_summary = (si.get("summary") or {}).get("top_signals") or []
    verticals = []
    for etf, info in by_theme.items():
        ts = float(info.get("composite_inflection_score") or 0)
        if ts <= 0:
            continue
        nm = [b["ticker"] for b in board if b.get("vertical") == etf][:6]
        verticals.append({"theme": etf, "tightness": round(ts, 1),
                          "phase": theme_phase.get(etf),
                          "n_strong_tightening": info.get("n_strong_tightening"),
                          "top_signals": [s.get("signal") for s in (info.get("top_signals") or [])][:3],
                          "candidate_names": nm})
    verticals.sort(key=lambda v: -v["tightness"])

    out = {
        "engine": "scarcity-radar", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1),
        "thesis": ("The next Micron before it's a Micron: fuses vertical tightness + company capture + "
                   "oligopoly pricing power (scarcity) with cheap + un-moved + quiet + early-phase (stealth). "
                   "High on BOTH = a shortage building that no one is pricing yet."),
        "inputs_live": {k: bool(v) for k, v in [("supply-inflection", si), ("bottleneck-boom", bb),
                        ("chokepoint", cp), ("supply-chain-graph", scg), ("narrative-vs-tape", nvt),
                        ("themes-detected", td)]},
        "counts": {"candidates": len(board), "headline": len(headline)},
        "headline_board": headline,
        "scarcity_leaders": sorted(board, key=lambda x: -x["scarcity"])[:15],
        "stealth_leaders": sorted([b for b in board if b["scarcity"] >= 25], key=lambda x: -x["stealth"])[:15],
        "vertical_tightness": verticals,
        "strong_tightening_signals": [s for s in sig_summary if str(s.get("flag", "")).upper().startswith("STRONG")][:10],
        "method": "scarcity = 0.42 vertical_tightness + 0.33 capture + 0.25 pricing_power; "
                  "stealth = 0.34 cheap + 0.28 quiet + 0.20 un_moved + 0.18 phase; composite = sqrt(scarcity*stealth)",
        "disclaimer": "Synthesis of the platform's own supply/scarcity engines — research, not advice.",
    }

    # closed loop: grade the strongest headline names forward vs SPY
    try:
        nowt = datetime.now(timezone.utc)
        tbl = boto3.resource("dynamodb", "us-east-1").Table("justhodl-signals")
        logged = 0
        for r in headline[:8]:
            tbl.put_item(Item={
                "signal_id": f"scarcity-radar#{r['ticker']}#{nowt.date().isoformat()}",
                "signal_type": "scarcity_radar", "predicted_direction": "UP",
                "signal_value": str(r["composite"]), "confidence": Decimal("0.55"),
                "measure_against": "ticker_vs_benchmark", "benchmark": "SPY",
                "check_windows": ["day_21", "day_63", "day_126"], "outcomes": {}, "accuracy_scores": {},
                "status": "pending", "logged_at": nowt.isoformat(), "logged_epoch": int(nowt.timestamp()),
                "horizon_days_primary": 63, "schema_version": "2",
                "ttl": int(nowt.timestamp()) + 200 * 86400,
                "metadata": {"engine": "scarcity-radar", "v": VERSION, "vertical": r.get("vertical"),
                             "scarcity": r["scarcity"], "stealth": r["stealth"], "engines": r["engines"]},
                "rationale": f"{r['ticker']} scarcity {r['scarcity']} x stealth {r['stealth']} "
                             f"vertical {r.get('vertical')} via {r['engines']}"})
            logged += 1
        out["signals_logged"] = logged
    except Exception as e:
        print(f"[loop] {str(e)[:80]}")

    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[scarcity-radar] candidates={len(board)} headline={len(headline)} "
          f"top_verticals={[v['theme'] for v in verticals[:4]]} {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps(out["counts"])}
