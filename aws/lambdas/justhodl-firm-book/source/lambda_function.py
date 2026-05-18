"""
justhodl-firm-book -- the consolidated cross-desk position blotter.
==================================================================
WHY THIS EXISTS
---------------
The platform has seven strategy desks and a Desk Allocator that sizes
each desk's share of firm capital. But nothing answers the question a
real trading desk asks first every morning: "what does the firm
actually OWN, netted, right now?" The desks each publish their own book;
the allocator publishes desk-level capital weights; no layer collapses
all of that into a single name-level book.

This is the BLOTTER -- the #1 screen on any multi-manager trading desk
(Millennium / Citadel / Point72). It takes the allocator's desk weights,
each desk's positions, and produces ONE firm-level book:

  * every desk's positions are sized within the desk by conviction and
    scaled to that desk's allocator capital weight;
  * the same ticker held by more than one desk is NETTED -- a name long
    in Best Ideas and short in Risk Radar offsets, exactly as it would
    in a real prime-brokerage account;
  * the firm-level risk numbers a desk head watches roll up: gross and
    net exposure, long/short split, top-10 concentration, sector tilts,
    largest single-name risk;
  * cross-desk CONVICTION OVERLAP is surfaced (names multiple desks
    independently want -- the highest-confidence book) and so are
    DESK CONFLICTS (one desk long, another short the same name).

Two sleeves are kept separate, because they do not net against each
other: the EQUITY sleeve (single stocks -- best-ideas, merger-arb,
spin-off, index-recon, risk-radar, and both pairs legs) and the MACRO
sleeve (trend-engine's cross-asset futures/ETF book).

Reads only existing S3 sidecars. Pure synthesis -- no external API,
nothing redundant. The model book the desk stack implies; distinct from
pm-decision, which acts on the user's actual portfolio.

OUTPUT   data/firm-book.json          SCHEDULE  daily 01:00 UTC
"""
import json
import time
from datetime import datetime, timezone

import boto3

S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/firm-book.json"
ALLOC_KEY = "data/desk-allocator.json"
SCHEMA = "1.0"

s3 = boto3.client("s3", region_name="us-east-1")

EPS = 1e-9
INDEX_RECON_CAP = 25   # top-N per side from index-recon -- a real desk
#                        concentrates the flow book on the highest-edge names


# ---- desk extraction registry ---------------------------------------------
# mode per array: LONG / SHORT (fixed side), DIRECTION (read item field),
# PAIR (split long_leg / short_leg, dollar-neutral).
DESK_SPECS = {
    "best-ideas": {
        "json": "data/best-ideas.json", "sleeve": "EQUITY",
        "arrays": [("stack", "LONG")],
        "sym": "symbol", "name": "name", "price": "price",
        "sector": "sector", "score": "conviction_score",
    },
    "merger-arb": {
        "json": "data/merger-arb.json", "sleeve": "EQUITY",
        "arrays": [("all_priced", "LONG")],
        "sym": "target", "name": "target_name", "price": "target_price",
        "sector": None, "score": "reward_risk",
        "score_alt": "annualized_return_pct",
    },
    "spinoff-desk": {
        "json": "data/spinoff-desk.json", "sleeve": "EQUITY",
        "arrays": [("top_setups", "LONG")],
        "sym": "symbol", "name": "name", "price": "price",
        "sector": "sector", "score": "spinoff_score",
    },
    "risk-radar": {
        "json": "data/risk-radar.json", "sleeve": "EQUITY",
        "arrays": [("stack", "SHORT")],
        "sym": "symbol", "name": "name", "price": "price",
        "sector": "sector", "score": "deterioration_score",
    },
    "index-recon": {
        "json": "data/index-recon.json", "sleeve": "EQUITY",
        "arrays": [("russell_2000_additions", "LONG"),
                   ("russell_2000_deletions", "SHORT")],
        "sym": "symbol", "name": "name", "price": "price",
        "sector": "sector", "score": "edge_score",
        "cap_per_array": INDEX_RECON_CAP,
    },
    "trend-engine": {
        "json": "data/trend-engine.json", "sleeve": "MACRO",
        "arrays": [("positions", "DIRECTION")],
        "sym": "symbol", "name": "name", "price": "price",
        "sector": "asset_class", "score": "target_weight_pct",
        "direction_field": "direction",
    },
    "pairs-arb": {
        "json": "data/pairs-arb.json", "sleeve": "EQUITY",
        "arrays": [("pairs", "PAIR")],
        "score": "score", "sector": "sector",
        "long_field": "long_leg", "short_field": "short_leg",
    },
}

DESK_NAMES = {
    "best-ideas": "Best Ideas", "merger-arb": "Merger-Arb Desk",
    "spinoff-desk": "Spin-Off Desk", "risk-radar": "Risk Radar",
    "index-recon": "Index-Recon Desk", "trend-engine": "Trend Desk",
    "pairs-arb": "Pairs Desk",
}


# ---- helpers ---------------------------------------------------------------
def get_json(key):
    try:
        return json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def num(v, default=0.0):
    try:
        f = float(v)
        if f != f:          # NaN
            return default
        return f
    except Exception:
        return default


def pos_score(v):
    """Conviction weight floored to a small positive -- never zero/negative."""
    f = num(v, 0.0)
    return f if f > EPS else 0.05


def extract_desk(desk_key, spec):
    """Pull a desk's raw positions: list of dicts with symbol/side/score.

    side is +1 long / -1 short. For PAIR arrays each pair yields two
    rows. Returns (positions, note).
    """
    doc = get_json(spec["json"])
    if not isinstance(doc, dict):
        return [], "sidecar missing"
    out = []
    for arr_name, mode in spec["arrays"]:
        rows = doc.get(arr_name)
        if not isinstance(rows, list):
            continue
        if mode == "PAIR":
            for r in rows:
                if not isinstance(r, dict):
                    continue
                lg = r.get(spec["long_field"])
                sg = r.get(spec["short_field"])
                sc = pos_score(r.get(spec["score"]))
                sec = r.get(spec.get("sector")) or "Unknown"
                if lg:
                    out.append({"symbol": str(lg).upper().strip(),
                                "name": str(lg).upper().strip(),
                                "sector": sec, "price": None,
                                "side": 1, "raw_score": sc,
                                "pair_leg": True})
                if sg:
                    out.append({"symbol": str(sg).upper().strip(),
                                "name": str(sg).upper().strip(),
                                "sector": sec, "price": None,
                                "side": -1, "raw_score": sc,
                                "pair_leg": True})
            continue
        # rank/cap by score if the array is capped
        cap = spec.get("cap_per_array")
        items = rows
        if cap and len(rows) > cap:
            items = sorted(
                [r for r in rows if isinstance(r, dict)],
                key=lambda r: -pos_score(r.get(spec["score"])))[:cap]
        for r in items:
            if not isinstance(r, dict):
                continue
            sym = r.get(spec["sym"])
            if not sym:
                continue
            if mode == "DIRECTION":
                d = str(r.get(spec.get("direction_field", "direction"),
                              "")).lower()
                side = -1 if d.startswith("short") else 1
            elif mode == "SHORT":
                side = -1
            else:
                side = 1
            sc = pos_score(r.get(spec["score"]))
            if "score_alt" in spec and num(r.get(spec["score"])) <= EPS:
                sc = pos_score(r.get(spec["score_alt"]))
            out.append({
                "symbol": str(sym).upper().strip(),
                "name": r.get(spec.get("name")) or str(sym).upper().strip(),
                "sector": (r.get(spec["sector"]) if spec.get("sector")
                           else "Special Situations") or "Unknown",
                "price": num(r.get(spec.get("price")), None)
                if spec.get("price") else None,
                "side": side, "raw_score": sc, "pair_leg": False,
            })
    return out, ("%d positions" % len(out))


# ---- core ------------------------------------------------------------------
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    alloc = get_json(ALLOC_KEY)
    if not isinstance(alloc, dict) or not alloc.get("desks"):
        return {"statusCode": 500,
                "body": json.dumps({"ok": False,
                                     "error": "desk-allocator.json missing"})}

    # desk capital weights (pct of firm capital) from the allocator
    cap = {}
    desk_status = {}
    for d in alloc.get("desks", []):
        cap[d.get("key")] = num(d.get("capital_weight_pct"), 0.0)
        desk_status[d.get("key")] = d.get("status")

    # ---- build each desk's sized, signed positions ----
    desk_rows = {}
    desk_meta = {}
    for dk, spec in DESK_SPECS.items():
        raw, note = extract_desk(dk, spec)
        desk_cap = cap.get(dk, 0.0)
        # within-desk normalisation: sum of |weight| within a desk == 1.0
        tot_score = sum(p["raw_score"] for p in raw) or 1.0
        sized = []
        for p in raw:
            within = p["raw_score"] / tot_score      # gross share in desk
            if p["pair_leg"]:
                within *= 0.5                        # pair leg = half-pair
            firm_signed = p["side"] * within * desk_cap   # pct of firm cap
            sized.append({**p, "within_share": within,
                          "firm_signed_pct": firm_signed,
                          "firm_gross_pct": abs(firm_signed)})
        desk_rows[dk] = sized
        desk_meta[dk] = {
            "name": DESK_NAMES.get(dk, dk),
            "capital_pct": round(desk_cap, 2),
            "status": desk_status.get(dk),
            "sleeve": spec["sleeve"],
            "n_positions": len(sized),
            "gross_contributed_pct": round(
                sum(p["firm_gross_pct"] for p in sized), 2),
            "note": note,
        }

    # ---- net the EQUITY sleeve by symbol ----
    book = {}     # symbol -> aggregate
    for dk, rows in desk_rows.items():
        if DESK_SPECS[dk]["sleeve"] != "EQUITY":
            continue
        for p in rows:
            sym = p["symbol"]
            b = book.setdefault(sym, {
                "symbol": sym, "name": p["name"], "sector": p["sector"],
                "price": p["price"], "net_pct": 0.0, "gross_pct": 0.0,
                "long_pct": 0.0, "short_pct": 0.0,
                "desks": {}, "n_desks": 0})
            b["net_pct"] += p["firm_signed_pct"]
            b["gross_pct"] += p["firm_gross_pct"]
            if p["firm_signed_pct"] >= 0:
                b["long_pct"] += p["firm_signed_pct"]
            else:
                b["short_pct"] += p["firm_signed_pct"]
            b["desks"][dk] = round(
                b["desks"].get(dk, 0.0) + p["firm_signed_pct"], 4)
            if p["price"] and not b["price"]:
                b["price"] = p["price"]
            if p["name"] and (not b["name"] or b["name"] == sym):
                b["name"] = p["name"]
            if p["sector"] and b["sector"] in (None, "Unknown"):
                b["sector"] = p["sector"]

    equity = []
    conflicts = []
    overlap = []
    for sym, b in book.items():
        b["n_desks"] = len(b["desks"])
        b["net_pct"] = round(b["net_pct"], 4)
        b["gross_pct"] = round(b["gross_pct"], 4)
        b["side"] = ("LONG" if b["net_pct"] > EPS else
                     "SHORT" if b["net_pct"] < -EPS else "FLAT")
        # desk conflict: at least one long-leaning and one short-leaning desk
        signs = [1 if w > EPS else -1 if w < -EPS else 0
                 for w in b["desks"].values()]
        b["desk_conflict"] = (1 in signs and -1 in signs)
        equity.append(b)
        if b["desk_conflict"]:
            conflicts.append(b)
        elif b["n_desks"] >= 2:
            overlap.append(b)

    equity.sort(key=lambda x: -x["gross_pct"])
    overlap.sort(key=lambda x: (-x["n_desks"], -x["gross_pct"]))
    conflicts.sort(key=lambda x: -x["gross_pct"])

    # ---- MACRO sleeve (kept separate -- does not net vs equities) ----
    macro = []
    for p in desk_rows.get("trend-engine", []):
        macro.append({
            "symbol": p["symbol"], "name": p["name"],
            "asset_class": p["sector"],
            "side": "LONG" if p["side"] > 0 else "SHORT",
            "weight_pct": round(p["firm_signed_pct"], 4),
            "gross_pct": round(p["firm_gross_pct"], 4),
        })
    macro.sort(key=lambda x: -x["gross_pct"])

    # ---- firm-level roll-up ----
    eq_gross = sum(b["gross_pct"] for b in equity)
    eq_net = sum(b["net_pct"] for b in equity)
    eq_long = sum(b["long_pct"] for b in equity)
    eq_short = sum(b["short_pct"] for b in equity)
    macro_gross = sum(m["gross_pct"] for m in macro)
    macro_net = sum(m["weight_pct"] for m in macro)
    firm_gross = eq_gross + macro_gross
    firm_net = eq_net + macro_net

    top10 = equity[:10]
    top10_gross = sum(b["gross_pct"] for b in top10)

    # sector exposure (equity sleeve)
    sec = {}
    for b in equity:
        s = sec.setdefault(b["sector"] or "Unknown",
                           {"sector": b["sector"] or "Unknown",
                            "net_pct": 0.0, "gross_pct": 0.0, "n": 0})
        s["net_pct"] += b["net_pct"]
        s["gross_pct"] += b["gross_pct"]
        s["n"] += 1
    sectors = sorted(sec.values(), key=lambda x: -x["gross_pct"])
    for s in sectors:
        s["net_pct"] = round(s["net_pct"], 3)
        s["gross_pct"] = round(s["gross_pct"], 3)

    longs = [b for b in equity if b["side"] == "LONG"]
    shorts = [b for b in equity if b["side"] == "SHORT"]
    biggest_long = max(longs, key=lambda x: x["net_pct"]) if longs else None
    biggest_short = min(shorts, key=lambda x: x["net_pct"]) if shorts else None

    ls_ratio = (abs(eq_long / eq_short) if abs(eq_short) > EPS else None)

    headline = (
        "Firm book: %d net equity names, %.0f%% gross / %+.0f%% net "
        "(%.0f%% long vs %.0f%% short), top-10 = %.0f%% of gross. "
        "%d cross-desk conviction overlaps, %d desk conflicts. "
        "Macro sleeve %d positions, %+.0f%% net."
        % (len(equity), eq_gross, eq_net, eq_long, abs(eq_short),
           (top10_gross / eq_gross * 100 if eq_gross > EPS else 0),
           len(overlap), len(conflicts), len(macro), macro_net))

    def slim(b, extra=False):
        d = {"symbol": b["symbol"], "name": b["name"],
             "sector": b["sector"], "price": b["price"],
             "side": b["side"], "net_pct": b["net_pct"],
             "gross_pct": b["gross_pct"], "n_desks": b["n_desks"],
             "desks": {DESK_NAMES.get(k, k): v
                       for k, v in b["desks"].items()},
             "desk_conflict": b["desk_conflict"]}
        return d

    payload = {
        "schema_version": SCHEMA,
        "engine": "justhodl-firm-book",
        "generated_at": now.isoformat(),
        "build_seconds": round(time.time() - t0, 2),
        "headline": headline,
        "firm": {
            "gross_exposure_pct": round(firm_gross, 2),
            "net_exposure_pct": round(firm_net, 2),
            "equity_gross_pct": round(eq_gross, 2),
            "equity_net_pct": round(eq_net, 2),
            "equity_long_pct": round(eq_long, 2),
            "equity_short_pct": round(eq_short, 2),
            "long_short_ratio": round(ls_ratio, 2) if ls_ratio else None,
            "macro_gross_pct": round(macro_gross, 2),
            "macro_net_pct": round(macro_net, 2),
            "n_equity_names": len(equity),
            "n_longs": len(longs),
            "n_shorts": len(shorts),
            "n_macro_positions": len(macro),
            "top10_concentration_pct": round(
                top10_gross / eq_gross * 100, 1) if eq_gross > EPS else 0,
            "biggest_long": ({"symbol": biggest_long["symbol"],
                              "net_pct": biggest_long["net_pct"]}
                             if biggest_long else None),
            "biggest_short": ({"symbol": biggest_short["symbol"],
                               "net_pct": biggest_short["net_pct"]}
                              if biggest_short else None),
        },
        "equity_book": [slim(b) for b in equity],
        "macro_book": macro,
        "sector_exposure": sectors,
        "conviction_overlap": [slim(b) for b in overlap[:25]],
        "desk_conflicts": [slim(b) for b in conflicts],
        "desk_contributions": desk_meta,
        "how_to_read": (
            "This is the firm's consolidated book -- what the desk stack "
            "implies the fund holds once every desk is sized to its "
            "allocator capital weight and the same ticker is netted "
            "across desks. Gross is total exposure long plus short; net "
            "is the directional tilt. Conviction overlap lists names more "
            "than one desk independently wants -- the highest-confidence "
            "positions. Desk conflicts are names one desk is long and "
            "another is short; they net down but flag genuine internal "
            "disagreement worth a look. The macro sleeve (Trend Desk "
            "cross-asset futures) is shown separately because it does not "
            "net against single stocks."),
        "methodology": (
            "Each desk's positions are weighted within the desk by "
            "conviction (Best Ideas conviction_score, Merger-Arb "
            "reward/risk, Spin-Off score, Risk Radar deterioration score, "
            "Index-Recon edge score, Trend target weight, Pairs spread "
            "score) so the sum of position gross inside a desk is 1.0, "
            "then scaled by that desk's Desk Allocator capital weight. "
            "Long-only desks deploy long, Risk Radar short, Pairs "
            "dollar-neutral per pair, Index-Recon long the index "
            "additions and short the deletions, Trend by signal "
            "direction. Index-Recon is concentrated to its top %d names "
            "per side. Stock-for-stock merger hedges (short the acquirer) "
            "are not modelled in v1 -- the target long is the book. "
            "Research and education only, not investment advice."
            % INDEX_RECON_CAP),
        "disclaimer": (
            "Model book implied by the strategy desks at the allocator's "
            "capital weights -- not the user's actual portfolio and not "
            "investment advice."),
    }

    body = json.dumps(payload, default=str).encode("utf-8")
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=body,
                  ContentType="application/json", CacheControl="max-age=300")

    return {"statusCode": 200,
            "body": json.dumps({"ok": True,
                                "n_equity_names": len(equity),
                                "n_macro": len(macro),
                                "firm_gross_pct": round(firm_gross, 2),
                                "firm_net_pct": round(firm_net, 2),
                                "overlaps": len(overlap),
                                "conflicts": len(conflicts)})}
