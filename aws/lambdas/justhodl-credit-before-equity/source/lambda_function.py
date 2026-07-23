"""justhodl-credit-before-equity v1.0 — CANARY #17.

The premise: the bond desk reprices a company's risk before the equity desk
does. Credit analysts read covenants, maturity walls and cash flow; they move
on balance-sheet facts weeks before those facts show up in an earnings
headline. So when an issuer's CREDIT is improving while its EQUITY is flat or
falling, that gap is information — and it resolves in credit's favour more
often than not.

AUDIT (ops 3754/3755 — why this is not a duplicate):
  · credit-equity-divergence = INDEX level (HYG vs SPY). Macro read; cannot
    name a company.
  · cds-monitor = CreditGrades structural model per name (distance-to-default,
    synthetic CDS bp). Excellent, but POINT-IN-TIME — it answers "who is risky
    today", never "whose credit improved this month while the stock has not
    noticed". The DIRECTION over time is the entire canary, and no ledger
    existed to measure it.
  · credit-composite / credit-stress / cds-proxy = aggregate + sovereign.
This engine adds the missing time dimension and the equity cross.

METHOD
  Leg 1 CREDIT  Δ distance-to-default and Δ synthetic CDS bp per issuer,
                measured against a self-building ledger
                (credit/credit-before-equity-history.json). Credit IMPROVING
                = DD rising and/or synthetic spread tightening.
  Leg 2 EQUITY  price change over the same window from the universe feed.
  SIGNAL        credit improving + equity flat/down = CREDIT_LEADS_UP
                credit deteriorating + equity flat/up = CREDIT_LEADS_DOWN
                (the more dangerous one — bonds smell trouble first)
  GUARD         both legs must clear a minimum move, or it is noise; and the
                engine reports nothing until the ledger has >=2 observations.
                It NEVER manufactures a lead from a single snapshot.

HONEST GAP (shipped in `gaps[]` every run): HY primary ISSUANCE windows are
NOT included. Probed ops 3755 — FRED's issuance series are NBER historical
archives (discontinued) or quarterly Z.1 flow-of-funds; neither is a current
issuance-window signal. SIFMA/TRACE are paid. OAS (BAMLH0A0HYM2) is a PRICE,
not a volume, and is deliberately NOT substituted for issuance.
"""
import json
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
BUCKET = "justhodl-dashboard-live"
SRC_KEY = "data/cds-monitor.json"
OUT_KEY = "data/credit-before-equity.json"
HIST_KEY = "credit/credit-before-equity-history.json"
S3 = boto3.client("s3", region_name="us-east-1")

MIN_DD_MOVE = 0.05        # distance-to-default units
MIN_CDS_MOVE_BP = 5.0     # synthetic spread bp
FLAT_BAND_PCT = 3.0       # equity "hasn't noticed" band
HIST_KEEP = 180


def _load(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print("[cbe] load %s: %s" % (key, str(e)[:90]))
        return default


def _f(v):
    try:
        f = float(v)
        return f if f == f else None      # NaN guard
    except (TypeError, ValueError):
        return None


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    degraded, gaps = [], []

    cm = _load(SRC_KEY) or {}
    snc = cm.get("single_name_cds") or {}
    names = []
    for grp in ("corporates", "banks"):
        for r in (snc.get(grp) or []):
            if r.get("ticker"):
                names.append(r)
    if not names:
        degraded.append("cds-monitor single_name_cds empty — no credit leg")

    uni = _load("data/universe.json") or {}
    px = {}
    for s0 in (uni.get("stocks") or []):
        t = (s0.get("symbol") or "").upper()
        p = _f(s0.get("price"))
        if t and p:
            px[t] = p

    hist = _load(HIST_KEY) or {"obs": {}}
    obs = hist.setdefault("obs", {})

    rows = []
    for r in names:
        tk = (r.get("ticker") or "").upper()
        dd = _f(r.get("distance_to_default"))
        cds = _f(r.get("synthetic_cds_bp"))
        price = px.get(tk)

        rec = obs.setdefault(tk, {})
        rec[today] = {"dd": dd, "cds": cds, "px": price}

        dates = sorted(rec.keys())
        prior_d = dates[-2] if len(dates) >= 2 else None
        prior = rec.get(prior_d) if prior_d else None

        d_dd = d_cds = d_px = None
        if prior:
            if dd is not None and _f(prior.get("dd")) is not None:
                d_dd = round(dd - _f(prior["dd"]), 3)
            if cds is not None and _f(prior.get("cds")) is not None:
                d_cds = round(cds - _f(prior["cds"]), 1)
            pp = _f(prior.get("px"))
            if price is not None and pp:
                d_px = round(100 * (price / pp - 1), 2)

        # credit direction: DD up OR spread tighter = improving
        credit_dir = None
        if d_dd is not None and abs(d_dd) >= MIN_DD_MOVE:
            credit_dir = "IMPROVING" if d_dd > 0 else "DETERIORATING"
        elif d_cds is not None and abs(d_cds) >= MIN_CDS_MOVE_BP:
            credit_dir = "IMPROVING" if d_cds < 0 else "DETERIORATING"

        equity_flat = d_px is not None and abs(d_px) <= FLAT_BAND_PCT
        signal = "INSUFFICIENT_HISTORY" if prior is None else "NONE"
        if credit_dir == "IMPROVING" and d_px is not None and (
                equity_flat or d_px < 0):
            signal = "CREDIT_LEADS_UP"
        elif credit_dir == "DETERIORATING" and d_px is not None and (
                equity_flat or d_px > 0):
            signal = "CREDIT_LEADS_DOWN"

        rows.append({
            "ticker": tk,
            "name": r.get("name"),
            "group": r.get("group"),
            "distance_to_default": dd,
            "synthetic_cds_bp": cds,
            "default_prob_5y_pct": _f(r.get("default_prob_5y_pct")),
            "regime": r.get("regime"),
            "peer_rank": r.get("peer_rank"),
            "market_cap_usd_bn": _f(r.get("market_cap_usd_bn")),
            "d_distance_to_default": d_dd,
            "d_synthetic_cds_bp": d_cds,
            "d_price_pct": d_px,
            "credit_direction": credit_dir,
            "equity_flat": equity_flat,
            "signal": signal,
            "prior_obs_date": prior_d,
            "hist_n": len(dates),
        })

    order = {"CREDIT_LEADS_DOWN": 0, "CREDIT_LEADS_UP": 1,
             "NONE": 2, "INSUFFICIENT_HISTORY": 3}
    rows.sort(key=lambda x: (order.get(x["signal"], 9),
                             -(abs(x["d_synthetic_cds_bp"] or 0))))

    leads = [r for r in rows if r["signal"].startswith("CREDIT_LEADS")]
    awaiting = [r for r in rows if r["signal"] == "INSUFFICIENT_HISTORY"]

    gaps.append("HY primary ISSUANCE windows NOT included: FRED issuance "
                "series are NBER historical archives (discontinued) or "
                "quarterly Z.1 flow-of-funds; SIFMA/TRACE are paid. OAS is a "
                "PRICE not a volume and is deliberately not substituted.")
    if awaiting:
        gaps.append("%d of %d names await a second observation — the lead is "
                    "measured against this engine's own ledger, which accretes "
                    "daily and cannot be back-filled honestly."
                    % (len(awaiting), len(rows)))

    out = {
        "version": VERSION,
        "generated_at": now.isoformat(),
        "n_names": len(rows),
        "n_leads": len(leads),
        "n_awaiting_history": len(awaiting),
        "names": rows,
        "leads": leads[:20],
        "degraded": degraded,
        "gaps": gaps,
        "thresholds": {"min_dd_move": MIN_DD_MOVE,
                       "min_cds_move_bp": MIN_CDS_MOVE_BP,
                       "equity_flat_band_pct": FLAT_BAND_PCT},
        "method": ("Credit direction per issuer from Δ distance-to-default "
                   "and Δ synthetic CDS bp against this engine's own daily "
                   "ledger; equity leg from the universe price. A lead fires "
                   "only when credit moves beyond a minimum threshold AND the "
                   "equity is flat or moving the other way — the bond desk "
                   "repricing risk before the equity desk. Single-snapshot "
                   "leads are impossible by construction: with no prior "
                   "observation the engine reports INSUFFICIENT_HISTORY "
                   "rather than inventing a direction."),
        "attribution": "structural credit from justhodl-cds-monitor "
                       "(CreditGrades); prices from justhodl universe",
    }

    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":"), default=str),
                  ContentType="application/json")

    for tk, rec in obs.items():
        if len(rec) > HIST_KEEP:
            for old in sorted(rec)[:-HIST_KEEP]:
                rec.pop(old, None)
    hist["updated_at"] = now.isoformat()
    S3.put_object(Bucket=BUCKET, Key=HIST_KEY,
                  Body=json.dumps(hist, separators=(",", ":"), default=str),
                  ContentType="application/json")

    print("[cbe] names=%d leads=%d awaiting=%d"
          % (len(rows), len(leads), len(awaiting)))
    return {"statusCode": 200,
            "body": json.dumps({"names": len(rows), "leads": len(leads)})}
