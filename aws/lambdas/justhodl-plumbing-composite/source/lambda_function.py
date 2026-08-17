"""justhodl-plumbing-composite v1.0.0 -- Fusion 2 engine.
Marker: plumbing-composite v1.0.0

Khalid doctrine: plumbing leads funding leads equities.  This engine
finally routes the repo master board (probe ops 4821: 832 series /
20 groups) into the decision layer through the one sanctioned
channel -- sizing context -- by publishing data/plumbing-composite.json
daily at 10:45 UTC, 20 minutes before risk-gate (11:05) reads it.

Legs (positive stress_z = MORE stress; polarity documented per leg):
  fails      .20  D_FAILS_T_RATIO (fallback DTCC-TREASURY-FAILS):
                  settlement fails ratio up = collateral/delivery
                  stress (+z).
  sofr_iorb  .15  D_SOFR_IORB: SOFR above IORB = cash scarcity (+z).
  dispersion .15  D_SOFR_P75_P25: rate dispersion widening = stressed
                  tape (+z), avg'd with D_DVP_SOFR (+z, Sep-2019-style
                  funding-cost spikes).
  scarcity   .12  D_BUND_EA_AAA: Bund richening BELOW the euro AAA
                  curve = collateral scarcity, so stress = MINUS z
                  (2016-17 QE-scarcity episode polarity).
  periphery  .10  D_BTP_BUND (monthly OECD): wider = stress (+z).
  haircuts   .10  margin-widening breadth across the 81 NY Fed
                  tri-party haircut series via repo.json's own chg.m
                  (share-of-collateral and federal-reserve series
                  excluded); z once >=60 banked days, fixed provisional
                  thresholds before that.
  fima       .08  WREPOFOR 13w DELTA: foreign officials draining the
                  Fed pool = offshore dollar demand, stress = MINUS z
                  of the delta (Mar-2020 polarity).
  srf   +escalator SRF_TAKEUP latest: >$2B adds +0.10, >$25B +0.25 --
                  the tail-stress tripwire (zero-inflated, no z).
  sftr  DEFERRED  bank holds n=1 week (probe 4821); auto-activates at
                  n>=26 -- honest INSUFFICIENT_DATA, never silent.

Deliberately EXCLUDED (no double-count): dealer positioning and bank
reserves -- risk-gate's funding leg already consumes both live
(fleet_inputs dealer_net_treasury_b; reserves_13w_pct), probe-proven.

Z discipline: trailing window by cadence (daily 756 / weekly 156 /
monthly 120 obs) EXCLUDING the latest point, min 120/60/60 obs else
leg excluded with reason, winsorize +/-4, stale legs (daily>7d,
weekly>21d, monthly>100d -- OECD monthlies publish with a
~6-10 week lag) excluded with reason, weights renormalized
over live legs with every exclusion NAMED in the output (renorm must
never hide a dead leg -- boom_score lesson).
"""
import gzip
import json
import os
import time
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
BOARD_KEY = "data/repo.json"
HIST_FMT = "data/repo-history/%s.json"
OUT_KEY = "data/plumbing-composite.json"
BANK_KEY = "plumbing-composite/history.json"
BANK_KEEP = 1600

WEIGHTS = {"fails": .20, "sofr_iorb": .15, "dispersion": .15,
           "scarcity": .12, "periphery": .10, "haircuts": .10,
           "fima": .08}
SRF_ID = "SRF_TAKEUP"
LEG_SERIES = {
    "fails": [("D_FAILS_T_RATIO", +1), ("DTCC-TREASURY-FAILS", +1)],
    "sofr_iorb": [("D_SOFR_IORB", +1)],
    "dispersion": [("D_SOFR_P75_P25", +1), ("D_DVP_SOFR", +1)],
    "scarcity": [("D_BUND_EA_AAA", -1)],
    "periphery": [("D_BTP_BUND", +1)],
    "fima": [("WREPOFOR", -1)],           # 13w delta, minus polarity
}
DELTA_LEGS = {"fima": 13}                  # obs steps for the delta
WINDOW = {"daily": 756, "weekly": 156, "monthly": 120}
MIN_N = {"daily": 120, "weekly": 60, "monthly": 60}
MAX_AGE = {"daily": 7, "weekly": 21, "monthly": 100}
BREADTH_MIN_BANK = 60
POSTURES = ((0.5, "PLUMBING_CALM"), (1.0, "PLUMBING_TIGHT"),
            (1.75, "PLUMBING_STRESS"), (99.0, "PLUMBING_SEVERE"))

s3 = boto3.client("s3")


def _g(key):
    try:
        raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:  # noqa: BLE001
        return None


def _put(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(obj, separators=(",", ":")).encode(),
                  ContentType="application/json")


def fnum(v):
    try:
        f = float(v)
        return f if f == f and abs(f) != float("inf") else None
    except (TypeError, ValueError):
        return None


def cadence_of(dates):
    if len(dates) < 8:
        return "monthly"
    tail = dates[-15:]
    gaps = []
    for a, b in zip(tail, tail[1:]):
        try:
            da = datetime.fromisoformat(a[:10])
            db = datetime.fromisoformat(b[:10])
            gaps.append((db - da).days)
        except ValueError:
            continue
    if not gaps:
        return "monthly"
    gaps.sort()
    med = gaps[len(gaps) // 2]
    return "daily" if med <= 3 else "weekly" if med <= 10 \
        else "monthly"


def zscore(vals):
    """z of the LAST value vs the trailing window excluding it.
    Returns (z, n_window, mean, std) or None."""
    if len(vals) < 3:
        return None
    hist, last = vals[:-1], vals[-1]
    mu = sum(hist) / len(hist)
    var = sum((v - mu) ** 2 for v in hist) / max(1, len(hist) - 1)
    sd = var ** 0.5
    if sd <= 1e-12:
        return None
    z = (last - mu) / sd
    z = max(-4.0, min(4.0, z))
    return z, len(hist), mu, sd


def series_leg(sid, polarity, delta=None):
    """One series -> stress reading dict or (None, reason)."""
    h = _g(HIST_FMT % sid)
    if not h:
        return None, "history_missing"
    dates = h.get("dates") or []
    vals = [fnum(v) for v in (h.get("values") or [])]
    pairs = [(d, v) for d, v in zip(dates, vals) if v is not None]
    if len(pairs) < 8:
        return None, "too_short(n=%d)" % len(pairs)
    dates = [p[0] for p in pairs]
    vals = [p[1] for p in pairs]
    cad = cadence_of(dates)
    try:
        age = (datetime.now(timezone.utc).date()
               - datetime.fromisoformat(dates[-1][:10]).date()).days
    except ValueError:
        return None, "bad_last_date"
    if age > MAX_AGE[cad]:
        return None, "stale(%dd,%s)" % (age, cad)
    if delta:
        if len(vals) <= delta:
            return None, "too_short_for_delta"
        vals = [b - a for a, b in zip(vals[:-delta], vals[delta:])]
    if len(vals) < MIN_N[cad]:
        return None, "insufficient(n=%d<%d,%s)" % (len(vals),
                                                   MIN_N[cad], cad)
    win = vals[-(WINDOW[cad] + 1):]
    zs = zscore(win)
    if not zs:
        return None, "flat_series"
    z, nwin, mu, sd = zs
    return {"id": sid, "age_days": age,
            "z": round(z, 3),
            "stress_z": round(polarity * z, 3),
            "value": round(vals[-1], 6), "date": dates[-1],
            "cadence": cad, "n_window": nwin,
            "mean": round(mu, 6), "std": round(sd, 6),
            "polarity": polarity,
            "delta_obs": delta}, None


def haircut_breadth(board_rows, bank_rows):
    """Share of NY Fed tri-party haircut MARGIN series widening m/m
    (chg.m>0); share-of-collateral + federal-reserve ids excluded.
    Z vs our own banked breadth once >=BREADTH_MIN_BANK days, fixed
    provisional thresholds before that."""
    n_up = n_all = 0
    for r in board_rows:
        sid = str(r.get("id") or "")
        if not sid.startswith("HAIRCUT-"):
            continue
        if "share" in sid or "federal-reserve" in sid:
            continue
        m = fnum((r.get("chg") or {}).get("m"))
        if m is None:
            continue
        n_all += 1
        if m > 0:
            n_up += 1
    if n_all < 20:
        return None, "board_haircut_rows_thin(%d)" % n_all
    share = n_up / n_all
    hist = [fnum(r.get("breadth")) for r in bank_rows]
    hist = [v for v in hist if v is not None]
    if len(hist) >= BREADTH_MIN_BANK:
        zs = zscore(hist + [share])
        stress = zs[0] if zs else 0.0
        mode = "z(banked n=%d)" % len(hist)
    else:
        stress = (2.0 if share > 0.75 else 1.0 if share > 0.60
                  else -0.5 if share < 0.40 else 0.0)
        mode = "provisional_thresholds(banked n=%d<%d)" \
            % (len(hist), BREADTH_MIN_BANK)
    return {"share_widening": round(share, 4), "n_series": n_all,
            "n_widening": n_up, "stress_z": round(stress, 3),
            "mode": mode}, None


def walk_series(doc):
    out = []
    for g in (doc.get("groups") or []):
        for r in (g.get("series") or []):
            if isinstance(r, dict) and r.get("id"):
                out.append(r)
    return out


def sftr_status(board_rows):
    ns = [r.get("n_obs") or 0 for r in board_rows
          if str(r.get("id") or "").startswith("SFTR-")]
    top = max(ns) if ns else 0
    if top >= 26:
        return None, top          # eligible -- future version wires it
    return "deferred_insufficient(bank n=%d<26w)" % top, top


def build():
    t0 = time.time()
    now = datetime.now(timezone.utc)
    doc = {"v": VERSION, "engine": "justhodl-plumbing-composite",
           "as_of": now.date().isoformat(),
           "generated_at": now.isoformat(),
           "doctrine": "plumbing leads funding leads equities; "
                       "context + sizing only, never selection",
           "legs": {}, "excluded": {}, "diag": {}}
    board = _g(BOARD_KEY)
    if not board:
        doc["status"] = "INSUFFICIENT_DATA"
        doc["why"] = "data/repo.json unreadable -- no synthetic legs"
        return doc
    rows = walk_series(board)
    doc["diag"]["board_rows"] = len(rows)
    doc["diag"]["board_as_of"] = board.get("as_of")
    bank = _g(BANK_KEY) or {"rows": []}

    for leg, series in LEG_SERIES.items():
        got = []
        why = []
        for sid, pol in series:
            rd, reason = series_leg(sid, pol,
                                    DELTA_LEGS.get(leg))
            if rd:
                got.append(rd)
            else:
                why.append("%s:%s" % (sid, reason))
            if got and leg == "fails":
                break             # fallback chain: first live wins
        if got:
            sz = sum(r["stress_z"] for r in got) / len(got)
            doc["legs"][leg] = {"stress_z": round(sz, 3),
                                "series": got,
                                "skipped": why or None}
        else:
            doc["excluded"][leg] = ";".join(why)

    hb, hbwhy = haircut_breadth(rows, bank["rows"])
    if hb:
        doc["legs"]["haircuts"] = hb
    else:
        doc["excluded"]["haircuts"] = hbwhy

    sf_reason, sf_n = sftr_status(rows)
    if sf_reason:
        doc["excluded"]["sftr"] = sf_reason
    doc["diag"]["sftr_bank_weeks"] = sf_n

    srf = series_leg(SRF_ID, +1)
    esc, srf_bn = 0.0, None
    if srf[0]:
        srf_bn = srf[0]["value"]
        esc = 0.25 if srf_bn > 25 else 0.10 if srf_bn > 2 else 0.0
    doc["srf"] = {"takeup_bn": srf_bn, "escalator": esc,
                  "note": srf[1] if not srf[0] else None}

    live = {k: v for k, v in doc["legs"].items() if k in WEIGHTS}
    if len(live) < 4:
        doc["status"] = "INSUFFICIENT_DATA"
        doc["why"] = ("only %d/%d weighted legs live -- composite "
                      "withheld, see excluded" % (len(live),
                                                  len(WEIGHTS)))
        return doc
    wsum = sum(WEIGHTS[k] for k in live)
    comp = sum(WEIGHTS[k] / wsum * live[k]["stress_z"]
               for k in live) + esc
    comp = round(comp, 3)
    doc["status"] = "LIVE"
    doc["composite"] = comp
    doc["weights_effective"] = {k: round(WEIGHTS[k] / wsum, 4)
                                for k in live}
    for cut, name in POSTURES:
        if comp < cut:
            doc["posture"] = name
            break
    doc["why"] = sorted(
        ((k, live[k]["stress_z"]) for k in live),
        key=lambda x: -abs(x[1]))
    doc["why"] = ["%s stress_z=%+.2f" % (k, z) for k, z in doc["why"]]

    row = {"date": doc["as_of"], "composite": comp,
           "posture": doc["posture"],
           "breadth": (doc["legs"].get("haircuts") or {})
           .get("share_widening"),
           "legs": {k: live[k]["stress_z"] for k in live}}
    if not bank["rows"] or bank["rows"][-1].get("date") \
            != row["date"]:
        bank["rows"].append(row)
        bank["rows"] = bank["rows"][-BANK_KEEP:]
        _put(BANK_KEY, bank)
    doc["diag"]["bank_days"] = len(bank["rows"])
    doc["diag"]["runtime_ms"] = int((time.time() - t0) * 1000)
    return doc


def lambda_handler(event, context):
    doc = build()
    _put(OUT_KEY, doc)
    return {"ok": doc.get("status") == "LIVE", "v": VERSION,
            "status": doc.get("status"),
            "composite": doc.get("composite"),
            "posture": doc.get("posture"),
            "legs_live": len(doc.get("legs") or {}),
            "excluded": list((doc.get("excluded") or {}))}
