"""justhodl-alpha-compass v2.0 — the desk sheet.

THE CONSUMER QUESTION
─────────────────────
"What do I buy RIGHT NOW, how much, where do I get out — and has this page
been right before?"

v1 was a joiner that joined on vocabularies that never matched:
  • regime read data/regime-flag.json — a key NO engine produces → "Unknown"
  • sizer join looked for by_subject/recommendations — sizer-v2 publishes
    ticker-keyed positions/setups → sizing always null
  • scorecard/magdist lookups used conviction FAMILY tokens against
    signal_type vocabularies → dash-wall on every card
  • conviction setups are THEMES with no tickers → nothing tradeable shown

v2 fixes every join against the REAL schemas and adds the institutional
layers a desk sheet needs:

  1. REGIME FUSION      regime-composite + RORO + factor-regime + dollar
                        transmission dial + sizer risk-multiplier + book
                        posture → one strip, per-source chips, playbook line.
  2. EXPRESSION LAYER   theme → liquid vehicles (direction-aware legs) +
                        single names joined from conviction.single_names,
                        best-setups (entry/stop/target), kill-theses (bear
                        case), sizer-v2 (per-ticker Kelly).
  3. STATS LADDER       magdist stack match (3 candidate vocabularies) →
                        scorecard aggregate → conviction prior. Every card
                        shows numbers and DECLARES its evidence tier.
  4. THEME KELLY        quarter-Kelly from resolved win-rate/payoff, scaled
                        by conviction and the sizer's regime risk-multiplier.
  5. TRACK RECORD       self-grading: every run snapshots its calls with
                        entry prints (FMP quote), grades ≥7d-old calls,
                        publishes trailing 30/90d hit-rate. Accountability.
  6. Δ SINCE LAST RUN   new / dropped / conviction moves vs previous output.

OUTPUT data/alpha-compass.json (schema 2.0) + data/alpha-compass-history.json
Consumed by alpha-compass.html. Runs every 3h at :50 (after conviction :45).
"""

import json
import os
import statistics
import time
import urllib.error
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

try:
    from _sentry_lite import track_errors
except ImportError:  # zip built without aws/shared injection — degrade
    def track_errors(f):
        return f

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

K_CONVICTION = "data/conviction.json"
K_MAGDIST    = "data/magnitude-distributions.json"
K_SCORECARD  = "data/signal-scorecard.json"
K_SIZER      = "portfolio/sizer-v2.json"
K_RCOMP      = "data/regime-composite.json"
K_RORO       = "data/risk-regime.json"
K_FACTOR     = "data/factor-regime.json"
K_DOLLAR     = "data/dollar-radar.json"
K_KILL       = "data/kill-theses.json"
K_BEST       = "data/best-setups.json"
K_MISS       = "data/miss-summary.json"
K_EMAP       = "data/engine-signal-map.json"
OUT_KEY      = "data/alpha-compass.json"
HIST_KEY     = "data/alpha-compass-history.json"

FMP_KEY = os.environ.get("FMP_API_KEY", "")

# Theme → liquid expression vehicles. Leg chosen by call direction.
SUBJECT_VEHICLES = {
    "Crypto": {
        "long": ["IBIT", "ETHA", "COIN", "MSTR"], "short": ["BITI"],
        "note": "spot-BTC/ETH ETFs + high-beta equities"},
    "US macro / housing cycle": {
        "long": ["ITB", "XHB", "DHI"], "short": ["ITB", "XHB"],
        "note": "homebuilder ETFs; short leg = short the builders"},
    "US equity — positioning": {
        "long": ["IWM", "SPY"], "short": ["RWM", "SH"],
        "note": "covering squeezes hit small-cap beta hardest"},
    "US equity — value tilt": {
        "long": ["IWD", "RSP", "IVE"], "short": ["IWF"],
        "note": "cleanest pair: long IWD / short IWF"},
    "Broad risk / equity beta": {
        "long": ["SPY", "QQQ"], "short": ["SH", "PSQ"],
        "note": "index beta, direction-signed"},
    "Cross-asset relative value": {
        "long": [], "short": [],
        "note": "pair-specific — see cross-asset-rv desk"},
    "Supply shortage / scarcity": {
        "long": [], "short": [],
        "note": "single-name driven — scarcity radar picks below"},
}

s3 = boto3.client("s3", region_name=REGION)


def _dec(o):
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, datetime):
        return o.isoformat()
    raise TypeError(f"unencodeable {type(o)}")


def safe_load(key: str) -> dict:
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except (ClientError, json.JSONDecodeError, KeyError) as e:
        print(f"[compass] could not load {key}: {e}")
        return {}


def first_of(d: dict, *keys):
    """First non-empty value among keys; supports 'a.b' dotted paths."""
    if not isinstance(d, dict):
        return None
    for k in keys:
        cur = d
        ok = True
        for part in k.split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                ok = False
                break
        if ok and cur not in (None, "", [], {}):
            return cur
    return None


def fnum(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def dir_sign(direction) -> int:
    d = str(direction or "").upper()
    if d.startswith("RISK-ON") or "LONG" in d:
        return 1
    if d.startswith("RISK-OFF") or "SHORT" in d or "DEFENSIVE" in d:
        return -1
    return 0


# ───────────────────────────── FMP quotes ─────────────────────────────

def fmp_eod(ticker, dfrom, dto) -> dict:
    """{date: close} via FMP /stable/historical-price-eod/light.
    Defensive on shape (list vs {'historical': [...]}); {} on failure."""
    if not FMP_KEY or not ticker:
        return {}
    url = ("https://financialmodelingprep.com/stable/historical-price-eod/"
           f"light?symbol={urllib.parse.quote(ticker)}"
           f"&from={dfrom}&to={dto}&apikey={FMP_KEY}")
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "jh-compass/2.1"})
        with urllib.request.urlopen(req, timeout=12) as r:
            data = json.loads(r.read().decode("utf-8"))
        rows = data.get("historical") if isinstance(data, dict) else data
        out = {}
        for r_ in rows if isinstance(rows, list) else []:
            d = r_.get("date")
            px = fnum(r_.get("price") or r_.get("close") or r_.get("adjClose"))
            if d and px:
                out[str(d)[:10]] = px
        return out
    except Exception as e:
        print(f"[compass] eod {ticker} failed: {e}")
        return {}


def px_on_or_before(series: dict, iso_date: str):
    """Close on iso_date, else nearest earlier session (≤5d back)."""
    if not series:
        return None
    from datetime import date, timedelta as _td
    try:
        d = date.fromisoformat(iso_date[:10])
    except Exception:
        return None
    for k in range(6):
        px = series.get((d - _td(days=k)).isoformat())
        if px:
            return px
    return None


TG_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TG_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "")
TG_CHAT_KEY = "data/_telegram-chat.json"   # self-discovered, fleet-reusable


def _tg(method, payload):
    try:
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TG_TOKEN}/{method}",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as r:
            return True, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return False, e.read().decode("utf-8", "replace")[:200]
    except Exception as e:
        return False, str(e)[:200]


def _discover_chat():
    """After the user presses /start, getUpdates reveals the chat id."""
    ok, d = _tg("getUpdates", {"limit": 10})
    if not ok or not isinstance(d, dict):
        return None
    for u in reversed(d.get("result") or []):
        chat = ((u.get("message") or u.get("my_chat_member") or {})
                .get("chat") or {})
        if chat.get("id"):
            return str(chat["id"])
    return None


def send_telegram(msg: str) -> bool:
    """Deliver, self-healing the chat id: env → S3-discovered → live
    getUpdates discovery on 403 ('bot can't initiate conversation',
    ops 3141). A discovered chat is persisted for every future run."""
    if not TG_TOKEN:
        return False
    chats = []
    if TG_CHAT:
        chats.append(("env", TG_CHAT))
    saved = safe_load(TG_CHAT_KEY).get("chat_id")
    if saved and saved != TG_CHAT:
        chats.insert(0, ("s3", saved))
    payload = {"text": msg, "parse_mode": "HTML",
               "disable_web_page_preview": True}
    last_err = None
    for src, cid in chats:
        ok, d = _tg("sendMessage", {**payload, "chat_id": cid})
        if ok:
            return True
        last_err = d
    disc = _discover_chat()
    if disc:
        ok, d = _tg("sendMessage", {**payload, "chat_id": disc})
        if ok:
            try:
                s3.put_object(Bucket=BUCKET, Key=TG_CHAT_KEY,
                              Body=json.dumps({
                                  "chat_id": disc,
                                  "discovered_at": datetime.now(
                                      timezone.utc).isoformat(),
                                  "via": "compass getUpdates self-heal",
                              }).encode(),
                              ContentType="application/json")
            except Exception as e:
                print(f"[compass] chat persist failed: {e}")
            print(f"[compass] telegram self-healed to chat {disc}")
            return True
        last_err = d
    print(f"[compass] telegram failed: {last_err}")
    return False


# ───────────────────────────── regime fusion ─────────────────────────────

_COLORS = {
    "RISK-ON": "#00e68a", "RISK_ON": "#00e68a", "EXPANSION": "#00e68a",
    "MILDLY RISK-ON": "#7ae0b0", "MILD_RISK_ON": "#7ae0b0", "GOLDILOCKS": "#00e68a",
    "NEUTRAL": "#4dabf7", "NORMAL": "#4dabf7", "STABLE": "#4dabf7",
    "NEUTRAL / MIXED": "#4dabf7", "MIXED": "#4dabf7",
    "TRANSITION": "#ffd43b", "CAUTION": "#ffd43b", "WATCH": "#ffd43b",
    "MILDLY RISK-OFF": "#ffa94d", "MILD_RISK_OFF": "#ffa94d",
    "RISK-OFF": "#ff4757", "RISK_OFF": "#ff4757", "CRISIS": "#ff4757",
    "CONTRACTION": "#ff922b", "STRESS": "#ff922b",
}


def _color(label) -> str:
    return _COLORS.get(str(label or "").upper(), "#8896b0")


def _lbl(v):
    """Coerce any regime-ish value to a display string (dig into dicts)."""
    if v is None:
        return None
    if isinstance(v, dict):
        return _lbl(first_of(v, "label", "meta_regime", "regime", "verdict",
                             "state", "stance", "posture", "call", "name"))
    s = str(v).strip()
    return s if s and not s.startswith("{") else None


def fuse_regime(rcomp, roro, factor, dollar, sizer, conviction) -> dict:
    sources = []

    comp_lbl = _lbl(first_of(rcomp, "meta_regime", "regime",
                             "composite_regime", "meta_class", "label",
                             "verdict"))
    comp_score = fnum(first_of(rcomp, "composite_score", "score", "master",
                               "master_score"))
    if comp_lbl or comp_score is not None:
        sources.append({"k": "composite", "label": "Regime Composite",
                        "value": comp_lbl, "score": comp_score,
                        "color": _color(comp_lbl)})

    roro_lbl = _lbl(first_of(roro, "risk_regime", "posture", "regime",
                             "state"))
    roro_score = fnum(first_of(roro, "risk_regime_score", "score",
                               "composite_score", "roro_score"))
    if roro_lbl:
        sources.append({"k": "roro", "label": "RORO", "value": roro_lbl,
                        "score": roro_score, "color": _color(roro_lbl)})

    fac_lbl = _lbl(first_of(factor, "appetite", "regime", "stance", "label"))
    if not fac_lbl:
        th = factor.get("thrusts") if isinstance(factor, dict) else None
        if isinstance(th, list) and th:
            fac_lbl = f"{len(th)} style thrusts live"
    if fac_lbl:
        sources.append({"k": "factor", "label": "Factor Regime",
                        "value": fac_lbl, "score": None,
                        "color": _color(fac_lbl)})

    rt = (dollar or {}).get("risk_transmission") or {}
    if rt.get("verdict"):
        sources.append({"k": "dollar", "label": "Risk-Asset Transmission",
                        "value": rt.get("verdict"), "score": fnum(rt.get("score")),
                        "color": _color(rt.get("verdict"))})

    book = _lbl(first_of(conviction, "book_posture"))
    book_net = fnum(first_of(conviction, "book_net_signal"))
    if book:
        sources.append({"k": "book", "label": "Engine Book", "value": book,
                        "score": book_net, "color": _color(book)})

    risk_mult = fnum(first_of(sizer, "risk_multiplier")) or 1.0
    decisive = first_of(sizer, "decisive_call")

    headline = comp_lbl or roro_lbl or book or "Unknown"
    playbook_bits = []
    if decisive and str(decisive).upper() not in ("", "UNKNOWN"):
        playbook_bits.append(f"Sizer: {decisive}")
    playbook_bits.append(f"risk multiplier ×{risk_mult:g}")
    if rt.get("verdict"):
        playbook_bits.append(f"$-transmission {rt['verdict']}")

    return {
        "label": str(headline).replace("_", " ").title()
                 if headline != "Unknown" else "Unknown",
        "color": _color(headline),
        "score": comp_score if comp_score is not None else roro_score,
        "risk_multiplier": round(risk_mult, 2),
        "decisive_call": decisive,
        "sources": sources,
        "playbook": " · ".join(playbook_bits),
    }


# ───────────────────────────── stats ladder ─────────────────────────────

def _engine_tokens(engines) -> tuple:
    """(engine-name set, family set) lowercased."""
    names, fams = set(), set()
    for e in engines or []:
        if not isinstance(e, dict):
            continue
        n = str(e.get("engine") or e.get("name") or "").strip().lower()
        f = str(e.get("family") or "").strip().lower()
        if n:
            names.add(n)
        if f:
            fams.add(f)
    return names, fams


def _mapped_signal_types(fams, emap) -> set:
    by_fam = (emap or {}).get("by_family") or {}
    out = set()
    for f in fams:
        for st in by_fam.get(f) or []:
            if st:
                out.add(str(st).strip().lower())
    return out


def _best_stack(stacks, candidate, horizon_hint=30):
    """Best realised-distribution stack whose signals are ALL in the
    candidate vocabulary (subset coverage). Jaccard was wrong here: stacks
    are mostly single-signal, so union-penalty buried every true match
    (ops 3137 forensics). Rank: preferred horizon, then sample size."""
    eligible = []
    for s in stacks:
        members = {str(x).strip().lower() for x in (s.get("signals") or []) if x}
        if members and members <= candidate:
            eligible.append((s.get("horizon_days") == horizon_hint,
                             s.get("n") or 0, s, sorted(members)))
    if not eligible:
        return None, 0.0, None
    eligible.sort(key=lambda t: (t[0], t[1]), reverse=True)
    _, _, best, matched = eligible[0]
    return best, len(eligible), matched


def resolve_stats(engines, magdist, scorecard, emap) -> dict:
    """Tier A magdist → Tier B scorecard → Tier C prior. Never dashes."""
    names, fams = _engine_tokens(engines)
    mapped = _mapped_signal_types(fams, emap)
    stacks = (magdist or {}).get("stacks") or []

    # Tier A — realised distribution (union of all three vocabularies;
    # a stack matches when every one of its signals is a candidate)
    candidate = names | fams | mapped
    best, n_eligible, matched = (_best_stack(stacks, candidate)
                                 if candidate else (None, 0, None))
    if best:
        return {
            "source": "magdist", "matched_signals": matched,
            "eligible_stacks": n_eligible,
            "n": best.get("n"), "median": fnum(best.get("median")),
            "p25": fnum(best.get("p25")), "p75": fnum(best.get("p75")),
            "win_rate": fnum(best.get("win_rate")),
            "horizon_days": best.get("horizon_days"),
        }

    # Tier B — scorecard aggregate across all three vocabularies
    by_type = (scorecard or {}).get("scorecard") \
        or (scorecard or {}).get("by_signal_type") \
        or (scorecard or {}).get("signals") or {}
    if isinstance(by_type, list):
        by_type = {str(r.get("signal_type") or r.get("name") or "").lower(): r
                   for r in by_type if isinstance(r, dict)}
    elif isinstance(by_type, dict):
        by_type = {str(k).lower(): v for k, v in by_type.items()}
    ns, lbs, ars = [], [], []
    for tok in (names | fams | mapped):
        rec = by_type.get(tok)
        if not isinstance(rec, dict):
            continue
        n = fnum(rec.get("n_scored") or rec.get("n"))
        lb = fnum(rec.get("wilson_lb") or rec.get("hit_rate_lb"))
        ar = fnum(rec.get("avg_return") or rec.get("mean_return"))
        if n:
            ns.append(n)
        if lb is not None:
            lbs.append(lb)
        if ar is not None:
            ars.append(ar)
    if lbs or ars:
        return {
            "source": "scorecard",
            "n": int(sum(ns)) if ns else None,
            "median": round(statistics.fmean(ars), 2) if ars else None,
            "p25": None, "p75": None,
            "win_rate": round(statistics.fmean(lbs), 3) if lbs else None,
            "engines_with_record": len(lbs) or len(ars),
        }

    # Tier C — conviction prior only
    return {"source": "prior", "n": None, "median": None, "p25": None,
            "p75": None, "win_rate": None}


# ───────────────────────────── sizing ─────────────────────────────

def theme_kelly(stats, conviction, risk_mult) -> dict:
    conv = (fnum(conviction) or 0) / 100.0
    src = stats.get("source")
    W = stats.get("win_rate")
    if src == "magdist" and W and stats.get("p25") is not None \
            and stats.get("p75") is not None and abs(stats["p25"]) > 0.05:
        R = abs(stats["p75"]) / abs(stats["p25"])
        f = max(0.0, W - (1 - W) / max(R, 0.1))
        basis = f"quarter-Kelly on realised dist (W={W:.0%}, R={R:.2f})"
    elif src == "scorecard" and W:
        R = 1.5
        f = max(0.0, W - (1 - W) / R)
        basis = f"quarter-Kelly est. (scorecard W={W:.0%}, R=1.5 assumed)"
    else:
        f = 0.08  # small prior full-Kelly proxy
        basis = "conservative prior — no graded history for this stack yet"
    pct = min(0.08, 0.25 * f * conv * risk_mult)
    return {
        "kelly_pct": round(pct * 100, 2),
        "dollar_at_100k": int(round(pct * 100000, -1)),
        "basis": basis,
        "source": src,
    }


# ───────────────────────────── expression layer ─────────────────────────────

def _index_best_setups(best) -> dict:
    rows = None
    for k in ("top_setups", "setups", "rows", "best_setups", "top", "items",
              "ranked"):
        v = (best or {}).get(k)
        if isinstance(v, list) and v and isinstance(v[0], dict):
            rows = v
            break
    out = {}
    for r in rows or []:
        tk = str(r.get("ticker") or "").upper()
        if not tk:
            continue
        out[tk] = {
            "conviction": fnum(r.get("conviction") or r.get("score")),
            "entry": fnum(r.get("entry")),
            "stop": fnum(r.get("stop") or r.get("stop_loss")),
            "target": fnum(first_of(r, "target", "t1", "price_target",
                                    "targets.0")),
        }
    return out


def _index_kill(kill) -> dict:
    out = {}
    for t in (kill or {}).get("theses") or []:
        if not isinstance(t, dict) or t.get("error"):
            continue
        tk = str(t.get("symbol") or t.get("ticker") or "").upper()
        if not tk:
            continue
        conds = t.get("kill_conditions") or []
        first = ""
        if conds and isinstance(conds[0], dict):
            first = (conds[0].get("risk") or conds[0].get("condition") or "")
        elif conds:
            first = str(conds[0])
        out[tk] = (first or t.get("risk") or "")[:160]
    return out


def _index_sizer(sizer) -> dict:
    out = {}
    for sec in ("positions", "setups"):
        for r in (sizer or {}).get(sec) or []:
            tk = str(r.get("ticker") or "").upper()
            if tk and tk not in out:
                out[tk] = {"weight_pct": fnum(r.get("weight_used")),
                           "dollar_size": fnum(r.get("dollar_size")),
                           "bucket": sec}
    return out


def express(subject, direction, single_by_src, best_idx, kill_idx, sizer_idx,
            notes_idx=None):
    sign = dir_sign(direction)
    spec = SUBJECT_VEHICLES.get(subject) or {"long": [], "short": [], "note": ""}
    leg = spec["long"] if sign >= 0 else spec["short"]
    side = "LONG" if sign > 0 else ("SHORT" if sign < 0 else "WATCH")
    vehicles = [{"ticker": t, "side": side} for t in leg[:4]]

    names = []
    pool = []
    if subject == "Supply shortage / scarcity":
        pool = single_by_src.get("scarcity-radar", [])
    elif subject == "US equity — value tilt":
        pool = single_by_src.get("opportunity", [])
    for r in pool[:3]:
        tk = str(r.get("ticker") or "").upper()
        if not tk:
            continue
        bs = best_idx.get(tk) or {}
        names.append({
            "ticker": tk, "company": r.get("company"),
            "verdict": r.get("verdict"), "score": fnum(r.get("score")),
            "entry": bs.get("entry"), "stop": bs.get("stop"),
            "target": bs.get("target"),
            "kill_risk": kill_idx.get(tk),
            "khalid_note": (notes_idx or {}).get(str(tk).upper()),
            "sizer": sizer_idx.get(tk),
        })

    primary = (names[0]["ticker"] if names
               else (vehicles[0]["ticker"] if vehicles else None))
    return {"vehicles": vehicles, "names": names, "primary": primary,
            "side": side, "note": spec.get("note", "")}


# ───────────────────────────── track record ─────────────────────────────

GRADE_HORIZON_D = 14          # calendar days, close-to-close
SNAP_PREFIX = "data/conviction/snapshots/"
BACKFILL_LOOKBACK_D = 120


def primary_vehicle(subject, direction):
    sgn = dir_sign(direction)
    if sgn == 0:
        return None, 0
    spec = SUBJECT_VEHICLES.get(subject) or {}
    leg = spec.get("long") if sgn > 0 else spec.get("short")
    return (leg[0] if leg else None), sgn


def list_snapshots(now):
    """Snapshot dates within the backfill window, ascending."""
    from datetime import timedelta as _td
    cutoff = (now.date() - _td(days=BACKFILL_LOOKBACK_D)).isoformat()
    dates, token = [], None
    try:
        while True:
            kw = {"Bucket": BUCKET, "Prefix": SNAP_PREFIX, "MaxKeys": 1000}
            if token:
                kw["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kw)
            for o in resp.get("Contents") or []:
                d = o["Key"][len(SNAP_PREFIX):][:10]
                if len(d) == 10 and d >= cutoff:
                    dates.append(d)
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
    except Exception as e:
        print(f"[compass] snapshot list failed: {e}")
    return sorted(set(dates))


def backfill_entries(hist, now):
    """One-time ingest of historical conviction top-3 calls as track
    entries (idempotent; dedup on (date, subject))."""
    if hist.get("backfill_done"):
        return 0
    entries = hist.setdefault("entries", [])
    have = {(e.get("d"), e.get("subject")) for e in entries}
    added = 0
    for d in list_snapshots(now):
        snap = safe_load(f"{SNAP_PREFIX}{d}.json")
        setups = snap.get("setups") or []
        setups.sort(key=lambda r: -(fnum(r.get("conviction")) or 0))
        for st in setups[:3]:
            subj = st.get("subject")
            tk, sgn = primary_vehicle(subj, st.get("direction"))
            if not tk or (d, subj) in have:
                continue
            entries.append({"d": d, "subject": subj, "dir": sgn, "tk": tk,
                            "px": None, "ret": None, "src": "backfill"})
            have.add((d, subj))
            added += 1
    hist["backfill_done"] = True
    print(f"[compass] backfill ingested {added} historical calls")
    return added


def update_track_record(hist, top_calls, now):
    """Fixed-horizon self-grading: every call is graded close-to-close at
    entry-date +{H}d, direction-signed, from FMP EOD. Comparable across
    time — no since-call drift."""
    from datetime import date, timedelta as _td
    today = now.date()
    today_iso = today.isoformat()
    entries = hist.setdefault("entries", [])

    # append today's live top calls
    have_today = {(e["d"], e["subject"]) for e in entries
                  if e.get("d") == today_iso}
    for c in top_calls:
        subj = c.get("subject")
        tk, sgn = primary_vehicle(subj, c.get("direction"))
        ex = (c.get("express") or {})
        tk = ex.get("primary") or tk
        if not tk or sgn == 0 or (today_iso, subj) in have_today:
            continue
        entries.append({"d": today_iso, "subject": subj, "dir": sgn,
                        "tk": tk, "px": None, "ret": None, "src": "live"})

    # fetch EOD series for every ticker still needing prices
    open_e = [e for e in entries if e.get("ret") is None and e.get("tk")]
    tickers = sorted({e["tk"] for e in open_e})[:15]
    dmin = min((e["d"] for e in open_e), default=today_iso)
    dfrom = (date.fromisoformat(dmin) - _td(days=6)).isoformat()
    series = {tk: fmp_eod(tk, dfrom, today_iso) for tk in tickers}
    quotes_ok = any(series.values())

    graded_now = 0
    for e in open_e:
        ser = series.get(e["tk"]) or {}
        if e.get("px") is None:
            e["px"] = px_on_or_before(ser, e["d"])
        try:
            gdate = date.fromisoformat(e["d"]) + _td(days=GRADE_HORIZON_D)
        except Exception:
            continue
        if today >= gdate and e.get("px"):
            exit_px = px_on_or_before(ser, gdate.isoformat())
            if exit_px:
                e["ret"] = round(e["dir"] * (exit_px / e["px"] - 1) * 100, 2)
                e["graded"] = today_iso
                graded_now += 1

    entries.sort(key=lambda e: e.get("d") or "")
    hist["entries"] = entries[-800:]

    def trail(days):
        cut = (today - _td(days=days)).isoformat()
        g = [e for e in hist["entries"]
             if e.get("ret") is not None and e.get("d", "") >= cut]
        if not g:
            return {"n": 0, "hit_rate": None, "avg_ret": None}
        hits = sum(1 for e in g if e["ret"] > 0)
        return {"n": len(g), "hit_rate": round(hits / len(g), 3),
                "avg_ret": round(statistics.fmean(e["ret"] for e in g), 2)}

    recent = [e for e in hist["entries"] if e.get("ret") is not None][-8:]
    return hist, {
        "method": f"fixed {GRADE_HORIZON_D}d horizon, EOD close-to-close, "
                  "direction-signed, primary vehicle",
        "horizon_days": GRADE_HORIZON_D,
        "trail_30d": trail(30), "trail_90d": trail(90),
        "graded_this_run": graded_now,
        "open_calls": sum(1 for e in hist["entries"]
                          if e.get("ret") is None),
        "recent": list(reversed(recent)),
        "quotes_available": quotes_ok,
    }


# ───────────────────────────── deltas ─────────────────────────────

def run_deltas(prev, cards):
    prev_map = {}
    for c in (prev.get("top_calls") or []) + (prev.get("watchlist") or []):
        if c.get("subject"):
            prev_map[c["subject"]] = fnum(c.get("conviction")) or 0
    new_map = {c["subject"]: fnum(c.get("conviction")) or 0
               for c in cards if c.get("subject")}
    entered = sorted(set(new_map) - set(prev_map))
    dropped = sorted(set(prev_map) - set(new_map))
    moves = []
    for s in set(new_map) & set(prev_map):
        d = new_map[s] - prev_map[s]
        if abs(d) >= 8:
            moves.append({"subject": s, "from": prev_map[s],
                          "to": new_map[s], "delta": round(d, 1)})
    moves.sort(key=lambda m: -abs(m["delta"]))
    delta_by_subject = {s: round(new_map[s] - prev_map[s], 1)
                        for s in set(new_map) & set(prev_map)}
    return ({"entered": entered, "dropped": dropped, "moves": moves[:6],
             "prev_generated_at": prev.get("generated_at")},
            delta_by_subject)


# ───────────────────────────── card builder ─────────────────────────────

def build_card(setup, rank, magdist, scorecard, emap, risk_mult,
               single_by_src, best_idx, kill_idx, sizer_idx,
               notes_idx):
    engines = setup.get("contributing_engines") or []
    stats = resolve_stats(engines, magdist, scorecard, emap)
    sizing = theme_kelly(stats, setup.get("conviction"), risk_mult)
    ex = express(setup.get("subject"), setup.get("direction"),
                 single_by_src, best_idx, kill_idx, sizer_idx, notes_idx)

    # stop/target resolution ladder (a non-negative bottom quartile is
    # not a stop — refuse degenerate p25>=0, ops 3138 beta card)
    stop_pct = stats.get("p25")
    target_pct = stats.get("p75")
    st_basis = "realised distribution (p25/p75)" if stop_pct is not None else None
    if stop_pct is not None and stop_pct >= 0:
        stop_pct = None
        st_basis = "realised p75 target; p25 non-negative -> no stop" \
            if target_pct is not None else None
    if stop_pct is None and ex["names"]:
        n0 = ex["names"][0]
        if n0.get("entry") and n0.get("stop"):
            stop_pct = round((n0["stop"] / n0["entry"] - 1) * 100, 1)
            st_basis = f"best-setups levels on {n0['ticker']}"
        if n0.get("entry") and n0.get("target"):
            target_pct = round((n0["target"] / n0["entry"] - 1) * 100, 1)

    return {
        "rank": rank,
        "subject": setup.get("subject"),
        "direction": setup.get("direction"),
        "conviction": setup.get("conviction"),
        "confidence_band": setup.get("confidence"),
        "n_engines": setup.get("n_engines"),
        "n_families": setup.get("n_families"),
        "agreement_pct": setup.get("agreement_pct"),
        "thesis": setup.get("thesis"),
        "invalidation": setup.get("invalidation"),
        "engines": [{
            "name": e.get("engine"), "family": e.get("family"),
            "signal": e.get("signal"), "read": e.get("read"),
            "skill": e.get("skill"),
        } for e in engines[:8] if isinstance(e, dict)],
        "stats": stats,
        "sizing": sizing,
        "express": ex,
        "stop_pct": stop_pct,
        "target_pct": target_pct,
        "stop_target_basis": st_basis,
    }


# ───────────────────────────── handler ─────────────────────────────

@track_errors
def handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    if isinstance(event, dict) and event.get("test_telegram"):
        ok = send_telegram("\u2705 <b>Alpha Compass</b> \u2014 tripwire "
                           "armed (top-call flips, entries/drops, "
                           "\u0394conv \u2265 15)")
        return {"statusCode": 200, "body": json.dumps({"telegram": ok})}

    conviction = safe_load(K_CONVICTION)
    magdist    = safe_load(K_MAGDIST)
    scorecard  = safe_load(K_SCORECARD)
    sizer      = safe_load(K_SIZER)
    rcomp      = safe_load(K_RCOMP)
    roro       = safe_load(K_RORO)
    factor     = safe_load(K_FACTOR)
    dollar     = safe_load(K_DOLLAR)
    kill       = safe_load(K_KILL)
    best       = safe_load(K_BEST)
    miss       = safe_load(K_MISS)
    emap       = safe_load(K_EMAP)
    prev       = safe_load(OUT_KEY)
    hist       = safe_load(HIST_KEY) or {"entries": []}

    regime = fuse_regime(rcomp, roro, factor, dollar, sizer, conviction)
    risk_mult = regime["risk_multiplier"]

    single_by_src = {}
    for r in conviction.get("single_names") or []:
        if isinstance(r, dict):
            single_by_src.setdefault(str(r.get("source") or ""), []).append(r)
    best_idx = _index_best_setups(best)
    # ops 3171: Khalid's own notes as desk context
    notes_idx = {k: {"n": v["n_notes"], "stance": v["stance"],
                     "score": v["stance_score"]}
                 for k, v in ((fetch_json("data/notes-index.json") or {})
                              .get("index") or {}).items()}
    kill_idx = _index_kill(kill)
    sizer_idx = _index_sizer(sizer)

    setups = conviction.get("setups") or []
    setups.sort(key=lambda r: -(fnum(r.get("conviction")) or 0))

    cards = [build_card(s, i + 1, magdist, scorecard, emap, risk_mult,
                        single_by_src, best_idx, kill_idx, sizer_idx,
                        notes_idx)
             for i, s in enumerate(setups[:13])]
    top_calls, watchlist = cards[:3], cards[3:]

    backfilled = backfill_entries(hist, now)
    hist, track = update_track_record(hist, top_calls, now)
    track["backfilled_this_run"] = backfilled
    changes, delta_map = run_deltas(prev, cards)

    # Telegram tripwire: top-call turnover or big conviction moves
    try:
        prev_top1 = ((prev.get("top_calls") or [{}])[0]).get("subject")
        new_top1 = (top_calls[0].get("subject") if top_calls else None)
        big = [m for m in changes.get("moves") or [] if abs(m["delta"]) >= 15]
        if prev.get("generated_at") and (
                changes.get("entered") or changes.get("dropped")
                or big or (prev_top1 and new_top1 != prev_top1)):
            t1 = top_calls[0] if top_calls else {}
            lines = [f"\U0001F9ED <b>Alpha Compass</b> \u00b7 {regime['label']}",
                     f"#1 {new_top1} \u2014 {t1.get('direction')} "
                     f"(conv {t1.get('conviction')})"]
            if prev_top1 and new_top1 != prev_top1:
                lines.append(f"top call flip: {prev_top1} \u2192 {new_top1}")
            for sub in changes.get("entered") or []:
                lines.append(f"+ entered: {sub}")
            for sub in changes.get("dropped") or []:
                lines.append(f"\u2212 dropped: {sub}")
            for m in big:
                lines.append(f"\u0394 {m['subject']}: {m['from']:g}"
                             f" \u2192 {m['to']:g}")
            lines.append("justhodl.ai/alpha-compass.html")
            send_telegram("\n".join(lines))
    except Exception as e:
        print(f"[compass] tripwire skipped: {e}")
    for c in cards:
        c["delta_conviction"] = delta_map.get(c["subject"])

    totals = (miss or {}).get("totals") or {}
    oou = totals.get("out_of_universe") or 0
    coverage = {
        "miss_summary_30d_totals": totals,
        "note": ("Misses are dominated by out-of-universe names — coverage "
                 "is a universe-expansion lever, not a signal-quality flaw."
                 if oou and oou >= max(
                     (v for k, v in totals.items()
                      if k != "out_of_universe" and isinstance(v, (int, float))),
                     default=0)
                 else "Miss mix is spread across causes — see miss desk."),
    }

    def feed_meta(d, extra=None):
        m = {"present": bool(d), "as_of": first_of(d or {}, "generated_at",
                                                   "as_of", "ts")}
        if extra:
            m.update(extra)
        return m

    out = {
        "schema_version": "2.0",
        "generated_at": now.isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "regime": regime,
        "track_record": track,
        "changes": changes,
        "top_calls": top_calls,
        "watchlist": watchlist,
        "coverage": coverage,
        "source_feeds": {
            "conviction": feed_meta(conviction),
            "magnitude_distributions": feed_meta(
                magdist, {"stacks": len((magdist or {}).get("stacks") or [])}),
            "scorecard": feed_meta(scorecard),
            "sizer": feed_meta(sizer),
            "regime_composite": feed_meta(rcomp),
            "risk_regime": feed_meta(roro),
            "factor_regime": feed_meta(factor),
            "dollar_radar": feed_meta(dollar),
            "kill_theses": feed_meta(kill),
            "best_setups": feed_meta(best),
            "miss_summary": feed_meta(miss),
            "engine_signal_map": feed_meta(
                emap, {"families": len((emap or {}).get("by_family") or {})}),
        },
    }

    s3.put_object(Bucket=BUCKET, Key=HIST_KEY,
                  Body=json.dumps(hist, default=_dec,
                                  separators=(",", ":")).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=300")
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=_dec,
                                  separators=(",", ":")).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=300")

    tiers = [c["stats"]["source"] for c in cards]
    print(f"[compass] v2 cards={len(cards)} regime={regime['label']} "
          f"sources={len(regime['sources'])} tiers={tiers} "
          f"graded={track['graded_this_run']} "
          f"quotes={track['quotes_available']}")

    return {"statusCode": 200,
            "body": json.dumps({"ok": True, "cards": len(cards),
                                "regime": regime["label"]})}


lambda_handler = handler
