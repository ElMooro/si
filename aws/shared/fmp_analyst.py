"""fmp_analyst — analyst rating transitions + price-target revisions sourced
from FMP /stable (Khalid's entitled key), replacing the dead Benzinga-via-
Massive feeds for justhodl-analyst-actions.

Verified live endpoints (ops 3318):
  - grades-latest-news        -> rating transitions (up/downgrades)
      fields: symbol, publishedDate, newGrade, previousGrade,
              gradingCompany, action, priceWhenPosted
  - price-target-latest-news  -> price-target revisions
      fields: symbol, publishedDate, analystName, priceTarget,
              adjPriceTarget, priceWhenPosted, analystCompany

fetch_ratings() returns records in the SAME shape the engine's scoring
already expects, so no change to lambda_function scoring/rollup/output:
  ticker, company, firm, rating, previous_rating, rating_dir
  (UPGRADE/DOWNGRADE/None), pt, pt_prev, pt_pct, pt_dir (RAISE/CUT/None),
  importance (constant 3 -> weight 1.0; FMP has no importance field),
  date

Guidance: FMP /stable has no direct company-guidance raise/cut feed, so
fetch_guidance() returns [] (page renders its guidance sections empty,
exactly as the schema tolerates). fetch_analyst_insights() -> [] too;
PT numbers already ride on the ratings records.
"""
import json
import os
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone

_BASE = "https://financialmodelingprep.com/stable"
_CACHE = {}

# grade tiers for direction classification
BUY_TIER = {"BUY", "OUTPERFORM", "OVERWEIGHT", "POSITIVE", "STRONG BUY",
            "ACCUMULATE", "ADD", "CONVICTION BUY", "TOP PICK", "MARKET OUTPERFORM"}
SELL_TIER = {"SELL", "UNDERPERFORM", "UNDERWEIGHT", "NEGATIVE", "STRONG SELL",
             "REDUCE", "MARKET UNDERPERFORM"}
HOLD_TIER = {"HOLD", "NEUTRAL", "MARKET PERFORM", "EQUAL WEIGHT", "MIXED",
             "SECTOR PERFORM", "IN-LINE", "PEER PERFORM", "SECTOR WEIGHT",
             "EQUAL-WEIGHT"}
_RANK = {"SELL": 0, "HOLD": 1, "BUY": 2}


def _key():
    if "k" in _CACHE:
        return _CACHE["k"]
    k = os.environ.get("FMP_KEY") or os.environ.get("FMP_API_KEY")
    if not k:
        try:
            import boto3
            k = boto3.client("ssm", "us-east-1").get_parameter(
                Name="/justhodl/fmp-api-key", WithDecryption=True
            )["Parameter"]["Value"]
        except Exception:
            k = ""
    _CACHE["k"] = k
    return k


def _get(path, params, timeout=25):
    k = _key()
    if not k:
        return None
    p = {**params, "apikey": k}
    qs = "&".join(f"{a}={b}" for a, b in p.items())
    url = f"{_BASE}/{path}?{qs}"
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "justhodl-analyst/2.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"[fmp_analyst] {path}: {e}")
        return None


def _tier(grade):
    g = (grade or "").upper().strip()
    if g in BUY_TIER:
        return "BUY"
    if g in SELL_TIER:
        return "SELL"
    if g in HOLD_TIER:
        return "HOLD"
    return None


def _within(pub_iso, days_back):
    try:
        d = datetime.fromisoformat(pub_iso.replace("Z", "+00:00"))
        return d >= datetime.now(timezone.utc) - timedelta(days=days_back)
    except Exception:
        return True  # keep if unparseable rather than silently drop


def fetch_ratings(days_back=7, min_importance=1, limit=1000):
    """Rating transitions + PT moves, one flat list in engine's expected shape."""
    grades = _get("grades-latest-news", {"limit": min(limit, 1000)}) or []
    pts = _get("price-target-latest-news", {"limit": min(limit, 1000)}) or []
    if not isinstance(grades, list):
        grades = []
    if not isinstance(pts, list):
        pts = []

    # index most-recent PT revision per symbol so a grade change can carry
    # its PT delta (Benzinga bundled these; FMP splits them across 2 feeds)
    pt_by_sym = {}
    for p in pts:
        sym = p.get("symbol")
        if not sym or not _within(p.get("publishedDate", ""), days_back):
            continue
        cur = pt_by_sym.get(sym)
        if not cur or (p.get("publishedDate", "") > cur.get("publishedDate", "")):
            pt_by_sym[sym] = p

    out = []
    seen_pt_syms = set()

    # 1. rating transitions (may also carry a PT if same symbol revised)
    for g in grades:
        sym = g.get("symbol")
        if not sym or not _within(g.get("publishedDate", ""), days_back):
            continue
        prev, new = g.get("previousGrade"), g.get("newGrade")
        pt_dir = pt = pt_prev = pt_pct = None
        p = pt_by_sym.get(sym)
        if p:
            seen_pt_syms.add(sym)
            new_pt = p.get("adjPriceTarget") or p.get("priceTarget")
            base = p.get("priceWhenPosted")
            pt = new_pt
            if new_pt and base:
                try:
                    pt_pct = round((float(new_pt) - float(base)) / float(base) * 100, 1)
                    pt_dir = "RAISE" if pt_pct > 0 else "CUT" if pt_pct < 0 else None
                    pt_prev = base
                except (TypeError, ValueError, ZeroDivisionError):
                    pass

        # direction from action field first, else tier comparison
        action = (g.get("action") or "").lower()
        rating_dir = None
        if "upgrade" in action:
            rating_dir = "UPGRADE"
        elif "downgrade" in action:
            rating_dir = "DOWNGRADE"
        else:
            pt_, nt_ = _tier(prev), _tier(new)
            if pt_ and nt_:
                if _RANK[nt_] > _RANK[pt_]:
                    rating_dir = "UPGRADE"
                elif _RANK[nt_] < _RANK[pt_]:
                    rating_dir = "DOWNGRADE"

        out.append({
            "ticker": sym, "company": sym,
            "firm": g.get("gradingCompany") or "",
            "rating": new, "previous_rating": prev,
            "rating_dir": rating_dir,
            "pt": pt, "pt_prev": pt_prev, "pt_pct": pt_pct, "pt_dir": pt_dir,
            "importance": 3,  # FMP has no importance -> weight 1.0
            "date": g.get("publishedDate"),
        })

    # 2. pure PT revisions with no accompanying grade change this window
    for sym, p in pt_by_sym.items():
        if sym in seen_pt_syms:
            continue
        new_pt = p.get("adjPriceTarget") or p.get("priceTarget")
        base = p.get("priceWhenPosted")
        pt_pct = pt_dir = None
        if new_pt and base:
            try:
                pt_pct = round((float(new_pt) - float(base)) / float(base) * 100, 1)
                pt_dir = "RAISE" if pt_pct > 0 else "CUT" if pt_pct < 0 else None
            except (TypeError, ValueError, ZeroDivisionError):
                pass
        if not pt_dir:
            continue
        out.append({
            "ticker": sym, "company": sym,
            "firm": p.get("analystCompany") or "",
            "rating": None, "previous_rating": None, "rating_dir": None,
            "pt": new_pt, "pt_prev": base, "pt_pct": pt_pct, "pt_dir": pt_dir,
            "importance": 3, "date": p.get("publishedDate"),
        })

    print(f"[fmp_analyst] grades={len(grades)} pts={len(pts)} "
          f"-> ratings_records={len(out)}")
    return out


def fetch_guidance(days_back=21, min_importance=1, limit=1000):
    """FMP /stable has no company guidance raise/cut feed. Return empty;
    the page's guidance sections render empty, which the schema tolerates."""
    return []


def fetch_analyst_insights(days_back=7, limit=500):
    """PT numbers already ride on fetch_ratings records. No separate feed."""
    return []
