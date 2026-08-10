"""aws/shared/impact_mapper.py — the ONE impact contract (wo4580).

Every engine that names beneficiaries/victims does it through this module,
so the fleet ships one methodology instead of ten drifting ones.

Doctrine (inherits ops-4559):
  * Two honest classes of percentage point, never conflated:
      - measured  : arithmetic on real data (dark share +9pp, float -4.2%/yr,
                    implied demand 12bps of ADV). No model. No CI needed.
      - estimated : model output. MUST carry ci=[lo,hi], n_obs, r2 basis.
                    A bare estimated pp is the confident-blind failure class
                    (BUG-4) and is rejected at construction time.
  * No beta history → INSUFFICIENT_HISTORY, never a silent guess.
  * Every row states its basis so a reader can audit the number.

Payload contract (schema 1.0) — engines embed under key "impact_map":
  {
    "schema": "impact-map/1.0",
    "engine": "<producer>",
    "factor": "<what shocked, e.g. port_throughput_yoy>",
    "generated_at": iso,
    "benefiting": [row...],   # sorted, strongest first
    "suffering":  [row...],
    "insufficient": [ {name, kind, reason} ... ],   # honest non-answers
    "method": str,
    "basis_note": str,
  }
  row = {
    "name": str, "kind": "company"|"industry",
    "pp": float,                     # the number, in percentage points
    "pp_kind": "measured"|"estimated",
    "unit": str,                     # what the pp is OF (e.g. "share_of_volume",
                                     # "float_per_year", "bps_adv_day",
                                     # "rel_return_60d")
    "basis": str,                    # how it was computed
    # estimated-only (enforced):
    "ci": [lo, hi], "n_obs": int, "r2": float|None,
    "tier": "measured_fact"|"estimated_beta"|"structural_exposure",
  }
"""
import json
from datetime import datetime, timezone

import boto3

_S3 = boto3.client("s3", region_name="us-east-1")
_BUCKET = "justhodl-dashboard-live"
SCHEMA = "impact-map/1.0"

GRAPH_KEY = "data/impact/exposure-graph.json"
BETAS_KEY = "data/impact/betas.json"

_CACHE = {}


def _get_json(key):
    try:
        return json.loads(_S3.get_object(Bucket=_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def load_graph(force=False):
    """Exposure graph: {"tickers": {T: {industry, sector, mcap, adv_usd,
    shares_out}}, "industries": {name: {n, mcap, tickers[]}}, ...}.
    None when the graph engine hasn't produced yet — callers must treat
    that as reduced capability, not invent structure."""
    if not force and "graph" in _CACHE:
        return _CACHE["graph"]
    g = _get_json(GRAPH_KEY)
    _CACHE["graph"] = g
    return g


def load_betas(force=False):
    if not force and "betas" in _CACHE:
        return _CACHE["betas"]
    b = _get_json(BETAS_KEY)
    _CACHE["betas"] = b
    return b


def measured_row(name, kind, pp, unit, basis):
    """A measured percentage point — arithmetic on real data."""
    if pp is None or not isinstance(pp, (int, float)):
        raise ValueError("measured_row: pp must be a real number")
    if kind not in ("company", "industry"):
        raise ValueError("measured_row: kind must be company|industry")
    return {"name": str(name), "kind": kind, "pp": round(float(pp), 2),
            "pp_kind": "measured", "unit": str(unit), "basis": str(basis),
            "tier": "measured_fact"}


def estimated_row(name, kind, pp, unit, basis, ci, n_obs, r2=None):
    """An estimated percentage point — REQUIRES interval + sample size.
    Raises rather than let a naked model number into a payload."""
    if pp is None or ci is None or len(ci) != 2 or n_obs is None:
        raise ValueError("estimated_row: pp, ci=[lo,hi], n_obs all required")
    lo, hi = float(ci[0]), float(ci[1])
    if not (lo <= float(pp) <= hi):
        raise ValueError("estimated_row: pp outside its own ci")
    if int(n_obs) < 8:
        raise ValueError("estimated_row: n_obs < 8 is INSUFFICIENT_HISTORY, "
                         "emit an insufficient entry instead")
    return {"name": str(name), "kind": kind, "pp": round(float(pp), 2),
            "pp_kind": "estimated", "unit": str(unit), "basis": str(basis),
            "ci": [round(lo, 2), round(hi, 2)], "n_obs": int(n_obs),
            "r2": (round(float(r2), 3) if r2 is not None else None),
            "tier": "estimated_beta"}


def structural_row(name, kind, basis, direction):
    """Direction-only exposure (no defensible pp yet). pp=None, honest."""
    return {"name": str(name), "kind": kind, "pp": None,
            "pp_kind": "structural", "unit": "direction_only",
            "basis": str(basis), "tier": "structural_exposure",
            "direction": "benefit" if direction >= 0 else "suffer"}


def insufficient(name, kind, reason):
    return {"name": str(name), "kind": kind, "reason": str(reason)}


def build(engine, factor, benefiting, suffering, method,
          insufficient_rows=None, basis_note=""):
    """Assemble + validate the impact_map block. Fail-closed: an invalid
    row raises here, in the producer, not in a page at 2am."""
    for side in (benefiting, suffering):
        for row in side:
            if row.get("pp_kind") == "estimated" and (
                    row.get("ci") is None or row.get("n_obs") is None):
                raise ValueError("naked estimated pp rejected: %r" % row)
            if row.get("pp_kind") == "measured" and row.get("pp") is None:
                raise ValueError("measured row without pp: %r" % row)
    key = lambda r: -abs(r["pp"] or 0) if r.get("pp") is not None else 0.0
    return {
        "schema": SCHEMA,
        "engine": engine,
        "factor": factor,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "benefiting": sorted(benefiting, key=key)[:25],
        "suffering": sorted(suffering, key=key)[:25],
        "insufficient": (insufficient_rows or [])[:25],
        "method": method,
        "basis_note": basis_note,
    }


def industry_rollup(ticker_rows, graph, min_names=2):
    """Measured company rows → industry rows by graph membership.
    pp = mcap-weighted mean of member pps; carries n_members. Industries
    with < min_names members are skipped (one name is not an industry
    read). Returns [] when the graph is absent — never guesses."""
    if not graph or not isinstance(graph.get("tickers"), dict):
        return []
    tk = graph["tickers"]
    by_ind = {}
    for row in ticker_rows:
        info = tk.get(row["name"])
        if not info or not info.get("industry"):
            continue
        ind = info["industry"]
        w = float(info.get("mcap") or 0) or 1.0
        d = by_ind.setdefault(ind, {"wsum": 0.0, "psum": 0.0, "n": 0,
                                    "unit": row["unit"], "basis": row["basis"]})
        d["wsum"] += w
        d["psum"] += w * float(row["pp"] or 0)
        d["n"] += 1
    out = []
    for ind, d in by_ind.items():
        if d["n"] < min_names or d["wsum"] <= 0:
            continue
        r = measured_row(ind, "industry", d["psum"] / d["wsum"], d["unit"],
                         "mcap-weighted mean of %d member names — %s"
                         % (d["n"], d["basis"]))
        r["n_members"] = d["n"]
        out.append(r)
    return out


def beta_impact(tickers_or_industries, factor, shock_value, kind="industry"):
    """Estimated rows from the beta store for a factor shock. Names without
    a qualifying beta come back in the insufficient list — the caller
    publishes both. shock_value in the factor's native unit."""
    rows, missing = [], []
    b = load_betas()
    store = ((b or {}).get("betas") or {}).get(factor) or {}
    for name in tickers_or_industries:
        e = store.get(name)
        if not (isinstance(e, dict) and e.get("beta") is not None
                and (e.get("n_obs") or 0) >= 8):
            missing.append(insufficient(
                name, kind, "no qualifying beta for %s (need n_obs>=8; "
                "factor history is accruing nightly)" % factor))
            continue
        pp = float(e["beta"]) * float(shock_value)
        se = float(e.get("se") or abs(e["beta"]) * 0.5)
        half = 1.96 * se * abs(float(shock_value))
        rows.append(estimated_row(
            name, kind, pp, e.get("unit", "rel_return_60d"),
            "beta %.3f (n=%d) x shock %.2f — %s"
            % (e["beta"], e["n_obs"], shock_value,
               e.get("basis", "OLS on archived factor history")),
            [pp - half, pp + half], e["n_obs"], e.get("r2")))
    return rows, missing
