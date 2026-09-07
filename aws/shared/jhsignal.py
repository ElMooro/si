"""aws/shared/jhsignal.py -- JHSIGNAL-1.0, the JustHodl universal signal envelope.

This is the language-native twin of schemas/jhsignal-1.0.json. It is pure
Python (no jsonschema dependency at Lambda runtime) and STRICT: a malformed
signal is rejected, never coerced. Adapters build signals through
``make_signal`` so that ids, timestamps and horizon fields are consistent,
and the bus/state store call ``validate`` again on the way in so a bad
producer cannot poison the current-state store.

Doctrine baked in (mirrors the Fusion spec, phases 1, 6, 54):

  * seven canonical directions with a numeric twin in [-1, +1]
  * four canonical horizons (TACTICAL / SWING / INTERMEDIATE / STRUCTURAL)
  * every signal carries observed_at / data_asof / published_at, a half-life
    and a freshness TTL -- stale data is never treated as fresh
  * exponential decay:  w = exp(-ln2 * age_days / half_life_days)
  * canonical entity ids ("equity:NVDA", "etf:SMH", "crypto:BTC", ...)
    instead of bare tickers, with symbol canonicalisation
  * event envelopes carry event_id / parent / root / propagation_depth /
    origin_engine so a derived signal can never trigger itself forever

Nothing in here talks to AWS.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

SCHEMA_VERSION = "JHSIGNAL-1.0"

# ---------------------------------------------------------------------------
# Canonical vocabularies
# ---------------------------------------------------------------------------
DIRECTIONS: Dict[str, float] = {
    "strong_bearish": -1.0,
    "bearish": -0.66,
    "slightly_bearish": -0.33,
    "neutral": 0.0,
    "slightly_bullish": 0.33,
    "bullish": 0.66,
    "strong_bullish": 1.0,
}
_DIRECTION_ORDER = list(DIRECTIONS.keys())

HORIZONS: Dict[str, Tuple[int, int]] = {
    "TACTICAL": (0, 5),
    "SWING": (5, 90),
    "INTERMEDIATE": (90, 365),
    "STRUCTURAL": (365, 3650),
}

ENGINE_FAMILIES = ("MACRO", "FLOW", "FUNDAMENTAL", "CATALYST", "MARKET", "RISK")

CATEGORIES = (
    "macro", "liquidity", "credit", "risk", "smart_money", "institutional_flow",
    "options_positioning", "fundamental_growth", "quality", "valuation",
    "price_confirmation", "catalyst", "sentiment", "geopolitics", "positioning",
    "cycle",
)

# Evidence clusters (phase 12) -- the independence unit for fusion.
EVIDENCE_CLUSTERS = (
    "fundamental_growth", "quality", "valuation", "smart_money",
    "institutional_flow", "options_positioning", "price_confirmation",
    "macro_support", "credit", "liquidity", "catalyst", "geopolitics", "risk",
    "sentiment", "positioning", "cycle",
)

ENTITY_TYPES = (
    "equity", "etf", "crypto", "commodity", "fx", "country", "sector",
    "industry", "index", "central_bank", "market", "theme", "bond",
)

_ENTITY_RE = re.compile(r"^(%s):[A-Z0-9][A-Z0-9_\-\.]{0,63}$" % "|".join(ENTITY_TYPES))
_ENGINE_RE = re.compile(r"^[a-z0-9][a-z0-9_]{1,63}$")
_TYPE_RE = re.compile(r"^[a-z0-9][a-z0-9_]{1,63}$")
_TICKER_RE = re.compile(r"^[A-Z0-9][A-Z0-9\-\.]{0,15}$")
_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")

REQUIRED = (
    "schema_version", "signal_id", "engine_id", "engine_version",
    "entity_id", "entity_type", "observed_at", "data_asof", "published_at",
    "category", "signal_type", "direction", "score", "confidence",
    "horizon", "horizon_min_days", "horizon_max_days", "half_life_days",
    "freshness_ttl_seconds", "supports", "affects", "dependencies",
    "invalidation", "evidence", "quality", "metadata",
)
OPTIONAL = ("ticker", "direction_numeric", "magnitude", "percentile")
_ALLOWED = set(REQUIRED) | set(OPTIONAL)

FUTURE_SKEW_SECONDS = 300  # a timestamp more than 5 minutes in the future is rejected


class JHSignalError(ValueError):
    """Raised when a signal violates JHSIGNAL-1.0."""

    def __init__(self, problems: List[str]):
        self.problems = list(problems)
        super().__init__("; ".join(self.problems))


# ---------------------------------------------------------------------------
# Time helpers (UTC everywhere)
# ---------------------------------------------------------------------------
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_ts(value: Any) -> Optional[datetime]:
    """Parse an ISO-8601 timestamp (or a plain YYYY-MM-DD) to an aware UTC datetime.

    Returns None when the value cannot be parsed -- callers decide whether that
    is fatal. A bare date is interpreted as midnight UTC of that day.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        s = s + "T00:00:00+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        try:
            dt = datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# Entity ids
# ---------------------------------------------------------------------------
def canonical_symbol(symbol: str) -> str:
    """Canonical ticker form: upper-case, share-class dots become hyphens
    (BRK.B -> BRK-B, the Finviz/Yahoo form the fleet joins on), whitespace
    stripped. Crypto pairs lose a trailing USD/USDT quote ("X:BTCUSD" -> "BTC").
    """
    s = str(symbol or "").strip().upper()
    if s.startswith("X:"):
        s = s[2:]
        for q in ("USDT", "USD"):
            if s.endswith(q) and len(s) > len(q):
                s = s[: -len(q)]
                break
    s = s.replace(" ", "")
    if s.startswith("^"):
        s = s[1:]
    s = s.replace(".", "-")
    return s


def entity_id(entity_type: str, symbol: str) -> str:
    if entity_type not in ENTITY_TYPES:
        raise JHSignalError([f"unknown entity_type {entity_type!r}"])
    sym = canonical_symbol(symbol)
    eid = f"{entity_type}:{sym}"
    if not _ENTITY_RE.match(eid):
        raise JHSignalError([f"cannot build a canonical entity id from {symbol!r}"])
    return eid


def split_entity_id(eid: str) -> Tuple[str, str]:
    if not isinstance(eid, str) or ":" not in eid:
        raise JHSignalError([f"malformed entity_id {eid!r}"])
    t, s = eid.split(":", 1)
    return t, s


# ---------------------------------------------------------------------------
# Direction / horizon helpers
# ---------------------------------------------------------------------------
def direction_from_numeric(x: float) -> str:
    """Bucket a normalised value in [-1, 1] into the seven canonical directions.

    Thresholds are symmetric: |x| < 0.15 neutral, < 0.45 slightly, < 0.8 plain,
    else strong. The numeric twin keeps the exact value; the label is for
    humans and coarse grouping.
    """
    if x is None or (isinstance(x, float) and math.isnan(x)):
        raise JHSignalError(["direction_from_numeric: value is missing/NaN"])
    x = max(-1.0, min(1.0, float(x)))
    a = abs(x)
    if a < 0.15:
        return "neutral"
    if a < 0.45:
        lab = "slightly_"
    elif a < 0.8:
        lab = ""
    else:
        lab = "strong_"
    return lab + ("bullish" if x > 0 else "bearish")


def numeric_from_direction(direction: str) -> float:
    if direction not in DIRECTIONS:
        raise JHSignalError([f"unknown direction {direction!r}"])
    return DIRECTIONS[direction]


def horizon_for_days(days: float) -> str:
    """Canonical horizon for a holding period expressed in days."""
    d = float(days)
    if d < 5:
        return "TACTICAL"
    if d < 90:
        return "SWING"
    if d < 365:
        return "INTERMEDIATE"
    return "STRUCTURAL"


def horizon_bounds(horizon: str) -> Tuple[int, int]:
    if horizon not in HORIZONS:
        raise JHSignalError([f"unknown horizon {horizon!r}"])
    return HORIZONS[horizon]


# ---------------------------------------------------------------------------
# Freshness & decay (phase 6)
# ---------------------------------------------------------------------------
def freshness_weight(age_days: float, half_life_days: float) -> float:
    """exp(-ln2 * age / half_life). 1.0 when fresh, 0.5 at one half-life."""
    if half_life_days is None or half_life_days <= 0:
        raise JHSignalError(["half_life_days must be > 0"])
    age = max(0.0, float(age_days))
    return math.exp(-math.log(2.0) * age / float(half_life_days))


def age_days(signal: Dict[str, Any], now: Optional[datetime] = None) -> float:
    now = now or utcnow()
    asof = parse_ts(signal.get("data_asof"))
    if asof is None:
        raise JHSignalError(["data_asof unparseable"])
    return max(0.0, (now - asof).total_seconds() / 86400.0)


def freshness_state(signal: Dict[str, Any], now: Optional[datetime] = None) -> str:
    """FRESH within the TTL, STALE between 1x and 2x TTL, EXPIRED after 2x TTL.

    The bus keeps STALE signals visible (decayed and flagged) so a consumer
    can see *why* coverage dropped; EXPIRED signals leave the active set.
    """
    now = now or utcnow()
    asof = parse_ts(signal.get("data_asof"))
    if asof is None:
        return "INVALID"
    ttl = float(signal.get("freshness_ttl_seconds") or 0)
    if ttl <= 0:
        return "INVALID"
    age = (now - asof).total_seconds()
    if age <= ttl:
        return "FRESH"
    if age <= 2 * ttl:
        return "STALE"
    return "EXPIRED"


def expires_at(signal: Dict[str, Any]) -> datetime:
    asof = parse_ts(signal.get("data_asof"))
    if asof is None:
        raise JHSignalError(["data_asof unparseable"])
    return asof + timedelta(seconds=2 * float(signal["freshness_ttl_seconds"]))


def effective_strength(signal: Dict[str, Any], *, now: Optional[datetime] = None,
                       reliability_weight: float = 1.0, regime_fit: float = 1.0,
                       independence_weight: float = 1.0) -> Dict[str, float]:
    """Spec phase 6 / 14 composition, returned with every factor exposed:

        effective = score * confidence * freshness * reliability * regime_fit
                    * independence * evidence_quality
    """
    fw = freshness_weight(age_days(signal, now), float(signal["half_life_days"]))
    q = signal.get("quality") or {}
    eq = float(q.get("source_reliability", 1.0)) * float(q.get("data_completeness", 1.0)) \
        * float(q.get("calculation_quality", 1.0))
    eq = eq ** (1.0 / 3.0)  # geometric mean keeps the scale near the inputs
    val = float(signal["score"]) * float(signal["confidence"]) * fw * float(reliability_weight) \
        * float(regime_fit) * float(independence_weight) * eq
    return {
        "effective": val,
        "score": float(signal["score"]),
        "confidence": float(signal["confidence"]),
        "freshness_weight": fw,
        "reliability_weight": float(reliability_weight),
        "regime_fit": float(regime_fit),
        "independence_weight": float(independence_weight),
        "evidence_quality": eq,
    }


# ---------------------------------------------------------------------------
# Validation (strict, never coerces)
# ---------------------------------------------------------------------------
def _is_num(x: Any) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool) and not (isinstance(x, float) and (math.isnan(x) or math.isinf(x)))


def validate(signal: Any, *, now: Optional[datetime] = None) -> List[str]:
    """Return a list of problems (empty == valid). Does not mutate the input."""
    p: List[str] = []
    if not isinstance(signal, dict):
        return ["signal must be an object"]
    now = now or utcnow()
    extra = set(signal.keys()) - _ALLOWED
    if extra:
        p.append(f"unknown fields: {sorted(extra)}")
    for k in REQUIRED:
        if k not in signal:
            p.append(f"missing {k}")
    if p:
        return p

    if signal["schema_version"] != SCHEMA_VERSION:
        p.append(f"schema_version must be {SCHEMA_VERSION}")
    if not isinstance(signal["signal_id"], str) or not _UUID_RE.match(signal["signal_id"]):
        p.append("signal_id must be a lowercase uuid4")
    if not isinstance(signal["engine_id"], str) or not _ENGINE_RE.match(signal["engine_id"]):
        p.append("engine_id must match ^[a-z0-9][a-z0-9_]{1,63}$")
    if not isinstance(signal["engine_version"], str) or not 1 <= len(signal["engine_version"]) <= 32:
        p.append("engine_version must be a 1-32 char string")
    eid = signal["entity_id"]
    if not isinstance(eid, str) or not _ENTITY_RE.match(eid):
        p.append(f"entity_id {eid!r} is not canonical (type:SYMBOL)")
    else:
        et, _ = split_entity_id(eid)
        if signal["entity_type"] != et:
            p.append(f"entity_type {signal['entity_type']!r} disagrees with entity_id prefix {et!r}")
    if signal["entity_type"] not in ENTITY_TYPES:
        p.append(f"entity_type {signal['entity_type']!r} unknown")
    t = signal.get("ticker")
    if t is not None and (not isinstance(t, str) or not _TICKER_RE.match(t)):
        p.append("ticker must be canonical upper-case or null")

    ts: Dict[str, datetime] = {}
    for k in ("observed_at", "data_asof", "published_at"):
        v = signal[k]
        dt = parse_ts(v) if isinstance(v, str) else None
        if dt is None:
            p.append(f"{k} is not an ISO-8601 timestamp")
        else:
            ts[k] = dt
            if (dt - now).total_seconds() > FUTURE_SKEW_SECONDS:
                p.append(f"{k} is in the future ({v})")
    if len(ts) == 3:
        if ts["data_asof"] > ts["observed_at"] + timedelta(seconds=FUTURE_SKEW_SECONDS):
            p.append("data_asof is after observed_at")
        if ts["observed_at"] > ts["published_at"] + timedelta(seconds=FUTURE_SKEW_SECONDS):
            p.append("observed_at is after published_at")

    if signal["category"] not in CATEGORIES:
        p.append(f"category {signal['category']!r} unknown")
    if not isinstance(signal["signal_type"], str) or not _TYPE_RE.match(signal["signal_type"]):
        p.append("signal_type must match ^[a-z0-9][a-z0-9_]{1,63}$")
    d = signal["direction"]
    if d not in DIRECTIONS:
        p.append(f"direction {d!r} not one of {_DIRECTION_ORDER}")
    sc = signal["score"]
    if not _is_num(sc) or not -1.0 <= sc <= 1.0:
        p.append("score must be a number in [-1, 1]")
    elif d in DIRECTIONS:
        dn = DIRECTIONS[d]
        if dn == 0.0 and abs(sc) >= 0.15:
            p.append("direction neutral but |score| >= 0.15")
        elif dn != 0.0 and sc != 0.0 and (sc > 0) != (dn > 0):
            p.append("direction sign disagrees with score sign")
    dnum = signal.get("direction_numeric")
    if dnum is not None:
        if not _is_num(dnum) or not -1.0 <= dnum <= 1.0:
            p.append("direction_numeric must be in [-1, 1]")
        elif d in DIRECTIONS and direction_from_numeric(dnum) != d:
            p.append("direction_numeric does not bucket to direction")
    c = signal["confidence"]
    if not _is_num(c) or not 0.0 <= c <= 1.0:
        p.append("confidence must be a number in [0, 1]")
    m = signal.get("magnitude")
    if m is not None and not _is_num(m):
        p.append("magnitude must be a number or null")
    pc = signal.get("percentile")
    if pc is not None and (not _is_num(pc) or not 0 <= pc <= 100):
        p.append("percentile must be in [0, 100] or null")

    h = signal["horizon"]
    if h not in HORIZONS:
        p.append(f"horizon {h!r} unknown")
    hmin, hmax = signal["horizon_min_days"], signal["horizon_max_days"]
    if not (isinstance(hmin, int) and not isinstance(hmin, bool) and hmin >= 0):
        p.append("horizon_min_days must be an int >= 0")
    if not (isinstance(hmax, int) and not isinstance(hmax, bool) and hmax >= 1):
        p.append("horizon_max_days must be an int >= 1")
    if isinstance(hmin, int) and isinstance(hmax, int) and hmin > hmax:
        p.append("horizon_min_days > horizon_max_days")
    hl = signal["half_life_days"]
    if not _is_num(hl) or hl <= 0:
        p.append("half_life_days must be > 0")
    ttl = signal["freshness_ttl_seconds"]
    if not (isinstance(ttl, int) and not isinstance(ttl, bool) and ttl >= 60):
        p.append("freshness_ttl_seconds must be an int >= 60")

    sup = signal["supports"]
    if not isinstance(sup, list) or not all(isinstance(x, str) for x in sup):
        p.append("supports must be a list of entity ids")
    aff = signal["affects"]
    if not isinstance(aff, list):
        p.append("affects must be a list")
    else:
        for i, a in enumerate(aff):
            if not isinstance(a, dict) or set(a.keys()) != {"entity_id", "relationship", "strength"}:
                p.append(f"affects[{i}] must have exactly entity_id/relationship/strength")
                continue
            if not isinstance(a["entity_id"], str) or not _ENTITY_RE.match(a["entity_id"]):
                p.append(f"affects[{i}].entity_id not canonical")
            if not _is_num(a["strength"]) or not 0.0 <= a["strength"] <= 1.0:
                p.append(f"affects[{i}].strength must be in [0, 1]")
    dep = signal["dependencies"]
    if not isinstance(dep, list) or not all(isinstance(x, str) for x in dep):
        p.append("dependencies must be a list of strings")
    inv = signal["invalidation"]
    if not isinstance(inv, dict) or inv.get("type") not in ("text", "level", "state", "time") \
            or not isinstance(inv.get("description"), str) or not inv.get("description"):
        p.append("invalidation must be {type: text|level|state|time, description: non-empty}")
    ev = signal["evidence"]
    if not isinstance(ev, list):
        p.append("evidence must be a list")
    else:
        for i, e in enumerate(ev):
            if not isinstance(e, dict) or "field" not in e or "value" not in e:
                p.append(f"evidence[{i}] must carry field and value")
    q = signal["quality"]
    if not isinstance(q, dict) or set(q.keys()) != {"source_reliability", "data_completeness", "calculation_quality"}:
        p.append("quality must have exactly source_reliability/data_completeness/calculation_quality")
    else:
        for k, v in q.items():
            if not _is_num(v) or not 0.0 <= v <= 1.0:
                p.append(f"quality.{k} must be in [0, 1]")
    if not isinstance(signal["metadata"], dict):
        p.append("metadata must be an object")
    return p


def assert_valid(signal: Dict[str, Any], *, now: Optional[datetime] = None) -> Dict[str, Any]:
    problems = validate(signal, now=now)
    if problems:
        raise JHSignalError(problems)
    return signal


# ---------------------------------------------------------------------------
# Builder
# ---------------------------------------------------------------------------
def make_signal(*, engine_id: str, engine_version: str, entity_type: str, symbol: str,
                category: str, signal_type: str, score: float, confidence: float,
                data_asof: Any, horizon: Optional[str] = None,
                half_life_days: Optional[float] = None, freshness_ttl_seconds: Optional[int] = None,
                observed_at: Any = None, published_at: Any = None,
                magnitude: Optional[float] = None, percentile: Optional[float] = None,
                affects: Optional[Iterable[Dict[str, Any]]] = None,
                dependencies: Optional[Iterable[str]] = None,
                invalidation: Optional[Dict[str, Any]] = None,
                evidence: Optional[Iterable[Dict[str, Any]]] = None,
                quality: Optional[Dict[str, float]] = None,
                metadata: Optional[Dict[str, Any]] = None,
                now: Optional[datetime] = None) -> Dict[str, Any]:
    """Build and validate a JHSIGNAL-1.0 signal.

    ``score`` is the normalised strength in [-1, 1]; the direction label is
    derived from it (never the other way round) so the two can't disagree.
    ``data_asof`` is the producing engine's own timestamp -- pass it through,
    never "now". ``half_life_days`` defaults from the horizon (TACTICAL 1,
    SWING 21, INTERMEDIATE 90, STRUCTURAL 365) and the TTL defaults to 2x the
    half-life, floored at one hour.
    """
    now = now or utcnow()
    if not _is_num(score):
        raise JHSignalError(["score missing -- adapters must not fabricate a value"])
    score = max(-1.0, min(1.0, float(score)))
    if not _is_num(confidence):
        raise JHSignalError(["confidence missing -- adapters must not fabricate a value"])
    asof = parse_ts(data_asof)
    if asof is None:
        raise JHSignalError([f"data_asof unparseable: {data_asof!r}"])
    obs = parse_ts(observed_at) if observed_at else now
    pub = parse_ts(published_at) if published_at else now
    if horizon is None:
        horizon = "SWING" if half_life_days is None else horizon_for_days(2.0 * float(half_life_days))
    hmin, hmax = horizon_bounds(horizon)
    if half_life_days is None:
        half_life_days = {"TACTICAL": 1.0, "SWING": 21.0, "INTERMEDIATE": 90.0, "STRUCTURAL": 365.0}[horizon]
    if freshness_ttl_seconds is None:
        freshness_ttl_seconds = int(max(3600, round(2 * float(half_life_days) * 86400)))
    eid = entity_id(entity_type, symbol)
    sig = {
        "schema_version": SCHEMA_VERSION,
        "signal_id": str(uuid.uuid4()),
        "engine_id": engine_id,
        "engine_version": str(engine_version),
        "entity_id": eid,
        "entity_type": entity_type,
        "ticker": canonical_symbol(symbol) if entity_type in ("equity", "etf", "crypto", "index", "commodity", "fx", "bond") else None,
        "observed_at": iso(obs),
        "data_asof": iso(asof),
        "published_at": iso(pub),
        "category": category,
        "signal_type": signal_type,
        "direction": direction_from_numeric(round(score, 6)),
        "direction_numeric": round(score, 6),
        "score": round(score, 6),
        "confidence": round(max(0.0, min(1.0, float(confidence))), 6),
        "magnitude": (round(float(magnitude), 6) if _is_num(magnitude) else None),
        "percentile": (round(float(percentile), 3) if _is_num(percentile) else None),
        "horizon": horizon,
        "horizon_min_days": int(hmin),
        "horizon_max_days": int(hmax),
        "half_life_days": float(half_life_days),
        "freshness_ttl_seconds": int(freshness_ttl_seconds),
        "supports": [eid],
        "affects": [dict(a) for a in (affects or [])],
        "dependencies": list(dependencies or []),
        "invalidation": dict(invalidation or {"type": "text", "description": "producing engine withdraws or reverses the read"}),
        "evidence": [dict(e) for e in (evidence or [])],
        "quality": dict(quality or {"source_reliability": 1.0, "data_completeness": 1.0, "calculation_quality": 1.0}),
        "metadata": dict(metadata or {}),
    }
    return assert_valid(sig, now=now)


def signal_key(signal: Dict[str, Any]) -> str:
    """Current-state sort key: one live signal per engine x type x horizon."""
    return f"{signal['engine_id']}#{signal['signal_type']}#{signal['horizon']}"


def idempotency_key(signal: Dict[str, Any]) -> str:
    """Stable hash of the fact -- the same engine read republished from the same
    data_asof is the same fact, not a new signal (phase 54 dedupe)."""
    basis = "|".join([
        signal["engine_id"], signal["entity_id"], signal["signal_type"], signal["horizon"],
        signal["data_asof"], f"{float(signal['score']):.4f}",
    ])
    return hashlib.sha256(basis.encode()).hexdigest()[:32]


def compact(signal: Dict[str, Any]) -> Dict[str, Any]:
    """The read-model projection: what a page or the fusion engine needs."""
    return {
        "signal_id": signal["signal_id"],
        "engine_id": signal["engine_id"],
        "engine_version": signal["engine_version"],
        "entity_id": signal["entity_id"],
        "entity_type": signal["entity_type"],
        "ticker": signal.get("ticker"),
        "category": signal["category"],
        "signal_type": signal["signal_type"],
        "direction": signal["direction"],
        "score": signal["score"],
        "confidence": signal["confidence"],
        "magnitude": signal.get("magnitude"),
        "percentile": signal.get("percentile"),
        "horizon": signal["horizon"],
        "half_life_days": signal["half_life_days"],
        "freshness_ttl_seconds": signal["freshness_ttl_seconds"],
        "data_asof": signal["data_asof"],
        "published_at": signal["published_at"],
        "affects": signal["affects"],
        "quality": signal["quality"],
        "invalidation": signal["invalidation"],
        "evidence": signal["evidence"][:6],
        "metadata": signal["metadata"],
    }


# ---------------------------------------------------------------------------
# Event envelopes (phase 4 / 54)
# ---------------------------------------------------------------------------
EVT_SIGNAL_PUBLISHED = "jhsignal.published"
EVT_SIGNAL_EXPIRED = "jhsignal.expired"
EVT_SIGNAL_INVALIDATED = "jhsignal.invalidated"
EVT_SIGNAL_REVISED = "jhsignal.revised"
EVT_BATCH_PUBLISHED = "jhsignal.batch_published"
EVT_HARD_VETO = "jhsignal.hard_veto"
EVT_SOFT_VETO = "jhsignal.soft_veto"
EVT_FUSION_CHANGED = "jhsignal.fusion_changed"
EVT_CONVICTION_ACCELERATION = "jhsignal.conviction_acceleration"
EVT_CRITICAL_DEPENDENCY_FAILED = "jhsignal.critical_dependency_failed"
EVT_REGIME_CHANGED = "jhsignal.regime_changed"

DEFAULT_MAX_PROPAGATION_DEPTH = 3


def envelope(detail: Dict[str, Any], *, origin_engine: str, parent: Optional[Dict[str, Any]] = None,
             max_depth: int = DEFAULT_MAX_PROPAGATION_DEPTH) -> Dict[str, Any]:
    """Wrap a detail payload with loop-protection fields.

    Raises JHSignalError when the parent's depth already sits at the cap --
    the caller must NOT publish (phase 54: a derived signal may not trigger
    itself endlessly).
    """
    event_id = str(uuid.uuid4())
    if parent:
        depth = int(parent.get("propagation_depth", 0)) + 1
        root = parent.get("root_event_id") or parent.get("event_id") or event_id
        parent_id = parent.get("event_id")
    else:
        depth, root, parent_id = 0, event_id, None
    if depth > max_depth:
        raise JHSignalError([f"propagation depth {depth} exceeds max {max_depth}"])
    out = dict(detail)
    out.update({
        "event_id": event_id,
        "parent_event_id": parent_id,
        "root_event_id": root,
        "propagation_depth": depth,
        "origin_engine": origin_engine,
        "emitted_at": iso(utcnow()),
    })
    return out


def dumps(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), sort_keys=True, default=str)
