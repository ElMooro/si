"""Governed fleet-input discovery for the AI decision desk.

The engine registry is the source of truth for what the system claims is wired.
This module reads every unique registered feed, records provenance/freshness and
creates a bounded digest. Raw heterogeneous documents are never copied into the
LLM prompt. Untimestamped, stale, missing, malformed and explicitly dead feeds
remain visible in coverage, but are not eligible evidence for a market call.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

try:
    from private_artifact import is_private_source as _canonical_private_source
except Exception:
    _canonical_private_source = None

REGISTRY_KEY = "data/engine-wiring.json"
DEFAULT_SLA_H = 48.0
MAX_FUTURE_SKEW_H = 5.0 / 60.0
MIN_REGISTRY_VERSION = 2
MIN_UNIQUE_FEEDS = 150
MAX_VALUE_CHARS = 96
SIGNAL_KEYS = re.compile(
    r"(score|signal|stance|regime|state|status|verdict|posture|trend|direction|"
    r"probability|confidence|risk|phase|decision|allow|veto|rating)$",
    re.I,
)
SYMBOL_KEYS = {"ticker", "symbol", "asset", "entity_id"}
PRIVATE_TOKENS = ("brain", "note", "journal", "portfolio", "credential", "secret")
PRIVATE_FEEDS = frozenset({
    "data/brain.json", "data/brain-history.json", "data/journal-graded.json",
    "data/my-brief.json", "data/devils-advocate.json", "data/notes-index.json",
    "data/notes-themes.json", "data/playbook-rules.json", "data/risk-sizer.json",
    "data/pm-decision.json", "data/pm-decision-history.json",
    "data/behavior-mirror.json", "data/ai-brief.json", "data/user-watchlist.json",
    "data/vol-regime-private.json", "data/user-trades.json",
    "data/user-trades-stats.json", "data/portfolio-manager-brief.json",
    "data/tradingview-notes.json", "data/ai-brief.md", "data/_telegram-chat.json",
    "data/tv-sources.json", "data/history/behavior-mirror-history.jsonl",
    "portfolio/snapshot.json", "portfolio/risk.json", "portfolio/sizing.json",
    "portfolio/catalysts.json", "portfolio/holdings.json", "portfolio/pm-history.json",
    "portfolio/catalyst-alert-history.json", "portfolio/risk-alert-history.json",
    "portfolio/sizing-alert-history.json",
    "risk/recommendations.json",
})
PRIVATE_PREFIXES = (
    "data/_askdesk/", "data/search/index/", "equity-research-history/",
    "backtest/ledger/", "data/ai-commentary/history/portfolio/",
)


def _load_json(s3, bucket: str, key: str):
    try:
        raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        return json.loads(raw), None
    except Exception as exc:
        msg = str(exc)
        kind = "MISSING" if any(x in msg.lower() for x in ("nosuchkey", "not found", "404")) else "ERROR"
        return None, {"kind": kind, "detail": msg[:160]}


def _stamp(doc: Any) -> Optional[str]:
    if not isinstance(doc, dict):
        return None
    for key in ("generated_at", "as_of", "updated_at", "generated", "timestamp", "run_at", "date"):
        value = doc.get(key)
        if isinstance(value, str) and len(value) >= 10:
            return value
    return None


def _age_h(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        text = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(text if "T" in text else text + "T00:00:00+00:00")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return round((datetime.now(timezone.utc) - dt).total_seconds() / 3600.0, 1)
    except Exception:
        return None


def _safe_scalar(value: Any):
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, (int, float)):
        return round(float(value), 5)
    if isinstance(value, str):
        return value[:MAX_VALUE_CHARS]
    return None


def _walk_signals(value: Any, path: str = "", depth: int = 0) -> Iterable[tuple]:
    if depth > 4:
        return
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = (path + "." + str(key)).strip(".")
            scalar = _safe_scalar(child)
            if scalar is not None and SIGNAL_KEYS.search(str(key)):
                yield child_path, scalar
            if isinstance(child, (dict, list)):
                yield from _walk_signals(child, child_path, depth + 1)
    elif isinstance(value, list):
        for index, child in enumerate(value[:12]):
            yield from _walk_signals(child, "%s.%d" % (path, index), depth + 1)


def _walk_symbols(value: Any, depth: int = 0) -> Iterable[str]:
    if depth > 4:
        return
    if isinstance(value, dict):
        for key, child in value.items():
            if str(key).lower() in SYMBOL_KEYS and isinstance(child, str):
                symbol = child.split(":")[-1].upper()
                if re.fullmatch(r"[A-Z0-9.\-]{1,15}", symbol):
                    yield symbol
            elif isinstance(child, (dict, list)):
                yield from _walk_symbols(child, depth + 1)
    elif isinstance(value, list):
        for child in value[:30]:
            yield from _walk_symbols(child, depth + 1)


def _is_private(row: Dict[str, Any]) -> bool:
    feed = str(row.get("feed") or "").lstrip("/")
    if ((_canonical_private_source is not None and _canonical_private_source(feed))
            or row.get("declared_class") == "INTERNAL" or row.get("private") is True
            or feed in PRIVATE_FEEDS or any(feed.startswith(prefix) for prefix in PRIVATE_PREFIXES)):
        return True
    blob = " ".join(str(row.get(k) or "") for k in ("engine", "feed", "page", "title")).lower()
    return any(token in blob for token in PRIVATE_TOKENS)


def build_fleet_snapshot(s3, public_bucket: str, registry_key: str = REGISTRY_KEY,
                         default_sla_h: float = DEFAULT_SLA_H) -> Dict[str, Any]:
    registry, err = _load_json(s3, public_bucket, registry_key)
    if err or not isinstance(registry, dict):
        return {
            "registry_key": registry_key,
            "status": "ERROR",
            "error": err or {"kind": "ERROR", "detail": "registry is not an object"},
            "summary": {"declared": 0, "unique_feeds": 0, "eligible": 0},
            "feeds": [],
            "digest": [],
        }

    declarations = []
    for declared_class in ("wired", "internal", "dead"):
        for row in registry.get(declared_class) or []:
            if isinstance(row, dict) and row.get("feed"):
                declarations.append({**row, "declared_class": declared_class.upper()})

    by_feed: Dict[str, Dict[str, Any]] = {}
    for row in declarations:
        feed = str(row["feed"])
        item = by_feed.setdefault(feed, {
            "feed": feed,
            "engines": [],
            "pages": [],
            "titles": [],
            "schema_versions": [],
            "declared_classes": [],
            "private": False,
        })
        for target, value in (
            ("engines", row.get("engine")),
            ("pages", row.get("page")),
            ("titles", row.get("title")),
            ("schema_versions", row.get("schema_version")),
            ("declared_classes", row.get("declared_class")),
        ):
            if value and value not in item[target]:
                item[target].append(value)
        item["private"] = item["private"] or _is_private(row)

    feeds: List[Dict[str, Any]] = []
    for feed, item in sorted(by_feed.items()):
        if "DEAD" in item["declared_classes"]:
            item.update({"status": "DEAD", "eligible": False, "reason": "registry marks feed dead"})
            feeds.append(item)
            continue
        doc, load_error = _load_json(s3, public_bucket, feed)
        if load_error:
            item.update({"status": load_error["kind"], "eligible": False, "error": load_error["detail"]})
            feeds.append(item)
            continue
        stamp = _stamp(doc)
        age_h = _age_h(stamp)
        if not stamp or age_h is None:
            status = "UNTIMESTAMPED"
        elif age_h < -MAX_FUTURE_SKEW_H:
            status = "FUTURE"
        elif age_h > default_sla_h:
            status = "STALE"
        else:
            status = "FRESH"
        signals = []
        seen = set()
        for path, value in _walk_signals(doc):
            if path in seen:
                continue
            seen.add(path)
            signals.append({"path": path, "value": value})
            if len(signals) >= 8:
                break
        symbols = []
        for symbol in _walk_symbols(doc):
            if symbol not in symbols:
                symbols.append(symbol)
            if len(symbols) >= 8:
                break
        eligible = status == "FRESH" and not item["private"]
        item.update({
            "status": status,
            "eligible": eligible,
            "generated_at": stamp,
            "age_h": age_h,
            "sla_h": default_sla_h,
            "signals": signals,
            "symbols": symbols,
            "reason": "private feed excluded from model prompt" if item["private"] else None,
        })
        feeds.append(item)

    counts: Dict[str, int] = {}
    for row in feeds:
        counts[row["status"]] = counts.get(row["status"], 0) + 1
    eligible = [row for row in feeds if row.get("eligible")]
    digest = [{
        "feed": row["feed"],
        "engines": row["engines"][:3],
        "page": (row["pages"] or [None])[0],
        "generated_at": row.get("generated_at"),
        "signals": row.get("signals") or [],
        "symbols": row.get("symbols") or [],
    } for row in eligible]
    version_ok = registry.get("v") == MIN_REGISTRY_VERSION
    complete = len(feeds) >= MIN_UNIQUE_FEEDS
    coverage = (len(eligible) / len(feeds)) if feeds else 0.0
    ready = version_ok and complete and coverage >= 0.80
    blockers = []
    if not version_ok:
        blockers.append("registry version must equal %s" % MIN_REGISTRY_VERSION)
    if not complete:
        blockers.append("registry has %d unique feeds; minimum is %d" % (len(feeds), MIN_UNIQUE_FEEDS))
    if coverage < 0.80:
        blockers.append("eligible fleet coverage %.1f%% is below 80%%" % (coverage * 100.0))
    return {
        "registry_key": registry_key,
        "registry_version": registry.get("v"),
        "registry_generated_by": registry.get("generated_by"),
        "status": "READY" if ready else "BLOCKED",
        "release_blockers": blockers,
        "summary": {
            "declared": len(declarations),
            "unique_feeds": len(feeds),
            "eligible": len(eligible),
            "private_excluded": sum(1 for row in feeds if row.get("private")),
            "schema_versioned": sum(1 for row in declarations if row.get("schema_version") not in (None, "", "unspecified")),
            "by_status": counts,
        },
        "feeds": feeds,
        "digest": digest,
    }


def prompt_digest(snapshot: Dict[str, Any], max_chars: int = 24000) -> str:
    """Serialize only eligible, bounded evidence; keep coverage failures separate."""
    rows = snapshot.get("digest") or []
    text = json.dumps(rows, separators=(",", ":"), default=str)
    return text[:max_chars]
