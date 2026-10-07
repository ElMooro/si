"""Pure, evidence-linked research composition; no portfolio or capital authority.

Specialists are deterministic reviewers over declared native adapters. They
publish observations, questions and disagreements, not invented investment
conclusions. Full original packets remain accessible by content hash.
"""
from datetime import datetime, timezone
import hashlib
import json
import re
from research_network_registry import GROUPS, SOURCES

CONTRACT = "research-network.v1"
PERMISSIONS = {"calls_eligible": False, "forecast_qualified": False,
               "sizing_eligible": False, "execution_eligible": False,
               "promotion_eligible": False, "independent_investment_votes": 0}
CLOCK_FIELDS = ("observation_date", "session_date", "period_end", "as_of", "asof", "date", "period", "filed_date")
CLAIM_FIELDS = ("direction", "signal", "posture", "verdict", "state", "thesis", "why", "rationale", "summary", "call", "risk", "reasons", "key_themes", "invalidation", "invalidations", "horizon", "horizon_days", "timeframe")
OMIT = {"research_network", "research_context", "t360_context", "raw", "original", "raw_body_base64"}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def stamp(value):
    if not isinstance(value, str):
        return None
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return result.astimezone(timezone.utc) if result.tzinfo else None
    except ValueError:
        return None


def pointer(document, path):
    value = document
    for part in path.strip("/").split("/") if path else []:
        part = part.replace("~1", "/").replace("~0", "~")
        if isinstance(value, dict):
            value = value.get(part)
        elif isinstance(value, list) and part.isdigit() and int(part) < len(value):
            value = value[int(part)]
        else:
            return None
    return value


def escape(value):
    return str(value).replace("~", "~0").replace("/", "~1")


def identity(symbol, row, spec):
    """A reported-symbol grouping, explicitly NOT a validated instrument join."""
    if not isinstance(symbol, str):
        return None
    symbol = symbol.strip().upper()
    if not re.fullmatch(r"[A-Z0-9][A-Z0-9.^:/-]{0,24}", symbol):
        return None
    scope = spec["identity_scope"]
    reported = str(row.get("asset_class", "")).lower()
    if scope == "explicit":
        if reported in ("crypto", "cryptocurrency", "digital_asset"):
            scope = "crypto"
        elif reported in ("equity", "stock", "stocks", "etf", "equities", "listed_security"):
            scope = "listed_security"
        else:
            scope = "unresolved:" + spec["id"]
    # Preserve symbol spelling and share classes. No heuristic aliases.
    return {"id": scope + ":" + symbol, "symbol": symbol, "scope": scope,
            "reported_asset_class": row.get("asset_class"),
            "join_basis": "reported_symbol_within_declared_source_scope",
            "instrument_identity_qualified": False}


def facts(row):
    """Scalar facts retain field names/units. Nested claims retain full values.

    This is a display projection, not the source archive. Projection omissions
    are enumerated, and every row links to its full original JSON pointer.
    """
    return {k: v for k, v in row.items() if k not in OMIT and
            (v is None or isinstance(v, (str, bool, int, float)) or k in CLAIM_FIELDS)}


def source_summary(doc):
    # Compact root scalars only; never guess that a nested score is authoritative.
    return facts({k: v for k, v in doc.items() if k not in CLAIM_FIELDS or not isinstance(v, (list, dict))})


def rows(doc, spec):
    if spec['id'] == 'ticker-360-native':
        tickers = doc.get('tickers')
        if not isinstance(tickers, dict): raise ValueError('native ticker index shape changed')
        for symbol, item in tickers.items():
            if not isinstance(item, dict) or not isinstance(item.get('domains'), dict):
                raise ValueError('native ticker domain shape changed')
            for domain, value in item['domains'].items():
                data = value.get('data') if isinstance(value, dict) else None
                # Shared market summaries are not independent ticker evidence.
                if not isinstance(data, dict) or data.get('market_wide') is True: continue
                yield '/tickers/'+escape(symbol)+'/domains/'+escape(domain)+'/data', symbol, data, {
                    'native_domain': domain, 'native_as_of': value.get('as_of'),
                    'native_source_key': value.get('source_key')}
        return
    if spec["id"] == "fundamental-census-matrix":
        symbols, columns = doc.get("tickers"), doc.get("cols")
        if not isinstance(symbols, list) or not isinstance(columns, dict):
            raise ValueError("census shape changed")
        if any(not isinstance(values, list) or len(values) != len(symbols) for values in columns.values()):
            raise ValueError("census column alignment changed")
        for i, symbol in enumerate(symbols):
            row = {"ticker": symbol, **{key: values[i] for key, values in columns.items()}}
            yield "/tickers/" + str(i), symbol, row, {"column_index": i, "column_root": "/cols"}
        return
    for path, field in spec["rows"]:
        container = pointer(doc, path)
        if not isinstance(container, (list, dict)):
            raise ValueError("declared row path unavailable: " + path)
        if field == "@key" and not isinstance(container, dict):
            raise ValueError("declared map shape changed: " + path)
        for key, row in (container.items() if isinstance(container, dict) else enumerate(container)):
            if not isinstance(row, dict):
                raise ValueError("declared row is not an object: " + path)
            yield path + "/" + escape(key), key if field == "@key" else row.get(field), row, {}


def ingest(spec, doc, raw_sha, received_at, snapshot_key=None):
    if not isinstance(doc, dict):
        raise ValueError("source root must be an object")
    published = doc.get("generated_at")
    clock = stamp(published)
    age = (received_at - clock).total_seconds() / 3600 if clock else None
    freshness = "unknown" if age is None else "future" if age < 0 else "stale" if age > spec["publication_sla_hours"] else "within_publication_sla"
    meta = {"source_id": spec["id"], "key": spec["key"], "sha256": raw_sha,
            "snapshot_key": snapshot_key, "generated_at": published,
            "received_at": received_at.isoformat(), "reported_clocks": {k: doc[k] for k in CLOCK_FIELDS if k in doc},
            "publication_age_hours": round(age, 3) if age is not None else None,
            "publication_freshness": freshness, "observation_freshness_verified": False,
            "reported_contract": doc.get("contract", doc.get("schema_version")),
            "reported_status": doc.get("status"), "summary": source_summary(doc),
            "reported_permissions": {k: doc[k] for k in PERMISSIONS if k in doc},
            "reported_dependency_roots": doc.get("dependency_roots"),
            "research_available": True, "status": "context_only", "records": 0,
            "unresolved_rows": 0, "groups": spec["groups"], **PERMISSIONS}
    receipt = doc.get("research_network")
    if isinstance(receipt, dict) and receipt.get("contract") == CONTRACT:
        meta["consumer_receipt"] = {k: receipt.get(k) for k in
                                    ("consumer", "status", "publication_id", "source_sha256", "read_at")}
    observations = []
    try:
        for path, symbol, row, extra in rows(doc, spec):
            ident = identity(symbol, row, spec)
            if not ident:
                meta["unresolved_rows"] += 1
                continue
            projected = facts(row)
            record = {"source_id": spec["id"], "source_sha256": raw_sha,
                      "pointer": path, "identity": ident, "facts": projected,
                      "projection_omitted_fields": sorted(set(row) - set(projected)),
                      "reported_clocks": {k: row[k] for k in CLOCK_FIELDS if k in row},
                      "horizon": {k: row[k] for k in ("horizon", "horizon_days", "timeframe", "frame") if k in row},
                      "reported_permissions": {k: row[k] for k in PERMISSIONS if k in row},
                      "source_error": row.get("error", row.get("err")),
                      "groups": spec["groups"], **extra, **PERMISSIONS}
            record["evidence_id"] = digest({"source": raw_sha, "pointer": path, "identity": ident["id"]})
            observations.append(record)
        meta["status"] = "available" if observations else "empty" if spec["rows"] else "context_only"
    except ValueError as exc:
        # Never publish a partially adapted source as complete coverage.
        observations = []
        meta.update(status="adapter_mismatch", adapter_error=str(exc))
    meta["records"] = len(observations)
    return meta, observations


def review_group(group_id, sources, excluded=()):
    title, question, names = GROUPS[group_id]
    names = [name for name in names.split() if name not in excluded]
    selected = [sources[name] for name in names]
    missing = [s["source_id"] for s in selected if not s.get("research_available")]
    concerns = [{"source": s["source_id"], "issue": s.get("publication_freshness", s["status"])}
                for s in selected if s.get("publication_freshness") != "within_publication_sla"]
    concerns += [{"source": s["source_id"], "issue": "adapter_mismatch"}
                 for s in selected if s["status"] == "adapter_mismatch"]
    concerns += [{"source": s["source_id"], "issue": "unresolved_identity_rows", "rows": s['unresolved_rows']}
                 for s in selected if s.get('unresolved_rows')]
    return {"specialist": group_id, "title": title, "question": question,
            "method": "deterministic_evidence_review", "sources": names,
            "processed_publications": {s["source_id"]: s.get("sha256") for s in selected},
            "status": "unavailable" if len(missing) == len(selected) else "partial" if missing or concerns else "available",
            "missing": missing, "concerns": concerns,
            "independence": "not_established; shared upstream data may occur in several engines",
            "portfolio_scope": "public_model_context_only; owner holdings are joined privately" if group_id == "portfolio" else None,
            **PERMISSIONS}


def explicit_direction(record):
    if record.get("source_error"):
        return None
    # No conversion of scores, tone, options flow or a setup into direction.
    value = record["facts"].get("direction")
    return {"UP": "UP", "BULLISH": "UP", "LONG": "UP", "DOWN": "DOWN", "BEARISH": "DOWN", "SHORT": "DOWN"}.get(value) if isinstance(value, str) else None


def dossier(entity_id, records, sources, previous=None):
    unique = {r["evidence_id"]: r for r in records}
    records = list(unique.values())
    current_ids = sorted(unique)
    groups = sorted({g for r in records for g in r["groups"]})
    supporting = [r["evidence_id"] for r in records if explicit_direction(r) == "UP"]
    opposing = [r["evidence_id"] for r in records if explicit_direction(r) == "DOWN"]
    invalidations = [{"evidence_id": r["evidence_id"], "value": r["facts"].get("invalidation", r["facts"].get("invalidations"))}
                     for r in records if r["facts"].get("invalidation") or r["facts"].get("invalidations")]
    problems = [{"source": s, "issue": sources[s]["publication_freshness"]}
                for s in sorted({r["source_id"] for r in records}) if sources[s]["publication_freshness"] != "within_publication_sla"]
    missing = [g for g in ("fundamentals", "ownership", "technical", "catalysts") if g not in groups]
    prior_ids = set((previous or {}).get("evidence_ids", []))
    fingerprints = {}
    for record in records:
        fingerprints.setdefault(record['source_id'], []).append(digest({
            k: record[k] for k in ('identity', 'facts', 'reported_clocks', 'horizon', 'reported_permissions', 'source_error')}))
    fingerprints = {s: digest(sorted(values)) for s, values in fingerprints.items()}
    previous_fingerprints = (previous or {}).get('observation_fingerprints', {})
    return {"entity_id": entity_id, "identity": records[0]["identity"], "status": "research_available",
            "evidence": records, "evidence_ids": current_ids, "groups": groups,
            "observation_fingerprints": fingerprints,
            "thesis": {"what_is_happening": "Source observations below; conclusions remain attributed to their producers.",
                       "why_it_matters": [GROUPS[g][1] for g in groups],
                       "reported_upward_evidence": supporting, "reported_downward_evidence": opposing,
                       "independent_support": "not_established",
                       "contradictions": [{"status": "opposing_reported_directions_require_horizon_and_clock_review", "up": supporting, "down": opposing}] if supporting and opposing else [],
                       "missing": missing, "data_concerns": problems,
                       "horizons": [{"evidence_id": r["evidence_id"], "reported": r["horizon"]} for r in records if r["horizon"]],
                       "invalidation": invalidations,
                       "invalidation_status": "reported_not_validated" if invalidations else "not_supplied",
                       "portfolio_impact": "available_only_in_authenticated_portfolio_context",
                       "changes": {"baseline_available": previous is not None,
                                   "changed_observation_sources": sorted(s for s in fingerprints.keys() | previous_fingerprints.keys()
                                                                         if fingerprints.get(s) != previous_fingerprints.get(s)) if previous is not None else [],
                                   "added": sorted(set(current_ids) - prior_ids) if previous is not None else [],
                                   "removed": sorted(prior_ids - set(current_ids)) if previous is not None else [],
                                   "basis": "source_publication_and_pointer; a changed source hash is not necessarily a changed economic observation"}},
            **PERMISSIONS}


def compose(sources, observations, generated_at, prior_entities=None):
    grouped = {}
    for record in observations:
        grouped.setdefault(record["identity"]["id"], []).append(record)
    entities = {eid: dossier(eid, records, sources, (prior_entities or {}).get(eid)) for eid, records in sorted(grouped.items())}
    reviews = {gid: review_group(gid, sources) for gid in GROUPS}
    publication_id = digest({"contract": CONTRACT, "sources": {k: {f: v.get(f) for f in ("sha256", "status", "publication_freshness")} for k, v in sources.items()},
                             "registry": SOURCES, "groups": GROUPS})
    manifest = {"contract": CONTRACT, "publication_id": publication_id, "generated_at": generated_at.isoformat(),
                "access": "PUBLIC_RESEARCH", "sources": sources, "specialists": reviews,
                "entity_count": len(entities), "source_count": len(sources),
                "available_sources": sum(s.get("research_available") is True for s in sources.values()),
                "briefing": {"available_angles": [g for g, r in reviews.items() if r["status"] != "unavailable"],
                             "review_required": [g for g, r in reviews.items() if r["status"] != "available"],
                             "decision_qualification": "separate; authoritative Khalid risk policy remains in force",
                             "risk_source": "khalid-risk", "research_can_be_available_during_data_hold": True},
                "dependency_receipt": {"consumer": "ticker-360", "processed": {k: v.get("sha256") for k, v in sources.items()},
                                       "publication_acceptance_is_consumer_processing": False}, **PERMISSIONS}
    return manifest, entities
