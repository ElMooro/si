"""Pure public publication boundary shared by engines and privacy migration.

Full notes and personalized prose remain in IAM-private originals. This module
removes only known private-text paths, keeping market/risk measurements intact.
It performs no IO and never logs the input.
"""
from copy import deepcopy
import math
import re

PUBLIC_CONTEXT_PRIVACY_VERSION = "20260909-public-inputs-v1"
PUBLIC_VOL_UNIVERSE = ("SPY", "QQQ", "IWM", "DIA", "GLD", "TLT", "IBIT", "VXX")
PUBLIC_DEFAULT_SCENARIO = {"schema_version": "public-default-scenario.v1",
                           "scope": "PUBLIC_DEFAULT_MODEL", "contains_caller_inputs": False}


def _rows(value):
    return list(value.values()) if isinstance(value, dict) else value if isinstance(value, list) else []


def _walk(value):
    if isinstance(value, dict):
        yield value
        for child in list(value.values()):
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def public_notes_block(note):
    if not isinstance(note, dict):
        return {}
    return {**{k: note[k] for k in ("n_notes", "stance", "stance_score", "latest_at", "levels", "note_ids") if k in note},
            "note_text_private": True}


def notes_public(kind, doc):
    out = {k: doc[k] for k in ("generated_at", "version", "n_notes", "n_tickers", "n_macro_notes", "theme_counts", "llm_views") if k in doc}
    out["private_text"] = True
    if kind == "notes-index":
        out["index"] = {ticker: {**{k: row[k] for k in ("n_notes", "stance_score", "stance", "last_note_at", "levels", "themes", "note_ids") if k in row},
                                    "private_text": True} for ticker, row in doc.get("index", {}).items()}
    elif kind == "notes-themes":
        out["themes"] = {theme: {"n_notes": row.get("n_notes"), "avg_stance": row.get("avg_stance"),
                                   "recent": [{"note_id": n.get("note_id"), "at": n.get("at"), "private_text": True} for n in row.get("recent", [])]}
                         for theme, row in doc.get("themes", {}).items()}
    else:
        raise ValueError("unknown notes projection")
    return out


def playbook_public(doc):
    curve = (doc.get("flagship") or {}).get("yield_curve") or {}
    return {**{k: doc[k] for k in ("generated_at", "source_notes", "n_rules", "families") if k in doc},
            "flagship": {"yield_curve": {k: curve[k] for k in ("series", "latest", "most_recent_inversion_onset", "months_elapsed", "khalid_lag_months", "lag_marker_date", "status") if k in curve}},
            "private_text": True,
            "rules": [{**{k: r[k] for k in ("id", "symbol", "family", "params") if k in r}, "text_private": True}
                      for r in doc.get("rules", [])]}


def brief_public(doc):
    return {"engine": "my-brief", "generated_at": doc.get("generated_at"), "brief": None,
            "brief_available": bool(doc.get("brief")), "private_text": True,
            "note": "Sign in as the Brain owner to read the personalized brief."}


def devils_public(doc, allowed_tickers=()):
    rows = [{"ticker": c["ticker"],
             "risk_level": c.get("risk_level") if c.get("risk_level") in {"low", "medium", "high"} else "unknown",
             "rule_violation": bool(c.get("violates_your_rule")), "private_text": True,
             "bear_case": "Private review available to the signed-in Brain owner."}
            for c in doc.get("cases", []) if isinstance(c, dict)
            and c.get("ticker") in allowed_tickers and isinstance(c.get("ticker"), str) and re.fullmatch(r"[A-Z0-9.^=-]{1,20}", c["ticker"])]
    return {"engine": "devils-advocate", "generated_at": doc.get("generated_at"), "cases": rows,
            "by_ticker": {c["ticker"]: c for c in rows}, "private_text": True,
            "n_rule_violations": sum(c["rule_violation"] for c in rows),
            "note": "Sign in as the Brain owner to read the personalized review."}


def vault_search_rows(payload):
    items = (payload or {}).get("symbols") or []
    if isinstance(items, dict):
        items = [dict(value, symbol=symbol) if isinstance(value, dict) else {"symbol": symbol}
                 for symbol, value in items.items()]
    rows = []
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict) or item.get("status") != "LIVE" or not item.get("symbol"):
            continue
        symbol = str(item["symbol"])
        exchanges = item.get("exchanges") or []
        if not isinstance(exchanges, list):
            exchanges = [exchanges]
        rows.append({"id": "tradingview-vault-live:" + symbol, "title": symbol,
                     "kind": "instrument_ref", "search": " ".join(str(x) for x in (
                         item.get("category"), item.get("source"), item.get("resolved_via"),
                         " ".join(exchanges)) if x is not None)[:1000], "hot": True})
    return rows


def fleet_public(document):
    """Metadata-only fleet contract; arbitrary engine/SDK/provider text is private."""
    def numbers(row, keys):
        return {key: row[key] for key in keys if key in row and
                (row[key] is None or (type(row[key]) in (int, float) and math.isfinite(row[key])))}

    data = document.get("data_outputs") or {}
    compute = document.get("compute") or {}
    out = {"engine": "fleet-monitor", "schema_version": "1.0",
           "generated_at": document.get("generated_at"),
           "privacy_version": "fleet-metadata-20260909-v1",
           "system_status": document.get("system_status") if document.get("system_status") in {"red", "yellow", "green"} else "unknown",
           "summary": numbers(document.get("summary") or {}, ("data_outputs_total", "data_outputs_fresh", "data_outputs_aging",
               "data_outputs_stale_or_degraded", "data_outputs_static", "data_outputs_cadence_mapped", "lambda_count", "dependencies_down", "dependencies_degraded")),
           "data_outputs": {"available": data.get("available") is True, **numbers(data, ("total", "green", "n_yellow", "n_red", "n_degraded", "n_static", "manifest_outputs", "private_content_checks_skipped"))},
           "compute": {"available": compute.get("available") is True, **numbers(compute, ("n_functions",))},
           "dependencies": [],
           "note": "Top-level data JSON age/size and public status checks; personal content is excluded. Dependency diagnostics contain fixed status categories only."}
    out.update(numbers(document, ("elapsed_s",)))
    if data.get("error"):
        out["data_outputs"]["error"] = "DATA_INVENTORY_UNAVAILABLE"
    if compute.get("available") is not True:
        out["compute"].update(error="COMPUTE_INVENTORY_UNAVAILABLE", note="Lambda inventory unavailable; data-output health is reported separately.")
    labels = {"red": "stale or empty output", "yellow": "aging output", "degraded": "engine reports an error"}
    for group in ("red", "yellow", "degraded", "static"):
        out["data_outputs"][group] = []
        for row in _rows(data.get(group)):
            if not isinstance(row, dict):
                continue
            name = row.get("output")
            safe = {"output": name if isinstance(name, str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,180}", name) else "unknown-output",
                    **numbers(row, ("age_hours", "size", "cadence_hours"))}
            if group != "static":
                safe["issue"] = row.get("issue") if row.get("issue") in {"engine reports ok=false", "output status inspection unavailable"} else labels[group]
            out["data_outputs"][group].append(safe)
    providers = {"Anthropic API", "FRED", "FMP", "Polygon", "AlphaVantage", "CoinMarketCap"}
    safe_details = {"ANTHROPIC_API_KEY not set on this function", "responding — credits OK", "CREDIT EXHAUSTED — every AI feature degrades",
                    "rate limited", "auth failed — bad/expired key", "rate-limit notice in response", "key valid",
                    "auth failed (HTTP 401) — key dead/expired", "auth failed (HTTP 403) — key dead/expired"}
    for row in _rows(document.get("dependencies")):
        if not isinstance(row, dict):
            continue
        detail = row.get("detail")
        if not (isinstance(detail, str) and (detail in safe_details or re.fullmatch(r"dependency unavailable \(HTTP (?:[1-5][0-9]{2}|NO_RESPONSE)\)", detail))):
            detail = "Dependency diagnostic text withheld; check reported status."
        out["dependencies"].append({"name": row.get("name") if row.get("name") in providers else "Dependency",
                                    "status": row.get("status") if row.get("status") in {"green", "yellow", "red", "unknown"} else "unknown", "detail": detail})
    return out


PUBLIC_FRESHNESS_REPORT = {"schema_version": "public-freshness-report.v1",
                           "scope": "PUBLIC_ENGINE_HEALTH", "contains_private_data": False}
FRESHNESS_REASONS = {
    "ZERO_BYTE_OBJECT": "Zero-byte object.", "INVALID_JSON": "Content is not valid JSON.",
    "EMPTY_JSON": "Empty JSON document.", "REQUIRED_FIELD_MISSING": "Required schema field missing.",
    "SCHEMA_VERSION_MISMATCH": "Schema version mismatch.", "SOURCE_TIME_FUTURE": "Source timestamp is in the future.",
    "SOURCE_TIME_STALE": "Source timestamp exceeds the freshness threshold.", "CONTENT_READ_FAILED": "Content inspection unavailable.",
    "INVALID_FRESHNESS_SLA": "Freshness threshold is invalid.", "FRESHNESS_MANIFEST_UNREADABLE": "Freshness rules unavailable.",
    "FRESHNESS_MANIFEST_MISSING": "Freshness rules missing.", "FRESHNESS_MANIFEST_INVALID": "Freshness rules are invalid or empty.",
    "FEED_ENUMERATION_FAILED": "Feed enumeration unavailable.", "EXPECTED_MANIFEST_UNAVAILABLE": "Expected output contract unavailable.",
    "EXPECTED_HEAD_BUDGET_EXHAUSTED": "Expected-output metadata budget exhausted.",
    "EXPECTED_OUTPUT_MISSING": "Required declared output absent.", "EXPECTED_OUTPUT_UNREADABLE": "Expected-output metadata unavailable.",
    "PRIVATE_SOURCE_EXCLUDED": "Private source excluded from public content inspection.",
    "LEGACY_PUBLIC_REPORT_WITHHELD": "Public health report is awaiting safe regeneration.",
    "NO_SOURCE_TIMESTAMP": "No valid source timestamp found.",
}


def freshness_public(document):
    """Only fixed diagnostics and typed public-key health metadata may escape."""
    from datetime import datetime, timezone
    from private_artifact import is_private_source

    def timestamp(value):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed.astimezone(timezone.utc).isoformat() if parsed.tzinfo else None
        except Exception:
            return None

    def number_fields(row, fields):
        return {k: row[k] for k in fields if k in row and (row[k] is None or
                type(row[k]) in (int, float) and math.isfinite(row[k]))}

    def public_path(value):
        return (isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_./*=-]{1,1024}", value)
                and not is_private_source(value))

    def reason(row):
        code = row.get("reason_code")
        return {"reason_code": code, "reason": FRESHNESS_REASONS[code]} if code in FRESHNESS_REASONS else {}

    def result(row):
        if not isinstance(row, dict) or not public_path(row.get("key")):
            return None
        out = {"key": row["key"], "status": row.get("status") if row.get("status") in
               {"FRESH", "STALE", "EMPTY", "INVALID", "SOURCE_STALE", "UNKNOWN", "MISSING"} else "UNKNOWN",
               **number_fields(row, ("age_h", "artifact_age_h", "source_age_h", "max_age_h", "size", "n_top_level")), **reason(row)}
        for k in ("last_modified", "source_generated_at", "last_seen"):
            if k in row:
                out[k] = timestamp(row[k])
        for k in ("validated", "partial_validation"):
            if k in row:
                out[k] = row[k] is True
        if row.get("source_ts_field") in {"generated_at", "as_of", "updated_at", "updated", "timestamp", "ts", "last_updated", "run_ts", "configured_timestamp_field"}:
            out["source_ts_field"] = row["source_ts_field"]
        engine = row.get("engine")
        if isinstance(engine, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,128}", engine):
            out["engine"] = engine
        if row.get("partial_validation"):
            out["note"] = "Large object: head-only timestamp check; complete JSON validity was not verified."
        return out

    def rules(rows):
        return [{"prefix": row["prefix"], "recursive": row.get("recursive") is True,
                 **number_fields(row, ("default_max_age_h", "n_objects")),
                 **({"truncated": row["truncated"] is True} if "truncated" in row else {})}
                for row in rows if isinstance(row, dict) and public_path(row.get("prefix"))]

    marker = document.get("publication")
    if not (isinstance(marker, dict) and marker == PUBLIC_FRESHNESS_REPORT and marker.get("contains_private_data") is False
            and document.get("schema_version") == "fleet-freshness-monitor.v3"):
        return freshness_public({"schema_version": "fleet-freshness-monitor.v3", "publication": dict(PUBLIC_FRESHNESS_REPORT),
                "version": "3.0.0", "status": "UNKNOWN", "full_expected_coverage": False,
                "results": [], "coverage": {"results_complete": False, "enumeration_complete": False},
                "reason_code": "LEGACY_PUBLIC_REPORT_WITHHELD"})
    out = {"schema_version": "fleet-freshness-monitor.v3", "publication": dict(PUBLIC_FRESHNESS_REPORT),
           "version": "3.0.0", "generated_at": timestamp(document.get("generated_at")),
           "status": document.get("status") if document.get("status") in {"UNKNOWN", "HEALTHY", "DEGRADED"} else "UNKNOWN",
           **reason(document), **number_fields(document, ("n_keys_tracked", "n_stale", "n_fresh", "n_unknown", "n_invalid_or_empty", "n_source_stale", "n_missing", "n_alerts_raised", "n_alerts_suppressed", "elapsed_s"))}
    for k in ("full_expected_coverage", "telegram_sent", "sns_sent", "notifications_suppressed"):
        if k in document:
            out[k] = document[k] is True
    for group in ("results", "unknown", "missing", "invalid_or_empty", "source_stale", "stale", "stale_top_50", "source_stale_top_50", "declared_absent_never_seen"):
        out[group] = [safe for row in document.get(group, []) if (safe := result(row)) is not None]
    coverage = document.get("coverage") or {}
    out["coverage"] = {**number_fields(coverage, ("keys_enumerated", "bodies_validated", "expected_keys_declared", "expected_keys_checked", "expected_keys_headed", "private_sources_excluded", "expected_private_sources_excluded", "missing", "declared_absent_never_seen", "results_returned")),
                       "rules": rules(coverage.get("rules") or []),
                       "truncated_rules": [p for p in coverage.get("truncated_rules", []) if public_path(p)],
                       "unresolved_key_families": [p for p in coverage.get("unresolved_key_families", []) if public_path(p)]}
    for k in ("head_budget_exhausted", "results_complete", "enumeration_complete"):
        out["coverage"][k] = coverage.get(k) is True
    out["manifest_rules"] = rules(document.get("manifest_rules") or [])
    out["thresholds"] = number_fields(document.get("thresholds") or {}, ("default_max_age_h", "alert_ratio", "dedupe_hours"))
    out["semantics"] = "Artifact age measures S3 LastModified; source age measures the parsed source timestamp. FRESH requires both within threshold. UNKNOWN is unresolved content or time provenance. Results include every evaluated public output; coverage separately records enumeration limits, unresolved families and private exclusions."
    out["summary_limits"] = {"stale_top_50": 50, "source_stale_top_50": 50, "complete_results_field": "results"}
    return out


def fleet_error_category(text, *, throttles=0, errors=0):
    """Fixed labels only; raw log/error text never crosses the public boundary."""
    text = text if isinstance(text, str) else ""
    for label, tokens in (("TIMEOUT", ("Task timed out", "TimeoutError")),
                          ("THROTTLED", ("TooManyRequestsException", "Rate exceeded")),
                          ("ACCESS_DENIED", ("AccessDenied", "UnauthorizedOperation")),
                          ("IMPORT_ERROR", ("Runtime.ImportModuleError", "ModuleNotFoundError")),
                          ("OUT_OF_MEMORY", ("OutOfMemory", "MemoryError"))):
        if any(token in text for token in tokens):
            return label
    if isinstance(throttles, (int, float)) and throttles > 0:
        return "THROTTLED"
    return "RUNTIME_ERROR" if text or (isinstance(errors, (int, float)) and errors > 0) else "DETAIL_UNAVAILABLE"


def fleet_errors_public(document):
    def numeric(row, fields):
        return {key: row[key] for key in fields if key in row and
                (row[key] is None or (type(row[key]) in (int, float) and math.isfinite(row[key])))}
    def dlq(value):
        value = value if isinstance(value, dict) else {}
        ready = not value.get("error") and all(type(value.get(key)) in (int, float) and math.isfinite(value[key]) and value[key] >= 0 for key in ("visible", "inflight", "total"))
        return {"available": ready, **numeric(value, ("visible", "inflight", "total")),
                **({"error": "DLQ_STATUS_UNAVAILABLE"} if not ready else {})}
    allowed_categories = {"TIMEOUT", "THROTTLED", "ACCESS_DENIED", "IMPORT_ERROR", "OUT_OF_MEMORY", "RUNTIME_ERROR", "DETAIL_UNAVAILABLE", "METRIC_READ_UNAVAILABLE", "DLQ_BACKLOG"}
    out = {"engine": "fleet-error-monitor", "version": document.get("version"),
           "run_id": document.get("run_id"), "generated_at": document.get("generated_at"),
           "privacy_version": "fleet-errors-metadata-20260909-v1",
           "alerts_scope": "ALL_CURRENT_ALARMS" if type(document.get("n_alerts_detected")) in (int, float) else "LEGACY_NEW_ALERTS_ONLY",
           **numeric(document, ("n_lambdas_scanned", "n_alerts_detected", "n_alerts_raised", "n_alerts_suppressed", "elapsed_s")),
           "dlq_status": dlq(document.get("dlq_status")), "alerts": [],
           "thresholds": numeric(document.get("thresholds") or {}, ("error_rate_pct", "min_invocations", "lookback_minutes", "dlq_depth", "dedupe_minutes")),
           "telegram_sent": document.get("telegram_sent") is True, "sns_sent": document.get("sns_sent") is True,
           "notification_mode": "suppressed_for_audit" if document.get("notification_mode") == "suppressed_for_audit" else "normal",
           "diagnostic_text_private": True}
    for row in _rows(document.get("alerts")):
        if not isinstance(row, dict):
            continue
        name = row.get("lambda")
        category = row.get("error_category")
        if category not in allowed_categories:
            category = fleet_error_category(row.get("last_error_log"), throttles=row.get("throttles"), errors=row.get("errors"))
        safe = {"lambda": name if isinstance(name, str) and (name == "<DLQ>" or re.fullmatch(r"[A-Za-z0-9_-]{1,64}", name)) else "unknown-function",
                "severity": row.get("severity") if row.get("severity") in {"CRITICAL", "WARNING"} else "UNKNOWN",
                "error_category": category, "diagnostic_text_private": True,
                **numeric(row, ("invocations", "errors", "throttles", "error_rate_pct"))}
        if "dlq_depth" in row:
            safe["dlq_depth"] = dlq(row["dlq_depth"])
            safe["error_category"] = "DLQ_BACKLOG"
        if row.get("metric_status") == "UNAVAILABLE":
            safe["metric_status"] = "UNAVAILABLE"
        out["alerts"].append(safe)
    return out


SOURCE_MAP_PUBLICATION = {"scope": "PUBLIC_MARKET_SOURCE_METADATA", "contains_private_data": False,
                          "raw_source_text_private": True, "raw_diagnostics_private": True}
SOURCE_MAP_FAMILIES = frozenset({"FRED", "US-TREASURY", "BLS", "BEA", "CENSUS-US", "ECB", "EUROSTAT", "BOJ",
    "MOF-JAPAN", "ESTAT-JAPAN", "BOE", "SNB", "NORGES", "BCRP-PERU", "BCB-BRAZIL", "PBOC", "MOEA-TAIWAN",
    "CFTC", "SEC-EDGAR", "OFR", "IMF", "HKMA", "OECD", "WORLD-BANK", "COINMETRICS", "COINGECKO", "EIA",
    "MARKET-VENUES", "UNMAPPED", "OTHER-OFFICIAL"})
SOURCE_MAP_SYMBOL = re.compile(r'^(?:ECONOMICS|FRED|NASDAQ|NYSE|AMEX|ARCA|CBOE|CME|CBOT|COMEX|NYMEX|ICEUS|TVC|CRYPTOCAP|BINANCE|COINBASE|BITSTAMP|KRAKEN|OANDA|FX):[A-Z0-9][A-Z0-9_.!^/-]{0,39}$')


def source_map_public(document):
    """A typed public projection; legacy browser prose never becomes public."""
    from datetime import datetime, timezone
    def number(value):
        return value if type(value) in (int, float) and math.isfinite(value) and value >= 0 else None
    def timestamp(value):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed.astimezone(timezone.utc).isoformat() if parsed.tzinfo else None
        except Exception:
            return None
    def families(value):
        return {key: value for key, value in (value.items() if isinstance(value, dict) else [])
                if key in SOURCE_MAP_FAMILIES and number(value) is not None}
    marker = document.get("publication")
    marked = (document.get("schema_version") == "public-source-map.v1" and document.get("engine") == "justhodl-source-map"
              and isinstance(marker, dict) and marker == SOURCE_MAP_PUBLICATION
              and marker.get("contains_private_data") is False and marker.get("raw_source_text_private") is True
              and marker.get("raw_diagnostics_private") is True)
    doc = document if marked else {"errors": ["LEGACY_PUBLIC_SOURCE_MAP_WITHHELD"]}
    out = {"schema_version": "public-source-map.v1", "engine": "justhodl-source-map",
           "marker": "source-map engine v3 public-source-map.v1", "publication": dict(SOURCE_MAP_PUBLICATION),
           "input_artifact": "data/tv-sources.json", "input_status": "AVAILABLE" if doc.get("input_status") == "AVAILABLE" else "UNAVAILABLE",
           "classification_method": "Fixed agency-family keyword classification; no independent publisher attestation. Unrecognized text is withheld."}
    for key in ("generated_at", "input_generated_at", "macro_input_generated_at"):
        out[key] = timestamp(doc.get(key))
    for key in ("symbols_with_source", "distinct_sources", "junk_filtered", "agency_rows", "venue_rows", "economics_symbols",
                "macro_attributed", "macro_unattributed", "macro_coverage_pct", "public_symbol_count", "withheld_symbol_count", "unmapped_source_rows"):
        out[key] = number(doc.get(key))
    for key in ("known_families", "agency_families"):
        out[key] = families(doc.get(key))
    out["economics_agencies"] = [{"source_family": row["source_family"], "n_symbols": row["n_symbols"]}
        for row in _rows(doc.get("economics_agencies")) if isinstance(row, dict)
        and isinstance(row.get("source_family"), str) and row["source_family"] in SOURCE_MAP_FAMILIES and number(row.get("n_symbols")) is not None]
    progress = doc.get("harvest_progress") if isinstance(doc.get("harvest_progress"), dict) else {}
    out["harvest_progress"] = {key: number(progress.get(key)) for key in ("walked", "total", "pct", "tier1_done", "rate_per_min", "elapsed_s", "matched", "eta_hours")}
    cleaned = doc.get("cleaned_sources") if isinstance(doc.get("cleaned_sources"), dict) else {}
    out["cleaned_sources"] = {symbol: {"source_family": row["source_family"], "updated": timestamp(row.get("updated"))}
        for symbol, row in cleaned.items() if isinstance(symbol, str) and SOURCE_MAP_SYMBOL.fullmatch(symbol)
        and isinstance(row, dict) and isinstance(row.get("source_family"), str) and row["source_family"] in SOURCE_MAP_FAMILIES}
    out["errors"] = [value for value in _rows(doc.get("errors"))
                     if value in ("SOURCE_INPUT_UNAVAILABLE", "LEGACY_PUBLIC_SOURCE_MAP_WITHHELD")]
    return out


def sanitize_public(key, document, *, vault=None):
    """Return a copy; canonical and historical root-alias keys share one contract."""
    if not isinstance(document, dict):
        raise ValueError("public artifact must be an object")
    out = deepcopy(document)
    name = key.removeprefix("data/")
    if name == "source-map.json":
        return source_map_public(out)
    elif name == "_fleet-monitor.json":
        out = fleet_errors_public(document)
    elif name == "_health/fleet.json":
        out = fleet_public(document)
    elif name == "_freshness-monitor.json":
        out = freshness_public(document)
    elif name == "brain-compiler.json":
        for row in out.get("claims", []):
            row.pop("claim", None)
            row["claim_text_private"] = True
        for row in out.get("build_queue", []):
            row.pop("sample_claims", None)
            row.setdefault("note_ids", list(dict.fromkeys(c["note_id"] for c in out.get("claims", [])
                if c.get("note_id") and row.get("concept") in c.get("concepts", []))))
            row["claim_text_private"] = True
    elif name == "tv-workbench.json":
        for row in _rows(out.get("symbols")):
            row["notes"] = [{"note_id": n.get("note_id"), "ts": n.get("ts"), "text_private": True} for n in row.get("notes", [])]
        out["note"] = "Notes are private. Note IDs and counts link this public workbench to the authenticated Brain."
    elif name == "canary-warroom.json":
        out["brain_playbook"] = [{"note_id": n.get("note_id"), "cat": n.get("cat"), "pinned": n.get("pinned"), "text_private": True}
                                 for n in out.get("brain_playbook", [])]
    elif name in {"tradingview.json", "domain-barometers.json"}:
        for row in _rows(out.get("symbols")):
            row.pop("note_snippet", None)
            row["note_text_private"] = True
            if name == "domain-barometers.json" and (str(row.get("tier", "")).startswith(("T1a", "T2"))
                    or "his wording" in str(row.get("evidence", "")) or "deciding terms" in str(row.get("evidence", ""))):
                row["evidence"] = "Classification uses private note references; see the authenticated Brain for source text."
    elif name == "best-setups.json":
        for row in _walk(out):
            if isinstance(row.get("brain_aligned"), str) and row["brain_aligned"]:
                row["brain_aligned"] = "Aligned with private sector policy" if "sector" in row["brain_aligned"].lower() else "Aligned with private theme policy"
            if isinstance(row.get("khalid_note"), dict):
                row["khalid_note"]["view"] = None
                row["khalid_note"]["view_private"] = True
            for field in ("playbook_ctx", "_playbook_ctx", "playbook_context"):
                if isinstance(row.get(field), list):
                    row[field] = [{**{k: n[k] for k in ("id", "symbol", "family", "hit_rate", "score") if k in n}, "text_private": True}
                                  for n in row[field] if isinstance(n, dict)]
    elif name == "master-allocation.json":
        for row in _walk(out):
            if isinstance(row.get("brain_posture"), dict):
                source = row["brain_posture"]
                intensity = source.get("intensity", source.get("value", 0)) or 0
                source["regime"] = "DEFENSIVE" if intensity > 0 else "RISK_ON" if intensity < 0 else "NEUTRAL"
                source["source_text_private"] = True
    elif name == "position-sizing.json":
        mult = out.get("posture_mult")
        posture = "aggressive" if mult == 1.3 else "defensive" if mult == 0.6 else "balanced"
        out["risk_posture"] = posture
        regime = out.get("regime") or {}
        for row in out.get("sized_positions", []):
            row["rationale"] = (f"conv {row.get('conviction')} × {posture} posture ({mult}×) × regime "
                f"{regime.get('bond_vol')}/{regime.get('plumbing')}/gamma ({regime.get('combined_mult')}×)")
    elif name == "engine-conflicts.json":
        for row in _walk(out):
            if row.get("type") == "CONVICTION vs YOUR RULES":
                row["bear"] = "Private rule review flagged a conflict; see the authenticated review for details."
                row.setdefault("private_review_ref", {"engine": "devils-advocate", "ticker": row.get("ticker")})
    elif name == "sizing.json":
        # Prior producer versions mixed actual holdings into public model sizing.
        # Keep historical computed risk numbers, but remove the private book and
        # ticker references. Do not relabel old book-adjusted numbers model-only.
        legacy = not (out.get("holdings") is None and out.get("holdings_publication") == "REDACTED_ACCOUNT_PRIVATE"
                      and out.get("book_status") in {"READY", "BLOCKED"})
        had_book = bool(out.get("holdings"))
        out["holdings"] = None
        for row in out.get("recommendations", []):
            flags = row.get("overlap_flags") or []
            had_book = had_book or any(isinstance(v, str) and v.startswith("book:") for v in flags)
            row["overlap_flags"] = [v for v in flags if not (isinstance(v, str) and v.startswith("book:"))]
        if legacy or had_book or out.get("historical_book_context_removed"):
            out["book_status"] = "BLOCKED"
            out["historical_book_context_removed"] = True
            out["execution_eligible"] = False
            out["publication_note"] = "Personal holdings and book references withheld; historical sizing awaits account reconciliation."
        out["holdings_publication"] = "REDACTED_ACCOUNT_PRIVATE"
    elif name == "ai-commentary/portfolio.json":
        if out.get("privacy_version") != PUBLIC_CONTEXT_PRIVACY_VERSION:
            out = {"page": "portfolio", "generated_at": out.get("generated_at"),
                   "privacy_version": PUBLIC_CONTEXT_PRIVACY_VERSION, "private_context_removed": True,
                   "commentary": {"error": "private_context_removed", "headline": "Public research commentary is awaiting regeneration."},
                   "preserved_from": None, "llm_attempt_failed": True}
    elif name == "vol-regime.json":
        rows = [r for r in out.get("tickers", []) if isinstance(r, dict) and r.get("ticker") in PUBLIC_VOL_UNIVERSE]
        out["tickers"] = rows
        out["n_tickers"] = len(rows)
        out["n_with_iv"] = sum(r.get("iv_atm_30d") is not None for r in rows)
        regimes = {}
        for row in rows:
            regime = row.get("regime") or "UNKNOWN"
            regimes[regime] = regimes.get(regime, 0) + 1
        out["regime_counts"] = regimes
        ranks = {"COMPLACENT": 0, "NORMAL": 25, "CONCERNED": 60, "PANIC": 100}
        ranked = sorted([r for r in rows if r.get("regime")],
                        key=lambda r: ranks.get(r["regime"], 0) + (r.get("rv_z") or 0) * 5, reverse=True)
        out["most_stressed"] = [{"ticker": r["ticker"], "regime": r["regime"], "rv_z": r.get("rv_z"), "iv_rv": r.get("iv_rv_ratio")}
                                for r in ranked[:10]]
        out["universe_scope"] = "PUBLIC_CORE_MODEL"
    elif name in {"wealth-plan-snapshot.json", "tax-plan-snapshot.json"}:
        publication = out.get("publication")
        # Only the producers' new scheduled default-model path can issue this
        # exact marker. Legacy snapshots may contain any caller's full financial
        # scenario; retain no input, calculation, timestamp or free text from it.
        if not (isinstance(publication, dict) and publication == PUBLIC_DEFAULT_SCENARIO
                and publication.get("contains_caller_inputs") is False):
            out = {"engine": "justhodl-" + name.removesuffix("-snapshot.json"),
                   "status": "PUBLIC_MODEL_UNAVAILABLE", "available": False,
                   "private_scenario_removed": True,
                   "note": "Public model scenario is awaiting regeneration."}
    elif name == "search/providers/tradingview_vault_live.json.gz":
        if not isinstance(vault, dict):
            raise ValueError("vault source required to rebuild search fields")
        by_id = {r["id"]: r["search"] for r in vault_search_rows(vault)}
        for row in out.get("rows", []):
            if not isinstance(row, list) or len(row) != 7:
                raise ValueError("unexpected search shard row contract")
            if row[2] == "instrument_ref":
                row[3] = by_id.get(row[0], "")
    elif key.startswith("equity-research/") and key.endswith(".json"):
        if "khalid_notes" in out:
            out["khalid_notes"] = public_notes_block(out["khalid_notes"])
    else:
        raise ValueError("unregistered public artifact")
    return out
