"""Source-snapshot scope observations only. Pure projection; no decision authority or I/O."""
from datetime import datetime, timedelta, timezone
import math

SCHEMA = "khalid-user-scope.v1"
MAX_SAFE = 2**53 - 1
# Bounded existing-product vocabulary, not a newly invented or complete taxonomy.
# Exact Fortress IND_ETF keys at 064e4fe5403b073176615291d3c0ce0c792f1692.
# Only labels are reused; no ETF mapping or investment semantics are imported.
SUPPORTED_INDUSTRIES = frozenset({
    'Aerospace & Defense',
    'Airlines',
    'Airports & Air Services',
    'Aluminum',
    'Apparel Retail',
    'Auto Manufacturers',
    'Auto Parts',
    'Banks - Diversified',
    'Banks - Regional',
    'Biotechnology',
    'Building Materials',
    'Building Products & Equipment',
    'Coking Coal',
    'Copper',
    'Department Stores',
    'Discount Stores',
    'Electrical Equipment & Parts',
    'Engineering & Construction',
    'Farm & Heavy Construction Machinery',
    'Furnishings, Fixtures & Appliances',
    'Gold',
    'Home Improvement Retail',
    'Information Technology Services',
    'Infrastructure Operations',
    'Insurance - Diversified',
    'Insurance - Life',
    'Insurance - Property & Casualty',
    'Insurance - Reinsurance',
    'Insurance - Specialty',
    'Insurance Brokers',
    'Integrated Freight & Logistics',
    'Internet Content & Information',
    'Internet Retail',
    'Marine Shipping',
    'Medical Devices',
    'Medical Instruments & Supplies',
    'Oil & Gas Drilling',
    'Oil & Gas E&P',
    'Oil & Gas Equipment & Services',
    'Other Industrial Metals & Mining',
    'Other Precious Metals & Mining',
    'Railroads',
    'Residential Construction',
    'Semiconductor Equipment & Materials',
    'Semiconductors',
    'Silver',
    'Software - Application',
    'Software - Infrastructure',
    'Solar',
    'Specialty Industrial Machinery',
    'Specialty Retail',
    'Steel',
    'Thermal Coal',
    'Trucking',
    'Uranium',
    'Utilities - Renewable',
})
DEFINITIONS = [
    {"id": "instrument", "label": "Source-reported instrument type", "unit": None,
     "definition": "Stocks, ETFs, metals and crypto are the stated scope. This slice accepts only native STOCK/ETF/CRYPTO source identities. Economic exposure labels, default types and guessed metals remain unavailable."},
    {"id": "biotech", "label": "Biotechnology exclusion observation", "unit": None,
     "source_labels": sorted(SUPPORTED_INDUSTRIES),
     "vocabulary_ref": "justhodl-fortress/source/lambda_function.py:IND_ETF keys at 064e4fe5403b073176615291d3c0ce0c792f1692; incomplete label vocabulary only",
     "definition": "For a source-bound stock, exact Finviz industry Biotechnology fails; another exact label in the bounded existing Fortress IND_ETF vocabulary passes this observation only. Labels outside that vocabulary are unavailable, not assumed non-biotech. Healthcare is not biotechnology. Fund holdings and crypto applicability are unresolved."},
    {"id": "cap", "label": "Existing stock-cap convention", "unit": "USD",
     "definition": "Product convention, not a user-chosen cutoff: SMALL is $300M to below $2B; MID $2B to below $10B; LARGE $10B to below $200B; MEGA >=$200B. SMALL fails this convention check; MID/LARGE/MEGA pass it only. Below $300M and non-stock applicability remain unresolved. Fortress further separates MICRO/NANO at $50M; Khalid groups both as MICRO."},
]
REASONS = {
    "observed": "Source-bound observation at the named snapshot; not full strategy qualification or current eligibility certification.",
    "missing": "No source-bound Fortress/Katlin record for this candidate.",
    "identity": "Missing, unsupported, duplicate or conflicting source identity/class; no ticker-only cross-asset join is permitted.",
    "source": "Producer identity, artifact, health, research clock or existing freshness rule did not qualify.",
    "clock": "Required upstream snapshot clock is absent, malformed or later than research; no observation time was invented.",
    "industry": "Industry is missing, malformed, non-operating or outside the bounded exact source-label vocabulary; it is not assumed non-biotech.",
    "conflict": "Source observations disagree; no preferred source or replacement was silently chosen.",
    "cap": "Cap must be positive, finite numeric USD within the exact JSON integer range; no coercion or unit guessing.",
    "bucket": "Supplied cap bucket is malformed or inconsistent with the producer's numeric USD convention.",
    "applicability": "Exclusion applicability to this non-stock instrument is unresolved; absence of a SMALL/biotech label is not a pass.",
    "below_small": "Below $300M: MICRO/NANO exclusion scope is unresolved; this is not a user-scope pass.",
}
SPECS = (
    ("fortress", "fortress-execution", "justhodl-fortress", 84, ("board", "etfs", "ledger")),
    ("katlin", "katlin", "justhodl-katlin", 36, ("picks", "watch")),
)


def stamp(value):
    if not isinstance(value, str) or len(value) > 40:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc) if dt.tzinfo else None
    except (ValueError, OverflowError):
        return None


def expiry(value, hours):
    try:
        return (value + timedelta(hours=hours)).isoformat() if value else None
    except OverflowError:
        return None


def numeric(value):
    return (type(value) in (int, float) and -MAX_SAFE <= value <= MAX_SAFE
            and math.isfinite(value))


def industry(value):
    return (isinstance(value, str) and value in SUPPORTED_INDUSTRIES and 0 < len(value) <= 120 and value == value.strip()
            and not any(ord(c) < 32 for c in value)
            and value.lower() not in {"unknown", "n/a", "na", "none", "null", "other", "-", "?"}
            and not value.startswith(("Exchange Traded", "Closed-End", "Shell Companies")))


def bucket(value, producer):
    if value >= 200e9: return "MEGA"
    if value >= 10e9: return "LARGE"
    if value >= 2e9: return "MID"
    if value >= 300e6: return "SMALL"
    return "NANO" if producer == "fortress" and value < 50e6 else "MICRO"


def unavailable(reason):
    return ["UNAVAILABLE", None, reason]


def project(output, feeds):
    """Read native containers, never normalized scoring inputs or external identity joins.

    The existing producer SLAs expire these observations. Upstream clocks describe
    dated snapshots only: no new SLA, effective time, or historical availability.
    """
    now = stamp(output.get("generated_at"))
    health = output.get("source_health") or []
    sources, indexed = [], {}
    for sid, (name, alias, engine, ttl, containers) in enumerate(SPECS):
        feed = feeds.get(name)
        feed = feed if isinstance(feed, dict) else {}
        hs = [h for h in health if isinstance(h, dict) and h.get("name") == name]
        h = hs[0] if len(hs) == 1 else {}
        published = feed.get("as_of") if name == "fortress" else feed.get("generated_at")
        research = (feed.get("research_generated_at") or
                    (None if feed.get("permission_refreshed_at") else published)) if name == "katlin" else published
        pub, res = stamp(published), stamp(research)
        clocks = feed.get("inputs" if name == "fortress" else "feeds_asof")
        clocks = clocks if isinstance(clocks, dict) else {}
        finviz = clocks.get("finviz_universe" if name == "fortress" else "finviz")
        census = clocks.get("fundamental_census_matrix" if name == "fortress" else "census")
        good = bool(now and pub and res and res <= pub <= now and
                    now - res <= timedelta(hours=ttl) and feed.get("engine") == engine and
                    h.get("status") == "FRESH" and h.get("key") == "data/" + name + ".json" and
                    h.get("producer") == engine and stamp(h.get("as_of")) == pub and
                    numeric(h.get("max_age_h")) and h["max_age_h"] == ttl and
                    (name != "katlin" or (feed.get("research_status") == "FRESH" and
                     numeric(feed.get("research_max_age_h")) and feed["research_max_age_h"] == ttl)))
        sources.append({"id": name, "artifact": "data/" + name + ".json", "producer": engine,
                        "status": "AVAILABLE" if good else "UNAVAILABLE",
                        "published_at": published if pub else None, "research_at": research if res else None,
                        "expires_at": expiry(res, ttl),
                        "max_age_h": ttl,
                        "native_research_status": feed.get("research_status") if feed.get("research_status") in ("FRESH", "STALE") else None,
                        "finviz_snapshot_at": finviz if stamp(finviz) else None,
                        "census_snapshot_at": census if stamp(census) else None,
                        "fields": {"instrument": "container (Fortress) / asset_class (Katlin)",
                                   "biotech": "industry", "cap": "market_cap" if name == "fortress" else "mcap"}})
        for container in containers:
            rows = feed.get(container)
            if not isinstance(rows, list): continue
            for index, raw in enumerate(rows):
                if not isinstance(raw, dict): continue
                ticker = raw.get("ticker")
                if not isinstance(ticker, str) or not ticker or ticker != ticker.strip().upper(): continue
                ac = ("ETF" if container == "etfs" else "STOCK") if name == "fortress" else raw.get("asset_class")
                if name == "katlin": ac = {"stock": "STOCK", "etf": "ETF", "crypto": "CRYPTO"}.get(ac) if isinstance(ac, str) else None
                explicit = raw.get("asset_class")
                if name == "fortress" and explicit is not None and explicit not in (ac, ac.lower()): ac = None
                indexed.setdefault(ticker, []).append((sid, container + "/" + str(index), ac, raw))
    rows = []
    for index, candidate in enumerate(output.get("opportunity_radar") or []):
        ac, ticker = candidate.get("asset_class"), candidate.get("ticker")
        all_matches = indexed.get(ticker, []) if isinstance(ticker, str) else []
        aliases = candidate.get("sources") if isinstance(candidate.get("sources"), list) else []
        expected = {sid for sid, spec in enumerate(SPECS) if spec[1] in aliases}
        matches = [m for m in all_matches if m[0] in expected]
        refs = [[m[0], m[1]] for m in matches]
        checks = [unavailable("missing") for _ in DEFINITIONS]
        if matches:
            # Reject even a known collision in another native container. Never
            # reinterpret a STOCK/ETF ticker as a CRYPTO/COMMODITY identity.
            if (any(m[2] != ac for m in all_matches) or ac not in {"STOCK", "ETF", "CRYPTO"}
                    or {m[0] for m in matches} != expected
                    or len(matches) != len({m[0] for m in matches})):
                checks = [unavailable("identity") for _ in DEFINITIONS]
            elif any(sources[m[0]]["status"] != "AVAILABLE" for m in matches):
                checks = [unavailable("source") for _ in DEFINITIONS]
            else:
                checks[0] = ["PASS", ac, "observed"]
                if ac != "STOCK":
                    checks[1:] = [["UNRESOLVED", None, "applicability"] for _ in range(2)]
                else:
                    dated = lambda m, key: bool(stamp(sources[m[0]][key]) and
                                               stamp(sources[m[0]][key]) <= stamp(sources[m[0]]["research_at"]))
                    inds = [m[3].get("industry") for m in matches]
                    if not all(dated(m, "finviz_snapshot_at") for m in matches): checks[1] = unavailable("clock")
                    elif not all(industry(v) for v in inds): checks[1] = unavailable("industry")
                    elif len(set(inds)) != 1: checks[1] = unavailable("conflict")
                    else: checks[1] = ["FAIL" if inds[0] == "Biotechnology" else "PASS", inds[0], "observed"]
                    caps = [m[3].get("market_cap" if m[0] == 0 else "mcap") for m in matches]
                    # Both possible upstream origins must be dated: the native
                    # producer does not retain which cap fallback supplied a row.
                    if not all(dated(m, key) for m in matches for key in ("finviz_snapshot_at", "census_snapshot_at")):
                        checks[2] = unavailable("clock")
                    elif not all(numeric(v) and v > 0 for v in caps): checks[2] = unavailable("cap")
                    elif len(set(caps)) != 1: checks[2] = unavailable("conflict")
                    elif any(m[3].get("cap_bucket") is not None and
                             (not isinstance(m[3]["cap_bucket"], str) or
                              m[3]["cap_bucket"].upper() != bucket(v, SPECS[m[0]][0])) for m, v in zip(matches, caps)):
                        checks[2] = unavailable("bucket")
                    else:
                        val = caps[0]
                        checks[2] = ["UNRESOLVED" if val < 300e6 else "FAIL" if val < 2e9 else "PASS",
                                     val, "below_small" if val < 300e6 else "observed"]
        rows.append({"i": index, "refs": refs, "checks": checks})
    return {"schema_version": SCHEMA, "generated_at": output.get("generated_at"),
            "qualification_revision": (output.get("qualification_evidence") or {}).get("revision"),
            "identity_ref": "qualification_evidence.rows[i] / opportunity_radar[i]",
            "check_encoding": "checks follow definitions order; each tuple is [status, value, reason_id]; refs are [source_index, native_row_path]",
            "authority": "Research-only source-snapshot observations. No full strategy match, eligibility certification, ranking or capital authority.",
            "attribution": "Only asset scope and exclusions come from the supplied user criteria. The unchanged 23-item legacy contract includes inherited scanner definitions, not 23 newly confirmed user requirements.",
            "clock_policy": "Existing producer freshness only. Finviz/census clocks name snapshots, not independently freshness-qualified observations. Cap origin can be Finviz or census fallback; both clocks are retained. No historical effective/availability time or new SLA is inferred.",
            "effective_at": None, "available_at": None,
            "definitions": DEFINITIONS, "reasons": REASONS, "sources": sources, "rows": rows}
