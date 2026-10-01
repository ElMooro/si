"""Consumer-facing FINRA short-interest projection with explicit identity gaps.

The canonical research pipeline (justhodl-short-interest + the
short-interest-original-research.v1 evidence contract) is UNTOUCHED. This
module builds an additive, consumer-facing artifact from the published
record shards:

    data/short-interest-tickers.json

Every value below is a DESCRIPTIVE measurement: reported settlement
positions, provider days-to-cover conventions and reconciled change
arithmetic. No squeeze probability, covering inference, directional
forecast or position-sizing authority follows from these observations.
Consumers must treat the artifact as measurements, not signals.
"""

from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import json, re

from short_interest_measurements import POINT_FIELDS, GRAIN

TICKERS_KEY = "data/short-interest-tickers.json"
SCHEMA_VERSION = "1.0"
SOURCE = "FINRA consolidatedShortInterest (api.finra.org otcMarket/consolidatedShortInterest)"
DISCLAIMER = (
    "Descriptive measurements only. Reported FINRA settlement positions, "
    "provider days-to-cover conventions, zero denominators, revisions and "
    "stock splits are retained explicitly. No squeeze probability, covering "
    "inference, directional forecast or position-sizing authority follows "
    "from these observations."
)

# Point-list positions derived from the shared field contract so a reorder
# upstream cannot silently misalign this module.
_IDX = {name: POINT_FIELDS.index(name) for name in POINT_FIELDS}

# Reconciled provider displays; a display floor remains a convention.
_TRUSTED_DTC = ("matches_reconstructed_rounded_ratio", "provider_display_floor_one")


def _utcnow():
    """Current UTC time as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def _decimal_or_none(value):
    """Parse a provider decimal string; return None when unparseable."""
    if value is None or isinstance(value, bool):
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError, ArithmeticError):
        return None
    return result if result.is_finite() else None


def _point_dict(point):
    """Map a compact observation list to a field-name dict.

    Raises ValueError on malformed points so callers can fail-soft per record.
    """
    if not isinstance(point, list) or len(point) != len(POINT_FIELDS):
        raise ValueError("Complete compact short-position observation required")
    return {name: point[_IDX[name]] for name in POINT_FIELDS}


def _record_latest_observation(record):
    """Validate one complete reported issue history; never select by lexical date."""
    if not isinstance(record, dict):
        raise ValueError("Issue history record required")
    identity = record.get("identity")
    if not isinstance(identity, dict) or any(not isinstance(identity.get(k), str)
            or not identity[k].strip() or len(identity[k]) > 500 for k in GRAIN):
        raise ValueError("Complete reported issue identity required")
    observations = record.get("observations")
    if not isinstance(observations, list) or not observations:
        raise ValueError("Nonempty observation history required")
    points = [_point_dict(p) for p in observations]
    dates = set()
    for point in points:
        stamp = point.get("settlementDate")
        if (not isinstance(stamp, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", stamp)
                or date.fromisoformat(stamp).isoformat() != stamp or stamp in dates):
            raise ValueError("Distinct actual settlement dates required")
        if any(point.get(k) != identity[k] for k in GRAIN):
            raise ValueError("Observation and reported issue identities differ")
        dates.add(stamp)
    return identity, max(points, key=lambda p: p["settlementDate"])


def _ticker_entry(symbol, identity, point, latest_settlement_present):
    """Build the consumer-facing per-ticker dict from one latest observation."""
    adv = _decimal_or_none(point.get("averageDailyVolumeQuantity"))
    reported_dtc = _decimal_or_none(point.get("daysToCoverQuantity"))
    reconstructed_dtc = _decimal_or_none(
        point.get("reconstructed_position_to_reported_adv_days")
    )
    dtc_status = point.get("days_to_cover_status")
    if dtc_status in _TRUSTED_DTC and reported_dtc is not None:
        effective_dtc = reported_dtc
    else:
        effective_dtc = reconstructed_dtc
    return {
        "ticker": symbol,
        "issue_name": identity.get("issueName"),
        "exchange_code": identity.get("issuerServicesGroupExchangeCode"),
        "market_class": identity.get("marketClassCode"),
        "short_interest": point.get("currentShortPositionQuantity"),
        "prev_short_interest": point.get("previousShortPositionQuantity"),
        "change_shares": point.get("computed_change_shares"),
        "change_pct": point.get("computed_change_pct"),
        "change_pct_reported": point.get("changePercent"),
        "change_pct_reconciliation": point.get("change_pct_reconciliation"),
        "avg_daily_volume": point.get("averageDailyVolumeQuantity"),
        "days_to_cover": point.get("daysToCoverQuantity"),
        "dtc_reconstructed": point.get("reconstructed_position_to_reported_adv_days"),
        "dtc_effective": format(effective_dtc, "f") if effective_dtc is not None else None,
        "dtc_status": dtc_status,
        "settlement_date": point.get("settlementDate"),
        "prev_settlement_date": point.get("previous_settlement_date"),
        "revision_flag": point.get("revisionFlag"),
        "split_flag": point.get("stockSplitFlag"),
        "zero_adv": adv == 0 if adv is not None else None,
        "prior_matched": point.get("matches_reported_previous_quantity"),
        "latest": latest_settlement_present if type(latest_settlement_present) is bool else None,
        "latest_flag_status": "reported_boolean" if type(latest_settlement_present) is bool else "invalid_or_missing",
        "source": SOURCE,
    }


def build_tickers_projection(shards, symbols=None):
    """Retain every source occurrence; ambiguous symbols have no ticker projection.

    A symbol is a reported label, not a unique security identifier. Even a
    malformed issue can collide with a valid issue, so group before validation.
    This does not verify provider identity, freshness or continuity.
    """
    out = {"by_ticker": {}, "ambiguous_symbols": [], "unresolved_occurrences": [],
           "source_occurrences": 0, "identity_verified": False}
    if not isinstance(shards, dict):
        return out
    wanted = None
    if symbols is not None:
        try:
            wanted = {s.upper() for s in symbols if isinstance(s, str)
                      and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.:-]{0,31}", s)}
        except TypeError:
            return out
    grouped = {}
    for prefix, shard in shards.items():
        records = shard.get("records") if isinstance(shard, dict) else None
        if not isinstance(records, dict):
            out["unresolved_occurrences"].append({"shard": prefix, "reason": "invalid_record_container"})
            continue
        for record_id, record in records.items():
            out["source_occurrences"] += 1
            identity = record.get("identity") if isinstance(record, dict) else None
            reported = identity.get("symbolCode") if isinstance(identity, dict) else None
            occurrence = {"shard": prefix, "record_id": record_id,
                          "reported_symbol": reported if isinstance(reported, str) else None,
                          "status": "unresolved", "identity_verified": False}
            if not isinstance(reported, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.:-]{0,31}", reported):
                occurrence["reason"] = "invalid_reported_symbol"
                out["unresolved_occurrences"].append(occurrence)
                continue
            symbol = reported.upper()
            if wanted is not None and symbol not in wanted:
                continue
            grouped.setdefault(symbol, []).append((occurrence, None))
            try:
                identity, latest = _record_latest_observation(record)
                entry = _ticker_entry(symbol, identity, latest, record.get("latest_settlement_present"))
            except (ValueError, TypeError, KeyError, AttributeError, OverflowError):
                occurrence["reason"] = "invalid_issue_history"
                out["unresolved_occurrences"].append(occurrence)
                continue
            occurrence.update(status="descriptive_issue", reported_identity=dict(identity),
                              settlement_date=entry["settlement_date"])
            grouped[symbol][-1] = (occurrence, entry)
    for symbol, occurrences in sorted(grouped.items()):
        if len(occurrences) != 1:
            out["ambiguous_symbols"].append({"ticker": symbol,
                "occurrences": [item[0] for item in occurrences],
                "reason": "multiple_reported_issue_occurrences"})
        elif occurrences[0][1] is not None:
            out["by_ticker"][symbol] = occurrences[0][1]
    return out


def build_tickers_view(shards, symbols=None):
    """Compatibility projection; ambiguous reported symbols are withheld."""
    return build_tickers_projection(shards, symbols)["by_ticker"]


def _list_entry(ticker, row):
    """Compact ranked-list entry for one ticker row."""
    return {
        "ticker": ticker,
        "short_interest": row.get("short_interest"),
        "change_pct": row.get("change_pct"),
        "days_to_cover": row.get("days_to_cover"),
        "dtc_effective": row.get("dtc_effective"),
        "settlement_date": row.get("settlement_date"),
    }


def build_ranked_lists(by_ticker, limit=20):
    """Build descriptive ranked lists from the per-ticker view.

    Only records flagged latest=True participate. Sort keys are raw
    measurements, not signals; each list's basis is documented in meta.
    """
    if not isinstance(by_ticker, dict):
        by_ticker = {}
    current = [r for r in by_ticker.values()
               if isinstance(r, dict) and r.get("latest") is True]

    def si_of(row):
        """Reported short interest as Decimal, or None."""
        return _decimal_or_none(row.get("short_interest"))

    def dtc_of(row):
        """Effective days-to-cover as Decimal, or None."""
        return _decimal_or_none(row.get("dtc_effective"))

    def pct_of(row):
        """Computed settlement-to-settlement change_pct as Decimal, or None."""
        return _decimal_or_none(row.get("change_pct"))

    crowded = sorted(
        (r for r in current if si_of(r) is not None),
        key=lambda r: si_of(r), reverse=True,
    )[:limit]
    high_dtc = sorted(
        (r for r in current if dtc_of(r) is not None),
        key=lambda r: dtc_of(r), reverse=True,
    )[:limit]
    squeeze_risk = sorted(
        (r for r in current if si_of(r) is not None and dtc_of(r) is not None),
        key=lambda r: si_of(r) * dtc_of(r), reverse=True,
    )[:limit]
    rising = sorted(
        (r for r in current if pct_of(r) is not None and pct_of(r) > 0),
        key=lambda r: pct_of(r), reverse=True,
    )[:limit]
    covering = sorted(
        (r for r in current if pct_of(r) is not None and pct_of(r) < 0),
        key=lambda r: pct_of(r),
    )[:limit]
    return {
        "top_crowded": [_list_entry(r["ticker"], r) for r in crowded],
        "top_squeeze_risk": [_list_entry(r["ticker"], r) for r in squeeze_risk],
        "top_high_dtc": [_list_entry(r["ticker"], r) for r in high_dtc],
        "top_rising_si": [_list_entry(r["ticker"], r) for r in rising],
        "top_covering": [_list_entry(r["ticker"], r) for r in covering],
    }


def build_artifact(by_ticker, settlement_date, generated_at):
    """Assemble the full data/short-interest-tickers.json artifact."""
    if not isinstance(by_ticker, dict):
        by_ticker = {}
    ranked = build_ranked_lists(by_ticker)
    n_latest = sum(1 for r in by_ticker.values()
                   if isinstance(r, dict) and r.get("latest") is True)
    artifact = {
        "contract": "short-interest-tickers.v1",
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at,
        "settlement_date": settlement_date,
        "source": SOURCE,
        "disclaimer": DISCLAIMER,
        "by_ticker": by_ticker,
        **ranked,
        "meta": {
            "settlement_date": settlement_date,
            "generated_at": generated_at,
            "n_tickers": len(by_ticker),
            "n_latest_settlement": n_latest,
            "schema_version": SCHEMA_VERSION,
            "stale_after_days": 21,
            "list_bases": {
                "top_crowded": "Largest reported short interest (shares). Descriptive only.",
                "top_squeeze_risk": "Largest short_interest * effective days-to-cover. "
                                    "Descriptive crowding/coverage arithmetic, not a squeeze signal.",
                "top_high_dtc": "Largest effective days-to-cover. Descriptive only.",
                "top_rising_si": "Largest positive settlement-to-settlement change_pct. Descriptive only.",
                "top_covering": "Most negative settlement-to-settlement change_pct. Descriptive only.",
            },
        },
    }
    return artifact


def load_tickers(s3_client, bucket, key=TICKERS_KEY):
    """Read a complete bounded ticker packet; failures remain unavailable, not zero."""
    def pairs(items):
        out = {}
        for name, value in items:
            if name in out:
                raise ValueError("Duplicate ticker JSON field")
            out[name] = value
        return out
    def number(text):
        exact = Decimal(text)
        value = float(exact)
        if not exact.is_finite() or Decimal(str(value)) != exact:
            raise ValueError("Ticker JSON number loses precision")
        return value
    def constant(_):
        raise ValueError("Nonfinite ticker JSON")
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        stream = response["Body"]
        try:
            raw = stream.read(16 * 1024 * 1024 + 1)
        finally:
            stream.close()
        if not isinstance(raw, bytes) or len(raw) > 16 * 1024 * 1024:
            raise ValueError("Whole ticker packet exceeds byte bound")
        doc = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs,
                         parse_float=number, parse_constant=constant)
    except Exception:
        # Keep the existing fail-soft caller contract without hiding a partial
        # document behind parsed rows or leaking storage exception details.
        return {}
    if not isinstance(doc, dict) or not isinstance(doc.get("by_ticker"), dict):
        return {}
    return doc


def project_verified_tickers(shards, expected_head, publication_at):
    """Pure projection of hash-verified canonical input; no transport or clock.

    Retained-input replay supplies the complete source head, every referenced
    record shard and the publication clock from the input manifest. Storage
    validation remains the caller's responsibility. No forecast is granted.
    """
    import short_interest_research_model as research
    expected = research.strict(expected_head)
    projection = build_tickers_projection(shards)
    packet = build_artifact(projection['by_ticker'], expected['settlement_date'], publication_at)
    packet.update(measurement_contract='short-interest-ticker-projection.v1',
        source_generated_at=expected['generated_at'], research_replay=expected.get('replay'),
        source_head_sha256=research.sha(expected_head), source_head_bytes=len(expected_head),
        source_record_shards=expected['record_shards'],
        identity_report={k:v for k,v in projection.items() if k!='by_ticker'},
        independent_investment_votes=0, call=None, **research.PERMISSIONS)
    return packet


def publish_tickers_artifact(s3_client, bucket, shards, settlement_date,
                             generated_at, key=TICKERS_KEY, *, expected_head=None,
                             compiler_paths=None):
    """Retain replay inputs and compare-and-swap the one declared ticker head.

    The canonical source must still be exactly this producer run after the
    previous ticker head is captured. That ordering prevents an older producer
    from overwriting a newer consumer publication. No head write is retried.
    """
    from pathlib import Path
    import context_evidence_store as retention
    import short_interest_research_model as research
    if key != TICKERS_KEY or not isinstance(expected_head, bytes):
        raise ValueError("Declared ticker head and exact source publication required")
    if not isinstance(compiler_paths, dict) or not compiler_paths:
        raise ValueError("Complete reviewed compiler inventory required")
    expected = research.strict(expected_head)
    if (not isinstance(expected, dict) or expected.get('contract') != research.CONTRACT
            or expected.get('settlement_date') != settlement_date
            or expected.get('generated_at') != generated_at):
        raise ValueError("Canonical projection clock and settlement required")
    replay = expected.get('replay')
    if (retention.clock(generated_at) is None or not isinstance(settlement_date, str)
            or not re.fullmatch('[0-9]{4}-[0-9]{2}-[0-9]{2}', settlement_date)
            or date.fromisoformat(settlement_date).isoformat() != settlement_date
            or not isinstance(replay, dict)
            or replay.get('output_sha256') != research.digest({k:v for k,v in expected.items() if k!='replay'})
            or any(expected.get(k) is not False for k in research.PERMISSIONS)):
        raise ValueError("Exact descriptive canonical head and source clocks required")
    if set(shards) != set(expected.get('record_shards', {})):
        raise ValueError("Complete canonical shard inventory required")
    for prefix, shard in shards.items():
        if research.record_identity(shard) != expected['record_shards'][prefix]:
            raise ValueError("Projection shard differs from canonical evidence")
    paths = dict(compiler_paths)
    paths['short_interest_tickers.py'] = Path(__file__)
    paths['context_evidence_store.py'] = Path(retention.__file__)
    class PublicationClient:
        def get_object(self, **request):
            return s3_client.get_object(**request)
        def put_object(self, **request):
            if request.get('Key') == TICKERS_KEY:
                request['CacheControl'] = 'no-store'
            return s3_client.put_object(**request)
    publisher = retention.ContextStore(PublicationClient(), bucket, TICKERS_KEY,
        {'research': research.CURRENT},
        'audit-private/20260909-originals/short-interest-ticker-projection/',
        'short-interest-ticker-projection.v1', paths,
        acquisition_budget_s=30, publication_budget_s=90)
    def project(attempts, originals, publication_at):
        attempt = attempts.get('research', {})
        ref = attempt.get('original_ref', {})
        if attempt.get('status') != 'received' or originals.get(ref.get('key')) != expected_head:
            raise ValueError("Canonical head changed or unavailable; preserve ticker publication")
        return project_verified_tickers(shards, expected_head, publication_at)
    artifact, _ = publisher.publish(project)
    return artifact['meta']
