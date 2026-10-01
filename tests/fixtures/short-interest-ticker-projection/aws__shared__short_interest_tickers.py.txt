"""Consumer-facing per-ticker FINRA short-interest layer (Bloomberg parity 3/10).

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

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json

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

# DTC statuses where the provider display is a trustworthy ratio.
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
    """Return (identity_dict, latest_point_dict) for one issue history record."""
    if not isinstance(record, dict):
        raise ValueError("Issue history record required")
    identity = record.get("identity")
    if not isinstance(identity, dict) or any(identity.get(k) is None for k in GRAIN):
        raise ValueError("Complete reported issue identity required")
    observations = record.get("observations")
    if not isinstance(observations, list) or not observations:
        raise ValueError("Nonempty observation history required")
    latest = max(
        (_point_dict(p) for p in observations),
        key=lambda d: str(d.get("settlementDate") or ""),
    )
    return identity, latest


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
        "latest": bool(latest_settlement_present),
        "source": SOURCE,
    }


def build_tickers_view(shards, symbols=None):
    """Build {SYMBOL: per-ticker dict} from published record shards.

    shards: {prefix2: {"contract": ..., "records": {identity_hash: record}}}
    as stored under data/short-interest-research/records/.
    symbols: optional iterable of symbols to include; None means all.

    Several issue identities can share one symbolCode (different issue
    names / market classes). The record with the most recent settlement
    date wins; ties break toward larger reported short interest, then
    toward latest-settlement presence. Malformed records are skipped
    (fail-soft); an empty or fully malformed input yields {}.
    """
    if not isinstance(shards, dict):
        return {}
    wanted = None
    if symbols is not None:
        try:
            wanted = {str(s).strip().upper() for s in symbols if str(s).strip()}
        except TypeError:
            return {}
    best = {}
    for shard in shards.values():
        records = shard.get("records") if isinstance(shard, dict) else None
        if not isinstance(records, dict):
            continue
        for record in records.values():
            try:
                identity, latest = _record_latest_observation(record)
            except (ValueError, TypeError, KeyError, AttributeError):
                continue
            symbol = str(identity.get("symbolCode", "")).strip().upper()
            if not symbol or (wanted is not None and symbol not in wanted):
                continue
            stamp = str(latest.get("settlementDate") or "")
            size = _decimal_or_none(latest.get("currentShortPositionQuantity"))
            rank = (
                stamp,
                size if size is not None else Decimal(-1),
                bool(record.get("latest_settlement_present")),
            )
            if symbol not in best or rank > best[symbol][0]:
                try:
                    entry = _ticker_entry(
                        symbol, identity, latest,
                        record.get("latest_settlement_present"),
                    )
                except (ValueError, TypeError, KeyError, AttributeError):
                    continue
                best[symbol] = (rank, entry)
    return {symbol: entry for symbol, (_, entry) in sorted(best.items())}


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
    """Read the tickers artifact from S3; return {} on any failure.

    Fail-soft: missing key, access errors, malformed JSON and schema
    drift all yield {} so consumers degrade to no-SI-data, never crash.
    """
    try:
        raw = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
        doc = json.loads(raw)
    except (ValueError, TypeError, AttributeError, KeyError):
        return {}
    except Exception:
        # Covers ClientError/NoSuchKey and transport errors without
        # importing botocore here; consumers must not crash on S3 issues.
        return {}
    if not isinstance(doc, dict) or not isinstance(doc.get("by_ticker"), dict):
        return {}
    return doc


def publish_tickers_artifact(s3_client, bucket, shards, settlement_date,
                             generated_at, key=TICKERS_KEY):
    """Build the tickers view from shards and publish it to S3.

    Raises on S3 write failure so the caller can decide fail-soft policy.
    Returns the artifact meta dict.
    """
    by_ticker = build_tickers_view(shards)
    artifact = build_artifact(by_ticker, settlement_date, generated_at)
    body = json.dumps(artifact, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")
    s3_client.put_object(Bucket=bucket, Key=key, Body=body,
                         ContentType="application/json", CacheControl="no-store")
    return artifact["meta"]
