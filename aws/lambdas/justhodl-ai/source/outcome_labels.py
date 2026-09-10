"""Outcome labels derived from executable price paths.

Calendar horizons mature at the first observation on or after their target.
Observation horizons mature exactly N path observations after entry.  Costs
are round-trip basis points and are subtracted once from directional return.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional, Sequence, Tuple


class OutcomeLabelError(ValueError):
    pass


def _utc(value: datetime, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise OutcomeLabelError("%s must be a timezone-aware datetime" % field)
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, order=True)
class PricePoint:
    timestamp: datetime
    price: float

    def __post_init__(self) -> None:
        timestamp = _utc(self.timestamp, "price timestamp")
        if isinstance(self.price, bool) or not isinstance(self.price, (int, float)):
            raise OutcomeLabelError("price must be numeric")
        price = float(self.price)
        if not math.isfinite(price) or price <= 0:
            raise OutcomeLabelError("price must be positive and finite")
        object.__setattr__(self, "timestamp", timestamp)
        object.__setattr__(self, "price", price)


@dataclass(frozen=True)
class Horizon:
    value: int
    unit: str = "calendar_days"
    anchor: str = "entry"

    def __post_init__(self) -> None:
        if isinstance(self.value, bool) or not isinstance(self.value, int) or self.value <= 0:
            raise OutcomeLabelError("horizon value must be a positive integer")
        if self.unit not in ("calendar_days", "seconds", "observations"):
            raise OutcomeLabelError("horizon unit must be calendar_days, seconds, or observations")
        if self.anchor not in ("entry", "prediction"):
            raise OutcomeLabelError("horizon anchor must be entry or prediction")
        if self.unit == "observations" and self.anchor != "entry":
            raise OutcomeLabelError("observation horizons must be anchored to entry")

    def target(self, prediction_time: datetime, entry_time: datetime) -> Optional[datetime]:
        anchor_time = entry_time if self.anchor == "entry" else prediction_time
        if self.unit == "calendar_days":
            return anchor_time + timedelta(days=self.value)
        if self.unit == "seconds":
            return anchor_time + timedelta(seconds=self.value)
        return None


@dataclass(frozen=True)
class OutcomeLabel:
    matured: bool
    label: Optional[str]
    side: str
    horizon: Horizon
    entry_time: Optional[datetime]
    exit_time: Optional[datetime]
    entry_price: Optional[float]
    exit_price: Optional[float]
    gross_return: Optional[float]
    transaction_cost: float
    net_return: Optional[float]
    reason: Optional[str] = None


def _ordered_path(prices: Iterable[PricePoint]) -> Tuple[PricePoint, ...]:
    values = tuple(prices)
    if any(not isinstance(row, PricePoint) for row in values):
        raise OutcomeLabelError("price path must contain PricePoint values")
    rows = tuple(sorted(values, key=lambda row: row.timestamp))
    if len({row.timestamp for row in rows}) != len(rows):
        raise OutcomeLabelError("price path contains duplicate timestamps")
    return rows


def label_price_path(
    prices: Iterable[PricePoint],
    prediction_time: datetime,
    horizon: Horizon,
    *,
    side: str = "LONG",
    transaction_cost_bps: float = 0.0,
    flat_threshold_bps: float = 0.0,
) -> OutcomeLabel:
    """Create a label without interpolating unavailable prices."""
    prediction_time = _utc(prediction_time, "prediction_time")
    if not isinstance(horizon, Horizon):
        raise OutcomeLabelError("horizon must be Horizon")
    if side not in ("LONG", "SHORT"):
        raise OutcomeLabelError("side must be LONG or SHORT")
    for value, name in ((transaction_cost_bps, "transaction_cost_bps"), (flat_threshold_bps, "flat_threshold_bps")):
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)) or value < 0:
            raise OutcomeLabelError("%s must be finite and non-negative" % name)
    cost = float(transaction_cost_bps) / 10_000.0
    threshold = float(flat_threshold_bps) / 10_000.0
    path = _ordered_path(prices)
    entry_index = next((index for index, row in enumerate(path) if row.timestamp >= prediction_time), None)
    if entry_index is None:
        return OutcomeLabel(False, None, side, horizon, None, None, None, None, None, cost, None, "entry price unavailable")
    entry = path[entry_index]

    exit_point = None
    if horizon.unit == "observations":
        exit_index = entry_index + horizon.value
        if exit_index < len(path):
            exit_point = path[exit_index]
    else:
        target = horizon.target(prediction_time, entry.timestamp)
        exit_point = next((row for row in path[entry_index + 1:] if row.timestamp >= target), None)
    if exit_point is None:
        return OutcomeLabel(
            False, None, side, horizon, entry.timestamp, None, entry.price, None,
            None, cost, None, "horizon has not matured",
        )

    raw = exit_point.price / entry.price - 1.0
    gross = raw if side == "LONG" else -raw
    net = gross - cost
    if net > threshold:
        label = "POSITIVE"
    elif net < -threshold:
        label = "NEGATIVE"
    else:
        label = "FLAT"
    return OutcomeLabel(
        True, label, side, horizon, entry.timestamp, exit_point.timestamp,
        entry.price, exit_point.price, gross, cost, net,
    )


def labels_for_horizons(
    prices: Iterable[PricePoint],
    prediction_time: datetime,
    horizons: Sequence[Horizon],
    **kwargs
) -> Tuple[OutcomeLabel, ...]:
    path = tuple(prices)
    return tuple(label_price_path(path, prediction_time, horizon, **kwargs) for horizon in horizons)
