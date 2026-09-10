"""Point-in-time feature observation and assembly interfaces.

Feature stores implement ``observations``.  The assembler uses only values
whose event and availability timestamps are no later than the requested
as-of time, making late revisions explicit and preventing look-ahead.
"""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Mapping, Optional, Protocol, Sequence, Tuple


class PointInTimeError(ValueError):
    pass


def _utc(value: datetime, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise PointInTimeError("%s must be a timezone-aware datetime" % field)
    return value.astimezone(timezone.utc)


def _json_value(value: Any) -> None:
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float) and math.isfinite(value):
        return
    raise PointInTimeError("feature value must be a finite JSON scalar")


@dataclass(frozen=True)
class FeatureObservation:
    entity_id: str
    feature_name: str
    event_time: datetime
    available_at: datetime
    value: Any
    source: str
    revision: int = 0
    tainted: bool = False

    def __post_init__(self) -> None:
        if not self.entity_id or not self.feature_name or not self.source:
            raise PointInTimeError("entity_id, feature_name and source are required")
        event_time = _utc(self.event_time, "event_time")
        available_at = _utc(self.available_at, "available_at")
        if available_at < event_time:
            raise PointInTimeError("available_at must not precede event_time")
        if isinstance(self.revision, bool) or not isinstance(self.revision, int) or self.revision < 0:
            raise PointInTimeError("revision must be a non-negative integer")
        if not isinstance(self.tainted, bool):
            raise PointInTimeError("tainted must be boolean")
        _json_value(self.value)
        object.__setattr__(self, "event_time", event_time)
        object.__setattr__(self, "available_at", available_at)


class PointInTimeFeatureStore(Protocol):
    def observations(
        self, entity_id: str, feature_names: Sequence[str], as_of: datetime
    ) -> Iterable[FeatureObservation]:
        """Return candidate observations; the assembler still enforces cutoffs."""


class InMemoryFeatureStore:
    """Deterministic reference store useful for local runs and contract tests."""

    def __init__(self, observations: Iterable[FeatureObservation] = ()) -> None:
        self._rows = list(observations)

    def add(self, observation: FeatureObservation) -> None:
        if not isinstance(observation, FeatureObservation):
            raise TypeError("observation must be FeatureObservation")
        self._rows.append(observation)

    def observations(
        self, entity_id: str, feature_names: Sequence[str], as_of: datetime
    ) -> Iterable[FeatureObservation]:
        wanted = set(feature_names)
        return [
            row for row in self._rows
            if row.entity_id == entity_id and row.feature_name in wanted
        ]


@dataclass(frozen=True)
class FeatureVector:
    entity_id: str
    as_of: datetime
    values: Mapping[str, Any]
    lineage: Tuple[Mapping[str, Any], ...]
    fingerprint: str


class PointInTimeFeatureAssembler:
    def __init__(self, store: PointInTimeFeatureStore) -> None:
        self.store = store

    def assemble(
        self,
        entity_id: str,
        as_of: datetime,
        feature_names: Sequence[str],
        *,
        max_age: Optional[timedelta] = None,
        require_all: bool = True,
        allow_tainted: bool = False,
    ) -> FeatureVector:
        as_of = _utc(as_of, "as_of")
        names = tuple(dict.fromkeys(feature_names))
        if not entity_id or not names or any(not isinstance(name, str) or not name for name in names):
            raise PointInTimeError("entity_id and at least one feature name are required")
        if max_age is not None and (not isinstance(max_age, timedelta) or max_age < timedelta(0)):
            raise PointInTimeError("max_age must be a non-negative timedelta")

        selected: Dict[str, FeatureObservation] = {}
        for row in self.store.observations(entity_id, names, as_of):
            if not isinstance(row, FeatureObservation):
                raise PointInTimeError("store returned a non-FeatureObservation")
            if row.entity_id != entity_id or row.feature_name not in names:
                continue
            if row.event_time > as_of or row.available_at > as_of:
                continue
            if max_age is not None and as_of - row.event_time > max_age:
                continue
            prior = selected.get(row.feature_name)
            rank = (row.event_time, row.revision, row.available_at, row.source)
            prior_rank = (
                (prior.event_time, prior.revision, prior.available_at, prior.source)
                if prior else None
            )
            if prior_rank is None or rank > prior_rank:
                selected[row.feature_name] = row

        missing = [name for name in names if name not in selected]
        if require_all and missing:
            raise PointInTimeError("missing point-in-time features: %s" % ", ".join(missing))
        tainted = sorted(name for name, row in selected.items() if row.tainted)
        if tainted and not allow_tainted:
            raise PointInTimeError("tainted point-in-time features rejected: %s" % ", ".join(tainted))

        values = {name: selected[name].value for name in names if name in selected}
        lineage = tuple({
            "feature_name": name,
            "source": selected[name].source,
            "event_time": selected[name].event_time.isoformat().replace("+00:00", "Z"),
            "available_at": selected[name].available_at.isoformat().replace("+00:00", "Z"),
            "revision": selected[name].revision,
            "tainted": selected[name].tainted,
        } for name in names if name in selected)
        fingerprint_payload = {
            "entity_id": entity_id,
            "as_of": as_of.isoformat().replace("+00:00", "Z"),
            "values": values,
            "lineage": lineage,
        }
        fingerprint = hashlib.sha256(
            json.dumps(fingerprint_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return FeatureVector(entity_id, as_of, values, lineage, fingerprint)


def assemble_point_in_time(
    store: PointInTimeFeatureStore,
    entity_id: str,
    as_of: datetime,
    feature_names: Sequence[str],
    **kwargs: Any
) -> FeatureVector:
    """Functional interface for consumers that do not need a long-lived assembler."""
    return PointInTimeFeatureAssembler(store).assemble(entity_id, as_of, feature_names, **kwargs)
