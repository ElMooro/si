"""Bounded point-in-time reads from a SageMaker Feature Store offline table.

The online Feature Store API returns only the latest record for a record ID and
therefore cannot reconstruct an arbitrary historical as-of view.  This adapter
uses the offline store's Glue table through Athena, then independently checks
every returned row before exposing it to ``PointInTimeFeatureAssembler``.

Both the query executor and clock/sleep behavior are injected so tests never
need AWS credentials or network access.
"""
from __future__ import annotations

import math
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Sequence

from point_in_time import FeatureObservation, PointInTimeError


MAX_FEATURE_NAMES = 200
MAX_QUERY_RESULTS = 1_000
MAX_ENTITY_LENGTH = 256
MAX_FEATURE_NAME_LENGTH = 256
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class FeatureStoreQuery(Protocol):
    def fetch_rows(
        self,
        *,
        feature_group_name: str,
        entity_id: str,
        feature_names: Sequence[str],
        as_of: datetime,
        limit: int,
    ) -> Iterable[Mapping[str, Any]]:
        """Return candidate offline-store rows for one entity and cutoff."""


def _utc(value: datetime, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise PointInTimeError("%s must be a timezone-aware datetime" % field)
    return value.astimezone(timezone.utc)


def _timestamp(value: Any, field: str) -> datetime:
    if isinstance(value, datetime):
        return _utc(value, field)
    if isinstance(value, bool):
        raise PointInTimeError("%s must be an epoch number or ISO-8601 timestamp" % field)
    if isinstance(value, (int, float)):
        if not math.isfinite(float(value)):
            raise PointInTimeError("%s must be finite" % field)
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise PointInTimeError("%s is required" % field)
        try:
            return datetime.fromtimestamp(float(text), tz=timezone.utc)
        except ValueError:
            pass
        try:
            parsed = datetime.fromisoformat(text[:-1] + "+00:00" if text.endswith("Z") else text)
        except ValueError as exc:
            raise PointInTimeError("%s must be an epoch number or ISO-8601 timestamp" % field) from exc
        return _utc(parsed, field)
    raise PointInTimeError("%s must be an epoch number or ISO-8601 timestamp" % field)


def _tainted(value: Any) -> bool:
    if value is None or value is False:
        return False
    if value is True:
        return True
    if isinstance(value, str):
        return value.strip().upper() not in ("", "0", "FALSE", "CLEAN")
    return True


def _feature_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise PointInTimeError("feature value must be finite")
        return value
    if isinstance(value, str):
        text = value.strip()
        try:
            number = float(text)
        except ValueError:
            return value
        if not math.isfinite(number):
            raise PointInTimeError("feature value must be finite")
        return number
    raise PointInTimeError("feature value must be a JSON scalar")


class SageMakerFeatureStore:
    """Point-in-time store backed by a bounded, dependency-injected query."""

    def __init__(
        self,
        query: FeatureStoreQuery,
        feature_group_name: str,
        *,
        max_feature_names: int = MAX_FEATURE_NAMES,
        max_results: int = MAX_QUERY_RESULTS,
    ) -> None:
        if not feature_group_name or len(feature_group_name) > 64:
            raise PointInTimeError("feature_group_name is required and must be at most 64 characters")
        if isinstance(max_feature_names, bool) or not 1 <= int(max_feature_names) <= MAX_FEATURE_NAMES:
            raise PointInTimeError("max_feature_names is outside the reviewed bound")
        if isinstance(max_results, bool) or not 1 <= int(max_results) <= MAX_QUERY_RESULTS:
            raise PointInTimeError("max_results is outside the reviewed bound")
        self.query = query
        self.feature_group_name = feature_group_name
        self.max_feature_names = int(max_feature_names)
        self.max_results = int(max_results)

    def observations(
        self,
        entity_id: str,
        feature_names: Sequence[str],
        as_of: datetime,
    ) -> Iterable[FeatureObservation]:
        as_of = _utc(as_of, "as_of")
        if not isinstance(entity_id, str) or not entity_id.strip():
            raise PointInTimeError("entity_id is required")
        entity_id = entity_id.strip()
        if len(entity_id) > MAX_ENTITY_LENGTH:
            raise PointInTimeError("entity_id exceeds the %d-character limit" % MAX_ENTITY_LENGTH)
        names = tuple(dict.fromkeys(feature_names))
        if not names:
            raise PointInTimeError("at least one feature name is required")
        if len(names) > self.max_feature_names:
            raise PointInTimeError(
                "feature_names exceeds the %d-item Feature Store limit" % self.max_feature_names
            )
        if any(
            not isinstance(name, str)
            or not name
            or len(name) > MAX_FEATURE_NAME_LENGTH
            for name in names
        ):
            raise PointInTimeError(
                "feature names must be non-empty strings of at most %d characters"
                % MAX_FEATURE_NAME_LENGTH
            )

        candidates = self.query.fetch_rows(
            feature_group_name=self.feature_group_name,
            entity_id=entity_id,
            feature_names=names,
            as_of=as_of,
            limit=self.max_results + 1,
        )
        rows: List[FeatureObservation] = []
        for index, raw in enumerate(candidates):
            if index >= self.max_results:
                raise PointInTimeError(
                    "Feature Store result exceeds the %d-row limit" % self.max_results
                )
            if not isinstance(raw, Mapping):
                raise PointInTimeError("Feature Store returned a non-object row")
            row_entity = str(raw.get("entity_id") or raw.get("asset_id") or "").strip()
            feature_name = str(raw.get("feature_name") or raw.get("signal_name") or "").strip()
            if row_entity != entity_id:
                raise PointInTimeError("Feature Store violated entity scoping")
            if feature_name not in names:
                raise PointInTimeError("Feature Store returned an unrequested feature")
            event_time = _timestamp(raw.get("event_time"), "event_time")
            available_at = _timestamp(
                raw.get("available_at", raw.get("available_time")), "available_at"
            )
            if event_time > as_of or available_at > as_of:
                raise PointInTimeError("Feature Store returned future data beyond as_of")
            if _tainted(raw.get("tainted", raw.get("taint"))):
                continue
            revision_value = raw.get("revision", 0)
            try:
                revision = int(revision_value)
            except (TypeError, ValueError) as exc:
                raise PointInTimeError("Feature Store revision must be an integer") from exc
            rows.append(
                FeatureObservation(
                    entity_id=row_entity,
                    feature_name=feature_name,
                    event_time=event_time,
                    available_at=available_at,
                    value=_feature_value(raw.get("value", raw.get("signal_value"))),
                    source=str(
                        raw.get("source")
                        or raw.get("source_uri")
                        or ("sagemaker-feature-store:%s" % self.feature_group_name)
                    ),
                    revision=revision,
                    tainted=False,
                )
            )
        return rows


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_identifier(value: str, field: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise PointInTimeError("%s is not a safe Glue identifier" % field)
    return '"%s"' % value


class AthenaFeatureStoreQuery:
    """Execute a reviewed PIT query against a Feature Group's offline table."""

    TERMINAL_FAILURES = frozenset(("FAILED", "CANCELLED"))

    def __init__(
        self,
        sagemaker_client: Any,
        athena_client: Any,
        *,
        output_location: str,
        workgroup: str = "primary",
        max_polls: int = 120,
        poll_interval_seconds: float = 0.1,
        sleep: Any = time.sleep,
    ) -> None:
        if not isinstance(output_location, str) or not output_location.startswith("s3://"):
            raise PointInTimeError("Athena output_location must be an s3 URI")
        if not isinstance(workgroup, str) or not workgroup.strip():
            raise PointInTimeError("Athena workgroup is required")
        if isinstance(max_polls, bool) or not 1 <= int(max_polls) <= 600:
            raise PointInTimeError("max_polls is outside the reviewed bound")
        if not isinstance(poll_interval_seconds, (int, float)) or not 0 <= poll_interval_seconds <= 5:
            raise PointInTimeError("poll_interval_seconds is outside the reviewed bound")
        self.sagemaker = sagemaker_client
        self.athena = athena_client
        self.output_location = output_location.rstrip("/") + "/"
        self.workgroup = workgroup.strip()
        self.max_polls = int(max_polls)
        self.poll_interval_seconds = float(poll_interval_seconds)
        self.sleep = sleep

    def _table(self, feature_group_name: str) -> tuple[str, str, Optional[str]]:
        description = self.sagemaker.describe_feature_group(
            FeatureGroupName=feature_group_name
        )
        config = (
            (description.get("OfflineStoreConfig") or {})
            .get("DataCatalogConfig")
            or {}
        )
        database = config.get("Database")
        table = config.get("TableName")
        catalog = config.get("Catalog")
        if not database or not table:
            raise PointInTimeError("Feature Group offline Glue table is not available")
        return str(database), str(table), str(catalog) if catalog else None

    def build_query(
        self,
        *,
        database: str,
        table: str,
        entity_id: str,
        feature_names: Sequence[str],
        as_of: datetime,
        limit: int,
    ) -> str:
        if isinstance(limit, bool) or not 1 <= int(limit) <= MAX_QUERY_RESULTS + 1:
            raise PointInTimeError("query limit is outside the reviewed bound")
        qualified = "%s.%s" % (
            _sql_identifier(database, "database"),
            _sql_identifier(table, "table"),
        )
        wanted = ", ".join(_sql_string(name) for name in feature_names)
        cutoff = format(_utc(as_of, "as_of").timestamp(), ".6f")
        return (
            "WITH eligible AS ("
            " SELECT asset_id, signal_name, signal_value, source_uri,"
            " event_time, available_time, taint,"
            " ROW_NUMBER() OVER ("
            "PARTITION BY asset_id, signal_name "
            "ORDER BY event_time DESC, available_time DESC, api_invocation_time DESC"
            ") AS pit_rank"
            " FROM %s"
            " WHERE asset_id = %s"
            " AND signal_name IN (%s)"
            " AND event_time <= %s"
            " AND available_time <= %s"
            " AND (taint IS NULL OR UPPER(taint) IN ('', '0', 'FALSE', 'CLEAN'))"
            " AND (is_deleted IS NULL OR is_deleted = FALSE)"
            ")"
            " SELECT asset_id AS entity_id, signal_name AS feature_name,"
            " event_time, available_time AS available_at,"
            " signal_value AS value, source_uri AS source, taint"
            " FROM eligible WHERE pit_rank = 1"
            " ORDER BY feature_name LIMIT %d"
        ) % (
            qualified,
            _sql_string(entity_id),
            wanted,
            cutoff,
            cutoff,
            int(limit),
        )

    def fetch_rows(
        self,
        *,
        feature_group_name: str,
        entity_id: str,
        feature_names: Sequence[str],
        as_of: datetime,
        limit: int,
    ) -> Iterable[Mapping[str, Any]]:
        database, table, catalog = self._table(feature_group_name)
        query = self.build_query(
            database=database,
            table=table,
            entity_id=entity_id,
            feature_names=feature_names,
            as_of=as_of,
            limit=limit,
        )
        request: Dict[str, Any] = {
            "QueryString": query,
            "QueryExecutionContext": {"Database": database},
            "ResultConfiguration": {"OutputLocation": self.output_location},
            "WorkGroup": self.workgroup,
        }
        if catalog:
            request["QueryExecutionContext"]["Catalog"] = catalog
        started = self.athena.start_query_execution(**request)
        query_id = started.get("QueryExecutionId")
        if not query_id:
            raise PointInTimeError("Athena did not return a query execution ID")

        for _ in range(self.max_polls):
            execution = self.athena.get_query_execution(QueryExecutionId=query_id)
            state = (
                (execution.get("QueryExecution") or {})
                .get("Status", {})
                .get("State")
            )
            if state == "SUCCEEDED":
                break
            if state in self.TERMINAL_FAILURES:
                raise PointInTimeError("Athena Feature Store query did not succeed")
            self.sleep(self.poll_interval_seconds)
        else:
            try:
                self.athena.stop_query_execution(QueryExecutionId=query_id)
            except Exception:
                pass
            raise PointInTimeError("Athena Feature Store query timed out")

        output: List[Mapping[str, Any]] = []
        token: Optional[str] = None
        header_consumed = False
        while True:
            kwargs: Dict[str, Any] = {
                "QueryExecutionId": query_id,
                "MaxResults": min(1_000, limit + 1),
            }
            if token:
                kwargs["NextToken"] = token
            page = self.athena.get_query_results(**kwargs)
            result_set = page.get("ResultSet") or {}
            columns = [
                str(column.get("Name") or "")
                for column in ((result_set.get("ResultSetMetadata") or {}).get("ColumnInfo") or [])
            ]
            if not columns or any(not column for column in columns):
                raise PointInTimeError("Athena Feature Store result metadata is missing")
            for raw_row in result_set.get("Rows") or []:
                values = [
                    item.get("VarCharValue") if isinstance(item, Mapping) else None
                    for item in (raw_row.get("Data") or [])
                ]
                if not header_consumed and values == columns:
                    header_consumed = True
                    continue
                header_consumed = True
                output.append({
                    column: values[index] if index < len(values) else None
                    for index, column in enumerate(columns)
                })
                if len(output) >= limit:
                    return output
            token = page.get("NextToken")
            if not token:
                return output
