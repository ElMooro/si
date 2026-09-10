from __future__ import annotations

import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "source"
sys.path.insert(0, str(SRC))

from point_in_time import PointInTimeError, PointInTimeFeatureAssembler  # noqa: E402
from sagemaker_feature_store import (  # noqa: E402
    AthenaFeatureStoreQuery,
    SageMakerFeatureStore,
)


UTC = timezone.utc
T0 = datetime(2026, 9, 1, 14, 0, tzinfo=UTC)


class FakeQuery:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def fetch_rows(self, **request):
        self.calls.append(request)
        return list(self.rows)


class SageMakerFeatureStoreAdapterTests(unittest.TestCase):
    def test_adapter_and_assembler_choose_only_eligible_entity_rows(self):
        query = FakeQuery([
            {
                "entity_id": "BTC",
                "feature_name": "momentum",
                "event_time": T0.timestamp(),
                "available_at": T0.timestamp(),
                "value": "0.4",
                "source": "s3://features/old",
            },
            {
                "entity_id": "BTC",
                "feature_name": "momentum",
                "event_time": (T0 + timedelta(minutes=1)).isoformat(),
                "available_at": (T0 + timedelta(minutes=2)).isoformat(),
                "value": "0.7",
                "source": "s3://features/new",
            },
            {
                "entity_id": "BTC",
                "feature_name": "momentum",
                "event_time": T0.timestamp(),
                "available_at": T0.timestamp(),
                "value": "99",
                "taint": "TAINTED",
            },
        ])
        store = SageMakerFeatureStore(query, "justhodl-ai-signal-prod")
        vector = PointInTimeFeatureAssembler(store).assemble(
            "BTC", T0 + timedelta(minutes=3), ["momentum"], require_all=True
        )
        self.assertEqual(vector.values, {"momentum": 0.7})
        self.assertEqual(query.calls[0]["entity_id"], "BTC")
        self.assertEqual(query.calls[0]["limit"], 1_001)

    def test_future_entity_and_unrequested_rows_fail_closed(self):
        cases = [
            {
                "entity_id": "ETH",
                "feature_name": "momentum",
                "event_time": T0,
                "available_at": T0,
                "value": 1,
            },
            {
                "entity_id": "BTC",
                "feature_name": "future",
                "event_time": T0,
                "available_at": T0,
                "value": 1,
            },
            {
                "entity_id": "BTC",
                "feature_name": "momentum",
                "event_time": T0 + timedelta(seconds=1),
                "available_at": T0 + timedelta(seconds=1),
                "value": 1,
            },
            {
                "entity_id": "BTC",
                "feature_name": "momentum",
                "event_time": T0,
                "available_at": T0 + timedelta(seconds=1),
                "value": 1,
            },
        ]
        messages = ("entity scoping", "unrequested", "future", "future")
        for row, message in zip(cases, messages):
            with self.subTest(message=message):
                store = SageMakerFeatureStore(FakeQuery([row]), "justhodl-ai-signal-prod")
                with self.assertRaisesRegex(PointInTimeError, message):
                    list(store.observations("BTC", ["momentum"], T0))

    def test_taint_is_excluded_even_before_assembler_policy(self):
        store = SageMakerFeatureStore(FakeQuery([{
            "entity_id": "BTC",
            "feature_name": "momentum",
            "event_time": T0,
            "available_at": T0,
            "value": 1,
            "taint": "MODEL_GENERATED",
        }]), "justhodl-ai-signal-prod")
        self.assertEqual(list(store.observations("BTC", ["momentum"], T0)), [])
        with self.assertRaisesRegex(PointInTimeError, "missing"):
            PointInTimeFeatureAssembler(store).assemble(
                "BTC", T0, ["momentum"], allow_tainted=True
            )

    def test_inputs_and_results_are_bounded(self):
        query = FakeQuery([])
        store = SageMakerFeatureStore(
            query, "justhodl-ai-signal-prod", max_feature_names=2, max_results=2
        )
        with self.assertRaisesRegex(PointInTimeError, "2-item"):
            list(store.observations("BTC", ["a", "b", "c"], T0))
        self.assertEqual(query.calls, [])
        repeated = {
            "entity_id": "BTC",
            "feature_name": "a",
            "event_time": T0,
            "available_at": T0,
            "value": 1,
        }
        store = SageMakerFeatureStore(
            FakeQuery([repeated, repeated, repeated]),
            "justhodl-ai-signal-prod",
            max_results=2,
        )
        with self.assertRaisesRegex(PointInTimeError, "2-row"):
            list(store.observations("BTC", ["a"], T0))


class FakeSageMaker:
    def describe_feature_group(self, FeatureGroupName):
        self.name = FeatureGroupName
        return {
            "OfflineStoreConfig": {
                "DataCatalogConfig": {
                    "Catalog": "AwsDataCatalog",
                    "Database": "sagemaker_featurestore",
                    "TableName": "justhodl_ai_signal_prod_123",
                }
            }
        }


class FakeAthena:
    def __init__(self):
        self.start = None

    def start_query_execution(self, **request):
        self.start = request
        return {"QueryExecutionId": "query-1"}

    def get_query_execution(self, QueryExecutionId):
        return {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}

    def get_query_results(self, **kwargs):
        columns = [
            "entity_id", "feature_name", "event_time", "available_at",
            "value", "source", "taint",
        ]
        return {
            "ResultSet": {
                "ResultSetMetadata": {
                    "ColumnInfo": [{"Name": name} for name in columns]
                },
                "Rows": [
                    {"Data": [{"VarCharValue": name} for name in columns]},
                    {"Data": [{"VarCharValue": value} for value in (
                        "BTC", "momentum", str(T0.timestamp()), str(T0.timestamp()),
                        "0.5", "s3://features/1", "CLEAN",
                    )]},
                ],
            }
        }


class AthenaFeatureStoreQueryTests(unittest.TestCase):
    def test_query_is_scoped_cutoff_taint_filtered_and_bounded(self):
        sm = FakeSageMaker()
        athena = FakeAthena()
        query = AthenaFeatureStoreQuery(
            sm,
            athena,
            output_location="s3://private/ai/feature-store-query-results/",
            sleep=lambda _: None,
        )
        rows = list(query.fetch_rows(
            feature_group_name="justhodl-ai-signal-prod",
            entity_id="BTC'quoted",
            feature_names=["momentum"],
            as_of=T0,
            limit=2,
        ))
        sql = athena.start["QueryString"]
        self.assertIn("asset_id = 'BTC''quoted'", sql)
        self.assertEqual(sql.count("<= 1788271200.000000"), 2)
        self.assertIn("UPPER(taint)", sql)
        self.assertIn("PARTITION BY asset_id, signal_name", sql)
        self.assertIn("LIMIT 2", sql)
        self.assertEqual(rows[0]["feature_name"], "momentum")


if __name__ == "__main__":
    unittest.main()
