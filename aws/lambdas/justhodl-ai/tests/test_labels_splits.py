from __future__ import annotations

import math
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "source"
sys.path.insert(0, str(SRC))

from outcome_labels import (  # noqa: E402
    Horizon,
    OutcomeLabelError,
    PricePoint,
    label_price_path,
    labels_for_horizons,
)
from validation_splits import (  # noqa: E402
    SampleInterval,
    SplitError,
    combinatorial_purged_cv_splits,
    purged_walk_forward_splits,
)

UTC = timezone.utc
T0 = datetime(2026, 1, 1, tzinfo=UTC)


class OutcomeLabelTests(unittest.TestCase):
    def setUp(self):
        self.path = [
            PricePoint(T0, 100.0),
            PricePoint(T0 + timedelta(days=1), 101.0),
            PricePoint(T0 + timedelta(days=3), 104.0),
            PricePoint(T0 + timedelta(days=5), 90.0),
        ]

    def test_calendar_horizon_uses_first_price_at_or_after_target_and_cost(self):
        result = label_price_path(
            self.path, T0, Horizon(2), transaction_cost_bps=25
        )
        self.assertTrue(result.matured)
        self.assertEqual(result.exit_time, T0 + timedelta(days=3))
        self.assertAlmostEqual(result.gross_return, 0.04)
        self.assertAlmostEqual(result.net_return, 0.0375)
        self.assertEqual(result.label, "POSITIVE")

    def test_observation_horizon_and_short_direction(self):
        result = label_price_path(
            self.path, T0, Horizon(3, "observations"), side="SHORT",
            transaction_cost_bps=10,
        )
        self.assertEqual(result.exit_price, 90.0)
        self.assertAlmostEqual(result.net_return, 0.099)
        self.assertEqual(result.label, "POSITIVE")

    def test_flat_threshold_and_unmatured_horizon(self):
        flat = label_price_path(
            self.path, T0, Horizon(1, "observations"), flat_threshold_bps=150
        )
        self.assertEqual(flat.label, "FLAT")
        pending = label_price_path(self.path, T0, Horizon(10))
        self.assertFalse(pending.matured)
        self.assertEqual(pending.reason, "horizon has not matured")

    def test_calendar_horizon_anchor_is_explicit(self):
        prediction_time = T0 + timedelta(hours=12)
        path = [
            PricePoint(T0 + timedelta(days=1), 101.0),
            PricePoint(T0 + timedelta(days=2, hours=18), 103.0),
            PricePoint(T0 + timedelta(days=3), 104.0),
        ]
        entry_anchored = label_price_path(
            path, prediction_time, Horizon(2, anchor="entry")
        )
        prediction_anchored = label_price_path(
            path, prediction_time, Horizon(2, anchor="prediction")
        )
        self.assertEqual(entry_anchored.exit_time, T0 + timedelta(days=3))
        self.assertEqual(prediction_anchored.exit_time, T0 + timedelta(days=2, hours=18))
        with self.assertRaisesRegex(OutcomeLabelError, "anchored to entry"):
            Horizon(2, "observations", anchor="prediction")

    def test_multiple_horizons_reuse_path_and_invalid_prices_fail(self):
        labels = labels_for_horizons(
            iter(self.path), T0, [Horizon(1, "observations"), Horizon(2)]
        )
        self.assertEqual(len(labels), 2)
        with self.assertRaises(OutcomeLabelError):
            PricePoint(T0, math.inf)
        with self.assertRaisesRegex(OutcomeLabelError, "duplicate"):
            label_price_path(self.path + [PricePoint(T0, 99)], T0, Horizon(1))


class SplitTests(unittest.TestCase):
    def test_walk_forward_is_expanding_and_embargoed(self):
        rows = [SampleInterval(index, index) for index in range(12)]
        splits = purged_walk_forward_splits(
            rows, n_splits=2, test_size=2, min_train_size=3, embargo=1
        )
        self.assertEqual(splits[0].test_indices, (8, 9))
        self.assertEqual(splits[0].train_indices, tuple(range(7)))
        self.assertEqual(splits[1].test_indices, (10, 11))
        self.assertEqual(splits[1].train_indices, tuple(range(9)))

    def test_walk_forward_purges_overlapping_label_intervals(self):
        rows = [SampleInterval(index, index + 2) for index in range(12)]
        splits = purged_walk_forward_splits(
            rows, n_splits=2, test_size=2, min_train_size=3
        )
        self.assertEqual(splits[0].train_indices, tuple(range(6)))
        self.assertEqual(splits[1].train_indices, tuple(range(8)))
        for split in splits:
            self.assertTrue(set(split.train_indices).isdisjoint(split.test_indices))

    def test_combinatorial_count_partition_and_post_test_embargo(self):
        rows = [SampleInterval(index, index) for index in range(12)]
        splits = combinatorial_purged_cv_splits(
            rows, n_groups=4, test_groups=2, embargo=1
        )
        self.assertEqual(len(splits), math.comb(4, 2))
        chosen = next(split for split in splits if split.test_groups == (0, 2))
        self.assertEqual(chosen.test_indices, (0, 1, 2, 6, 7, 8))
        self.assertEqual(chosen.train_indices, (4, 5, 10, 11))
        self.assertTrue(set(chosen.train_indices).isdisjoint(chosen.test_indices))

    def test_split_contract_rejects_unsorted_and_impossible_requests(self):
        with self.assertRaisesRegex(SplitError, "sorted"):
            purged_walk_forward_splits(
                [SampleInterval(2, 2), SampleInterval(1, 1)],
                n_splits=1, test_size=1, min_train_size=1,
            )
        with self.assertRaisesRegex(SplitError, "not enough"):
            purged_walk_forward_splits(
                [SampleInterval(i, i) for i in range(3)],
                n_splits=2, test_size=1, min_train_size=2,
            )


if __name__ == "__main__":
    unittest.main()
