"""Leakage-aware walk-forward and combinatorial purged CV split generation."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import combinations
from typing import Any, Iterable, List, Sequence, Tuple


class SplitError(ValueError):
    pass


@dataclass(frozen=True)
class SampleInterval:
    start: Any
    end: Any

    def __post_init__(self) -> None:
        if self.end < self.start:
            raise SplitError("sample interval end must not precede start")


@dataclass(frozen=True)
class Split:
    train_indices: Tuple[int, ...]
    test_indices: Tuple[int, ...]
    test_groups: Tuple[int, ...]


def _zero_for(value: Any) -> Any:
    return timedelta(0) if isinstance(value, datetime) else 0


def _check_gap(gap: Any, sample: Any, name: str) -> Any:
    expected_timedelta = isinstance(sample, datetime)
    if expected_timedelta:
        if not isinstance(gap, timedelta) or gap < timedelta(0):
            raise SplitError("%s must be a non-negative timedelta" % name)
    elif isinstance(gap, bool) or not isinstance(gap, (int, float)) or gap < 0:
        raise SplitError("%s must be a non-negative number" % name)
    return gap


def _validate_intervals(intervals: Sequence[SampleInterval]) -> Tuple[SampleInterval, ...]:
    rows = tuple(intervals)
    if not rows or any(not isinstance(row, SampleInterval) for row in rows):
        raise SplitError("at least one SampleInterval is required")
    for left, right in zip(rows, rows[1:]):
        if right.start < left.start:
            raise SplitError("sample intervals must be sorted by start")
        if type(right.start) is not type(rows[0].start) or type(right.end) is not type(rows[0].end):
            raise SplitError("sample interval coordinate types must match")
    return rows


def _contiguous_blocks(indices: Sequence[int]) -> List[Tuple[int, ...]]:
    blocks: List[List[int]] = []
    for index in sorted(indices):
        if not blocks or index != blocks[-1][-1] + 1:
            blocks.append([index])
        else:
            blocks[-1].append(index)
    return [tuple(block) for block in blocks]


def _eligible_train(
    intervals: Sequence[SampleInterval],
    candidate_indices: Iterable[int],
    test_indices: Sequence[int],
    purge: Any,
    embargo: Any,
) -> Tuple[int, ...]:
    eligible = []
    blocks = _contiguous_blocks(test_indices)
    test_ranges = [
        (min(intervals[i].start for i in block), max(intervals[i].end for i in block))
        for block in blocks
    ]
    for index in candidate_indices:
        row = intervals[index]
        rejected = False
        for test_start, test_end in test_ranges:
            # Closed label intervals touching a test boundary are overlapping.
            if row.end >= test_start - purge and row.start <= test_end:
                rejected = True
                break
            # Embargo removes observations immediately following a test block.
            if row.start > test_end and row.start <= test_end + embargo:
                rejected = True
                break
        if not rejected:
            eligible.append(index)
    return tuple(eligible)


def purged_walk_forward_splits(
    intervals: Sequence[SampleInterval],
    *,
    n_splits: int,
    test_size: int,
    min_train_size: int,
    purge: Any = None,
    embargo: Any = None,
) -> Tuple[Split, ...]:
    """Generate expanding-window tests with purged labels and an entry embargo.

    ``embargo`` creates a gap immediately before each test in walk-forward
    mode.  This is equivalent to excluding the most recent training starts;
    post-test embargo has no effect because walk-forward never trains on future
    samples.
    """
    rows = _validate_intervals(intervals)
    if any(isinstance(value, bool) or not isinstance(value, int) or value <= 0
           for value in (n_splits, test_size, min_train_size)):
        raise SplitError("n_splits, test_size and min_train_size must be positive integers")
    zero = _zero_for(rows[0].start)
    purge = _check_gap(zero if purge is None else purge, rows[0].start, "purge")
    embargo = _check_gap(zero if embargo is None else embargo, rows[0].start, "embargo")
    needed = min_train_size + n_splits * test_size
    if needed > len(rows):
        raise SplitError("not enough samples for requested walk-forward splits")

    splits = []
    first_test = len(rows) - n_splits * test_size
    for split_number in range(n_splits):
        test_start_index = first_test + split_number * test_size
        test_indices = tuple(range(test_start_index, test_start_index + test_size))
        test_start = rows[test_start_index].start
        candidates = [
            index for index in range(test_start_index)
            if rows[index].start < test_start - embargo
        ]
        train_indices = _eligible_train(rows, candidates, test_indices, purge, zero)
        if len(train_indices) < min_train_size:
            raise SplitError("purge/embargo leaves fewer than min_train_size observations")
        splits.append(Split(train_indices, test_indices, (split_number,)))
    return tuple(splits)


def _groups(n_samples: int, n_groups: int) -> Tuple[Tuple[int, ...], ...]:
    base, remainder = divmod(n_samples, n_groups)
    groups = []
    cursor = 0
    for group in range(n_groups):
        size = base + (1 if group < remainder else 0)
        groups.append(tuple(range(cursor, cursor + size)))
        cursor += size
    return tuple(groups)


def combinatorial_purged_cv_splits(
    intervals: Sequence[SampleInterval],
    *,
    n_groups: int,
    test_groups: int,
    purge: Any = None,
    embargo: Any = None,
) -> Tuple[Split, ...]:
    """Generate every n-groups-choose-test-groups purged/embargoed split."""
    rows = _validate_intervals(intervals)
    if isinstance(n_groups, bool) or not isinstance(n_groups, int) or n_groups < 2:
        raise SplitError("n_groups must be an integer of at least two")
    if isinstance(test_groups, bool) or not isinstance(test_groups, int) or not 1 <= test_groups < n_groups:
        raise SplitError("test_groups must be between one and n_groups - 1")
    if n_groups > len(rows):
        raise SplitError("n_groups must not exceed sample count")
    zero = _zero_for(rows[0].start)
    purge = _check_gap(zero if purge is None else purge, rows[0].start, "purge")
    embargo = _check_gap(zero if embargo is None else embargo, rows[0].start, "embargo")
    groups = _groups(len(rows), n_groups)
    result = []
    all_indices = set(range(len(rows)))
    for selected in combinations(range(n_groups), test_groups):
        test_indices = tuple(sorted(index for group in selected for index in groups[group]))
        candidates = sorted(all_indices - set(test_indices))
        train_indices = _eligible_train(rows, candidates, test_indices, purge, embargo)
        if not train_indices:
            raise SplitError("purge/embargo produced an empty training set")
        result.append(Split(train_indices, test_indices, tuple(selected)))
    return tuple(result)
