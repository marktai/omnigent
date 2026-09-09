"""Tests for splitting an imported session's items into transport-sized batches."""

from __future__ import annotations

from omnigent.session_import.chunking import iter_item_chunks, total_item_bytes


def test_small_session_stays_one_batch() -> None:
    """Items under the budget ride together in a single batch."""
    items = [{"i": i} for i in range(5)]
    batches = list(iter_item_chunks(items, max_bytes=10_000))
    assert batches == [items]


def test_oversized_session_splits_and_preserves_order() -> None:
    """A session over budget splits into contiguous, order-preserving batches."""
    items = [{"i": i, "blob": "x" * 1000} for i in range(50)]
    batches = list(iter_item_chunks(items, max_bytes=5000))
    assert len(batches) > 1
    # Every item lands exactly once, in the original order.
    assert [item for batch in batches for item in batch] == items
    # No middle batch exceeds the budget (the last may be short, not over).
    for batch in batches:
        assert total_item_bytes(batch) <= 5000 or len(batch) == 1


def test_single_item_larger_than_budget_rides_alone() -> None:
    """One item bigger than the whole budget still ships, alone in its batch."""
    items = [{"small": 1}, {"huge": "x" * 20_000}, {"small": 2}]
    batches = list(iter_item_chunks(items, max_bytes=1000))
    # The oversized item is isolated so it neither drops nor drags neighbors over.
    assert [{"huge": "x" * 20_000}] in batches
    assert [item for batch in batches for item in batch] == items


def test_empty_session_yields_nothing() -> None:
    """No items means no batches (an empty import is rejected upstream, not here)."""
    assert list(iter_item_chunks([])) == []
