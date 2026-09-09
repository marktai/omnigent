"""Split an imported session's items into transport-sized batches.

One import session can be arbitrarily large, but the host->server tunnel caps a
single websocket message and the CLI posts one HTTP body — so a session past a
size threshold rides as several item batches appended to the same conversation
instead of one payload. The host stream and the CLI share this one budget so a
batch built on either side stays under the same wall.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Sequence
from typing import TypeVar

# Sessions whose serialized items total at or below this import in one shot (one
# frame / one POST body), exactly as before chunking existed; only larger ones
# are split and appended incrementally. Kept well under the 100 MB tunnel frame
# cap (:data:`RUNNER_TUNNEL_MAX_MESSAGE_BYTES`) so an individual batch, plus its
# frame envelope, never approaches it.
IMPORT_CHUNK_MAX_BYTES = 2 * 1024 * 1024

_T = TypeVar("_T")


def _item_bytes(item: object) -> int:
    """Approximate one item's serialized size, as the chunker measures it."""
    return len(json.dumps(item, separators=(",", ":")).encode())


def total_item_bytes(items: Sequence[object]) -> int:
    """Serialized byte total of ``items`` under the chunker's own measure."""
    return sum(_item_bytes(item) for item in items)


def iter_item_chunks(
    items: Sequence[_T], *, max_bytes: int = IMPORT_CHUNK_MAX_BYTES
) -> Iterator[list[_T]]:
    """Yield consecutive item batches each serializing to about ``max_bytes``.

    A batch holds as many consecutive items as fit under the budget. A single
    item larger than the budget rides alone in its own batch — the transport,
    not this split, is the hard cap on absolute size. A non-empty input always
    yields at least one batch; an empty input yields nothing.
    """
    batch: list[_T] = []
    batch_bytes = 0
    for item in items:
        size = _item_bytes(item)
        if batch and batch_bytes + size > max_bytes:
            yield batch
            batch = []
            batch_bytes = 0
        batch.append(item)
        batch_bytes += size
    if batch:
        yield batch
