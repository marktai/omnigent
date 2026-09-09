"""Tests for importing normalized local harness sessions."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import httpx
import pytest

from omnigent.db.utils import builtin_agent_id
from omnigent.errors import ErrorCode, OmnigentError
from omnigent.server.routes.imports import (
    ImportedSessionRef,
    LocalImportRequest,
    _consume_local_import_stream,
    _stream_local_sessions_from_host,
)
from omnigent.stores.agent_store.sqlalchemy_store import SqlAlchemyAgentStore
from omnigent.stores.conversation_store.sqlalchemy_store import SqlAlchemyConversationStore


def _seed_claude_agent(db_uri: str) -> str:
    """Seed the built-in agent because focused app tests skip lifespan startup."""
    agent_id = builtin_agent_id("claude-native-ui")
    SqlAlchemyAgentStore(db_uri).create(
        agent_id,
        name="claude-native-ui",
        bundle_location="builtin://claude-native-ui",
    )
    return agent_id


async def test_import_session_creates_normal_session_and_blocks_duplicate(
    client: httpx.AsyncClient,
    db_uri: str,
) -> None:
    """An import creates one native session and a retry is rejected."""
    agent_id = _seed_claude_agent(db_uri)
    payload = {
        "source": "claude",
        "external_session_id": "claude-session-1",
        "workspace": "/repo",
        "items": [
            {
                "type": "message",
                "response_id": "claude:turn-1",
                "data": {
                    "role": "user",
                    "content": [{"type": "input_text", "text": "inspect TODO.md"}],
                },
            },
            {
                "type": "message",
                "response_id": "claude:turn-1",
                "data": {
                    "role": "assistant",
                    "agent": "claude-native-ui",
                    "content": [{"type": "output_text", "text": "Done."}],
                },
            },
        ],
    }

    created = await client.post("/v1/imports", json=payload)
    repeated = await client.post("/v1/imports", json=payload)

    assert created.status_code == 201
    assert created.json()["status"] == "imported"
    assert repeated.status_code == 409
    assert created.json()["session_id"] in repeated.text
    assert "already been imported" in repeated.text

    session_id = created.json()["session_id"]
    conversation = SqlAlchemyConversationStore(db_uri).get_conversation(session_id)
    assert conversation is not None
    assert conversation.agent_id == agent_id
    assert conversation.external_session_id == "claude-session-1"
    assert conversation.workspace == "/repo"
    assert conversation.title == "inspect TODO.md"
    assert conversation.labels["omnigent.wrapper"] == "claude-code-native-ui"
    items = await client.get(f"/v1/sessions/{session_id}/items")
    assert items.status_code == 200
    assert [item["type"] for item in items.json()["data"]] == ["message", "message"]


async def test_import_session_uses_native_title_when_supplied(
    client: httpx.AsyncClient,
    db_uri: str,
) -> None:
    """A supplied harness title becomes the conversation title over the first message."""
    _seed_claude_agent(db_uri)
    payload = {
        "source": "claude",
        "external_session_id": "claude-titled-1",
        "title": "My renamed thread",
        "items": [
            {
                "type": "message",
                "response_id": "claude:turn-1",
                "data": {
                    "role": "user",
                    "content": [{"type": "input_text", "text": "inspect TODO.md"}],
                },
            }
        ],
    }

    created = await client.post("/v1/imports", json=payload)

    assert created.status_code == 201
    conversation = SqlAlchemyConversationStore(db_uri).get_conversation(
        created.json()["session_id"]
    )
    assert conversation is not None
    assert conversation.title == "My renamed thread"


async def test_concurrent_identical_imports_return_one_session(
    client: httpx.AsyncClient,
    db_uri: str,
) -> None:
    """Concurrent retries serialize on source identity and one is rejected."""
    _seed_claude_agent(db_uri)
    payload = {
        "source": "claude",
        "external_session_id": "claude-concurrent-1",
        "items": [
            {
                "type": "message",
                "response_id": "claude:turn-1",
                "data": {
                    "role": "user",
                    "content": [{"type": "input_text", "text": "hello"}],
                },
            }
        ],
    }

    first, second = await asyncio.gather(
        client.post("/v1/imports", json=payload),
        client.post("/v1/imports", json=payload),
    )

    assert {first.status_code, second.status_code} == {201, 409}
    imported = SqlAlchemyConversationStore(db_uri).find_imported_conversation(
        "claude", "claude-concurrent-1"
    )
    assert imported is not None


async def test_force_import_replaces_existing_session(
    client: httpx.AsyncClient,
    db_uri: str,
) -> None:
    """A forced retry replaces the transcript while retaining its stable id."""
    _seed_claude_agent(db_uri)
    payload = {
        "source": "claude",
        "external_session_id": "claude-force-1",
        "workspace": "/repo/old",
        "items": [
            {
                "type": "message",
                "response_id": "claude:old",
                "data": {
                    "role": "user",
                    "content": [{"type": "input_text", "text": "old prompt"}],
                },
            }
        ],
    }
    created = await client.post("/v1/imports", json=payload)
    payload["force"] = True
    payload["workspace"] = "/repo/new"
    payload["items"] = [
        {
            "type": "message",
            "response_id": "claude:new",
            "data": {
                "role": "user",
                "content": [{"type": "input_text", "text": "new prompt"}],
            },
        }
    ]

    replaced = await client.post("/v1/imports", json=payload)

    assert created.status_code == 201
    assert replaced.status_code == 201
    assert replaced.json()["session_id"] == created.json()["session_id"]
    conversation = SqlAlchemyConversationStore(db_uri).get_conversation(
        replaced.json()["session_id"]
    )
    assert conversation is not None
    assert conversation.workspace == "/repo/new"
    assert conversation.title == "new prompt"
    items = await client.get(f"/v1/sessions/{conversation.id}/items")
    assert items.status_code == 200
    assert [item["content"][0]["text"] for item in items.json()["data"]] == ["new prompt"]


def _msg(text: str, response_id: str = "claude:turn") -> dict[str, object]:
    """One valid normalized user message item for import payloads."""
    return {
        "type": "message",
        "response_id": response_id,
        "data": {"role": "user", "content": [{"type": "input_text", "text": text}]},
    }


async def test_chunked_import_creates_then_appends_across_requests(
    client: httpx.AsyncClient,
    db_uri: str,
) -> None:
    """A large session posts as create + append chunks into one conversation."""
    _seed_claude_agent(db_uri)
    base = {"source": "claude", "external_session_id": "claude-chunked-1"}

    first = await client.post(
        "/v1/imports",
        json={**base, "title": "Chunked", "items": [_msg("one", "claude:1")], "final": False},
    )
    assert first.status_code == 201
    session_id = first.json()["session_id"]

    second = await client.post(
        "/v1/imports",
        json={**base, "chunk_index": 1, "items": [_msg("two", "claude:2")], "final": True},
    )
    assert second.status_code == 200
    assert second.json()["session_id"] == session_id

    # Both chunks' items land in the one conversation, in order.
    items = await client.get(f"/v1/sessions/{session_id}/items")
    assert items.status_code == 200
    assert [i["content"][0]["text"] for i in items.json()["data"]] == ["one", "two"]


async def test_chunked_import_append_without_create_is_rejected(
    client: httpx.AsyncClient,
    db_uri: str,
) -> None:
    """An append chunk with no created session fails rather than orphaning items."""
    _seed_claude_agent(db_uri)
    resp = await client.post(
        "/v1/imports",
        json={
            "source": "claude",
            "external_session_id": "claude-orphan-1",
            "chunk_index": 1,
            "items": [_msg("stray")],
            "final": True,
        },
    )
    assert resp.status_code == 409
    assert "in-progress import" in resp.text


async def test_import_session_rejects_empty_history(client: httpx.AsyncClient) -> None:
    """An empty parser result cannot create a permanently claimed session."""
    response = await client.post(
        "/v1/imports",
        json={
            "source": "codex",
            "external_session_id": "empty-codex-session",
            "items": [],
        },
    )

    assert response.status_code == 422


def test_imported_session_ref_allows_null_title() -> None:
    """A batch session with no synthesizable title must not fail the response.

    ``title_from_items`` returns None when there is no first user message to
    derive a title from; the /imports/local batch builds one ImportedSessionRef
    per new session, so a None title must validate instead of 500-ing the run.
    """
    from omnigent.server.routes.imports import ImportedSessionRef

    assert ImportedSessionRef(session_id="conv_x").title is None
    assert ImportedSessionRef(session_id="conv_y", title=None).title is None


def test_exact_local_import_requires_one_harness_and_trims_id() -> None:
    """An exact id is normalized and cannot be paired with the all selector."""
    request = LocalImportRequest(host_id="h1", source="claude", session_id="  exact-id  ")
    assert request.session_id == "exact-id"

    with pytest.raises(ValueError, match="requires a specific harness"):
        LocalImportRequest(host_id="h1", source="all", session_id="exact-id")


async def test_stream_local_sessions_yields_each_then_stops_on_done() -> None:
    """The streaming consumer yields one session per frame, then cleans up on done.

    Fakes the tunnel by having ``send_text`` push session frames + a terminal
    ``done`` onto the per-request queue the generator just registered.
    """
    conn = SimpleNamespace(host_id="h1", pending_import_local={})
    canned = [
        {
            "external_session_id": "c1",
            "workspace": None,
            "items": [],
            "title": "one",
            "source": "claude",
            "total": 2,
        },
        {
            "external_session_id": "c2",
            "workspace": None,
            "items": [],
            "title": None,
            "source": "codex",
            "total": 2,
        },
    ]

    class _Reg:
        def send_text(self, host_conn: object, frame: str) -> None:
            (queue,) = conn.pending_import_local.values()
            for session in canned:
                queue.put_nowait(("session", session))
            queue.put_nowait(("done", {"status": "ok", "error": None}))

    got = [
        session
        async for session in _stream_local_sessions_from_host(
            host_registry=_Reg(),  # type: ignore[arg-type]
            host_conn=conn,  # type: ignore[arg-type]
            source="all",
            limit=5,
        )
    ]

    assert [s["external_session_id"] for s in got] == ["c1", "c2"]
    # The per-request queue is removed once the stream ends.
    assert conn.pending_import_local == {}


async def test_stream_local_sessions_sends_exact_session_id() -> None:
    """The server carries an exact id through the host tunnel request."""
    from omnigent.host.frames import HostImportLocalByIdFrame, decode_host_frame

    conn = SimpleNamespace(host_id="h1", pending_import_local={})
    sent: list[HostImportLocalByIdFrame] = []

    class _Reg:
        def send_text(self, host_conn: object, frame: str) -> None:
            decoded = decode_host_frame(frame)
            assert isinstance(decoded, HostImportLocalByIdFrame)
            sent.append(decoded)
            (queue,) = conn.pending_import_local.values()
            queue.put_nowait(("done", {"status": "ok", "error": None}))

    got = [
        session
        async for session in _stream_local_sessions_from_host(
            host_registry=_Reg(),  # type: ignore[arg-type]
            host_conn=conn,  # type: ignore[arg-type]
            source="codex",
            limit=10,
            session_id="session-exact",
        )
    ]

    assert got == []
    assert sent[0].source == "codex"
    assert sent[0].session_id == "session-exact"


async def test_stream_local_sessions_surfaces_host_failed_count() -> None:
    """The done frame's host-side unreadable count is exposed via ``stats``.

    Sessions the host enumerated but could not read send no session frame, only
    a count on the done frame; the consumer must surface it so the route folds
    it into ``failed`` instead of the batch silently under-reporting.
    """
    conn = SimpleNamespace(host_id="h1", pending_import_local={})

    class _Reg:
        def send_text(self, host_conn: object, frame: str) -> None:
            (queue,) = conn.pending_import_local.values()
            queue.put_nowait(("done", {"status": "ok", "error": None, "failed": 3}))

    stats: dict[str, int] = {}
    got = [
        session
        async for session in _stream_local_sessions_from_host(
            host_registry=_Reg(),  # type: ignore[arg-type]
            host_conn=conn,  # type: ignore[arg-type]
            source="all",
            limit=5,
            stats=stats,
        )
    ]

    assert got == []
    assert stats["host_failed"] == 3
    assert conn.pending_import_local == {}


async def test_stream_local_sessions_raises_on_failed_done() -> None:
    """A ``done`` frame with status='failed' surfaces the host's error, not a hang."""
    conn = SimpleNamespace(host_id="h1", pending_import_local={})

    class _Reg:
        def send_text(self, host_conn: object, frame: str) -> None:
            (queue,) = conn.pending_import_local.values()
            queue.put_nowait(("done", {"status": "failed", "error": "host blew up"}))

    with pytest.raises(OmnigentError, match="host blew up"):
        _ = [
            session
            async for session in _stream_local_sessions_from_host(
                host_registry=_Reg(),  # type: ignore[arg-type]
                host_conn=conn,  # type: ignore[arg-type]
                source="claude",
                limit=5,
            )
        ]
    assert conn.pending_import_local == {}


# ── _consume_local_import_stream (chunk reassembly + incremental append) ──


class _FakeConvStore:
    """Minimal conversation store recording what the chunk consumer persists."""

    def __init__(self, existing: set[tuple[str, str]] | None = None) -> None:
        self.existing = existing or set()
        self.appended: dict[str, list[object]] = {}
        self.deleted: list[str] = []

    def find_imported_conversation(self, source: str, external_id: str) -> object | None:
        if (source, external_id) in self.existing:
            return SimpleNamespace(id=f"conv-{external_id}")
        return None

    def append(self, conversation_id: str, items: list[object]) -> None:
        self.appended.setdefault(conversation_id, []).extend(items)

    async def delete_conversation(self, conversation_id: str) -> None:
        self.deleted.append(conversation_id)


def _chunk(
    external_id: str,
    texts: list[str],
    *,
    chunk_index: int,
    last_chunk: bool,
    source: str = "claude",
    title: str | None = None,
) -> dict[str, object]:
    """One streamed session-chunk dict, as host_tunnel enqueues it."""
    return {
        "total": 1,
        "chunk_index": chunk_index,
        "last_chunk": last_chunk,
        "external_session_id": external_id,
        "workspace": None,
        "title": title,
        "source": source,
        "items": [
            {
                "type": "message",
                "response_id": f"{source}:{external_id}:{i}",
                "data": {"role": "user", "content": [{"type": "input_text", "text": t}]},
            }
            for i, t in enumerate(texts)
        ],
    }


async def _astream(chunks: list[dict[str, object]]):  # type: ignore[no-untyped-def]
    for chunk in chunks:
        yield chunk


async def test_consume_stream_appends_multichunk_session_into_one_conversation() -> None:
    """A session split across chunks creates once and appends each later chunk."""
    store = _FakeConvStore()
    created: list[dict[str, object]] = []

    async def _persist(**kwargs: object) -> tuple[str, str | None]:
        created.append(kwargs)
        return "conv-big", "Big"

    result = await _consume_local_import_stream(
        _astream(
            [
                _chunk("big", ["a", "b"], chunk_index=0, last_chunk=False, title="Big"),
                _chunk("big", ["c"], chunk_index=1, last_chunk=False),
                _chunk("big", ["d"], chunk_index=2, last_chunk=True),
            ]
        ),
        conversation_store=store,  # type: ignore[arg-type]
        persist_import=_persist,
        user_id="u1",
        request_source="claude",
        stats={},
    )

    assert result.imported == 1 and result.failed == 0
    assert result.sessions == [ImportedSessionRef(session_id="conv-big", title="Big")]
    # Chunk 0 created with its two items; chunks 1 and 2 appended their items.
    assert len(created) == 1
    assert len(created[0]["items"]) == 2  # type: ignore[arg-type]
    assert len(store.appended["conv-big"]) == 2
    assert store.deleted == []


async def test_consume_stream_single_chunk_backward_compatible() -> None:
    """One frame (old host: chunk_index 0, last_chunk true) imports whole."""
    store = _FakeConvStore()

    async def _persist(**kwargs: object) -> tuple[str, str | None]:
        return "conv-s", "S"

    result = await _consume_local_import_stream(
        _astream([_chunk("s", ["only"], chunk_index=0, last_chunk=True)]),
        conversation_store=store,  # type: ignore[arg-type]
        persist_import=_persist,
        user_id="u1",
        request_source="claude",
        stats={},
    )
    assert result.imported == 1
    assert store.appended == {}  # a single chunk needs no append


async def test_consume_stream_skips_already_imported_and_drains_its_chunks() -> None:
    """An already-imported session is counted once; its later chunks are ignored."""
    store = _FakeConvStore(existing={("claude", "dup")})
    persisted = False

    async def _persist(**kwargs: object) -> tuple[str, str | None]:
        nonlocal persisted
        persisted = True
        return "conv-dup", None

    result = await _consume_local_import_stream(
        _astream(
            [
                _chunk("dup", ["a"], chunk_index=0, last_chunk=False),
                _chunk("dup", ["b"], chunk_index=1, last_chunk=True),
            ]
        ),
        conversation_store=store,  # type: ignore[arg-type]
        persist_import=_persist,
        user_id="u1",
        request_source="claude",
        stats={},
    )
    assert result.already_imported == 1 and result.imported == 0
    assert persisted is False and store.appended == {}


async def test_consume_stream_deletes_partial_when_a_later_chunk_fails() -> None:
    """A failed append chunk drops the partial conversation and tallies failed."""
    store = _FakeConvStore()

    async def _persist(**kwargs: object) -> tuple[str, str | None]:
        return "conv-bad", "Bad"

    def _boom(conversation_id: str, items: list[object]) -> None:
        raise ValueError("append blew up")

    store.append = _boom  # type: ignore[assignment,method-assign]

    result = await _consume_local_import_stream(
        _astream(
            [
                _chunk("bad", ["a"], chunk_index=0, last_chunk=False),
                _chunk("bad", ["b"], chunk_index=1, last_chunk=True),
            ]
        ),
        conversation_store=store,  # type: ignore[arg-type]
        persist_import=_persist,
        user_id="u1",
        request_source="claude",
        stats={},
    )
    assert result.imported == 0 and result.failed == 1
    assert store.deleted == ["conv-bad"]


async def test_consume_stream_drops_partial_when_stream_raises_midway() -> None:
    """A host drop mid-session deletes the partial before propagating the error."""
    store = _FakeConvStore()

    async def _persist(**kwargs: object) -> tuple[str, str | None]:
        return "conv-drop", "D"

    async def _raising_stream():  # type: ignore[no-untyped-def]
        yield _chunk("drop", ["a"], chunk_index=0, last_chunk=False)
        raise OmnigentError("host dropped", code=ErrorCode.CONFLICT)

    with pytest.raises(OmnigentError, match="host dropped"):
        await _consume_local_import_stream(
            _raising_stream(),
            conversation_store=store,  # type: ignore[arg-type]
            persist_import=_persist,
            user_id="u1",
            request_source="claude",
            stats={},
        )
    assert store.deleted == ["conv-drop"]
