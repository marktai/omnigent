"""
Integration tests for git worktree creation on the dedicated per-session
bind endpoint ``POST /v1/hosts/{host_id}/runners`` (``launch_runner``).

This is the endpoint the fork-resume flow uses to bind an already-existing
(unbound) session to a host + directory. Unlike ``POST /v1/sessions`` —
which creates the worktree before the conversation row exists —
``launch_runner`` operates on a row that already exists, so it must create
the worktree at bind time and roll it back if the bind/launch fails.

Drives the endpoint through the full app and a fake host that auto-replies
to the host control frames (``host.stat`` for workspace validation,
``host.create_worktree``, ``host.launch_runner``, and
``host.remove_worktree`` for rollback). See designs/SESSION_GIT_WORKTREE.md.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

from omnigent.host.frames import (
    HostAcquireWorktreeFrame,
    HostConfigureWorktreePoolFrame,
    HostCreateWorktreeFrame,
    HostHelloFrame,
    HostLaunchRunnerFrame,
    HostRemoveWorktreeFrame,
    HostReleaseWorktreeFrame,
    HostStatFrame,
    decode_host_frame,
)
from omnigent.runtime.agent_cache import AgentCache
from omnigent.server.app import create_app
from omnigent.server.auth import RESERVED_USER_LOCAL
from omnigent.server.host_registry import HostConnection
from omnigent.stores.agent_store.sqlalchemy_store import SqlAlchemyAgentStore
from omnigent.stores.artifact_store.local import LocalArtifactStore
from omnigent.stores.comment_store.sqlalchemy_store import SqlAlchemyCommentStore
from omnigent.stores.conversation_store.sqlalchemy_store import (
    SqlAlchemyConversationStore,
)
from omnigent.stores.file_store.sqlalchemy_store import SqlAlchemyFileStore
from omnigent.stores.host_store import HostStore
from tests.server.helpers import create_test_agent

pytestmark = pytest.mark.asyncio

_HOST_ID = "51dc949aba31e24ca8f047d6fba31a0d"
_SOURCE_REPO = "/Users/alice/myrepo"


@pytest.fixture()
def app(runtime_init: None, db_uri: str, tmp_path: Path) -> FastAPI:
    """FastAPI app wired WITH ``host_store`` so ``launch_runner`` can
    resolve host ownership and launch a runner.

    Overrides the shared ``app`` fixture (which passes
    ``host_store=None`` and so can't run the dedicated launch endpoint).
    The shared ``client`` fixture depends on this ``app``.

    :param runtime_init: Initializes the runtime + mock LLM.
    :param db_uri: SQLite database URI.
    :param tmp_path: Pytest temp dir for artifacts and cache.
    :returns: A configured FastAPI app with host routes mounted.
    """
    artifact_store = LocalArtifactStore(str(tmp_path / "artifacts"))
    return create_app(
        agent_store=SqlAlchemyAgentStore(db_uri),
        file_store=SqlAlchemyFileStore(db_uri),
        conversation_store=SqlAlchemyConversationStore(db_uri),
        artifact_store=artifact_store,
        agent_cache=AgentCache(
            artifact_store=artifact_store,
            cache_dir=tmp_path / "cache",
        ),
        comment_store=SqlAlchemyCommentStore(db_uri),
        host_store=HostStore(db_uri),
    )


class _FakeWebSocket:
    """Minimal WebSocket stand-in (the registry only enqueues)."""

    async def send_text(self, data: str) -> None:
        """No-op send — frames flow through the outbound queue.

        :param data: JSON-encoded frame text (ignored).
        """


@dataclass
class _HostCapture:
    """
    Frames a fake host received during one ``launch_runner`` call.

    :param create: ``host.create_worktree`` frames received.
    :param launch: ``host.launch_runner`` frames received.
    :param remove: ``host.remove_worktree`` frames received (a non-empty
        list proves the rollback path fired).
    """

    create: list[HostCreateWorktreeFrame] = field(default_factory=list)
    launch: list[HostLaunchRunnerFrame] = field(default_factory=list)
    remove: list[HostRemoveWorktreeFrame] = field(default_factory=list)
    configure_pool: list[HostConfigureWorktreePoolFrame] = field(default_factory=list)
    acquire_pool: list[HostAcquireWorktreeFrame] = field(default_factory=list)
    release_pool: list[HostReleaseWorktreeFrame] = field(default_factory=list)


# register(*, create_status=, create_error=, launch_status=) -> _HostCapture
RegisterHost = Callable[..., _HostCapture]


@pytest_asyncio.fixture()
async def register_host(
    app: FastAPI,
    db_uri: str,
) -> AsyncIterator[RegisterHost]:
    """Yield a factory that registers a fake host with a replying drain.

    The drain answers ``host.stat`` (workspace validation passes),
    ``host.create_worktree``, ``host.launch_runner``, and
    ``host.remove_worktree`` — capturing each into a :class:`_HostCapture`.
    Every drain is poisoned and awaited at teardown so no background task
    leaks into the next test's event loop.

    :param app: App whose ``host_registry`` to register into.
    :param db_uri: DB URI so the ``host_id`` FK target row exists.
    :returns: Async iterator yielding a ``register`` factory. Kwargs:
        ``create_status`` (``"ok"``/``"failed"``), ``create_error``
        (host failure detail), ``launch_status``
        (``"launched"``/``"failed"``). Returns the :class:`_HostCapture`
        accumulating frames the host received.
    """
    conns: list[HostConnection] = []

    def _register(
        *,
        create_status: str = "ok",
        create_error: str | None = None,
        acquire_status: str = "ok",
        acquire_error: str | None = None,
        release_status: str = "ok",
        release_error: str | None = None,
        launch_status: str = "launched",
    ) -> _HostCapture:
        HostStore(db_uri).upsert_on_connect(_HOST_ID, "wt-host", RESERVED_USER_LOCAL)
        conn = app.state.host_registry.register(
            host_id=_HOST_ID,
            ws=_FakeWebSocket(),  # type: ignore[arg-type] — duck-typed
            hello=HostHelloFrame(version="0.1.0-test", frame_protocol_version=1, name="wt-host"),
            owner=RESERVED_USER_LOCAL,
        )
        cap = _HostCapture()

        async def _drain() -> None:
            """Answer stat/create/launch/remove frames; capture them."""
            while True:
                frame_text = await conn.outbound_queue.get()
                if frame_text is None:
                    return
                frame = decode_host_frame(frame_text)
                if isinstance(frame, HostStatFrame):
                    fut = conn.pending_stats.pop(frame.request_id, None)
                    if fut is not None and not fut.done():
                        fut.set_result(
                            {
                                "status": "ok",
                                "exists": True,
                                "type": "directory",
                                "canonical_path": frame.path,
                                "error": None,
                            }
                        )
                elif isinstance(frame, HostCreateWorktreeFrame):
                    cap.create.append(frame)
                    fut = conn.pending_create_worktrees.pop(frame.request_id, None)
                    if fut is not None and not fut.done():
                        if create_status == "ok":
                            dirname = frame.branch_name.replace("/", "-")
                            fut.set_result(
                                {
                                    "status": "ok",
                                    "worktree_path": f"{frame.repo_path}-worktrees/{dirname}",
                                    "branch": frame.branch_name,
                                    "error": None,
                                }
                            )
                        else:
                            fut.set_result(
                                {
                                    "status": "failed",
                                    "worktree_path": None,
                                    "branch": None,
                                    "error": create_error,
                                }
                            )
                elif isinstance(frame, HostLaunchRunnerFrame):
                    cap.launch.append(frame)
                    fut = conn.pending_launches.pop(frame.request_id, None)
                    if fut is not None and not fut.done():
                        fut.set_result(
                            {
                                "status": launch_status,
                                "runner_id": (
                                    "runner_from_host" if launch_status == "launched" else None
                                ),
                                "error": None if launch_status == "launched" else "boom",
                            }
                        )
                elif isinstance(frame, HostRemoveWorktreeFrame):
                    cap.remove.append(frame)
                    fut = conn.pending_remove_worktrees.pop(frame.request_id, None)
                    if fut is not None and not fut.done():
                        fut.set_result({"status": "ok", "error": None})
                elif isinstance(frame, HostConfigureWorktreePoolFrame):
                    cap.configure_pool.append(frame)
                    fut = conn.pending_configure_worktree_pools.pop(frame.request_id, None)
                    if fut is not None and not fut.done():
                        fut.set_result(
                            {
                                "status": "ok",
                                "pool_id": frame.pool_id or "default-pool",
                                "target_size": frame.target_size,
                                "total_slots": frame.target_size,
                                "idle_slots": frame.target_size,
                                "error": None,
                            }
                        )
                elif isinstance(frame, HostAcquireWorktreeFrame):
                    cap.acquire_pool.append(frame)
                    fut = conn.pending_acquire_worktrees.pop(frame.request_id, None)
                    if fut is not None and not fut.done():
                        if acquire_status == "ok":
                            fut.set_result(
                                {
                                    "status": "ok",
                                    "lease_id": "lease_test_1",
                                    "pool_id": frame.pool_id or "default-pool",
                                    "slot_id": "slot-1",
                                    "worktree_path": f"{frame.repo_path}-omnigent-pool/slot-1",
                                    "branch": frame.branch_name,
                                    "error": None,
                                }
                            )
                        else:
                            fut.set_result(
                                {
                                    "status": "failed",
                                    "lease_id": None,
                                    "pool_id": None,
                                    "slot_id": None,
                                    "worktree_path": None,
                                    "branch": None,
                                    "error": acquire_error,
                                }
                            )
                elif isinstance(frame, HostReleaseWorktreeFrame):
                    cap.release_pool.append(frame)
                    fut = conn.pending_release_worktrees.pop(frame.request_id, None)
                    if fut is not None and not fut.done():
                        fut.set_result(
                            {
                                "status": release_status,
                                "error": None if release_status == "ok" else release_error,
                            }
                        )

        conn._drain_task_for_test = asyncio.create_task(_drain())  # type: ignore[attr-defined]
        conns.append(conn)
        return cap

    yield _register

    for conn in conns:
        conn.outbound_queue.put_nowait(None)
        task = conn._drain_task_for_test  # type: ignore[attr-defined]
        with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError, Exception):
            await asyncio.wait_for(asyncio.shield(task), timeout=1.0)
        if not task.done():
            task.cancel()


async def _bare_session(client: httpx.AsyncClient, name: str) -> str:
    """Create an unbound session (agent only, no host/workspace).

    :param client: The test HTTP client.
    :param name: Agent name to create.
    :returns: The new session id.
    """
    agent = await create_test_agent(client, name=name)
    resp = await client.post("/v1/sessions", json={"agent_id": agent["id"]})
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


async def _launch(
    client: httpx.AsyncClient,
    session_id: str,
    *,
    git: dict[str, object] | None = None,
) -> httpx.Response:
    """POST the dedicated per-session bind+launch endpoint.

    :param client: The test HTTP client.
    :param session_id: Existing session to bind.
    :param git: Optional ``git`` block. Create mode, e.g.
        ``{"branch_name": "feature/x"}``; bind mode, e.g.
        ``{"branch_name": "feature/x", "existing_worktree": True}``.
    :returns: The raw HTTP response.
    """
    body: dict[str, object] = {"session_id": session_id, "workspace": _SOURCE_REPO}
    if git is not None:
        body["git"] = git
    return await client.post(f"/v1/hosts/{_HOST_ID}/runners", json=body)


async def test_configure_worktree_pool_precreates_fixed_slot_count(
    register_host: RegisterHost,
    client: httpx.AsyncClient,
) -> None:
    """The host pool endpoint sends a configure frame and returns slot status."""
    cap = register_host()

    resp = await client.post(
        f"/v1/hosts/{_HOST_ID}/worktree-pools",
        json={
            "repo_path": _SOURCE_REPO,
            "base_branch": "main",
            "pool": {"target_size": 3, "pool_id": "universe-main"},
        },
    )

    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "object": "worktree_pool",
        "pool_id": "universe-main",
        "target_size": 3,
        "total_slots": 3,
        "idle_slots": 3,
    }
    assert len(cap.configure_pool) == 1
    assert cap.configure_pool[0].repo_path == _SOURCE_REPO
    assert cap.configure_pool[0].base_branch == "main"
    assert cap.configure_pool[0].target_size == 3
    assert cap.configure_pool[0].pool_id == "universe-main"


async def test_launch_runner_with_git_creates_worktree_and_persists_branch(
    register_host: RegisterHost,
    client: httpx.AsyncClient,
    db_uri: str,
) -> None:
    """``launch_runner`` with a ``git`` block creates a worktree off the
    source repo and binds the session to the worktree path + branch.

    Proves the new worktree step on the dedicated bind endpoint: the
    request's branch reaches ``host.create_worktree``, and the resulting
    worktree path + branch are persisted on the (previously unbound)
    session row via the extended ``set_host_id``. Without the new code the
    session would bind to the source repo with ``git_branch=NULL``.
    """
    cap = register_host()
    session_id = await _bare_session(client, "wt-launch-agent")

    resp = await _launch(
        client, session_id, git={"branch_name": "feature/login", "base_branch": "main"}
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["runner_id"]  # a runner was bound

    # The host received exactly one create-worktree frame off the source
    # repo, carrying the requested branch + base ref.
    assert len(cap.create) == 1, f"expected one create_worktree frame, got {len(cap.create)}"
    assert cap.create[0].repo_path == _SOURCE_REPO
    assert cap.create[0].branch_name == "feature/login"
    assert cap.create[0].base_branch == "main"
    # Success path: no rollback.
    assert cap.remove == [], "worktree was rolled back on a successful launch"

    # Persisted row: workspace is the worktree path (NOT the source repo),
    # git_branch is the new branch, host_id is bound. A NULL git_branch
    # here means set_host_id didn't receive/persist the branch.
    conv = SqlAlchemyConversationStore(db_uri).get_conversation(session_id)
    assert conv is not None
    assert conv.workspace == f"{_SOURCE_REPO}-worktrees/feature-login"
    assert conv.git_branch == "feature/login"
    assert conv.host_id == _HOST_ID


async def test_launch_runner_with_git_pool_leases_slot_and_persists_labels(
    register_host: RegisterHost,
    client: httpx.AsyncClient,
    db_uri: str,
) -> None:
    """``git.pool`` leases a fixed-pool slot instead of creating a new worktree."""
    cap = register_host()
    session_id = await _bare_session(client, "pooled-wt-launch-agent")

    resp = await _launch(
        client,
        session_id,
        git={
            "branch_name": "feature/pooled",
            "base_branch": "main",
            "pool": {"target_size": 2, "pool_id": "universe-main"},
        },
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["runner_id"]

    assert cap.create == [], "pooled launch should not create an unbounded worktree"
    assert len(cap.acquire_pool) == 1
    acquire = cap.acquire_pool[0]
    assert acquire.repo_path == _SOURCE_REPO
    assert acquire.base_branch == "main"
    assert acquire.target_size == 2
    assert acquire.pool_id == "universe-main"
    assert acquire.branch_name == "feature/pooled"
    assert acquire.session_id == session_id
    assert acquire.runner_id is not None
    assert len(cap.launch) == 1
    assert cap.launch[0].workspace == f"{_SOURCE_REPO}-omnigent-pool/slot-1"
    assert cap.release_pool == []

    conv = SqlAlchemyConversationStore(db_uri).get_conversation(session_id)
    assert conv is not None
    assert conv.workspace == f"{_SOURCE_REPO}-omnigent-pool/slot-1"
    assert conv.git_branch == "feature/pooled"
    assert conv.host_id == _HOST_ID
    assert conv.labels["omnigent.worktree_pool.lease_id"] == "lease_test_1"
    assert conv.labels["omnigent.worktree_pool.id"] == "universe-main"
    assert conv.labels["omnigent.worktree_pool.slot_id"] == "slot-1"


async def test_pooled_runner_disconnect_before_idle_timeout_keeps_lease(
    register_host: RegisterHost,
    client: httpx.AsyncClient,
    db_uri: str,
    app: FastAPI,
) -> None:
    """A fresh disconnect keeps the pooled binding resumable."""
    cap = register_host()
    session_id = await _bare_session(client, "pooled-wt-fresh-disconnect-agent")

    resp = await _launch(
        client,
        session_id,
        git={
            "branch_name": "feature/pooled",
            "base_branch": "main",
            "pool": {"target_size": 2, "pool_id": "universe-main"},
        },
    )
    assert resp.status_code == 200, resp.text

    conv = SqlAlchemyConversationStore(db_uri).get_conversation(session_id)
    assert conv is not None
    assert conv.runner_id is not None
    await app.state._on_runner_disconnect_for_test(conv.runner_id)
    await asyncio.sleep(0.05)

    assert cap.release_pool == []
    persisted = SqlAlchemyConversationStore(db_uri).get_conversation(session_id)
    assert persisted is not None
    assert persisted.runner_id == conv.runner_id
    assert persisted.host_id == _HOST_ID
    assert persisted.workspace == f"{_SOURCE_REPO}-omnigent-pool/slot-1"
    eviction_task = app.state._pool_idle_eviction_tasks_for_test.pop(session_id, None)
    if eviction_task is not None:
        eviction_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await eviction_task


async def test_pooled_runner_disconnect_after_idle_timeout_evicts_lease(
    register_host: RegisterHost,
    client: httpx.AsyncClient,
    db_uri: str,
    app: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stale disconnected pooled session releases its fixed-pool slot."""
    import omnigent.server.app as server_app

    cap = register_host()
    session_id = await _bare_session(client, "pooled-wt-stale-disconnect-agent")

    resp = await _launch(
        client,
        session_id,
        git={
            "branch_name": "feature/pooled",
            "base_branch": "main",
            "pool": {"target_size": 2, "pool_id": "universe-main"},
        },
    )
    assert resp.status_code == 200, resp.text

    conv = SqlAlchemyConversationStore(db_uri).get_conversation(session_id)
    assert conv is not None
    assert conv.runner_id is not None
    monkeypatch.setattr(
        server_app,
        "now_epoch",
        lambda: conv.updated_at + server_app._worktree_pool_idle_eviction_s() + 1,
    )

    await app.state._on_runner_disconnect_for_test(conv.runner_id)
    for _ in range(100):
        if cap.release_pool:
            break
        await asyncio.sleep(0.01)

    assert len(cap.release_pool) == 1
    assert cap.release_pool[0].lease_id == "lease_test_1"
    assert cap.release_pool[0].delete_branch is True
    persisted = SqlAlchemyConversationStore(db_uri).get_conversation(session_id)
    assert persisted is not None
    assert persisted.runner_id is None
    assert persisted.host_id is None
    assert persisted.workspace is None
    assert persisted.git_branch is None
    assert "omnigent.worktree_pool.lease_id" not in persisted.labels
    assert "omnigent.worktree_pool.id" not in persisted.labels
    assert "omnigent.worktree_pool.slot_id" not in persisted.labels


async def test_pooled_runner_idle_release_failure_keeps_binding(
    register_host: RegisterHost,
    client: httpx.AsyncClient,
    db_uri: str,
    app: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed finalize/release preserves the session's branch binding."""
    import omnigent.server.app as server_app

    cap = register_host(
        release_status="failed",
        release_error="git push failed",
    )
    session_id = await _bare_session(client, "pooled-wt-release-failure-agent")

    resp = await _launch(
        client,
        session_id,
        git={
            "branch_name": "feature/pooled",
            "base_branch": "main",
            "pool": {"target_size": 2, "pool_id": "universe-main"},
        },
    )
    assert resp.status_code == 200, resp.text

    conv = SqlAlchemyConversationStore(db_uri).get_conversation(session_id)
    assert conv is not None
    assert conv.runner_id is not None
    monkeypatch.setattr(
        server_app,
        "now_epoch",
        lambda: conv.updated_at + server_app._worktree_pool_idle_eviction_s() + 1,
    )

    await app.state._on_runner_disconnect_for_test(conv.runner_id)
    for _ in range(100):
        if cap.release_pool:
            break
        await asyncio.sleep(0.01)

    assert len(cap.release_pool) == 1
    persisted = SqlAlchemyConversationStore(db_uri).get_conversation(session_id)
    assert persisted is not None
    assert persisted.runner_id == conv.runner_id
    assert persisted.host_id == _HOST_ID
    assert persisted.workspace == f"{_SOURCE_REPO}-omnigent-pool/slot-1"
    assert persisted.git_branch == "feature/pooled"
    assert persisted.labels["omnigent.worktree_pool.lease_id"] == "lease_test_1"


async def test_launch_runner_with_git_pool_releases_lease_on_launch_failure(
    register_host: RegisterHost,
    client: httpx.AsyncClient,
    db_uri: str,
) -> None:
    """A failed pooled launch releases the slot and fully unbinds the session."""
    cap = register_host(launch_status="failed")
    session_id = await _bare_session(client, "pooled-wt-rollback-agent")

    resp = await _launch(
        client,
        session_id,
        git={
            "branch_name": "feature/pooled",
            "base_branch": "main",
            "pool": {"target_size": 2},
        },
    )

    assert resp.status_code == 502, resp.text
    assert len(cap.acquire_pool) == 1
    assert len(cap.release_pool) == 1
    assert cap.release_pool[0].lease_id == "lease_test_1"
    assert cap.release_pool[0].delete_branch is True
    assert cap.remove == [], "pooled rollback should release the lease, not remove a worktree"

    conv = SqlAlchemyConversationStore(db_uri).get_conversation(session_id)
    assert conv is not None
    assert conv.runner_id is None
    assert conv.host_id is None
    assert conv.workspace is None
    assert conv.git_branch is None


async def test_launch_runner_with_git_pool_capacity_error_returns_409(
    register_host: RegisterHost,
    client: httpx.AsyncClient,
    db_uri: str,
) -> None:
    """Pool capacity errors are conflicts and do not bind or launch a runner."""
    cap = register_host(
        acquire_status="failed",
        acquire_error="worktree pool 'universe-main' has no available slots",
    )
    session_id = await _bare_session(client, "pooled-wt-capacity-agent")

    resp = await _launch(
        client,
        session_id,
        git={
            "branch_name": "feature/pooled",
            "base_branch": "main",
            "pool": {"target_size": 1, "pool_id": "universe-main"},
        },
    )

    assert resp.status_code == 409, resp.text
    assert "no available slots" in resp.text
    assert len(cap.acquire_pool) == 1
    assert cap.launch == []
    assert cap.release_pool == []

    conv = SqlAlchemyConversationStore(db_uri).get_conversation(session_id)
    assert conv is not None
    assert conv.runner_id is None
    assert conv.host_id is None
    assert conv.workspace is None
    assert conv.git_branch is None


async def test_launch_runner_without_git_binds_source_dir_no_worktree(
    register_host: RegisterHost,
    client: httpx.AsyncClient,
    db_uri: str,
) -> None:
    """Without a ``git`` block the endpoint binds the source directory
    directly and creates no worktree (the same-directory resume path).

    Pins that the new worktree code is inert when ``git`` is omitted:
    no ``host.create_worktree`` frame, workspace stays the source repo,
    and ``git_branch`` stays NULL.
    """
    cap = register_host()
    session_id = await _bare_session(client, "no-wt-agent")

    resp = await _launch(client, session_id, git=None)
    assert resp.status_code == 200, resp.text

    assert cap.create == [], "no worktree should be created without a git block"
    conv = SqlAlchemyConversationStore(db_uri).get_conversation(session_id)
    assert conv is not None
    assert conv.workspace == _SOURCE_REPO
    assert conv.git_branch is None
    assert conv.host_id == _HOST_ID


async def test_launch_runner_with_existing_worktree_persists_without_creating(
    register_host: RegisterHost,
    client: httpx.AsyncClient,
    db_uri: str,
) -> None:
    """``git.existing_worktree`` binds the existing worktree dir and records
    its branch without creating a worktree (the existing-worktree resume path).

    The workspace is already a worktree, so no ``host.create_worktree``
    frame is sent; ``branch_name`` is persisted as ``git_branch`` so the
    sidebar shows it and the opt-in delete flow can offer to remove it.
    """
    cap = register_host()
    session_id = await _bare_session(client, "existing-wt-agent")

    resp = await _launch(
        client,
        session_id,
        git={"branch_name": "feature/existing", "existing_worktree": True},
    )
    assert resp.status_code == 200, resp.text

    assert cap.create == [], "no worktree should be created for an existing worktree"
    conv = SqlAlchemyConversationStore(db_uri).get_conversation(session_id)
    assert conv is not None
    assert conv.workspace == _SOURCE_REPO
    assert conv.git_branch == "feature/existing"
    assert conv.host_id == _HOST_ID


async def test_launch_runner_rolls_back_worktree_on_launch_failure(
    register_host: RegisterHost,
    client: httpx.AsyncClient,
    db_uri: str,
) -> None:
    """When the host fails the launch, the just-created worktree is
    rolled back AND the runner binding is cleared so the picker can retry.

    The worktree is created (status ok) but the launch reports
    ``failed`` → the endpoint returns 502, sends a
    ``host.remove_worktree`` for the created worktree, and clears the
    session's ``runner_id``. If the rollback were missing, ``cap.remove``
    would be empty; if the binding weren't cleared, ``runner_id`` would
    stay set and a retry would dead-end on the atomic ``set_runner_id``
    CAS with "session already has a runner bound" (the whole point of the
    fork-resume picker is that the user can retry after a failed bind).
    """
    cap = register_host(launch_status="failed")
    session_id = await _bare_session(client, "wt-rollback-agent")

    resp = await _launch(client, session_id, git={"branch_name": "feature/x"})

    # Launch failed → 502 (host fault), as the no-git path already does.
    assert resp.status_code == 502, resp.text
    # The worktree was created, then removed (rollback fired) for the same path.
    assert len(cap.create) == 1
    assert len(cap.remove) == 1, "expected a rollback remove_worktree frame after launch failure"
    created_path = f"{_SOURCE_REPO}-worktrees/feature-x"
    assert cap.remove[0].worktree_path == created_path
    assert cap.remove[0].delete_branch is True  # orphan branch also cleaned up

    # The session is fully unbound so the DB matches the host's actual
    # state (worktree removed) and a retry starts clean. A non-None
    # runner_id would stick the session as "already bound"; a leftover
    # workspace/git_branch would point at the deleted worktree/branch and
    # could wrongly trigger worktree-cleanup paths (git_branch IS NOT NULL).
    conv = SqlAlchemyConversationStore(db_uri).get_conversation(session_id)
    assert conv is not None
    assert conv.runner_id is None, (
        "runner_id should be cleared after a failed launch so the picker can "
        f"rebind; got {conv.runner_id!r} (retry would 400 'already has a runner bound')"
    )
    assert conv.host_id is None, f"host_id should be cleared on rollback; got {conv.host_id!r}"
    assert conv.workspace is None, (
        f"workspace should be cleared (worktree was removed); got {conv.workspace!r}"
    )
    assert conv.git_branch is None, (
        f"git_branch should be cleared (branch was removed); got {conv.git_branch!r}"
    )


async def test_launch_runner_retry_succeeds_after_failed_launch(
    register_host: RegisterHost,
    client: httpx.AsyncClient,
    db_uri: str,
) -> None:
    """A second bind succeeds after the first launch failed.

    End-to-end proof of the cleared-binding fix: a failed launch (502)
    must leave the session re-bindable. The retry creates a fresh
    worktree and binds the runner. Without clearing ``runner_id`` on the
    first failure, this retry returns 400 "session already has a runner
    bound" — the dead-end the fork-resume picker would otherwise hit.
    """
    register_host(launch_status="failed")
    session_id = await _bare_session(client, "wt-retry-agent")

    first = await _launch(client, session_id, git={"branch_name": "feature/x"})
    assert first.status_code == 502, first.text

    # Re-register the host to launch successfully this time (newest-wins
    # replaces the failing connection), then retry the bind.
    cap_ok = register_host(launch_status="launched")
    second = await _launch(client, session_id, git={"branch_name": "feature/y"})

    # Retry binds cleanly — proves runner_id was released by the failure.
    assert second.status_code == 200, second.text
    assert second.json()["runner_id"]
    assert len(cap_ok.create) == 1, "retry created a fresh worktree off the source repo"
    conv = SqlAlchemyConversationStore(db_uri).get_conversation(session_id)
    assert conv is not None
    assert conv.workspace == f"{_SOURCE_REPO}-worktrees/feature-y"
    assert conv.git_branch == "feature/y"
