"""Server-side proxies for host worktree-pool tunnel frames."""

from __future__ import annotations

import asyncio
import secrets
from dataclasses import dataclass

from omnigent.host.frames import (
    HostAcquireWorktreeFrame,
    HostConfigureWorktreePoolFrame,
    HostReleaseWorktreeFrame,
    encode_host_frame,
)
from omnigent.server.host_registry import HostConnection, HostRegistry

_POOL_TIMEOUT_S: float = 150.0


class WorktreePoolProxyError(Exception):
    """Raised when a host reports a worktree-pool operation failure."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class WorktreePoolHostUnavailableError(WorktreePoolProxyError):
    """Raised when the host cannot be reached for a pool operation."""


@dataclass
class PoolStatus:
    """Result of configuring a host worktree pool."""

    pool_id: str
    target_size: int
    total_slots: int
    idle_slots: int


@dataclass
class AcquiredWorktree:
    """Result of leasing a host worktree-pool slot."""

    lease_id: str
    pool_id: str
    slot_id: str
    worktree_path: str
    branch: str | None


async def _await_result(
    *,
    host_registry: HostRegistry,
    host_conn: HostConnection,
    pending: dict[str, asyncio.Future[dict[str, object]]],
    request_id: str,
    frame: str,
    op: str,
) -> dict[str, object]:
    future: asyncio.Future[dict[str, object]] = asyncio.get_running_loop().create_future()
    pending[request_id] = future
    try:
        try:
            host_registry.send_text(host_conn, frame)
        except ConnectionError as exc:
            raise WorktreePoolHostUnavailableError(
                f"host '{host_conn.host_id}' connection lost during {op}"
            ) from exc
        try:
            return await asyncio.wait_for(future, timeout=_POOL_TIMEOUT_S)
        except asyncio.TimeoutError as exc:
            raise WorktreePoolHostUnavailableError(
                f"host '{host_conn.host_id}' did not respond to {op} within "
                f"{_POOL_TIMEOUT_S:.0f}s"
            ) from exc
    finally:
        pending.pop(request_id, None)


async def configure_worktree_pool_on_host(
    *,
    host_registry: HostRegistry,
    host_conn: HostConnection,
    repo_path: str,
    base_branch: str,
    target_size: int,
    pool_id: str | None,
) -> PoolStatus:
    """Configure a fixed-size worktree pool on a host."""
    request_id = secrets.token_hex(8)
    result = await _await_result(
        host_registry=host_registry,
        host_conn=host_conn,
        pending=host_conn.pending_configure_worktree_pools,
        request_id=request_id,
        frame=encode_host_frame(
            HostConfigureWorktreePoolFrame(
                request_id=request_id,
                repo_path=repo_path,
                base_branch=base_branch,
                target_size=target_size,
                pool_id=pool_id,
            )
        ),
        op="worktree pool configuration",
    )
    if result.get("status") != "ok":
        detail = result.get("error") or "host reported no detail"
        raise WorktreePoolProxyError(
            f"worktree pool configuration failed: {detail}"
        )
    resolved_pool_id = result.get("pool_id")
    resolved_target_size = result.get("target_size")
    total_slots = result.get("total_slots")
    idle_slots = result.get("idle_slots")
    if not (
        isinstance(resolved_pool_id, str)
        and isinstance(resolved_target_size, int)
        and isinstance(total_slots, int)
        and isinstance(idle_slots, int)
    ):
        raise WorktreePoolProxyError("host returned an incomplete worktree pool status")
    return PoolStatus(
        pool_id=resolved_pool_id,
        target_size=resolved_target_size,
        total_slots=total_slots,
        idle_slots=idle_slots,
    )


async def acquire_worktree_on_host(
    *,
    host_registry: HostRegistry,
    host_conn: HostConnection,
    repo_path: str,
    base_branch: str,
    target_size: int,
    pool_id: str | None,
    branch_name: str | None,
    session_id: str | None,
    runner_id: str | None,
) -> AcquiredWorktree:
    """Acquire one worktree-pool slot from a host."""
    request_id = secrets.token_hex(8)
    result = await _await_result(
        host_registry=host_registry,
        host_conn=host_conn,
        pending=host_conn.pending_acquire_worktrees,
        request_id=request_id,
        frame=encode_host_frame(
            HostAcquireWorktreeFrame(
                request_id=request_id,
                repo_path=repo_path,
                base_branch=base_branch,
                target_size=target_size,
                pool_id=pool_id,
                branch_name=branch_name,
                session_id=session_id,
                runner_id=runner_id,
            )
        ),
        op="worktree acquire",
    )
    if result.get("status") != "ok":
        raise WorktreePoolProxyError(
            f"worktree acquire failed: {result.get('error') or 'host reported no detail'}"
        )
    lease_id = result.get("lease_id")
    resolved_pool_id = result.get("pool_id")
    slot_id = result.get("slot_id")
    worktree_path = result.get("worktree_path")
    branch = result.get("branch")
    if not (
        isinstance(lease_id, str)
        and isinstance(resolved_pool_id, str)
        and isinstance(slot_id, str)
        and isinstance(worktree_path, str)
        and (branch is None or isinstance(branch, str))
    ):
        raise WorktreePoolProxyError("host returned an incomplete worktree acquire result")
    return AcquiredWorktree(
        lease_id=lease_id,
        pool_id=resolved_pool_id,
        slot_id=slot_id,
        worktree_path=worktree_path,
        branch=branch,
    )


async def release_worktree_on_host(
    *,
    host_registry: HostRegistry,
    host_conn: HostConnection,
    lease_id: str,
    delete_branch: bool,
) -> None:
    """Release a leased worktree-pool slot on a host."""
    request_id = secrets.token_hex(8)
    result = await _await_result(
        host_registry=host_registry,
        host_conn=host_conn,
        pending=host_conn.pending_release_worktrees,
        request_id=request_id,
        frame=encode_host_frame(
            HostReleaseWorktreeFrame(
                request_id=request_id,
                lease_id=lease_id,
                delete_branch=delete_branch,
            )
        ),
        op="worktree release",
    )
    if result.get("status") != "ok":
        raise WorktreePoolProxyError(
            f"worktree release failed: {result.get('error') or 'host reported no detail'}"
        )
