"""Fixed-size host-managed git worktree pools."""

from __future__ import annotations

import secrets
import threading
from dataclasses import dataclass
from pathlib import Path

from omnigent.host.git_worktree import (
    WorktreeError,
    _ensure_base_resolvable,
    _git_error,
    _main_work_tree,
    _run_git,
    _sanitize_dirname,
    validate_branch_name,
)


class WorktreePoolError(Exception):
    """Raised when a worktree pool operation fails."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


@dataclass
class PoolStatus:
    """Status returned after configuring a pool."""

    pool_id: str
    target_size: int
    total_slots: int
    idle_slots: int


@dataclass
class AcquiredWorktree:
    """A leased pool worktree."""

    lease_id: str
    pool_id: str
    slot_id: str
    worktree_path: str
    branch: str | None


@dataclass
class _Lease:
    """In-memory lease record for one pool slot."""

    lease_id: str
    pool_id: str
    slot_id: str
    repo_root: str
    worktree_path: str
    base_branch: str
    target_size: int
    branch: str | None
    session_id: str | None
    runner_id: str | None


class WorktreePoolManager:
    """Manage fixed-size git worktree pools for one host process."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._leases_by_path: dict[str, _Lease] = {}
        self._leases_by_id: dict[str, _Lease] = {}
        self._quarantined: set[str] = set()

    def configure_pool(
        self,
        *,
        repo_path: str,
        base_branch: str,
        target_size: int,
        pool_id: str | None = None,
    ) -> PoolStatus:
        """Precreate and reconcile a fixed-size pool."""
        with self._lock:
            repo_root = _repo_root(repo_path)
            resolved_pool_id = _pool_id(repo_root, base_branch, pool_id)
            _validate_target_size(target_size)
            _ensure_base(repo_root, base_branch)
            slots = _slot_paths(repo_root, resolved_pool_id, target_size)
            _prune_worktrees(repo_root)
            for slot_id, slot_path in slots:
                self._ensure_slot(repo_root, base_branch, slot_id, slot_path)
            return self._status(repo_root, resolved_pool_id, target_size)

    def acquire(
        self,
        *,
        repo_path: str,
        base_branch: str,
        target_size: int,
        branch_name: str | None = None,
        pool_id: str | None = None,
        session_id: str | None = None,
        runner_id: str | None = None,
    ) -> AcquiredWorktree:
        """Acquire one idle slot from a fixed-size pool."""
        if branch_name is not None:
            validate_branch_name(branch_name)
        with self._lock:
            repo_root = _repo_root(repo_path)
            resolved_pool_id = _pool_id(repo_root, base_branch, pool_id)
            self.configure_pool(
                repo_path=repo_root,
                base_branch=base_branch,
                target_size=target_size,
                pool_id=resolved_pool_id,
            )
            for slot_id, slot_path in _slot_paths(repo_root, resolved_pool_id, target_size):
                slot_key = str(slot_path)
                if slot_key in self._leases_by_path or slot_key in self._quarantined:
                    continue
                try:
                    self._restore_slot(repo_root, base_branch, slot_path)
                    if branch_name is not None:
                        result = _run_git(["checkout", "-b", branch_name], cwd=str(slot_path))
                        if result.returncode != 0:
                            raise _git_error("git checkout -b failed", result)
                except WorktreeError as exc:
                    self._quarantined.add(slot_key)
                    continue
                lease_id = f"lease_{secrets.token_hex(16)}"
                lease = _Lease(
                    lease_id=lease_id,
                    pool_id=resolved_pool_id,
                    slot_id=slot_id,
                    repo_root=repo_root,
                    worktree_path=slot_key,
                    base_branch=base_branch,
                    target_size=target_size,
                    branch=branch_name,
                    session_id=session_id,
                    runner_id=runner_id,
                )
                self._leases_by_path[slot_key] = lease
                self._leases_by_id[lease_id] = lease
                return AcquiredWorktree(
                    lease_id=lease_id,
                    pool_id=resolved_pool_id,
                    slot_id=slot_id,
                    worktree_path=slot_key,
                    branch=branch_name,
                )
            raise WorktreePoolError(
                f"worktree pool {resolved_pool_id!r} has no available slots "
                f"(target_size={target_size})"
            )

    def release(self, *, lease_id: str, delete_branch: bool = False) -> None:
        """Release a leased slot and restore it to its base branch."""
        with self._lock:
            lease = self._leases_by_id.get(lease_id)
            if lease is None:
                raise WorktreePoolError(f"unknown worktree lease: {lease_id}")
            try:
                if delete_branch and lease.branch is not None:
                    self._finalize_branch_before_cleanup(lease)
                self._restore_slot(lease.repo_root, lease.base_branch, Path(lease.worktree_path))
                if delete_branch and lease.branch is not None:
                    result = _run_git(["branch", "-D", lease.branch], cwd=lease.repo_root)
                    if result.returncode != 0:
                        raise _git_error("git branch -D failed", result)
                self._leases_by_id.pop(lease_id, None)
                self._leases_by_path.pop(lease.worktree_path, None)
                self._quarantined.discard(lease.worktree_path)
                self.configure_pool(
                    repo_path=lease.repo_root,
                    base_branch=lease.base_branch,
                    target_size=lease.target_size,
                    pool_id=lease.pool_id,
                )
            except WorktreeError as exc:
                self._quarantined.add(lease.worktree_path)
                raise WorktreePoolError(exc.message) from exc

    def _ensure_slot(
        self,
        repo_root: str,
        base_branch: str,
        slot_id: str,
        slot_path: Path,
    ) -> None:
        if slot_path.exists():
            if not (slot_path / ".git").exists():
                raise WorktreePoolError(
                    f"pool slot path exists but is not a git worktree: {slot_path}"
                )
            return
        slot_path.parent.mkdir(parents=True, exist_ok=True)
        result = _run_git(
            ["worktree", "add", "--detach", str(slot_path), "--end-of-options", base_branch],
            cwd=repo_root,
        )
        if result.returncode != 0:
            error = _git_error(f"git worktree add failed for {slot_id}", result)
            raise WorktreePoolError(error.message)

    def _restore_slot(self, repo_root: str, base_branch: str, slot_path: Path) -> None:
        _ensure_base(repo_root, base_branch)
        for args in (["merge", "--abort"], ["rebase", "--abort"]):
            _run_git(args, cwd=str(slot_path))
        for args in (
            ["reset", "--hard"],
            ["clean", "-ffd"],
            ["checkout", "--detach", "--end-of-options", base_branch],
        ):
            result = _run_git(args, cwd=str(slot_path))
            if result.returncode != 0:
                raise _git_error(f"git {' '.join(args)} failed", result)

    def _finalize_branch_before_cleanup(self, lease: _Lease) -> None:
        """Commit and push dirty branch work before destructive cleanup."""
        worktree_path = lease.worktree_path
        status = _run_git(["status", "--porcelain=v1", "--untracked-files=all"], cwd=worktree_path)
        if status.returncode != 0:
            raise _git_error("git status failed", status)
        if not status.stdout.strip():
            return

        if lease.branch is None:
            raise WorktreeError("cannot finalize dirty pool slot without a branch")

        current_branch = _run_git(["branch", "--show-current"], cwd=worktree_path)
        if current_branch.returncode != 0:
            raise _git_error("git branch --show-current failed", current_branch)
        if current_branch.stdout.strip() != lease.branch:
            raise WorktreeError(
                "cannot finalize dirty pool slot: "
                f"expected branch {lease.branch!r}, found {current_branch.stdout.strip()!r}"
            )

        remote = _run_git(["remote", "get-url", "origin"], cwd=worktree_path)
        if remote.returncode != 0:
            raise WorktreeError(
                "cannot finalize dirty pool slot: remote 'origin' is not configured"
            )

        add = _run_git(["add", "-A"], cwd=worktree_path)
        if add.returncode != 0:
            raise _git_error("git add failed", add)

        staged = _run_git(["diff", "--cached", "--quiet"], cwd=worktree_path)
        if staged.returncode == 0:
            return
        if staged.returncode != 1:
            raise _git_error("git diff --cached failed", staged)

        message = f"Omnigent session {lease.session_id or lease.lease_id}"
        commit = _run_git(["commit", "-m", message], cwd=worktree_path)
        if commit.returncode != 0:
            raise _git_error("git commit failed", commit)

        push = _run_git(["push", "origin", f"HEAD:refs/heads/{lease.branch}"], cwd=worktree_path)
        if push.returncode != 0:
            raise _git_error("git push failed", push)

    def _status(self, repo_root: str, pool_id: str, target_size: int) -> PoolStatus:
        slots = _slot_paths(repo_root, pool_id, target_size)
        idle = 0
        for _, slot_path in slots:
            slot_key = str(slot_path)
            if slot_key not in self._leases_by_path and slot_key not in self._quarantined:
                idle += 1
        return PoolStatus(
            pool_id=pool_id,
            target_size=target_size,
            total_slots=len(slots),
            idle_slots=idle,
        )


def _repo_root(repo_path: str) -> str:
    try:
        return _main_work_tree(repo_path)
    except WorktreeError as exc:
        raise WorktreePoolError(exc.message) from exc


def _ensure_base(repo_root: str, base_branch: str) -> None:
    try:
        _ensure_base_resolvable(repo_root, base_branch)
    except WorktreeError as exc:
        raise WorktreePoolError(exc.message) from exc


def _validate_target_size(target_size: int) -> None:
    if isinstance(target_size, bool) or target_size < 1:
        raise WorktreePoolError("target_size must be a positive integer")


def _pool_id(repo_root: str, base_branch: str, pool_id: str | None) -> str:
    raw = pool_id or f"{Path(repo_root).name}-{base_branch}"
    cleaned = _sanitize_dirname(raw).replace("@", "-")
    return "".join(ch if ch.isalnum() or ch in "._-" else "-" for ch in cleaned).strip("-")


def _slot_paths(repo_root: str, pool_id: str, target_size: int) -> list[tuple[str, Path]]:
    root = Path(repo_root)
    base_dir = root.parent / f"{root.name}-omnigent-pool" / pool_id
    return [(f"slot-{idx}", base_dir / f"slot-{idx}") for idx in range(1, target_size + 1)]


def _prune_worktrees(repo_root: str) -> None:
    _run_git(["worktree", "prune"], cwd=repo_root)
