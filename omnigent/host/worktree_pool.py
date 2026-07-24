"""Fixed-capacity worktree management owned by the host process."""

from __future__ import annotations

import os
import secrets
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from omnigent.host.git_worktree import (
    WorktreeError,
    _ensure_base_resolvable,
    _git_error,
    _main_work_tree,
    _run_git,
    validate_branch_name,
)


class WorktreePoolError(Exception):
    """Raised when a managed worktree operation fails."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


@dataclass(frozen=True)
class AcquiredWorktree:
    """A managed worktree assigned to one session."""

    lease_id: str
    pool_id: str
    slot_id: str
    worktree_path: str
    branch: str


@dataclass
class _Lease:
    """In-memory ownership record for one managed worktree."""

    lease_id: str
    pool_id: str
    slot_id: str
    repo_root: str
    worktree_path: str
    base_branch: str
    branch_remote: str
    branch: str
    session_id: str
    runner_id: str | None
    idle_since: float | None = None


@dataclass(frozen=True)
class ManagedWorktreeRepo:
    """A fixed set of existing worktrees adopted when the host starts."""

    repo_id: str
    repo_root: str
    base_branch: str
    branch_remote: str
    worktrees: tuple[str, ...]


@dataclass(frozen=True)
class ManagedWorktreeConfig:
    """Host-owned worktree configuration loaded from ``config.yaml``."""

    repos: tuple[ManagedWorktreeRepo, ...]
    idle_eviction_seconds: int = 60 * 60


class WorktreePoolManager:
    """Assign precreated worktrees without exceeding configured capacity."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._leases_by_path: dict[str, _Lease] = {}
        self._leases_by_id: dict[str, _Lease] = {}
        self._leases_by_session: dict[str, _Lease] = {}
        self._leases_by_branch: dict[tuple[str, str], _Lease] = {}
        self._quarantined: set[str] = set()
        self._managed_pools: dict[str, ManagedWorktreeRepo] = {}
        self._managed_idle_eviction_seconds = 60 * 60

    def adopt_managed_config(self, config: ManagedWorktreeConfig) -> None:
        """Validate and adopt precreated worktrees without creating new ones."""
        with self._lock:
            self._managed_idle_eviction_seconds = config.idle_eviction_seconds
            for repo in config.repos:
                if repo.repo_id in self._managed_pools:
                    raise WorktreePoolError(f"duplicate managed repo id: {repo.repo_id}")
                _ensure_base(repo.repo_root, repo.base_branch)
                seen: set[str] = set()
                for worktree in repo.worktrees:
                    path = str(Path(worktree).expanduser().resolve())
                    if path in seen:
                        raise WorktreePoolError(
                            f"managed repo {repo.repo_id!r} lists worktree twice: {path}"
                        )
                    seen.add(path)
                    if not Path(path).is_dir() or not (Path(path) / ".git").exists():
                        raise WorktreePoolError(f"managed worktree does not exist: {path}")
                    if _repo_root(path) != repo.repo_root:
                        raise WorktreePoolError(
                            f"managed worktree {path} does not belong to {repo.repo_root}"
                        )
                self._managed_pools[repo.repo_id] = repo

    def acquire_managed(
        self,
        *,
        repo_id: str,
        branch_name: str,
        session_id: str,
        runner_id: str,
    ) -> AcquiredWorktree:
        """Start or resume a session in one of the host's worktrees."""
        validate_branch_name(branch_name)
        with self._lock:
            self.evict_idle_managed()
            existing = self._leases_by_session.get(session_id)
            if existing is not None:
                if existing.pool_id != repo_id or existing.branch != branch_name:
                    raise WorktreePoolError(
                        f"session {session_id!r} is already bound to "
                        f"{existing.pool_id!r}/{existing.branch!r}"
                    )
                existing.runner_id = runner_id
                existing.idle_since = None
                return _acquired(existing)

            branch_key = (repo_id, branch_name)
            branch_lease = self._leases_by_branch.get(branch_key)
            if branch_lease is not None:
                raise WorktreePoolError(
                    f"managed branch {repo_id!r}/{branch_name!r} is already bound to "
                    f"session {branch_lease.session_id!r}"
                )

            repo = self._managed_pools.get(repo_id)
            if repo is None:
                raise WorktreePoolError(f"host has no managed worktree repo {repo_id!r}")
            for index, worktree_path in enumerate(repo.worktrees, start=1):
                if worktree_path in self._leases_by_path or worktree_path in self._quarantined:
                    continue
                try:
                    self._restore_slot(repo.repo_root, repo.base_branch, Path(worktree_path))
                    self._checkout_session_branch(repo, worktree_path, branch_name)
                except WorktreeError:
                    self._quarantined.add(worktree_path)
                    continue
                lease = _Lease(
                    lease_id=f"lease_{secrets.token_hex(16)}",
                    pool_id=repo_id,
                    slot_id=f"slot-{index}",
                    repo_root=repo.repo_root,
                    worktree_path=worktree_path,
                    base_branch=repo.base_branch,
                    branch_remote=repo.branch_remote,
                    branch=branch_name,
                    session_id=session_id,
                    runner_id=runner_id,
                )
                self._leases_by_path[worktree_path] = lease
                self._leases_by_id[lease.lease_id] = lease
                self._leases_by_session[session_id] = lease
                self._leases_by_branch[branch_key] = lease
                return _acquired(lease)
            raise WorktreePoolError(
                f"managed worktree repo {repo_id!r} has no available slots "
                f"(capacity={len(repo.worktrees)})"
            )

    def mark_runner_idle(self, runner_id: str) -> None:
        """Keep a session assignment resumable after its runner exits."""
        with self._lock:
            for lease in self._leases_by_id.values():
                if lease.runner_id == runner_id:
                    lease.runner_id = None
                    lease.idle_since = time.monotonic()

    def release_failed_launch(self, session_id: str) -> None:
        """Return a session's slot immediately when its runner never starts."""
        with self._lock:
            lease = self._leases_by_session.get(session_id)
            if lease is not None:
                self._release(lease)

    def workspace_for_session(self, session_id: str) -> str | None:
        """Return the physical worktree currently leased to a session."""
        with self._lock:
            lease = self._leases_by_session.get(session_id)
            return lease.worktree_path if lease is not None else None

    def evict_idle_managed(self) -> list[str]:
        """Finalize and release sessions idle longer than the configured limit."""
        with self._lock:
            now = time.monotonic()
            expired = [
                lease
                for lease in self._leases_by_id.values()
                if lease.idle_since is not None
                and now - lease.idle_since >= self._managed_idle_eviction_seconds
            ]
            released: list[str] = []
            for lease in expired:
                self._release(lease)
                released.append(lease.session_id)
            return released

    def _release(self, lease: _Lease) -> None:
        try:
            self._finalize_branch_before_cleanup(lease)
            self._restore_slot(lease.repo_root, lease.base_branch, Path(lease.worktree_path))
            result = _run_git(["branch", "-D", lease.branch], cwd=lease.repo_root)
            if result.returncode != 0:
                raise _git_error("git branch -D failed", result)
        except WorktreeError as exc:
            self._quarantined.add(lease.worktree_path)
            raise WorktreePoolError(exc.message) from exc
        self._leases_by_id.pop(lease.lease_id, None)
        self._leases_by_path.pop(lease.worktree_path, None)
        self._leases_by_session.pop(lease.session_id, None)
        self._leases_by_branch.pop((lease.pool_id, lease.branch), None)
        self._quarantined.discard(lease.worktree_path)

    def _checkout_session_branch(
        self,
        repo: ManagedWorktreeRepo,
        worktree_path: str,
        branch_name: str,
    ) -> None:
        remote_ref = f"refs/remotes/{repo.branch_remote}/{branch_name}"
        fetch = _run_git(
            ["fetch", repo.branch_remote, f"{branch_name}:{remote_ref}"],
            cwd=worktree_path,
        )
        if fetch.returncode == 0:
            result = _run_git(
                ["checkout", "-B", branch_name, f"{repo.branch_remote}/{branch_name}"],
                cwd=worktree_path,
            )
        else:
            local = _run_git(
                ["show-ref", "--verify", f"refs/heads/{branch_name}"],
                cwd=worktree_path,
            )
            if local.returncode == 0:
                result = _run_git(["checkout", branch_name], cwd=worktree_path)
            else:
                result = _run_git(
                    ["checkout", "-b", branch_name, "--end-of-options", repo.base_branch],
                    cwd=worktree_path,
                )
        if result.returncode != 0:
            raise _git_error("git checkout session branch failed", result)

    def _restore_slot(self, repo_root: str, base_branch: str, slot_path: Path) -> None:
        _ensure_base(repo_root, base_branch)
        for args in (["merge", "--abort"], ["rebase", "--abort"]):
            _run_git(args, cwd=str(slot_path))
        _remove_stale_index_lock_if_safe(str(slot_path))
        _run_git_with_lock_recovery(["reset", "--hard"], cwd=str(slot_path))
        clean = _run_git(["clean", "-ffd"], cwd=str(slot_path))
        if clean.returncode != 0:
            raise _git_error("git clean -ffd failed", clean)
        _run_git_with_lock_recovery(
            ["checkout", "--detach", "--end-of-options", base_branch],
            cwd=str(slot_path),
        )

    def _finalize_branch_before_cleanup(self, lease: _Lease) -> None:
        """Commit and push dirty branch work before destructive cleanup."""
        status = _run_git(
            ["status", "--porcelain=v1", "--untracked-files=all"],
            cwd=lease.worktree_path,
        )
        if status.returncode != 0:
            raise _git_error("git status failed", status)
        if not status.stdout.strip():
            return

        current_branch = _run_git(["branch", "--show-current"], cwd=lease.worktree_path)
        if current_branch.returncode != 0:
            raise _git_error("git branch --show-current failed", current_branch)
        if current_branch.stdout.strip() != lease.branch:
            raise WorktreeError(
                "cannot finalize dirty pool slot: "
                f"expected branch {lease.branch!r}, found {current_branch.stdout.strip()!r}"
            )

        remote = _run_git(["remote", "get-url", lease.branch_remote], cwd=lease.worktree_path)
        if remote.returncode != 0:
            raise WorktreeError(
                "cannot finalize dirty pool slot: "
                f"remote {lease.branch_remote!r} is not configured"
            )

        _run_git_with_lock_recovery(["add", "-A"], cwd=lease.worktree_path)
        staged = _run_git(["diff", "--cached", "--quiet"], cwd=lease.worktree_path)
        if staged.returncode == 0:
            return
        if staged.returncode != 1:
            raise _git_error("git diff --cached failed", staged)

        # TODO: Generate a commit message from the staged diff and session context.
        message = f"Omnigent session {lease.session_id}"
        commit = _run_git(["commit", "-m", message], cwd=lease.worktree_path)
        if commit.returncode != 0:
            raise _git_error("git commit failed", commit)

        push = _run_git(
            ["push", lease.branch_remote, f"HEAD:refs/heads/{lease.branch}"],
            cwd=lease.worktree_path,
        )
        if push.returncode != 0:
            raise _git_error("git push failed", push)


def _repo_root(repo_path: str) -> str:
    try:
        return _main_work_tree(repo_path)
    except WorktreeError as exc:
        raise WorktreePoolError(exc.message) from exc


def _acquired(lease: _Lease) -> AcquiredWorktree:
    return AcquiredWorktree(
        lease_id=lease.lease_id,
        pool_id=lease.pool_id,
        slot_id=lease.slot_id,
        worktree_path=lease.worktree_path,
        branch=lease.branch,
    )


def load_managed_worktree_config(path: Path) -> ManagedWorktreeConfig | None:
    """Load ``host.managed_worktrees`` from a host config file."""
    if not path.exists():
        return None
    raw: Any = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    host = raw.get("host") if isinstance(raw, dict) else None
    block = host.get("managed_worktrees") if isinstance(host, dict) else None
    if block is None:
        return None
    if not isinstance(block, dict):
        raise WorktreePoolError("host.managed_worktrees must be a mapping")
    repos_raw = block.get("repos")
    if not isinstance(repos_raw, dict) or not repos_raw:
        raise WorktreePoolError("host.managed_worktrees.repos must be a non-empty mapping")
    idle = block.get("idle_eviction_seconds", 60 * 60)
    if isinstance(idle, bool) or not isinstance(idle, int) or idle < 0:
        raise WorktreePoolError("host.managed_worktrees.idle_eviction_seconds must be >= 0")
    repos: list[ManagedWorktreeRepo] = []
    for repo_id, value in repos_raw.items():
        if not isinstance(repo_id, str) or not repo_id.strip() or not isinstance(value, dict):
            raise WorktreePoolError("each managed repo must be a named mapping")
        base_branch = value.get("base_branch")
        worktrees = value.get("worktrees")
        branch_remote = value.get("branch_remote", "origin")
        if not isinstance(base_branch, str) or not base_branch.strip():
            raise WorktreePoolError(f"managed repo {repo_id!r} requires base_branch")
        if not isinstance(branch_remote, str) or not branch_remote.strip():
            raise WorktreePoolError(f"managed repo {repo_id!r} requires branch_remote")
        if (
            not isinstance(worktrees, list)
            or not worktrees
            or not all(isinstance(item, str) and item.strip() for item in worktrees)
        ):
            raise WorktreePoolError(f"managed repo {repo_id!r} requires worktrees")
        resolved = tuple(str(Path(item).expanduser().resolve()) for item in worktrees)
        repo_root = _repo_root(resolved[0])
        repos.append(
            ManagedWorktreeRepo(
                repo_id=repo_id,
                repo_root=repo_root,
                base_branch=base_branch,
                branch_remote=branch_remote,
                worktrees=resolved,
            )
        )
    return ManagedWorktreeConfig(repos=tuple(repos), idle_eviction_seconds=idle)


def _ensure_base(repo_root: str, base_branch: str) -> None:
    try:
        _ensure_base_resolvable(repo_root, base_branch)
    except WorktreeError as exc:
        raise WorktreePoolError(exc.message) from exc


def _run_git_with_lock_recovery(
    args: list[str],
    *,
    cwd: str,
) -> subprocess.CompletedProcess[str]:
    """Run git once, remove a safe stale index lock, then retry once."""
    result = _run_git(args, cwd=cwd)
    if result.returncode == 0:
        return result
    _remove_stale_index_lock_if_safe(cwd)
    result = _run_git(args, cwd=cwd)
    if result.returncode != 0:
        raise _git_error(f"git {' '.join(args)} failed", result)
    return result


def _remove_stale_index_lock_if_safe(cwd: str) -> None:
    lock_path = _index_lock_path(cwd)
    if not lock_path.exists():
        return
    if _git_processes_active():
        raise WorktreeError(
            f"git index lock exists and git processes are active; refusing to remove {lock_path}"
        )
    lock_path.unlink()


def _index_lock_path(cwd: str) -> Path:
    result = _run_git(["rev-parse", "--absolute-git-dir"], cwd=cwd)
    if result.returncode != 0:
        raise _git_error("git rev-parse --absolute-git-dir failed", result)
    return Path(result.stdout.strip()) / "index.lock"


def _git_processes_active() -> bool:
    try:
        result = subprocess.run(
            ["ps", "-eo", "pid=,comm=,args="],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return True
    if result.returncode != 0:
        return True
    current_pid = str(os.getpid())
    for line in result.stdout.splitlines():
        fields = line.split(None, 2)
        if len(fields) < 2 or fields[0] == current_pid:
            continue
        command = Path(fields[1]).name
        if command == "git" or command.startswith("git-"):
            return True
        if len(fields) == 3:
            args = fields[2].split()
            argv0 = Path(args[0]).name if args else ""
            if argv0 == "git" or argv0.startswith("git-"):
                return True
    return False
