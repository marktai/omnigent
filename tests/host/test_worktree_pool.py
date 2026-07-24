"""Tests for fixed-size host-managed worktree pools."""

from __future__ import annotations

import os
import subprocess
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import call, patch

import pytest

import omnigent.host.worktree_pool as worktree_pool
from omnigent.host.worktree_pool import (
    ManagedWorktreeConfig,
    ManagedWorktreeRepo,
    WorktreePoolError,
    WorktreePoolManager,
    load_managed_worktree_config,
)

_GIT_ENV = {
    "GIT_AUTHOR_NAME": "t",
    "GIT_AUTHOR_EMAIL": "t@t",
    "GIT_COMMITTER_NAME": "t",
    "GIT_COMMITTER_EMAIL": "t@t",
}


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=repo,
        env={**os.environ, **_GIT_ENV},
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


@pytest.fixture()
def git_repo(tmp_path: Path) -> Iterator[Path]:
    repo = (tmp_path / "repo").resolve()
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "pool-test@example.com")
    _git(repo, "config", "user.name", "Pool Test")
    (repo / "README.md").write_text("hi")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "init")
    yield repo


def _add_origin(repo: Path) -> Path:
    remote = repo.parent / "remote.git"
    subprocess.run(["git", "init", "--bare", str(remote)], check=True, capture_output=True)
    _git(repo, "remote", "add", "origin", str(remote))
    _git(repo, "push", "-u", "origin", "main")
    return remote


def _git_bare(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "--git-dir", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def _worktree_count(repo: Path) -> int:
    return _git(repo, "worktree", "list", "--porcelain").count("worktree ")


def _precreate_worktrees(repo: Path, count: int) -> tuple[Path, ...]:
    paths = tuple((repo.parent / f"managed-{index}").resolve() for index in range(1, count + 1))
    for path in paths:
        _git(repo, "worktree", "add", "--detach", str(path), "main")
    return paths


def _managed_config(
    repo: Path,
    paths: tuple[Path, ...],
    *,
    idle: int = 3600,
) -> ManagedWorktreeConfig:
    return ManagedWorktreeConfig(
        repos=(
            ManagedWorktreeRepo(
                repo_id="universe",
                repo_root=str(repo),
                base_branch="main",
                branch_remote="origin",
                worktrees=tuple(str(path) for path in paths),
            ),
        ),
        idle_eviction_seconds=idle,
    )


def test_load_managed_worktree_config(git_repo: Path, tmp_path: Path) -> None:
    paths = _precreate_worktrees(git_repo, 2)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "host:\n"
        "  managed_worktrees:\n"
        "    idle_eviction_seconds: 42\n"
        "    repos:\n"
        "      universe:\n"
        "        base_branch: main\n"
        "        branch_remote: origin\n"
        "        worktrees:\n"
        f"          - {paths[0]}\n"
        f"          - {paths[1]}\n"
    )

    loaded = load_managed_worktree_config(config_path)

    assert loaded is not None
    assert loaded.idle_eviction_seconds == 42
    assert loaded.repos[0].repo_id == "universe"
    assert loaded.repos[0].worktrees == tuple(str(path) for path in paths)


def test_managed_pool_adopts_fixed_worktrees_and_resumes_session(git_repo: Path) -> None:
    _add_origin(git_repo)
    paths = _precreate_worktrees(git_repo, 2)
    manager = WorktreePoolManager()
    manager.adopt_managed_config(_managed_config(git_repo, paths))

    first = manager.acquire_managed(
        repo_id="universe",
        branch_name="feature/one",
        session_id="session-1",
        runner_id="runner-1",
    )
    manager.mark_runner_idle("runner-1")
    resumed = manager.acquire_managed(
        repo_id="universe",
        branch_name="feature/one",
        session_id="session-1",
        runner_id="runner-2",
    )
    second = manager.acquire_managed(
        repo_id="universe",
        branch_name="feature/two",
        session_id="session-2",
        runner_id="runner-3",
    )

    assert resumed.lease_id == first.lease_id
    assert resumed.worktree_path == first.worktree_path
    assert manager.workspace_for_session("session-1") == first.worktree_path
    assert second.worktree_path != first.worktree_path
    assert _worktree_count(git_repo) == 3
    with pytest.raises(WorktreePoolError, match="no available slots"):
        manager.acquire_managed(
            repo_id="universe",
            branch_name="feature/three",
            session_id="session-3",
            runner_id="runner-4",
        )


def test_managed_pool_rejects_duplicate_branch_binding(git_repo: Path) -> None:
    _add_origin(git_repo)
    paths = _precreate_worktrees(git_repo, 2)
    manager = WorktreePoolManager()
    manager.adopt_managed_config(_managed_config(git_repo, paths))
    manager.acquire_managed(
        repo_id="universe",
        branch_name="feature/shared",
        session_id="session-1",
        runner_id="runner-1",
    )

    with pytest.raises(WorktreePoolError, match="already bound to session 'session-1'"):
        manager.acquire_managed(
            repo_id="universe",
            branch_name="feature/shared",
            session_id="session-2",
            runner_id="runner-2",
        )


def test_managed_pool_idle_eviction_pushes_and_reuses_slot(git_repo: Path) -> None:
    remote = _add_origin(git_repo)
    paths = _precreate_worktrees(git_repo, 1)
    manager = WorktreePoolManager()
    manager.adopt_managed_config(_managed_config(git_repo, paths, idle=0))
    first = manager.acquire_managed(
        repo_id="universe",
        branch_name="feature/one",
        session_id="session-1",
        runner_id="runner-1",
    )
    (Path(first.worktree_path) / "result.txt").write_text("done\n")
    manager.mark_runner_idle("runner-1")

    second = manager.acquire_managed(
        repo_id="universe",
        branch_name="feature/two",
        session_id="session-2",
        runner_id="runner-2",
    )

    assert second.worktree_path == first.worktree_path
    assert _git_bare(remote, "show", "refs/heads/feature/one:result.txt") == "done\n"


def test_managed_pool_idle_eviction_pushes_clean_unpushed_commit(git_repo: Path) -> None:
    remote = _add_origin(git_repo)
    paths = _precreate_worktrees(git_repo, 1)
    manager = WorktreePoolManager()
    manager.adopt_managed_config(_managed_config(git_repo, paths, idle=0))
    acquired = manager.acquire_managed(
        repo_id="universe",
        branch_name="feature/clean-commit",
        session_id="session-clean",
        runner_id="runner-clean",
    )
    worktree = Path(acquired.worktree_path)
    (worktree / "committed.txt").write_text("committed before eviction\n")
    _git(worktree, "add", "committed.txt")
    _git(worktree, "commit", "-m", "clean local commit")
    assert not _git(worktree, "status", "--porcelain")

    manager.mark_runner_idle("runner-clean")
    assert manager.evict_idle_managed() == ["session-clean"]

    assert (
        _git_bare(remote, "show", "refs/heads/feature/clean-commit:committed.txt")
        == "committed before eviction\n"
    )


def test_refresh_managed_bases_updates_remote_tracking_ref(git_repo: Path) -> None:
    remote = _add_origin(git_repo)
    paths = _precreate_worktrees(git_repo, 1)
    updater = git_repo.parent / "updater"
    subprocess.run(
        ["git", "clone", "-q", "-b", "main", str(remote), str(updater)],
        check=True,
        capture_output=True,
    )
    _git(updater, "config", "user.email", "pool-test@example.com")
    _git(updater, "config", "user.name", "Pool Test")
    (updater / "latest.txt").write_text("latest base\n")
    _git(updater, "add", "latest.txt")
    _git(updater, "commit", "-q", "-m", "advance base")
    _git(updater, "push", "origin", "main")
    expected = _git(updater, "rev-parse", "HEAD").strip()
    assert _git(git_repo, "rev-parse", "origin/main").strip() != expected

    manager = WorktreePoolManager()
    manager.adopt_managed_config(
        ManagedWorktreeConfig(
            repos=(
                ManagedWorktreeRepo(
                    repo_id="universe",
                    repo_root=str(git_repo),
                    base_branch="origin/main",
                    branch_remote="origin",
                    worktrees=tuple(str(path) for path in paths),
                ),
            )
        )
    )

    assert manager.refresh_managed_bases() == {"universe": "origin/main"}
    assert _git(git_repo, "rev-parse", "origin/main").strip() == expected


def test_run_git_with_backoff_retries_transient_failure() -> None:
    transient = subprocess.CompletedProcess(
        args=["git", "fetch"],
        returncode=1,
        stdout="",
        stderr="fatal: cannot lock ref 'refs/remotes/origin/main'",
    )
    success = subprocess.CompletedProcess(
        args=["git", "fetch"],
        returncode=0,
        stdout="",
        stderr="",
    )

    with (
        patch.object(worktree_pool, "_run_git", side_effect=[transient, transient, success]),
        patch.object(worktree_pool.time, "sleep") as sleep,
    ):
        result = worktree_pool._run_git_with_backoff(
            ["fetch", "origin", "main"],
            cwd="/repo",
            operation="fetch managed base origin/main",
        )

    assert result.returncode == 0
    assert sleep.call_args_list == [call(0.5), call(1.0)]


def test_run_git_with_backoff_does_not_retry_permanent_failure() -> None:
    missing = subprocess.CompletedProcess(
        args=["git", "fetch"],
        returncode=128,
        stdout="",
        stderr="fatal: couldn't find remote ref feature/missing",
    )

    with (
        patch.object(worktree_pool, "_run_git", return_value=missing) as run_git,
        patch.object(worktree_pool.time, "sleep") as sleep,
    ):
        result = worktree_pool._run_git_with_backoff(
            ["fetch", "origin", "feature/missing"],
            cwd="/repo",
            operation="fetch session branch origin/feature/missing",
        )

    assert result is missing
    run_git.assert_called_once()
    sleep.assert_not_called()
