"""Tests for fixed-size host-managed worktree pools."""

from __future__ import annotations

import os
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest

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
    assert second.worktree_path != first.worktree_path
    assert _worktree_count(git_repo) == 3
    with pytest.raises(WorktreePoolError, match="no available slots"):
        manager.acquire_managed(
            repo_id="universe",
            branch_name="feature/three",
            session_id="session-3",
            runner_id="runner-4",
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
