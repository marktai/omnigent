"""Tests for fixed-size host-managed worktree pools."""

from __future__ import annotations

import os
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest

from omnigent.host.worktree_pool import WorktreePoolError, WorktreePoolManager

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


def _index_lock_path(worktree: Path) -> Path:
    git_dir = Path(_git(worktree, "rev-parse", "--git-dir").strip())
    if not git_dir.is_absolute():
        git_dir = (worktree / git_dir).resolve()
    return git_dir / "index.lock"


def test_configure_pool_precreates_target_size(git_repo: Path) -> None:
    manager = WorktreePoolManager()

    status = manager.configure_pool(
        repo_path=str(git_repo),
        base_branch="main",
        target_size=3,
        pool_id="unit",
    )

    assert status.pool_id == "unit"
    assert status.target_size == 3
    assert status.total_slots == 3
    assert status.idle_slots == 3
    assert _worktree_count(git_repo) == 4
    for idx in range(1, 4):
        assert (git_repo.parent / "repo-omnigent-pool" / "unit" / f"slot-{idx}").is_dir()


def test_acquire_never_exceeds_target_size(git_repo: Path) -> None:
    manager = WorktreePoolManager()

    first = manager.acquire(
        repo_path=str(git_repo),
        base_branch="main",
        target_size=2,
        pool_id="cap",
        branch_name="feature/one",
        session_id="conv_1",
        runner_id="runner_1",
    )
    second = manager.acquire(
        repo_path=str(git_repo),
        base_branch="main",
        target_size=2,
        pool_id="cap",
        branch_name="feature/two",
        session_id="conv_2",
        runner_id="runner_2",
    )

    assert first.worktree_path != second.worktree_path
    assert _worktree_count(git_repo) == 3
    with pytest.raises(WorktreePoolError, match="no available slots"):
        manager.acquire(
            repo_path=str(git_repo),
            base_branch="main",
            target_size=2,
            pool_id="cap",
            branch_name="feature/three",
            session_id="conv_3",
            runner_id="runner_3",
        )
    assert _worktree_count(git_repo) == 3


def test_release_restores_slot_for_reuse(git_repo: Path) -> None:
    manager = WorktreePoolManager()
    first = manager.acquire(
        repo_path=str(git_repo),
        base_branch="main",
        target_size=1,
        pool_id="reuse",
        branch_name="feature/one",
        session_id="conv_1",
        runner_id="runner_1",
    )

    manager.release(lease_id=first.lease_id, delete_branch=True)

    second = manager.acquire(
        repo_path=str(git_repo),
        base_branch="main",
        target_size=1,
        pool_id="reuse",
        branch_name="feature/two",
        session_id="conv_2",
        runner_id="runner_2",
    )
    assert second.worktree_path == first.worktree_path
    assert _worktree_count(git_repo) == 2


def test_release_removes_stale_index_lock_before_restore(
    git_repo: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = WorktreePoolManager()
    first = manager.acquire(
        repo_path=str(git_repo),
        base_branch="main",
        target_size=1,
        pool_id="stale-lock",
        branch_name="feature/one",
        session_id="conv_1",
        runner_id="runner_1",
    )
    lock_path = _index_lock_path(Path(first.worktree_path))
    lock_path.write_text("stale")
    monkeypatch.setattr("omnigent.host.worktree_pool._git_processes_active", lambda: False)

    manager.release(lease_id=first.lease_id, delete_branch=True)

    assert not lock_path.exists()
    second = manager.acquire(
        repo_path=str(git_repo),
        base_branch="main",
        target_size=1,
        pool_id="stale-lock",
        branch_name="feature/two",
        session_id="conv_2",
        runner_id="runner_2",
    )
    assert second.worktree_path == first.worktree_path


def test_release_refuses_index_lock_when_git_process_active(
    git_repo: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = WorktreePoolManager()
    acquired = manager.acquire(
        repo_path=str(git_repo),
        base_branch="main",
        target_size=1,
        pool_id="active-lock",
        branch_name="feature/one",
        session_id="conv_1",
        runner_id="runner_1",
    )
    lock_path = _index_lock_path(Path(acquired.worktree_path))
    lock_path.write_text("active")
    monkeypatch.setattr("omnigent.host.worktree_pool._git_processes_active", lambda: True)

    with pytest.raises(WorktreePoolError, match="git processes are active"):
        manager.release(lease_id=acquired.lease_id, delete_branch=True)

    assert lock_path.exists()
    with pytest.raises(WorktreePoolError, match="no available slots"):
        manager.acquire(
            repo_path=str(git_repo),
            base_branch="main",
            target_size=1,
            pool_id="active-lock",
            branch_name="feature/two",
            session_id="conv_2",
            runner_id="runner_2",
        )


def test_release_refuses_dirty_slot_without_origin(git_repo: Path) -> None:
    manager = WorktreePoolManager()
    acquired = manager.acquire(
        repo_path=str(git_repo),
        base_branch="main",
        target_size=1,
        pool_id="dirty-no-origin",
        branch_name="feature/one",
        session_id="conv_1",
        runner_id="runner_1",
    )

    dirty_file = Path(acquired.worktree_path, "dirty.txt")
    dirty_file.write_text("dirty")

    with pytest.raises(WorktreePoolError, match="remote 'origin' is not configured"):
        manager.release(lease_id=acquired.lease_id, delete_branch=True)

    assert dirty_file.read_text() == "dirty"
    assert _git(Path(acquired.worktree_path), "branch", "--show-current").strip() == "feature/one"
    with pytest.raises(WorktreePoolError, match="no available slots"):
        manager.acquire(
            repo_path=str(git_repo),
            base_branch="main",
            target_size=1,
            pool_id="dirty-no-origin",
            branch_name="feature/two",
            session_id="conv_2",
            runner_id="runner_2",
        )


def test_release_commits_and_pushes_dirty_branch_before_reuse(git_repo: Path) -> None:
    remote = _add_origin(git_repo)
    manager = WorktreePoolManager()
    first = manager.acquire(
        repo_path=str(git_repo),
        base_branch="main",
        target_size=1,
        pool_id="dirty-push",
        branch_name="feature/one",
        session_id="conv_1",
        runner_id="runner_1",
    )

    Path(first.worktree_path, "dirty.txt").write_text("dirty")
    manager.release(lease_id=first.lease_id, delete_branch=True)

    assert _git_bare(remote, "show", "refs/heads/feature/one:dirty.txt") == "dirty"
    assert not _git(git_repo, "branch", "--list", "feature/one").strip()
    assert not Path(first.worktree_path, "dirty.txt").exists()
    second = manager.acquire(
        repo_path=str(git_repo),
        base_branch="main",
        target_size=1,
        pool_id="dirty-push",
        branch_name="feature/two",
        session_id="conv_2",
        runner_id="runner_2",
    )
    assert second.worktree_path == first.worktree_path
