"""End-to-end coverage for host-managed fixed git worktree pools."""

from __future__ import annotations

import os
import signal
import subprocess
import time
import uuid
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest
import yaml

from tests._helpers.compat import (
    apply_server_env,
    compat_server_cwd,
    server_executable,
)
from tests.e2e.conftest import (
    find_free_port,
    lookup_agent_id,
    upload_agent,
    wait_for_server,
)
from tests.e2e.helpers import POLL_INTERVAL_S
from tests.e2e.test_host_e2e import (
    _pid_alive,
    _runner_pid_from_daemon_log,
    _spawn_host_daemon,
    _wait_for_host_online,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture()
def pool_live_server(
    tmp_path: Path,
    mock_llm_server_url: str,
) -> Iterator[str]:
    """Start an isolated local server with fast pooled-runner eviction."""
    port = find_free_port()
    db_path = tmp_path / "pool-e2e.db"
    artifact_dir = tmp_path / "artifacts"
    artifact_dir.mkdir()
    server_log = tmp_path / "pool-server.log"
    server_cfg = tmp_path / "pool-server.yaml"
    server_cfg.write_text(
        yaml.safe_dump(
            {
                "llm": {
                    "model": "_policy_llm_",
                    "connection": {
                        "base_url": f"{mock_llm_server_url}/v1",
                        "api_key": "mock-key",
                    },
                }
            }
        )
    )
    env = {
        **os.environ,
        "OPENAI_API_KEY": "mock-key",
        "OPENAI_BASE_URL": f"{mock_llm_server_url}/v1",
        "OMNIGENT_WORKTREE_POOL_IDLE_EVICTION_S": "1",
    }
    apply_server_env(env, _REPO_ROOT)
    log_handle = open(server_log, "w")  # noqa: SIM115
    proc = subprocess.Popen(
        [
            server_executable(),
            "-m",
            "omnigent.cli",
            "server",
            "--port",
            str(port),
            "--database-uri",
            f"sqlite:///{db_path}",
            "--artifact-location",
            str(artifact_dir),
            "--config",
            str(server_cfg),
        ],
        env=env,
        cwd=compat_server_cwd(),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
    )
    base_url = f"http://localhost:{port}"
    try:
        wait_for_server(base_url, timeout=30.0)
    except Exception as exc:
        log_contents = server_log.read_text() if server_log.exists() else ""
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
        log_handle.close()
        raise AssertionError(f"pool e2e server failed; log:\n{log_contents[-4000:]}") from exc
    try:
        yield base_url
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        log_handle.close()


@pytest.fixture()
def pool_http_client(pool_live_server: str) -> Iterator[httpx.Client]:
    """HTTP client for the isolated pool e2e server."""
    with httpx.Client(base_url=pool_live_server, timeout=120) as client:
        yield client


def _write_smoke_agent_yaml(tmp_path: Path) -> Path:
    """Create a minimal mock-backed agent bundle."""
    agent_dir = tmp_path / f"pool-agent-{uuid.uuid4().hex[:8]}"
    agent_dir.mkdir()
    (agent_dir / "agent.yaml").write_text(
        "\n".join(
            [
                f"name: {agent_dir.name}",
                "description: Minimal agent for worktree-pool e2e tests.",
                "executor:",
                "  harness: openai-agents",
                "  model: gpt-5.4",
                "os_env:",
                "  cwd: .",
                "prompt: |",
                "  You are only used to start a runner tunnel in tests.",
                "",
            ]
        )
    )
    return agent_dir


def _init_git_repo(path: Path) -> None:
    """Create a tiny git repository with a ``main`` branch."""
    path.mkdir()
    subprocess.run(["git", "init", "-b", "main"], cwd=path, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "pool-e2e@example.com"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.name", "Pool E2E"], cwd=path, check=True)
    (path / "README.md").write_text("pool e2e\n")
    subprocess.run(["git", "add", "README.md"], cwd=path, check=True)
    subprocess.run(["git", "commit", "-m", "initial"], cwd=path, check=True, capture_output=True)


def _add_origin(repo: Path) -> Path:
    """Add a local bare ``origin`` remote and push ``main``."""
    remote = repo.parent / "remote.git"
    subprocess.run(["git", "init", "--bare", str(remote)], check=True, capture_output=True)
    subprocess.run(["git", "remote", "add", "origin", str(remote)], cwd=repo, check=True)
    subprocess.run(["git", "push", "-u", "origin", "main"], cwd=repo, check=True, capture_output=True)
    return remote


def _git_bare(repo: Path, *args: str) -> str:
    """Run a git command against a bare repository."""
    result = subprocess.run(
        ["git", "--git-dir", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def _create_session(client: httpx.Client, agent_id: str) -> str:
    """Create an unbound session and return its id."""
    resp = client.post("/v1/sessions", json={"agent_id": agent_id}, timeout=60.0)
    resp.raise_for_status()
    return str(resp.json()["id"])


def _wait_for_runner_online(client: httpx.Client, runner_id: str, timeout: float = 30.0) -> None:
    """Poll until a host-spawned runner tunnel is online."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = client.get(f"/v1/runners/{runner_id}/status", timeout=5.0)
        if resp.status_code == 200 and resp.json().get("online") is True:
            return
        time.sleep(POLL_INTERVAL_S)
    raise AssertionError(f"runner {runner_id!r} did not come online")


def _wait_for_session_unbound(
    client: httpx.Client,
    session_id: str,
    timeout: float = 30.0,
) -> dict:
    """Poll until idle eviction clears the session's host binding."""
    deadline = time.monotonic() + timeout
    last: dict | None = None
    while time.monotonic() < deadline:
        resp = client.get(f"/v1/sessions/{session_id}", timeout=5.0)
        resp.raise_for_status()
        last = resp.json()
        labels = last.get("labels") or {}
        if (
            last.get("runner_id") is None
            and last.get("host_id") is None
            and last.get("workspace") is None
            and last.get("git_branch") is None
            and "omnigent.worktree_pool.lease_id" not in labels
        ):
            return last
        time.sleep(POLL_INTERVAL_S)
    raise AssertionError(f"session {session_id!r} was not evicted; last={last}")


def test_host_worktree_pool_capacity_eviction_and_reuse(
    pool_live_server: str,
    pool_http_client: httpx.Client,
    tmp_path: Path,
    mock_llm_server_url: str,
) -> None:
    """Real server + real host daemon exercise fixed-pool lifecycle."""
    daemon = _spawn_host_daemon(
        tmp_path=tmp_path,
        live_server=pool_live_server,
        mock_llm_server_url=mock_llm_server_url,
    )
    host_proc = daemon.proc
    repo = tmp_path / "repo"
    _init_git_repo(repo)
    remote = _add_origin(repo)

    try:
        _wait_for_host_online(pool_http_client, daemon.host_id, timeout=30.0)
        agent_name = upload_agent(pool_http_client, _write_smoke_agent_yaml(tmp_path))
        agent_id = lookup_agent_id(pool_http_client, agent_name)

        configure = pool_http_client.post(
            f"/v1/hosts/{daemon.host_id}/worktree-pools",
            json={
                "repo_path": str(repo),
                "base_branch": "main",
                "pool": {"target_size": 1, "pool_id": "main-pool"},
            },
            timeout=60.0,
        )
        configure.raise_for_status()
        assert configure.json()["total_slots"] == 1
        assert configure.json()["idle_slots"] == 1
        assert (
            tmp_path / "repo-omnigent-pool" / "main-pool" / "slot-1" / ".git"
        ).exists()

        first_session = _create_session(pool_http_client, agent_id)
        first_launch = pool_http_client.post(
            f"/v1/hosts/{daemon.host_id}/runners",
            json={
                "session_id": first_session,
                "workspace": str(repo),
                "git": {
                    "branch_name": "pool-e2e/first",
                    "base_branch": "main",
                    "pool": {"target_size": 1, "pool_id": "main-pool"},
                },
            },
            timeout=60.0,
        )
        first_launch.raise_for_status()
        first_runner = str(first_launch.json()["runner_id"])
        _wait_for_runner_online(pool_http_client, first_runner)

        first_snapshot = pool_http_client.get(f"/v1/sessions/{first_session}").json()
        assert first_snapshot["workspace"].endswith("repo-omnigent-pool/main-pool/slot-1")
        assert first_snapshot["git_branch"] == "pool-e2e/first"
        assert first_snapshot["labels"]["omnigent.worktree_pool.id"] == "main-pool"
        Path(first_snapshot["workspace"], "agent-output.txt").write_text("saved before cleanup")

        blocked_session = _create_session(pool_http_client, agent_id)
        blocked_launch = pool_http_client.post(
            f"/v1/hosts/{daemon.host_id}/runners",
            json={
                "session_id": blocked_session,
                "workspace": str(repo),
                "git": {
                    "branch_name": "pool-e2e/blocked",
                    "base_branch": "main",
                    "pool": {"target_size": 1, "pool_id": "main-pool"},
                },
            },
            timeout=60.0,
        )
        assert blocked_launch.status_code == 409, blocked_launch.text
        assert "no available slots" in blocked_launch.text

        first_pid = _runner_pid_from_daemon_log(daemon.daemon_log)
        assert first_pid is not None, daemon.daemon_log.read_text()
        os.kill(first_pid, signal.SIGKILL)
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline and _pid_alive(first_pid):
            time.sleep(POLL_INTERVAL_S)
        assert not _pid_alive(first_pid), f"runner pid {first_pid} did not die"
        _wait_for_session_unbound(pool_http_client, first_session)
        assert (
            _git_bare(remote, "show", "refs/heads/pool-e2e/first:agent-output.txt")
            == "saved before cleanup"
        )

        reuse_session = _create_session(pool_http_client, agent_id)
        reuse_launch = pool_http_client.post(
            f"/v1/hosts/{daemon.host_id}/runners",
            json={
                "session_id": reuse_session,
                "workspace": str(repo),
                "git": {
                    "branch_name": "pool-e2e/reuse",
                    "base_branch": "main",
                    "pool": {"target_size": 1, "pool_id": "main-pool"},
                },
            },
            timeout=60.0,
        )
        reuse_launch.raise_for_status()
        _wait_for_runner_online(pool_http_client, str(reuse_launch.json()["runner_id"]))
        reuse_snapshot = pool_http_client.get(f"/v1/sessions/{reuse_session}").json()
        assert reuse_snapshot["workspace"].endswith("repo-omnigent-pool/main-pool/slot-1")
        assert reuse_snapshot["git_branch"] == "pool-e2e/reuse"
    finally:
        if host_proc.poll() is None:
            host_proc.send_signal(signal.SIGTERM)
            try:
                host_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                host_proc.kill()
                host_proc.wait(timeout=5)
