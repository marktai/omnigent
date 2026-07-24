# Host-managed worktree mode

## Decision

The Omnigent host owns managed worktrees. The server does not create, size,
lease, reset, or release them.

An operator starts a host with a fixed list of existing git worktrees. When the
server asks the host to start or resume a session for a named repository, the
host selects a worktree, prepares the session branch, and starts the runner in
that directory. The server persists only the repository alias and branch; the
host keeps the physical worktree path private.

This keeps execution policy on the machine that owns the filesystem. The server
only orchestrates sessions and runners.

## Goals

- Cap concurrent managed sessions at the number of configured worktrees.
- Reuse worktrees without creating or deleting them at runtime.
- Keep a session on the same worktree across runner restarts.
- Resume a pushed session branch when a session gets a new worktree.
- Commit and push dirty work before destructive cleanup.
- Reset released worktrees to a known detached base state.
- Keep the existing user-created worktree APIs unchanged.

## Non-goals

- The server does not configure a host's pool.
- The host does not create replacement worktrees.
- Managed mode does not provide elastic capacity.
- This change does not replace direct workspace launches or the existing
  create, list, and remove worktree APIs.
- This change does not generate commit messages with a model. Cleanup uses a
  deterministic message and leaves model-generated messages as follow-up work.

## Host configuration

The host reads managed worktree settings from its local `config.yaml` at
startup:

```yaml
host:
  host_id: 72efa22aa64c4271a959d6c61fad5ae1
  name: managed-host
  managed_worktrees:
    idle_eviction_seconds: 3600
    repos:
      universe:
        base_branch: origin/main
        branch_remote: origin
        worktrees:
          - /home/user/universe-isaac-1
          - /home/user/universe-isaac-2
```

Each key under `repos` is a server-visible repository ID. The host infers the
common git repository from the configured worktrees and rejects startup when:

- a path does not exist;
- a path is not a linked git worktree;
- two entries resolve to the same path;
- worktrees in one repository ID do not share the same main worktree;
- the configured base ref cannot be resolved.

`branch_remote` defaults to `origin`. The number of paths is both the pool size
and the hard concurrency limit. No runtime request can increase it.

The operator creates these worktrees before starting the host. For example:

```bash
git -C ~/universe worktree add --detach ~/universe-isaac-1 origin/main
git -C ~/universe worktree add --detach ~/universe-isaac-2 origin/main
omnigent host
```

## Server request

The existing runner launch endpoint accepts a managed repository ID:

```http
POST /v1/hosts/{host_id}/runners
Content-Type: application/json

{
  "session_id": "session_123",
  "managed_repo": "universe",
  "git": {
    "branch_name": "feature/session-123"
  }
}
```

The server validates that `managed_repo` and git branch options are present. It
then sends one `host.launch_runner` frame containing:

- the binding token and derived runner identity;
- `session_id`;
- `managed_repo`;
- `git_branch`;
- the selected harness.

The server does not send a repository path, pool size, worktree path, lease ID,
or cleanup command. There are no configure, acquire, or release pool frames and
no worktree-pool REST endpoint.

## Launch lifecycle

1. The server atomically binds a runner ID to the session.
2. The server sends start or resume intent to the host.
3. The host checks whether it still has a transient lease for the session.
4. If the lease is still inside the idle-retention window, the host clears the
   idle timestamp and restarts the runner in that slot.
5. Otherwise, the host selects an unassigned configured worktree.
6. The host aborts stale merge or rebase state, removes a stale index lock only
   when no git process is active, runs `git reset --hard`, runs
   `git clean -ffd`, and checks out the configured base in detached mode.
7. The host fetches the requested branch from `branch_remote`.
8. If the remote branch exists, the host checks it out and resets the local
   branch to the remote branch. If it does not exist, the host creates it from
   `base_branch`.
9. The host starts the runner with the selected worktree as
   `OMNIGENT_RUNNER_WORKSPACE` and the branch as `OMNIGENT_RUNNER_GIT_BRANCH`.
10. The host returns the actual workspace and branch in
    `host.launch_runner_result` for launch diagnostics.
11. The server retains `managed://<repo>` as the durable workspace binding and
    stores the branch separately. It never persists the physical worktree path.

The runner exposes the branch in its identity payload. Harnesses and tools can
read it without learning how the host selected the worktree.

## Capacity

A configured worktree can be in one of three states:

- active, with a live runner;
- idle, retained for session resume;
- quarantined, after a git preparation or cleanup failure.

All three count against capacity. When every configured worktree is assigned or
quarantined, the host rejects launch with `managed_worktree_capacity`. The REST
endpoint returns `409 Conflict`.

The host does not queue the request and does not create another worktree. The
caller can retry after a runner exits and its idle lease is evicted, or an
operator can restart the host with more precreated worktrees in its local
configuration.

## Runner disconnect and resume

A runner process can stop without ending the session. On clean exit, crash, an
explicit stop request, or host shutdown, the host marks the session's worktree
idle. It does not reset or release the worktree at that point.

A resume request with the same `session_id`, repository ID, and branch reuses
the same worktree and clears its idle timestamp. A request that tries to change
the repository or branch for an existing session fails instead of mutating the
assignment.

The server continues to own runner connection state. A disconnected runner can
be relaunched, but the server does not decide which worktree to use.

The session has no durable affinity to a physical worktree. Its durable identity
is the repository alias plus branch. A physical slot is only retained while the
runner is active or during the idle-retention window. After eviction, the host
may resume the branch in any available configured slot.

## Idle eviction

The host runs a cleanup sweep every 15 seconds. A session becomes eligible when
its runner has been absent for at least `idle_eviction_seconds`.

For each eligible session, the host:

1. verifies that the expected session branch is checked out;
2. checks for tracked and untracked changes;
3. stages all changes with `git add -A`;
4. commits staged changes as `Omnigent session <session_id>`;
5. pushes `HEAD` to the configured branch on `branch_remote`;
6. aborts merge or rebase state;
7. removes a stale index lock only when no git process is active;
8. runs `git reset --hard`;
9. runs `git clean -ffd`;
10. checks out `base_branch` in detached mode;
11. deletes the local session branch;
12. removes the host-local session assignment;
13. sends `host.managed_session_released` to the server.

The server clears only `runner_id` when the notification comes from the host
currently stored on that session. It retains `host_id`, `managed://<repo>`, and
`git_branch`, so a later message can ask the same host to resume that branch in
any available slot. A delayed notification from an old host cannot affect a
newer binding.

If commit, push, reset, or cleanup fails, the host keeps the assignment and
quarantines the worktree. It does not notify the server or hand the directory to
another session. This favors preserving work over recovering capacity.

## Failed launch

If branch preparation fails, the host returns
`managed_worktree_prepare_failed`. The server clears the failed runner binding.
The worktree remains quarantined when its git state is not safe to reuse.

If the runner process fails before launch completes, the host immediately
finalizes and restores the session worktree. The server receives a failed launch
result and clears the binding. A successful cleanup makes the slot available to
the next request.

## Host restart

The initial implementation keeps active and idle assignments in host memory.
A host process restart therefore loses session-to-worktree affinity. The host
still validates the configured worktrees on startup, but it cannot prove which
session owned a checked-out branch before the restart.

Until host-local assignment persistence is added, operators must not restart a
managed host while sessions have uncommitted work. A follow-up should persist
`session_id`, repository ID, branch, worktree path, runner ID, and idle timestamp
under the host data directory, then reconcile that file with git state before
accepting launches. Reconciliation must preserve an occupied branch rather than
reset it blindly.

## Security

- The server authorizes the host and session before sending launch intent.
- Repository IDs come from host configuration, not request paths.
- The host validates branch names with git ref-format rules.
- The runner receives only its assigned workspace and branch.
- Cleanup refuses to remove an index lock while any git process is active.
- Cleanup never discards dirty work unless commit and push have succeeded.

## Observability

Host logs record:

- configured repository IDs and capacity at startup;
- session, repository ID, slot, branch, and runner ID on assignment;
- capacity and preparation failures;
- runner transitions to idle;
- commit and push failures;
- cleanup and quarantine results;
- eviction notifications sent to the server.

The launch response keeps the physical workspace host-local and persists the
managed repository marker plus branch in the session row, so existing session
inspection and UI surfaces show the host's decision.

## Testing

Unit tests cover:

- configuration parsing and validation;
- adoption of existing worktrees without creating new ones;
- fixed-capacity rejection;
- session affinity across runner restarts;
- branch creation and remote branch resume;
- dirty-work commit and push before cleanup;
- hard reset, `clean -ffd`, detached-base restore, and slot reuse;
- frame round trips for launch intent, resolved workspace, branch, and release
  notification;
- server persistence of the managed repository marker and branch;
- `409 Conflict` for exhausted capacity.

The local end-to-end test starts a real server and host against temporary git
repositories and precreated worktrees. It launches a managed session, writes a
file through the assigned workspace, stops the runner, waits for idle eviction,
verifies the commit on the bare remote, and launches another session into the
same restored slot.

## Rollout

1. Land the protocol and host implementation behind the presence of
   `host.managed_worktrees` in local host configuration.
2. Run hosts without that block in the existing direct-workspace mode.
3. Enable one development host with two precreated worktrees.
4. Verify capacity rejection, resume, push, cleanup, and slot reuse.
5. Add durable host-local assignment state before treating host restarts as a
   supported recovery path.
