# Host-managed git worktree pool

## Context

Isaac's Arca agent uses one long-lived agent process per git worktree. A task is
claimed by an agent that already owns a clean checkout, the agent prepares that
checkout for the task branch, runs the model, commits and pushes, then restores
the checkout to a detached base for reuse.

Omnigent already has host-side git worktree support, but it is session scoped:

- `omnigent/host/git_worktree.py` creates, lists, and removes linked worktrees.
- `omnigent/host/connect.py` handles `host.create_worktree`,
  `host.remove_worktree`, and `host.list_worktrees`.
- `omnigent/server/routes/sessions.py` can create a worktree before a new
  session row is inserted.
- `omnigent/server/routes/hosts.py` can create a worktree at bind time for
  `POST /v1/hosts/{host_id}/runners`, then roll it back when binding or launch
  fails.
- `omnigent/runner/routing.py` does not allocate runners. It only routes to the
  `runner_id` already stored on the conversation.

The current system can create one worktree per session. It does not yet keep a
bounded pool of reusable worktrees or assign runners to idle pool slots.

## Isaac behavior to copy

The notes in `~/tmp/isaac_git_workflow` map Isaac's git lifecycle. The parts
that matter for Omnigent are:

- Bootstrap creates numbered detached worktrees, for example
  `universe-isaac-1`, using `git worktree add <path> databricks/master --detach`.
- Each worker has exactly one worktree and all shell access is pinned to that
  root.
- Task prep chooses a fast path when the worktree already matches the session,
  otherwise it checks out or creates the target branch from the requested base.
- Pre-turn verification records the current state, cleans dirty residue, fetches
  the base branch when needed, and warms the index.
- Post-turn handling stages, commits, and pushes through a controlled git path.
- Cleanup restores the worktree to a detached base so the next task starts from
  a known state.
- Crash recovery aborts merge or rebase state, resets hard, cleans untracked
  files, removes stale index locks only after checking no git process is active,
  and can quarantine bad worktrees.

For Omnigent, the important abstraction is not Isaac's Scala implementation. It
is the lease lifecycle: prepare, lease, run, finalize, restore, release.

## Proposed Omnigent shape

Add a host-owned `WorktreePoolManager` next to `omnigent/host/git_worktree.py`.
Each pool has a configured target size, and the host precreates that many
worktrees before serving leases. The server asks the host for a lease, and the
host returns a concrete workspace path. The runner launch path remains unchanged
after that: the runner receives `OMNIGENT_RUNNER_WORKSPACE=<leased path>` and
connects with its token-bound `runner_id`.

The pool invariant is fixed capacity: for a given `(repo_root, base_branch,
pool_id)`, the host keeps exactly `target_size` usable slots when possible.
That same number is also the hard cap for concurrent pooled runners. Slots may
be `leased`, `restoring`, or `quarantined`, but they still count toward the
configured limit. If a slot must be removed from the pool because the directory
is gone, git metadata is corrupt, or an operator deletes it, the host creates a
replacement slot to bring the pool back to `target_size`.

Pool slot state should be host-local and durable enough to survive daemon
restart:

| Field | Purpose |
| --- | --- |
| `pool_id` | Stable pool name, usually derived from repo root and base branch |
| `slot_id` | Stable slot name, for example `universe-1` |
| `repo_root` | Main checkout or bare base used to create worktrees |
| `worktree_path` | Linked worktree path assigned to a runner |
| `base_branch` | Default branch to restore to when idle |
| `target_size` | Configured slot count and hard concurrency cap |
| `state` | `idle`, `preparing`, `leased`, `dirty`, `restoring`, `quarantined` |
| `lease_id` | Host-generated token for acquire and release idempotency |
| `session_id` | Conversation currently holding the lease |
| `runner_id` | Runner currently using the slot |
| `branch` | Current checked-out task branch, if any |
| `updated_at` | Reaper and debugging signal |

The host should be authoritative for pool state because it can inspect git,
runner processes, and local filesystem health. The server should persist only
what it already needs for routing and UI: `host_id`, `workspace`, `git_branch`,
and `runner_id`. If lease recovery needs server visibility later, add labels
such as `omnigent.worktree_pool.id` and `omnigent.worktree_pool.lease_id`
before adding schema columns.

## Protocol change

Keep the current direct worktree API for user-created branch worktrees. Add a
separate pool API so the semantics are not confused:

- `host.configure_worktree_pool`: server to host. Inputs: `repo_path`,
  `base_branch`, `pool_id`, and `target_size`. The host precreates or
  reconciles exactly that many slots.
- `host.configure_worktree_pool_result`: host to server. Outputs: `pool_id`,
  `target_size`, current slot counts, or a structured failure.
- `host.acquire_worktree`: server to host. Inputs: `repo_path`, `base_branch`,
  optional `pool_id`, optional `branch_name`, `session_id`, `runner_id`, and pool
  policy.
- `host.acquire_worktree_result`: host to server. Outputs: `lease_id`,
  `worktree_path`, `branch`, `slot_id`, or a structured failure.
- `host.release_worktree`: server to host. Inputs: `lease_id`, `session_id`,
  `runner_id`, and cleanup mode.
- `host.release_worktree_result`: host to server. Outputs final state or error.
- `host.list_worktree_pool`: optional debug and UI endpoint.

`POST /v1/hosts/{host_id}/runners` is the best first integration point. It
already owns the bind-time sequence:

1. Validate the host owner and session owner.
2. Validate the workspace against the agent `os_env.cwd`.
3. Optionally prepare a worktree.
4. Atomically bind `runner_id`.
5. Persist `host_id`, `workspace`, and `git_branch`.
6. Send `host.launch_runner`.
7. Roll back if launch fails.

For pool mode, replace step 3 with `host.acquire_worktree`. On failure after
acquire, call `host.release_worktree` instead of `host.remove_worktree`.
`git.pool` launch requests must include `base_branch` so the lease matches the
precreated pool identity.

`POST /v1/hosts/{host_id}/worktree-pools` configures and precreates a fixed
pool before runner assignment. Its body is `repo_path`, `base_branch`, and
`pool: {target_size, pool_id?}`. It returns `pool_id`, `target_size`,
`total_slots`, and `idle_slots`.

Create-time `POST /v1/sessions` can follow after the bind endpoint works. The
same helper should be used by both paths so rollback rules do not drift.

## Pool preparation

The host should create pool slots during configuration, then keep the configured
target count:

1. Resolve the main worktree with `git worktree list --porcelain`, matching the
   current helper behavior.
2. Create pool slots under a stable sibling directory, for example
   `<repo>-omnigent-pool/<pool_id>/<slot_id>`.
3. Add slots detached at `origin/<base>` or the user-selected base ref.
4. Run `git worktree prune` before creating slots to clear stale registrations.
5. Reconcile during explicit configuration, before acquire, and after release:
   count existing slots, create missing slots until the pool reaches
   `target_size`, and never create beyond that limit.
6. Before leasing, run a bounded restore:
   `merge --abort`, `rebase --abort`, stale lock check, `reset --hard`,
   `clean -ffd`, fetch base if online, checkout detached base.
7. If restore fails, mark the slot `quarantined` and try another slot.

`target_size` is the hard ceiling on total slots for that pool. If all target
slots are leased or unavailable, acquire should return a capacity error instead
of creating an extra worktree.

This should reuse the existing argv-only git runner style in
`omnigent/host/git_worktree.py`, not shell strings.

## Branch and commit policy

There are two viable modes:

- Session branch mode: acquire a clean slot, create or checkout
  `branch_name`, run the agent there, and leave commit or PR creation to the
  harness. This matches current Omnigent behavior most closely.
- Isaac-like controlled commit mode: the host owns stage, commit, push, and
  restore after the turn. This is better for fully autonomous coding workers
  but is a larger product boundary because Omnigent native harnesses currently
  let the vendor CLI run git directly.

Start with session branch mode during the active runner lifetime. It keeps
Omnigent compatible with existing native harnesses and uses the pool for
checkout isolation and slot reuse. When an idle pooled session is evicted and
the slot is about to be destructively cleaned up, the host finalizes dirty
branch work first: stage all changes, commit them to the session branch, and
push `HEAD:refs/heads/<branch>` to the existing `origin` remote. Omnigent does
not configure or mutate remotes. If `origin` is missing or the push fails, the
release fails and the session stays bound to the dirty slot.

Fully controlled per-turn commit and PR creation remains a later opt-in worker
mode. The current implementation finalizes only before cleanup would otherwise
discard branch changes.

## Release rules

A lease should be released when:

- session delete succeeds,
- host launch fails after acquire,
- runner exits before connecting,
- explicit runner stop succeeds,
- a stale runner binding is cleared and no replacement is launched for the same
  session.

A lease should not be released just because a turn completes. In Omnigent, a
runner is session-bound and may handle later turns for the same session. Release
on turn completion would break native TUI state and transcript continuity.

The first implementation releases on session delete, host launch rollback, lost
bind races, and `host.runner_exited` before runner tunnel connect. Explicit
runner stop still needs a release hook if pooled sessions get a stop endpoint.

On release, restore the worktree to detached base if the session is done. If
there are uncommitted changes and `delete_branch=true`, commit and push the
branch before restoring. Only after push succeeds may the host detach to base,
delete the local branch, and return the slot to the idle pool. If finalize or
push fails, keep the session binding and leave the slot unavailable so the user
does not lose branch work.

## Safety boundaries

- Keep host ownership checks before every host frame, as `_host_launch.py`
  already does.
- Keep workspace boundary validation before acquire, using the source repo path.
- Reject branch names with the existing `validate_branch_name`.
- Run git via argv lists and `--end-of-options` for user-supplied refs.
- Keep pool worktrees under a host-owned pool root, never inside another linked
  worktree.
- Pin runner cwd and tool access to the leased path.
- Never let one live runner share a worktree with another live runner.
- Treat missing host support as a clean version-skew error, not a fallback to
  shared workspace execution.

## Testing plan

Unit tests:

- Extend `tests/host/test_git_worktree.py` or add `tests/host/test_worktree_pool.py`
  for slot creation, lease state transitions, restore, quarantine, stale lock
  handling, and linked-worktree root resolution.
- Add frame encode/decode tests for acquire and release frames.

Server integration tests:

- Mirror `tests/server/integration/test_host_runner_launch_worktree.py` with a
  fake host that replies to acquire, launch, and release frames.
- Assert `POST /v1/hosts/{host_id}/worktree-pools` sends configure and returns
  fixed slot counts.
- Assert successful launch persists the leased path and branch.
- Assert launch failure releases the lease and clears `runner_id`, `host_id`,
  `workspace`, and `git_branch`.
- Assert concurrent launch attempts cannot lease the same slot.
- Assert host ownership and session ownership checks happen before acquire.

Host integration tests:

- Use a real temporary git repo and a real host daemon subprocess.
- Launch two sessions into the same source repo and assert different pool slots.
- Stop or delete one session and assert its slot returns to `idle`.
- Dirty one slot, release it, and assert the next lease skips or quarantines it.

Agentic tests:

- Reuse the host-native e2e pattern from `tests/e2e/test_host_*_native*_e2e.py`.
- Add an opt-in e2e for a coding prompt that writes a marker file in one
  session, starts another session from the same repo, and proves the second
  runner does not see uncommitted residue from the first.
- Add a fork/resume e2e using pool mode once the bind endpoint supports it.
- Keep the broader capability work in the harness bench. The worktree pool tests
  should prove isolation, lease lifecycle, and native runner continuity, not
  every harness capability.

## Rollout order

1. Add host pool manager and unit tests with no server wiring.
2. Add fixed-size pool configuration and reconciliation.
3. Add host frames and server proxy helpers.
4. Wire `POST /v1/hosts/{host_id}/runners` pool mode behind an explicit request
   flag.
5. Add rollback and release paths for failed launch, stop, and delete.
6. Add create-time session support.
7. Add e2e coverage for one native harness and one SDK or subprocess harness.
8. Add debug listing in the host UI or CLI once state exists.

## Open questions

- Should pool target size be configured per host, per repo, or per launch
  request?
- Should dirty release preserve user changes by default, or should pool mode be
  explicitly destructive because the pool owns the checkout?
- Should managed sandboxes prewarm pool slots during provisioning, or should the
  first session lazily create them?
- Do we need a server-visible lease table, or are session labels enough until
  the pool UI needs cross-replica observability?
