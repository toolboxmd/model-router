# Durable local runner

The runner behind Prism (Model Router). A planner in a T3 thread hands a
prepared task to the runner; a dispatcher (Luna) owns the job from then on.
Every dispatcher and worker turn runs as a T3 child thread of the planner
thread, so every agent a job starts is visible and reachable in T3. Python
standard library only; the runner talks to one T3 server over HTTP and opens
no listeners.

AgentsMD and the target project own workflow, authority, proof, and review.
The runner persists the handoff, follows one versioned route policy and the
T3 provider snapshot, and cannot grant new authority.

## Requirements

Python 3.11 or newer, Git, and a reachable T3 server (the Chromeria fork)
whose providers can run the routes: server URL from `--t3-server-url`, else
`T3_SERVER_URL`, else `http://127.0.0.1:3773`; bearer token from
`T3_SERVER_TOKEN`, else `t3 auth session issue` (honoring `T3_BIN` and
`T3CODE_HOME`, `T3_HOME` accepted as an alias, so an isolated server with a
`/tmp` home issues its own token). The token never lands in the ledger.
Without a token or a reachable server a turn blocks as `t3_unavailable`.
No credentials ship in this package; the provider logins live in T3.

## Public CLI

Set `MODEL_ROUTER_ROOT` to the installed plugin's real root. Its launcher
resolves its bundled runtime without changing the caller's working
directory. State belongs outside the plugin cache. `python -m runner` runs
from a source checkout. Chromeria's Prism tools (`prism_submit`,
`prism_status`, `prism_questions`, `prism_answer`) call this CLI for the
planner thread; their descriptions carry the usage guidance.

```
"$MODEL_ROUTER_ROOT/bin/model-router" --state-dir DIR submit --request-id ID (--task JSON | --task-file F) \
  --workspace PATH --planner-session SID --planner-t3-thread TID [--t3-server-url URL] \
  [--planner-harness claude|codex|opencode|grok|t3] [--planner-model M] [--planner-effort E] \
  [--lane L | --route R] [--max-attempts N] [--timeout-secs S] \
  [--job-kind ordinary|experiment|replay] [--replay-of ID] \
  [--handoff-summary TEXT | --handoff-summary-file F] [--start | --no-start]
"$MODEL_ROUTER_ROOT/bin/model-router" --state-dir DIR start --request-id ID
"$MODEL_ROUTER_ROOT/bin/model-router" --state-dir DIR status --request-id ID
"$MODEL_ROUTER_ROOT/bin/model-router" --state-dir DIR questions --request-id ID [--all | --clear QID]
"$MODEL_ROUTER_ROOT/bin/model-router" --state-dir DIR answer --request-id ID --qid Q --answer TEXT
"$MODEL_ROUTER_ROOT/bin/model-router" --state-dir DIR cancel --request-id ID
"$MODEL_ROUTER_ROOT/bin/model-router" --state-dir DIR recover (--request-id ID | --all)
"$MODEL_ROUTER_ROOT/bin/model-router" --state-dir DIR result --request-id ID
"$MODEL_ROUTER_ROOT/bin/model-router" --state-dir DIR capacity [--clear ROUTE] [--t3-server-url URL]
```

`submit` requires `--planner-t3-thread`: jobs run as T3 threads of that
planner thread, and questions and the terminal report are posted into it.
`--planner-harness`, `--planner-model` and `--planner-effort` describe the
agent inside the planner thread and are recorded as evidence only.
`submit` commits the task, workspace claim, policy identity, planner thread,
route, and attempt budget before it acknowledges. The same ID and payload
return the existing job, also with `--start`; a changed payload conflicts.
A new job's workspace must be an existing directory. A workspace that
equals, contains, or lies inside the workspace of an active or cancelling
job, or of a job whose controller still lives, is rejected; workspaces are
compared by device and inode along their ancestor chain, so symlinks, case
variants, and firmlink aliases on macOS are the same workspace.
`--timeout-secs` is accepted for compatibility and recorded, but bounds
nothing: agent turns and jobs carry no elapsed deadline (#88).

`--lane` picks an implementation lane: `small`, `default`, `hard` (Prism's
easy, medium, hard). `--route` names one implementation route directly.
Without either the default lane applies. Every policy lane starts on Muse on
Zen free, which carries no concurrency cap, so parallel jobs open parallel
Muse free threads by design; the `fewest running jobs` spread applies only
among routes with a `max_concurrent` cap. The `critical` lane is
planner-executed and `submit` rejects it: do that step in the planner
thread, then submit the remainder. `--start` launches a detached controller
after the commit.

The task JSON carries the goal and, optionally: `proof` (the project's own
proof command), `baseline_proof: false` (skip the base-commit check below),
`acceptance` (required acceptance evidence the completion envelope must
carry as `acceptance_evidence`), `draft_pr_allowed`, `handoff_summary`,
`links` (`{"rel", "href"}` references for the worker), and
`observer_task_id` (a stable Agent Observer task identity shared by related
jobs). All of them are preserved verbatim with the job. `--handoff-summary`
stores the durable handoff summary; without it the summary is derived from
the task packet.

`capacity` lists the error marks with their original provider evidence and
the T3 snapshot view (per route: eligible, ineligible with the reason, or
exhausted and degraded with the window and until when; `unknown` when the
snapshot cannot be read). `--clear ROUTE` forgets a route's marks after the
operator checked the provider allowance.

## Routing: policy, Prism preferences, and the T3 snapshot

`runner/policy.py` names each route as a T3 model selection (provider
instance, model, effort) with its family, pool, caps, and context window,
and orders them per stage: `dispatch`, the three implementation lanes,
`correction`, and `recovery`. These orders are the default role
preferences.

Before each controller step the runner reads the Prism provider snapshot,
`GET /api/prism/snapshot?projectId=<planner project>` with the same bearer
token (toolboxmd/t3code#19; `runner/t3snapshot.py`, cached for 30 seconds):

- **Eligibility.** A route is eligible when its T3 instance is present and
  enabled and offers the route's model. Turning a model off in T3 Providers
  removes it from routing.
- **Role preferences.** A non-empty Prism list for a role and lane replaces
  the policy order for that stage: `worker` easy, medium and hard for the
  small, default and hard lanes; `dispatcher`, `correction` and `recovery`
  for the job's lane. The first entry is the primary, the rest are
  fallbacks. An entry that matches a policy route keeps its caps; any other
  runs as `t3:<instance>:<model>@<effort>`.
- **Limits.** A pool's meter is one T3 driver's usage windows (`go` is
  OpenCode's single account meter, `codex`, `claude` and `xai` their own
  providers; Zen free has no reader). A window at 100 percent with a future
  `resetsAt` rests every route on that meter until then; at 100 percent
  without `resetsAt` it rests until a later read says otherwise; at 80
  percent or more the route is degraded; a passed `resetsAt` is eligible.
- **Unknown.** A snapshot that cannot be read (an older server without the
  endpoint, unreachable, a bad body) filters nothing: the policy defaults
  and the error marks apply.

Error marks come from turn errors. An exhaustion error rests its whole pool
(a Go limit never moves laterally to another Go model) until the reset the
error carries: OpenCode's retry `next` (epoch milliseconds), a `resetsAt`
or `reset_at` value, or a `retry_after` delay; an error naming none assumes
one 5-hour window, flagged `assumed`. Prose is never parsed for a reset.
Every mark expires at its reset on its own; a successful turn clears the
assumed marks on its pool. Overload and stalls rest the one route for the
15-minute cooldown.

Signal classes: `exhausted` moves the same model to the next pool
(`next_pool`, for example Muse free to Muse Go) or else the next family;
`overloaded` and `stalled` move to the next family in the lane; `context`
moves to a larger-context route in the lane; `hard` ends the turn as failed.
Moves stay inside the job's lane and skip exhausted, degraded, one-turn
routes already used, and full capped routes; with nothing eligible the job
blocks as `capacity_exhausted` with the route and signal. The correction
route sits in no lane: a capacity signal on it moves into the job's lane
(another family first). A preflight before every worker turn and dispatch
applies the same rules before any thread starts.

## T3 threads

A job's threads form a tree (#113). The dispatcher thread is a child of the
planner thread; worker, correction and recovery threads are children of the
dispatcher thread (the planner thread when no dispatcher thread was saved).
A child id follows the fork convention `sub.<parent>.<suffix>` and the
`thread.create` payload also carries `parentThreadId`. Every child's first
message names the job and links the planner thread. Dispatcher threads run
in T3's default interaction mode with runtime `auto` (OpenCode `plan`
agent); worker threads run `full-access` (OpenCode `build` agent). Effort
travels under each adapter's own option id (`reasoningEffort` for Codex and
Grok, `effort` for Claude, `variant` for OpenCode). The thread ids are saved
before the watch, so a restarted controller adopts a live turn instead of
starting a second writer.

Liveness is activity-based: a turn is healthy while assistant tokens stream
(the message's `updatedAt` advances) or a tool or task call is open
(`tool.started` until `tool.completed`/`tool.denied`, `task.started` until
`task.completed`), so a long test run is never a stall. A tool held behind
an unresolved approval or user-input request is not running. Silence with
no running tool past about a minute (`T3_SILENCE_SECS`, 60) is probed at
once with a fresh snapshot read; explicit provider errors act at once. A
message posted onto an existing thread is watched only through the turn it
starts. A stalled worker turn is interrupted and confirmed idle before any
route move. After a T3 server restart with `continueThreadsAfterServerUpdate`
on, interrupted Claude, Codex and OpenCode turns continue on the same
thread; Grok turns are re-sent by the router.

## Flow

1. **Baseline proof.** Before the first dispatch the task's `proof` runs
   once on `base_commit` (the workspace HEAD at submit): in place when the
   workspace is clean at that commit, so the project's installed
   dependencies are used, otherwise in a scratch detached worktree that is
   removed afterwards. A proof that already fails on the base blocks before
   any thread starts as `baseline_proof_failed: rc=<n>` with the failing
   tail (`outputs/<id>/baseline-proof.log`), so the planner fixes the packet
   or main once. The result is recorded (`baseline_proof` in the controller
   state and event) and never rerun. It is skipped, and the reason
   recorded, without a proof command, a base commit, a Git workspace, or
   with `"baseline_proof": false`.
2. **Dispatch.** The dispatcher thread starts on the first eligible
   dispatch route (Luna max on Codex, then Luna on Go in OpenCode plan
   mode). A dispatch route known exhausted or resting is skipped before the
   thread starts; a capped fallback route is reserved first
   (`max_concurrent`) or blocks as `capacity_exhausted`. An exhaustion
   error on the first dispatch marks the pool and moves to the fallback in
   the same step.
3. **Envelope.** Luna replies with one envelope: `planner_question`,
   `implementation`, or `completion` (`research` is reserved and blocks).
   The action is the last complete JSON object of the turn's last assistant
   message (prose then envelope, optionally fenced; a tail missing at most
   three closers is completed); the raw text stays in the ledger. A turn
   with no envelope blocks quoting its first 200 characters. The envelope
   is saved before its effect.
4. **Planner question.** The question is saved, then posted into the
   planner thread (`thread.turn.start`) with the stored handoff summary
   before it, and the planner's reply in that turn is persisted as the
   answer; the dispatcher thread then resumes with it. The posted message
   id is recorded first, so a recovering controller adopts the same post
   instead of asking twice. A reply that never comes leaves the question
   pending and blocks as `planner_question_pending`; `answer` plus
   `recover` continues. A stored answer is reused only when the stored
   prompt matches; a reused `qid` with a different prompt blocks with
   `planner_question_conflict`, cleared by `questions --clear QID` or a new
   qid. When the planner's reply fails after the question was answered
   through `answer`, the stored public answer wins.
5. **Implementation.** One worker thread runs on the job route. The worker
   commits as it works, pushes the branch, opens exactly one PR without
   merging it, and reports the PR URL. Its report returns to the dispatcher
   thread as structured evidence (paths, route, classes, candidate commit,
   the job branch's commits since base, and any open branch PR), never the
   worker's prose; finished work on the branch is never mistaken for no
   work.
6. **Completion.** The terminal result is saved before acknowledgment, but
   never while the latest turn's proof failed (`completion_refused: proof
   failed rc=<n>`, or `proof timed out rc=124`), while the bound proof is
   missing, skipped, or stale, while named acceptance evidence is missing,
   or while the PR check fails (missing, closed, wrong repository, another
   commit, a second PR identity, or a draft without `draft_pr_allowed`). A
   refusal hands the evidence back to the dispatcher once and blocks if it
   insists. The PR check reads the PR live with `gh pr view` once per
   completion; an unavailable check refuses as unverified. A verified PR URL
   becomes the job's single PR identity: correction and recovery update
   that PR, never open a second. An ordinary job in a workspace without an
   origin push remote completes without a PR; experiment and replay jobs
   need none. Nothing merges the PR.
7. **Terminal report.** Every terminal state (succeeded with the PR URL,
   blocked, failed, cancelled with the reason) is posted once into the
   planner thread with the request id and handoff summary. Delivery runs
   after the terminal persist and never changes the job's status or result;
   the delivered record is keyed by the terminal event, so a retry of the
   same event posts nothing, a later terminal event posts again, and
   recovery posts no duplicate. A record that cannot be written is never
   reported as delivered.

The controller runs at most 12 transitions per launch and then blocks with
`controller_step_budget_exhausted`, which `recover` restarts with a fresh
launch budget. A job budget of 48 transitions across all launches blocks
with `job_step_budget_exhausted`, which stays blocked. The default attempt
budget is 5 launches.

**Escalation ladder.** A turn fails when the worker or provider errors (not
a capacity signal, which moves routes instead) or when the task's own proof
exits non-zero. The first failure leads to a correction in the same worker
thread on the same route. The second leads to a fresh correction on the
correction route. The third is the single escalation to the recovery stage;
its pool moves are not second escalations, and recovery never reuses a
rung the job already ran. An ordinary implementation envelope never moves
the job, so a repeated default cannot undo a correction or recovery move.
Every rung move records a `recovery_decision` event; `recovery_next_attempt`
links it to the next attempt's seq and `recovery_attempt_result` keeps that
attempt's outcome. When every rung was used, the evidence returns to the
planner as a concrete decision through a planner question in its thread on
the next step (the decision required, the evidence, attempted remedies, the
eligible dispatcher routes, and the dispatcher's recommendation), never a
request for the planner to implement. The planner answers with direction
(an approach, or an eligible route the dispatcher relays as
`directed_route`), which permits exactly one authorized attempt; a failure
after it ends the job as `failed` with `ESCALATION_EXHAUSTED` and the same
decision content. The candidate stays dispatcher-owned: a failure never
authorizes the planner to take over implementation, debugging, test
execution, or verification. A dispatcher-directed route is assigned only
when it is assignable (an implementation-lane, correction, or recovery
route) and has capacity; anything else is rejected with a
`planner_route_rejected` event, never silently substituted.

## State

Each worker turn writes `outputs/<request_id>/turn-<seq>/` (retries on the
same seq use `turn-<seq>-1`, ...): `report.json` (route, policy version,
model and effort, the T3 thread id, changed files, the workspace HEAD the
evidence was taken against, proof command, exit code, proof class and
timestamps, failure class, blockers, a redacted worker summary),
`proof.log` (the task's proof run through `/bin/sh -c` in the workspace with
the runner's environment, or the truthful reason it was skipped),
`diff.patch` (`git diff HEAD` plus untracked files), and `worker.txt` (the
worker's full text, redacted). Capacity turns and failed turns skip the
suite (`proof_class skipped` with its reason). A proof that runs past its
budget has its whole process group stopped and classifies `timeout`
(rc 124), apart from `not_found` (rc 127). The proof group is recorded
durably while it runs (`proof-owner.json`), so cancel drains it and
`recover` blocks on unresolved proof ownership instead of treating the job
as stopped. Every executed proof is also a ledger row with
`stage='verification'` (kind `proof`) and its timestamps, exit code, proof
class and elapsed time, so Agent Observer measures verification outcomes.
Failure classes are timeout, stall, provider (exhausted, overloaded,
context), infrastructure, implementation, and verification; cancellation
lives on the job; unknown stays unknown.

Jobs record their kind (`ordinary`, `experiment`, `replay` with
`--replay-of`), the planner thread and harness, the workspace commit at
submit (`base_commit`) and completion (`head_commit`), and the T3 thread of
every slot (`t3_threads` in the controller state, `t3_thread` events).
`status` shows the job, launches, questions, recent events, the output
tail, capacity marks, and the installed runtime; `result` also lists the
turn reports and verification rows.

`--state-dir` (or `DURABLE_RUNNER_STATE_DIR`) is made absolute and is
`0700`. The detached controller starts from the package directory, so the
CLI works from any working directory. `jobs.db` (SQLite, WAL), `outputs/*`,
and `workers/*.json` are `0600`. Tokens and credentials are never written
to the database, events, or logs; worker text, proof output, and error
evidence are redacted before they are persisted or forwarded.

States: `pending -> running <-> question_pending -> succeeded | failed |
cancelled`, with `cancelling` while the controller and proof group stop.
`blocked` always has a reason. `recover` re-evaluates only its own blocks
(a dead controller, a pending launch, step budgets, unresolved proof
ownership); any other block stays until the planner answers or resubmits.
Recovery continues on the currently installed runtime only when the stored
job state is compatible with it (same policy id, readable controller state,
a route this policy knows); otherwise the job blocks as
`runtime_incompatible` with the specific reason and is never migrated.

## Verification

```
python3 scripts/test.py
python3 -m compileall -q runner scripts tests
```

`scripts/test.py` runs the suite with a disposable T3 home and a closed
server port, so no test reaches the user's own T3. The suite uses an
in-memory T3 orchestration fake (`tests/fakes.py`) and fake HTTP servers
for the orchestration and Prism snapshot wire shapes, plus real Git
repositories for proof, baseline and diff checks. It covers submission and
idempotency, workspace claims, the ladder and capacity moves, snapshot
eligibility and limits, planner questions, completion gates, terminal
reports, cancellation, and recovery. It does not prove live model behavior;
live verification uses an isolated T3 server (`T3CODE_HOME` under `/tmp`,
a free port), never the user's running T3, and its evidence is recorded on
the owning Issue.
