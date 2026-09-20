# Durable local runner

A small local runner that accepts prepared work from a Claude planner, hands
it to a persistent Codex Luna dispatcher, and returns questions and
completion without depending on the planner process staying open. Python
standard library only. No services, plugins, dependencies, or network
listeners beyond one owned loopback OpenCode server per implementation turn.

AgentsMD and the target project own workflow, authority, proof, and review.
The runner persists the handoff, owns child processes, and follows one
versioned route policy. It cannot grant new authority.

## Public CLI

```
python -m runner --state-dir DIR submit --request-id ID (--task JSON | --task-file F) \
  --workspace PATH --planner-session SID [--planner-cwd PATH] \
  [--planner-model M] [--planner-effort E] [--lane L | --route R] [--max-attempts N] \
  [--timeout-secs S] [--job-kind ordinary|experiment|replay] [--replay-of ID] \
  [--planner-harness claude] [--start | --no-start]
python -m runner --state-dir DIR start --request-id ID
python -m runner --state-dir DIR status --request-id ID
python -m runner --state-dir DIR questions --request-id ID [--all | --clear QID]
python -m runner --state-dir DIR answer --request-id ID --qid Q --answer TEXT
python -m runner --state-dir DIR cancel --request-id ID
python -m runner --state-dir DIR recover (--request-id ID | --all)
python -m runner --state-dir DIR result --request-id ID
python -m runner --state-dir DIR capacity [--clear ROUTE]
```

`submit` commits the task, workspace claim, policy identity, planner session,
planner model, effort, directory, route, attempt budget, and timeout before
it acknowledges. The same ID and payload return the existing job, also with
`--start`. A changed payload conflicts. A new job's workspace and planner directory
must be existing directories. A workspace that equals, contains,
or lies inside the workspace of an active or cancelling job, or of a job
that still owns a child process, is rejected. Workspaces are compared by
device and inode along their ancestor chain, so symlinks, case variants,
and firmlink aliases on macOS are the same workspace.
`--lane` picks an implementation lane (`default`, `small`, `hard`). Every lane's
first route is Muse on Zen free (`muse-spark-xhigh-free`), which carries no
concurrency cap and takes every new job: parallel jobs open parallel Muse free
sessions by design. The sticky-home `fewest running jobs` spread applies only
among routes that carry a `max_concurrent` cap (ties go to the earlier route;
a capped route already used by running jobs is excluded, as is a route the
capacity memory knows as exhausted or resting). A job leaves Muse free only on
provider evidence: a limit error moves the same model to the next pool (free
Muse to Go Muse with no retries), overload moves to the next model family
after the bounded retry window with the route degraded 15 minutes. A job walks
the lane only on capacity evidence. An identical resubmission returns the stored job unchanged: the
existing-request lookup runs first and only a new request computes a sticky
home, counted inside the same write transaction that records the route so two
submitters cannot both take a capped route. `--route` names one implementation route from the policy directly.
Without either, the default lane's sticky home applies. The
`critical` lane is planner-executed and `submit` rejects it: do that step in
the planner session, then submit the remainder. `--start` launches a detached
controller after the commit. `--planner-cwd` is the directory where the
planner session was started, because Claude stores sessions per project
directory; it defaults to the workspace.
`--job-kind` is `ordinary`, `experiment`, or `replay`; a replay names the
request it repeats with `--replay-of`. `--planner-harness` names the harness
hosting the planner session; only `claude` is implemented. The task JSON may
carry an optional `links` array of `{"rel": "...", "href": "..."}` objects
(references for the worker, kept verbatim in the stored task); it changes no
routing and needs no runner flag.

`capacity` lists remembered route capacity with its original provider
evidence. `--clear ROUTE` is an operator action after checking the provider
allowance; the runner never invents a reset time.

## Flow

1. Dispatch: `codex exec --json --output-last-message PATH --model
   gpt-5.6-luna -c model_reasoning_effort="max" --sandbox read-only --cd WS`.
   The first `thread.started` ID is saved as the Luna task. Luna's action is
   read only from its own final `item.completed` `agent_message` text, or
   the last-message file. Command output is never parsed for actions.
2. Luna replies with one envelope: `planner_question`, `implementation`, or
   `completion`. The envelope is saved before its effect.
3. `planner_question`: the question is saved, then the original planner is
   resumed with `claude --resume SID --model M --effort E --output-format
   json --tools "" -p PROMPT` in the planner directory. The answer counts
   only when the JSON result is a success from the same session ID. A running
   `claude` process that names the session in its arguments is a busy
   planner. Busy, failed, or mismatched callbacks block the job with a
   reason; the question stays pending for `answer` plus `recover`.
4. The answer is saved, then the same Luna task resumes with `codex exec
   resume ID --json -m gpt-5.6-luna -c model_reasoning_effort="max" -c
   sandbox_mode="read-only"`. A resume that reports another thread, or none,
   blocks.
5. `implementation`: one Muse turn runs on an owned `opencode serve --pure
   --hostname 127.0.0.1 --port 0`. See below. The worker report returns to
   the same Luna task.
6. `completion`: the terminal result is saved before acknowledgment.

The dispatcher's Codex sandbox is read-only, so Luna coordinates and verifies
but cannot edit. The controller runs at most 12 transitions per launch and
then blocks with `controller_step_budget_exhausted`; `recover` restarts such
a job with a fresh launch budget. A job blocked only by
`controller_step_budget_exhausted` therefore continues via `recover`.
A job budget of 48 transitions across all
launches blocks with `job_step_budget_exhausted`, which stays blocked.
The default attempt budget is 5 controller launches, so five 12-step
launches cover the 48-step job budget before launch-budget exhaustion.

Escalation ladder. A turn fails when the worker or provider errors (not a
capacity signal, which moves routes instead; hard errors end the turn as
`implementation_failed` for this ladder) or when the task's own proof
command exits non-zero. The first failure leads to a correction in the same
worker session on the same route. The second leads to a fresh correction on
the correction stage's route (Kimi K2.7 Code). The third is the single
escalation to the recovery stage (Grok 4.6 on Go, then on the xAI
subscription if Go is exhausted or the rung was already used; that pool move
is not a second escalation; recovery never reuses a rung the job already
ran, and when every rung was used the job blocks with
`recovery_exhausted`). After the escalation the evidence returns to the
planner, which may itself choose a rung from the policy's planner-chosen
rungs (Astra medium on Codex, Opus 5 high on the Claude harness) and run it
in its own session; the runner never selects those automatically. A fourth
failure ends the job as `failed` with `ESCALATION_EXHAUSTED` and
the turn reports listed in `result`, which is the planner's to act on. A
failed turn does not block the job: the dispatcher receives the report and
decides; the runner enforces the ceiling.

A stored answer is reused only when the stored prompt matches the
dispatcher's prompt; a reused `qid` with a different prompt blocks with
`planner_question_conflict`, cleared by `questions --clear QID` or by the
dispatcher using a new qid. When the live planner callback fails or reports
another session after the question was answered publicly through `answer`,
the stored public answer wins. A resume that fails before `thread.started`
records `codex_resume_failed` with its exit code, as recovery does.

Before resuming the planner, the runner also waits up to 30 seconds for the
session transcript to be unchanged for 5 seconds; a planner still writing
its transcript counts as busy.

## Implementation worker

Each turn starts a fresh `opencode serve` with a random password in its
environment only. The supervisor authenticates with Basic auth
(`opencode:<password>`) and scopes every call with `directory=<workspace>`.
It creates or reuses the saved session and saves the session ID before the
model request. The session's permission rules are policy data, resolved per
route by role. Implementation, correction, and recovery routes run with full
access: outside-workspace writes, web fetch, web search, and doom-loop prompts
are allowed as a per-route policy flag. Dispatch and review routes on OpenCode
run read-only in plan mode (`luna-go/max`, `luna-go-review`): `edit`,
outside-workspace writes, web fetch/search, and doom-loop prompts are denied
because the coordinator role never needs them; the plan agent is kept. Two
rules stay denied on every route: `question`, because an interactive question
stalls a headless session forever, and `task`, because spawning subagents would
bypass the policy and the runner's ownership and proof guarantees. OpenCode does not
confine Bash at the OS level; that remains a known
limit.

The prompt goes through `POST /session/{id}/prompt_async` with the job
route's model, variant, and agent from the policy (free Muse is
`opencode/muse-spark-1.3-contributor-free`, variant `xhigh`, agent `build`;
GLM, Qwen, MiniMax, Kimi, and DeepSeek on Go use the provider default variant;
Grok on Go or xAI uses `medium`). An unknown route is an error, never a
substitution.
The supervisor polls `GET /session/status` for that session only and reads
the new assistant messages when it is idle. The server group stops after
every turn.

Provider signals are classified from structured evidence only, never from
model-authored text:

- exhausted: a `retry` status with reason `free_tier_limit`, or an
  `APIError` whose provider `responseBody` names `FreeUsageLimitError`,
  `GoUsageLimitError`, or `insufficient_quota`. Zero retries: the session is
  aborted and confirmed idle, the route is remembered exhausted with its
  evidence and any trusted reset time, and the job moves the same model to
  the next pool where the route declares one (free Muse to Go Muse, Go Grok
  to xAI Grok); other Go routes (`glm-5.3-go`, `qwen3.8-flash-go`,
  `minimax-m3-go`, `deepseek-v4.1-flash-go`, `deepseek-v4-pro-go` have no `next_pool`)
  move to the next model family in their lane.
- overloaded: a `retry` status with reason `overloaded`, `rate_limit`, or
  `account_rate_limit`, an `APIError` naming `RateLimitError`,
  `rate_limit_exceeded`, or `overloaded_error`, or transport HTTP 503 or 529.
  The provider's own retries are allowed for at most 2 attempts inside 45
  seconds with `next` capped at 20 seconds (a `next` larger than the cap
  aborts instead of waiting); then the session is aborted and
  confirmed idle, the route rests for 15 minutes (`degraded` with a cooldown
  end), and the job moves to the next model family in its lane. The same
  family on another pool is not a lateral move.
- hard: `context_length_exceeded`, auth, region, and consent errors end the
  turn as `implementation_failed` for the ladder; they never move routes.

Before a turn starts, a route the capacity memory knows as exhausted or
resting is skipped the same way (`preflight_exhausted`, `preflight_degraded`),
as is a route whose `max_concurrent` is already used by running jobs
(`preflight_concurrent`), a one-turn route that already ran its turn
(`preflight_one_turn`), and one with no turn left in this job — all without a
child. The concurrency reservation is atomic: the target's running count is
read inside the same write transaction that records the new route, and a full
target moves to the next free route in the same transaction. The Go dispatch
fallback (`luna-go/max`) reserves the same way before any child starts. No eligible route left in the lane blocks the job with
`capacity_exhausted`. The `capacity` command lists remembered routes with
pool, model, window, state, evidence, and reset time; `--clear ROUTE` is an
operator action after checking the provider. Reset times come only from
provider evidence or the documented cooldown; an unknown reset stays
unknown. Zen balance overflow and direct paid APIs are disabled.

## Process ownership

Every model child is an invocation row committed with NULL PID before any
spawn. An independent supervisor process group starts the child, records its
PID, PGID, and start identity, waits for it, and writes the exit code and
result. File-backed output survives controller death.

- Before spawning, the supervisor claims its row under the database write
  lock and refuses if the job was cancelled, the lease changed, or the row
  was closed. A row no supervisor claimed within 60 seconds never started
  and is closed as `abandoned`; it is not a failed attempt.
- The child writes a private side record of its PID before it execs, and
  the supervisor adds the start time before its database commit. If the
  supervisor dies before the commit, recovery and cancellation use that
  record. A claimed row whose supervisor is proven dead, with no side record
  60 seconds after the attempt was recorded, never started a child and is
  closed as `abandoned`. While its supervisor runs, a row without a child
  PID is live; with the supervisor dead inside those 60 seconds it is
  unresolved, never death. A child whose start identity cannot be read
  (including one known only from its own side record, if the supervisor
  was killed before adding the start time) counts as live while its
  supervisor runs and unresolved otherwise, and is never signalled.
- A live child or supervisor is `live`. An owned OpenCode server whose
  supervisor died is `orphaned`: its password is gone, so `recover` stops
  that task-owned group. If it does not stop, the job blocks.
- The supervisor records its own PID and start time when it claims the
  row, and the child's right after spawning. Start times come from
  `ps -o lstart`. A signal is sent only when that identity is proven. A
  different start time means the recorded process ended. An unreadable or
  missing identity counts as possibly alive: the runner never signals it and
  never adopts it as a controller; an invocation with an unknown supervisor
  identity is waited on as live.
- The supervisor defers SIGTERM, SIGINT, and SIGHUP until the child's PID is
  committed, then stops the child and records the result. It rechecks for a
  stop request just before spawning; a signal in the instant after that
  check still spawns once, and the child is then stopped and recorded. Any
  failure before or at spawn, or while recording the child, closes the row
  as failed (killing the child's group if one exists). NUL bytes are removed
  from arguments.
- The controller takes a per-job `flock` before any other write and holds
  it until it exits; the kernel releases it on death. It then acknowledges
  its lease and only afterwards advertises its identity. A controller that
  cannot get the lock only gives back its own launch's lease, guarded on
  its token, and exits. It receives its lease token in
  the environment, records its own PID and start time at startup, and
  releases the lease when it exits.
  Invocation inserts, saved actions, questions, answers, phases, blocks,
  route changes, and completion recheck the lease under the database write
  lock; a controller that lost its lease, or whose job was cancelled or
  finished, stops without writing them.
  Collecting a finished child's output is idempotent and also done by
  `recover`, so it is not lease-bound.
- A job has one writer at a time: before a new child starts, any other live
  child of the job is adopted by waiting for its result. Capacity memory
  never redirects a turn that already started on the free route.
- Each invocation has an action key from its kind, command, prompt, model,
  and Luna turn number. A restarted controller replays from saved state. A
  finished action is reused, a live one is adopted by waiting for its
  result, and a failed model turn is not replayed: the job blocks with the
  reason. This prevents duplicate dispatches, planner turns, and workers.
- The Luna task ID comes only from the dispatch turn and is never replaced.
  A resume that reports another thread blocks.
- A Luna turn counts only with its `turn.completed` event, on the live path
  and in recovery.
- `recover` stops orphaned servers, consumes uncollected results once
  (including a finished planner answer), and restarts the controller for a
  dispatched job whose controller is gone (including a finished dispatch
  whose action was not saved yet). While the controller lock is
  held, or within 60 seconds of a launch (acknowledged or not), recover
  never replaces the controller and never blocks it for a missing
  handshake. It still blocks for orphaned servers, and for unresolved
  children once the controller's PID is recorded. A consumed Luna turn is
  applied only when it reports the saved thread, and a consumed failed or
  mismatched Luna turn blocks the job with its reason. A consumed failed or
  mismatched planner turn blocks only while its question is still pending;
  if the question was answered meanwhile, the stored answer wins. A failed Muse turn is left to the restarted controller,
  which may switch routes on provider evidence. After that, a free lock proves no controller has
  started; one that starts later fails the lock or its acknowledgment,
  because its token no longer holds the lease, and exits without changing
  the job.
  Never-started rows are not closed while another process holds the lock. A row a live controller has just
  inserted, not yet claimed by its supervisor, is normal startup. A controller that advertised the current
  lease token with a proven identity is adopted even with an old heartbeat,
  because heartbeats refresh only between steps. Unknown ownership blocks. Results of failed
  turns, and any result after cancellation was requested, are recorded but
  never applied.
- `blocked` jobs never restart through `recover`, except for recover's own
  ownership blocks, which it re-evaluates. A planner-question block clears
  through `answer`. `cancel` works on any non-terminal job.
- `cancel` and timeouts signal every proven child and supervisor group and
  keep the workspace claimed until they are confirmed dead. If ownership
  stays unresolved, the job blocks and `recover` finalizes it once the
  children are confirmed gone.
- One guarded statement finishes a stop: the outcome follows the stop
  intent stored last (a cancel request overwrites a pending timeout), never
  replaces a terminal status, and the result file mirrors what was
  committed.

Default child timeouts: Codex 1800 s, Claude 900 s, OpenCode 1800 s.
`--timeout-secs` bounds the whole job and is enforced by `recover`; a job
that completed before `recover` ran keeps its result. A timeout finalizes as
`failed` with `timeout`, a cancellation as `cancelled`.

## State

Each worker turn writes `outputs/<request_id>/turn-<seq>/` (retries on the
same seq use `turn-<seq>-1`, ... so no turn overwrites another):
`report.json` (route, policy version, observed model and variant as separate
fields, session, changed files, proof command and exit code, tokens verbatim
with a source label including reasoning and cache read and write, native
message identities, blockers, a redacted worker summary), `proof.log` (the
redacted output of the task's own `proof` command, run by the runner in the
workspace), `diff.patch` (`git diff HEAD` plus untracked files, or a note
when the workspace is not a checkout), and `worker.txt` (the worker's full
text, redacted). Harness-reported worker questions travel in the report's
blockers (redacted), never as live questions; the implementation harness
denies question permission, so blockers are typically empty. The dispatcher's resume message carries these paths and the
structured fields (route, policy version, models, variants, status, tokens,
native identities) instead of the worker's prose.

Every invocation records its stage, requested route, policy version, route
reason, harness version, elapsed time, terminal class (completed, failed,
crashed, cancelled, timeout, quota, overloaded, hard_error), usage counters
verbatim under a source label, the observed model and the observed variant as
separate fields, and native identities (Codex thread and turn ids, Claude
session and result ids with the callback's prompt digest, OpenCode session
and message ids). Jobs record their kind (`ordinary`, `experiment`, `replay`
with `--replay-of`), the planner harness (only `claude`), the workspace
commit at submit (`base_commit`) and completion (`head_commit`), and the
optional task `links`. Events and invocations carry `schema_version` 2;
Agent Observer reads this ledger directly. `status` and `result` show the
measurements including native identities, variants, and schema versions;
`status` keeps the redacted error evidence (signal, evidence, retry counts
and caps) so CLI readers need not query the database directly;
`result` also lists the turn reports.

`--state-dir` (or `DURABLE_RUNNER_STATE_DIR`) is made absolute and is
`0700`. Detached controllers and supervisors start from the package
directory, so the CLI works from any working directory. `jobs.db` (SQLite,
WAL), `outputs/*`, and `workers/*.json` are `0600`. Passwords and credentials are never written to the
database, events, or logs; worker text, proof output, error evidence, and
report errors are redacted for secret keys and free-text secret shapes
before they are persisted or forwarded.

States: `pending -> running <-> question_pending -> succeeded | failed |
cancelled`, with `cancelling` while children stop; the last stored stop
intent decides between `cancelled` and a timeout. `blocked` always has a reason. `status`
shows summaries: the lease token, the task body, Luna envelopes, worker
reports, the completion report, and child output stay in the private
database and output files. It does show Luna's planner questions and block
reasons, which can include redacted provider or exception text. `result` prints the full terminal result on
request.

## Harness seam

`runner/harnesses.py` defines the seam between the runner and the harnesses
that execute turns. A `Harness` base class declares the interface; `CodexCLI`,
`ClaudeCLI`, and `OpenCodeServer` are the concrete adapters. A registry holds
one instance per harness with lookup helpers: `HARNESSES` by name, `harness_for`
by invocation kind, `harness_named` by name, and `kind_for_cmd` from a command
line. A harness provides spawn specs, session and report parsing, provider
signal classification, and usage measurement; the core and supervisor call
these methods instead of branching on invocation kind. Each harness declares
its capabilities, and each policy stage declares the capabilities it needs.
`route_capability_blocker(route, stage)` names any capability the route's
harness lacks; a dispatch then fails with `RunnerError("route_capability_mismatch: ...")`
before any child starts. Dispatch has one fallback route: when the Codex CLI
dispatch route cannot start a task, the controller dispatches Luna on OpenCode
Go (`luna-go/max`, agent `plan`, one turn per job) through the owned OpenCode
server (`_dispatch_on_opencode` in `runner/controller.py`). Later turns resume
that OpenCode session while the saved dispatch route stays OpenCode.

## Policy

`runner/policy.py` (`durable-runner-policy-v2`) is the single policy source:
pools, routes, stages (lanes), each Go model's monthly dollar limit and its
usage windows, signal classes, and provenance. The Codex skill reference
`skills/model-routing/references/codex.md` is rendered from it with
`python -c "from runner import policy; policy.main(['render-skill'])"` and a
test keeps the two equal; `policy.main(['validate'])` checks the data.

Stages in order: planning (Fable 5.1 max in Claude Code, then Astra max on
Codex; Sonnet medium only as the explicit live-test override), dispatch (Luna
max on Codex read-only first; `luna-go/max` on Go in OpenCode plan mode as the
recorded fallback; Terra max is a manual-only option), the implementation
lanes default, small, and hard, planner-chosen rungs (Astra medium on Codex,
Opus 5 high on the Claude harness; the planner itself chooses and runs them),
critical
(planner-executed), correction (Kimi K2.7 Code after the same session), recovery
(Grok 4.6 on Go then on the xAI subscription, skipping rungs the job already
used), ticket review (Luna max on Codex, then Luna on OpenCode Go in plan
mode), and final review (Opus 5 high on the Claude harness, then Astra high
on Codex, then Luna max). Worker pools in order: Zen free, Go, xAI;
subscription logins only. The default lane carries the full Go implementer
chain in intelligence order: Muse xhigh on Zen free, Muse xhigh on Go, GLM
5.3 Flash, Qwen 3.8 Flash, DeepSeek V4.1 Flash, Hy3, MiniMax M3, MiMo 2.5,
MiniMax M2.7, LongCat 2.0, GLM 5.2, Kimi K2.6, GLM 5.1 (Qwen 3.7 Plus and
Qwen 3.6 Plus are absent: no known tier); the small lane starts from Muse free,
then Muse Go, then GLM 5.3 Flash onward; the hard lane runs Muse free, Muse Go, GLM-5.3, DeepSeek V4 Pro, Grok
4.6 on Go, Grok 4.6 on xAI; correction is Kimi K2.7 Code. Muse on Zen free
carries no concurrency cap and takes every new job (parallel Muse free
sessions by design); the `fewest running jobs` spread applies only among
capped routes. Never
implementers on Go: Grok 4.6 outside the hard lane's last Go rung and
recovery, Luna, Kimi K3, Qwen 3.8 Max, Qwen 3.7 Max. Each Go route carries
its monthly tier (60, 30, or 15 USD); $15-per-month Go models get one turn
per job and 15 and 30 USD tiers allow exactly one running job per route
(`max_concurrent` is exactly 1), reserved atomically inside the same write
transaction that records the job's route (first sticky selection, preflight
move, dispatch fallback on `luna-go/max`, and recovery move count running jobs
on the target inside the transaction, so two controllers cannot both take a
capped route). Recovery moves (`grok-4.6-go`, `grok-4.6-xai`) are valid from any
implementation lane and reason in the recovery stage. A manual or
planner-chosen route inside an implementation lane, the recovery stage, or the
dispatch order never validates. DeepSeek
V4.1 Flash is a 15 USD route from 2026-09-20. Every route in
the policy has an adapter today; the OpenCode adapter takes model, variant,
and agent from the route. Go Luna as a dispatch fallback arrives with the
adapter seam. Changing a stage's harness or model is a policy edit; adding a
role needs a policy row plus a report contract.

Signal classes are defined in the policy and applied by later work: exhaustion
(`free_tier_limit`, `FreeUsageLimitError`, `GoUsageLimitError`,
`insufficient_quota`) moves the same model to the next pool where the route
declares one (free Muse to Go Muse, Go Grok to xAI Grok), otherwise to the
next family; overload
(`overloaded`, `rate_limit`, `account_rate_limit`, HTTP 503 and 529) moves to
the next family after a bounded retry window; hard errors end the turn as
`implementation_failed` for the ladder and never move routes. The controller
applies them on the owned-server path, driven through the harness seam.

## Verification

```
python -m unittest discover -s tests
python -m compileall -q runner tests
```

The suite uses fake `codex`, `claude`, and `opencode` executables shaped like
the real contracts (`tests/fakes.py`) and real detached processes. It covers
duplicate and concurrent submission, workspace conflicts, launch races,
controller and worker death, killed supervisors, pending questions,
answer-plus-recover, cancellation, timeouts, trusted and untrusted quota
evidence, unsupported routes, and unknown ownership. It does not prove live
model behavior. Live verification evidence is recorded on the owning Issue.

Not verified live: the production planner default `claude-fable-5-1`, a real
free-to-Go transfer through this runner, and recovery routes. Known limits:
an interactive planner that is open but idle, without the session ID in its
arguments, is not detectable as busy; OpenCode does not confine Bash at the
OS level; any process running as the same user can read the state directory.
