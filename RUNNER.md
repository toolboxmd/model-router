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
  [--planner-model M] [--planner-effort E] [--route R] [--max-attempts N] \
  [--timeout-secs S] [--start | --no-start]
python -m runner --state-dir DIR start --request-id ID
python -m runner --state-dir DIR status --request-id ID
python -m runner --state-dir DIR questions --request-id ID [--all]
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
`--route` accepts only the implementation routes
`muse-spark-xhigh-free` and `muse-spark-xhigh-go`. `--start` launches a detached
controller after the commit. `--planner-cwd` is the directory where the
planner session was started, because Claude stores sessions per project
directory; it defaults to the workspace.

`launch`, `post-question`, `complete`, and `fail` are worker-internal helpers
used by the offline lease tests. A worker-reported failure never changes the
route or capacity memory.

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
then blocks with a reason instead of spinning.

Before resuming the planner, the runner also waits up to 30 seconds for the
session transcript to be unchanged for 5 seconds; a planner still writing
its transcript counts as busy.

## Implementation worker

Each turn starts a fresh `opencode serve` with a random password in its
environment only. The supervisor authenticates with Basic auth
(`opencode:<password>`) and scopes every call with `directory=<workspace>`.
It creates or reuses the saved session and saves the session ID before the
model request. The session gets permission rules that deny access outside the
workspace, subagents, interactive questions, doom-loop prompts, and web
access. OpenCode does not confine Bash at the OS level; that remains a known
limit.

The prompt goes through `POST /session/{id}/prompt_async` with model
`opencode/muse-spark-1.3-contributor-free`, variant `xhigh`, agent `build`.
The supervisor polls `GET /session/status` for that session only and reads
the new assistant messages when it is idle. The server group stops after
every turn.

Free to Go needs trusted provider evidence for the owned session:

- a `retry` status with `action.reason` `free_tier_limit` and
  `action.provider` `opencode`, or
- an assistant `APIError` whose provider `responseBody` names
  `FreeUsageLimitError`.

On status evidence the session is aborted and confirmed idle before the
route changes. The same session then continues on
`opencode-go/muse-spark-1.3-contributor` with the same effort. Exhausted free
capacity is remembered across jobs, with its original evidence, until a
trusted reset time passes or an operator clears it; an unknown reset stays
unknown. Model-authored text, generic 429,
`account_rate_limit`, timeouts, consent, region, and auth errors never
change the route. Zen balance overflow and direct paid APIs are disabled.

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

`--state-dir` (or `DURABLE_RUNNER_STATE_DIR`) is made absolute and is
`0700`. Detached controllers and supervisors start from the package
directory, so the CLI works from any working directory. `jobs.db` (SQLite,
WAL), `outputs/*`, and `workers/*.json` are `0600`. Passwords and credentials are never written to the
database, events, or logs.

States: `pending -> running <-> question_pending -> succeeded | failed |
cancelled`, with `cancelling` while children stop; the last stored stop
intent decides between `cancelled` and a timeout. `blocked` always has a reason. `status`
shows summaries: the lease token, the task body, Luna envelopes, worker
reports, the completion report, and child output stay in the private
database and output files. It does show Luna's planner questions and block
reasons, which can include redacted provider or exception text. `result` prints the full terminal result on
request.

## Policy

`runner/policy.py` (`durable-runner-policy-v1`) records the agreed roles:
planning Fable 5.1 max (`claude-fable-5-1`) or Astra max in Codex; Luna max
dispatch, Terra only after demonstrated coordination failure; Muse Spark 1.3
Contributor xhigh free first, then Go included allowance; Go capacity order
DeepSeek V4.1 Flash, GLM 5.3 Flash, MiniMax M3, then MiMo 2.5 and LongCat
2.0; one bounded correction, then Kimi K2.7 Code; recovery Grok 4.6 medium,
Astra medium, Opus 5 high; independent Luna max review; Opus 5 high combined
review. Only Luna dispatch, the Claude planner callback, and the two Muse
routes have runner adapters. Other routes return a precise blocker and are
never reported as a successful fallback. `claude-sonnet-5` / `medium` is an
explicit test override only.

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
