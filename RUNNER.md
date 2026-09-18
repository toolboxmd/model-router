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
```

`submit` commits the task, workspace claim, policy identity, planner session,
planner model, effort, and directory before it acknowledges. The same ID and
payload return the existing job. A changed payload conflicts. A second active
job on the same workspace is rejected. `--start` launches a detached
controller after the commit. `--planner-cwd` is the directory where the
planner session was started, because Claude stores sessions per project
directory; it defaults to the workspace.

`launch`, `post-question`, `complete`, and `fail` are worker-internal helpers
used by the offline lease tests.

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
   sandbox_mode="read-only"`. A resume that reports another thread blocks.
5. `implementation`: one Muse turn runs on an owned `opencode serve --pure
   --hostname 127.0.0.1 --port 0`. See below. The worker report returns to
   the same Luna task.
6. `completion`: the terminal result is saved before acknowledgment.

The dispatcher's Codex sandbox is read-only, so Luna coordinates and verifies
but cannot edit. The controller runs at most 12 transitions per launch and
then blocks with a reason instead of spinning.

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
capacity is remembered across jobs until a trusted reset time passes; an
unknown reset stays unknown. Model-authored text, generic 429,
`account_rate_limit`, timeouts, consent, region, and auth errors never
change the route. Zen balance overflow and direct paid APIs are disabled.

## Process ownership

Every model child is an invocation row committed with NULL PID before any
spawn. An independent supervisor process group starts the child, records its
PID, PGID, and start identity, waits for it, and writes the exit code and
result. File-backed output survives controller death.

- NULL PID or a changed start identity is unresolved ownership, never death.
- A live child or supervisor is `live`. An owned OpenCode server whose
  supervisor died is `orphaned`: its password is gone, so `recover` stops
  that task-owned group.
- Each invocation has an action key from its kind, command, prompt, model,
  and Luna turn number. A restarted controller replays from saved state. A
  finished action is reused, a live one is adopted by waiting for its
  result, and a failed model turn is not replayed: the job blocks with the
  reason. This prevents duplicate dispatches, planner turns, and workers.
- `recover` stops orphaned servers, consumes uncollected results once
  (including a finished planner answer), and restarts the controller for a
  dispatched job whose controller is gone. Unknown ownership blocks.
- `cancel` and timeouts signal every child and supervisor group and keep the
  workspace claimed until they are confirmed dead.

Default child timeouts: Codex 1800 s, Claude 900 s, OpenCode 1800 s.
`--timeout-secs` bounds the whole job and is enforced by `recover`.

## State

`--state-dir` (or `DURABLE_RUNNER_STATE_DIR`) is `0700`. `jobs.db` (SQLite,
WAL), `outputs/*`, and `workers/*.json` are `0600`. Status views never echo
task bodies or tokens. Passwords and credentials are never written to the
database, events, or logs.

States: `pending -> running <-> question_pending -> succeeded | failed |
cancelled`. `blocked` always has a reason and never spawns.

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
free-to-Go transfer through this runner, recovery routes, and interactive
planner sessions opened without the session ID in their arguments (the busy
check cannot see them).
