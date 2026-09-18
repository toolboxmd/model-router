# Durable local runner

Small production-shaped runner. Python standard library only. No services,
plugins, dependencies, credentials, or network listeners.

Project direction (`VISION.md`, `MISSION.md`, `OBJECTIVE.md`) and `VERSION`
are unchanged. AgentsMD owns workflow, authority, and proof; this runner
only persists handoff, owns process leases, and tracks explicit routes.

## Public CLI

```
python -m runner --state-dir DIR submit --request-id ID --task JSON|--task-file F \
  --workspace PATH --planner-session SID [--route R] [--max-attempts N] [--timeout-secs S] \
  [--planner-model M] [--planner-effort E] [--start|--no-start]
python -m runner --state-dir DIR start --request-id ID
python -m runner --state-dir DIR status --request-id ID
python -m runner --state-dir DIR questions --request-id ID [--all]
python -m runner --state-dir DIR answer --request-id ID --qid Q --answer TEXT
python -m runner --state-dir DIR cancel --request-id ID
python -m runner --state-dir DIR recover (--request-id ID | --all)
```

Worker-internal helpers (no new authority): `launch`, `post-question`,
`complete --token`, `fail --token --error JSON`.

`submit` persists policy identity, workspace ownership, planner/Claude
session ID, planner model/effort defaults, task metadata, output path,
and a `submitted` event in one SQLite transaction before ack. Same ID
plus identical payload returns the existing job; conflicting reuse is
rejected. A second active job on the same workspace is rejected. With
`--start` (or separate `start`), a detached controller spawns with
built-in adapters after persist; `--no-start` keeps deterministic tests
offline. Manual `launch` spawns a detached worker (`start_new_session`,
stdio to /dev/null) and persists the attempt before and after ack.
`recover` reconciles durable records with live ownership. It consumes
finished child output exactly once, adopts a live child, and blocks on
unresolved NULL-pid ownership. After a public `answer` on a saved Luna
task, `recover` resumes a detached controller instead of clearing the
lease and stalling. It never forks the planner or Luna session.

## State

`--state-dir` (or `DURABLE_RUNNER_STATE_DIR`), `0700`; `jobs.db` (SQLite
WAL), `outputs/*.log`, `outputs/*.result.json`, and `workers/*.json` are
`0600`. File-backed output is append-only; recovery never truncates
partial logs. Status views redact start tokens and never echo task bodies.
Events store hashes and IDs, never credentials.

States: `pending -> running <-> question_pending -> succeeded|failed|cancelled`.
`blocked` means unknown ownership, busy planner, or failed callback with
an explicit reason; it never spawns. Terminal states are final: no
resurrection, no budget reset, no retry loop. `max_attempts` (default 3,
1..10) bounds recovery. Planner, executor, Codex task, and OpenCode
session IDs are assigned once and preserved; recovery reuses the same
sessions and never forks the planner or the Luna task.

## Adapters and controller

Stdlib only (`runner/adapters.py`, `runner/controller.py`). Built-in
defaults require no executor/callback commands:

- Codex dispatcher: `codex exec --json --output-last-message PATH
  --model gpt-5.6-luna -c model_reasoning_effort="max" --sandbox
  workspace-write --cd WS`. The returned thread/task ID (from
  `thread.started`/`thread_id` or the completed agent message /
  output-last-message envelope, exact ID preserved) persists before the
  dispatch counts as accepted. Resume is `codex exec resume ID --json`
  with the saved workspace as cwd (never `--cd`/`--reasoning`).
- Claude planner: `claude --resume SID --model fable-5.1 --effort max`
  production default (explicit `claude-sonnet-5`/`medium` is only the
  bounded live-test override). Missing sessions are rejected; no new
  planner session is ever created implicitly.
- Implementation: `opencode run --format json --pure --dir WS --model
  opencode/muse-spark-1.3-contributor-free --variant xhigh --agent
  build` free-first (`opencode-go/muse-spark-1.3-contributor` only
  after exact free exhaustion; `--session` resumes the saved session).
  Command injection stays as a test seam.

Luna returns `planner_question`, `implementation`, or `completion`.
`planner_question` persists before Claude `--resume`; the answer persists
before resuming the same Luna task. `implementation` carries the saved
artifact/output. Busy planner or callback failure becomes durable
`blocked` with a reason; questions stay pending for `answer` + `recover`.

OpenCode quota uses an owned ephemeral per-job `opencode serve --pure
--hostname 127.0.0.1 --port 0` process (fresh in-memory password in the
child environment, URL parsed from durable stdout, never logged, never a
shared service) or an injectable equivalent. The public controller path
creates/saves the session on that server before prompting and observes
only that session's status. Never invent a random password for a server
the job does not own. Free to Go needs the exact vendor class
`FreeUsageLimitError` (including decoded `responseBody` JSON) or a retry
with reason `free_tier_limit` and provider `opencode` for the saved
session. The old free session aborts before transfer, ownership/idle
confirms, artifacts preserve. Route, adapter, model, effort, session IDs,
attempts, and redacted provider evidence persist without secrets. Full
task content is passed to Luna and adapters without silent clipping; the
Luna action envelope persists before its side effect.

Each model spawn persists immutable invocation intent (NULL pid/pgid)
before Popen. An independent supervisor process group waits on the child,
captures IDs from file-backed output, and writes rc/result. Killing only
the controller does not destroy that output. A NULL PID/PGID or start-
identity mismatch is unresolved ownership, never proof of death.
`recover` consumes finished output exactly once and will not rerun a
completed child. Cancellation and timeout signal every child and
supervisor process group and keep the workspace claimed until they are
confirmed dead. These seams are proved with fake executables, not live
model CLIs.

## Ownership

Lease: `jobs.owner_token` + `jobs.owner_pid`. The live worker advertises
`{token, pid, updated}` in `workers/<id>.json` with a fresh heartbeat.
Owned means token match plus pid match plus alive plus fresh. PID
aliveness alone never proves ownership. `complete`/`fail` require the
lease token. Unknown live tokens block with a reason instead of spawning
a possible duplicate.

## Policy

`runner/policy.py` (`durable-runner-policy-v1`): Luna max dispatch;
Terra only as recorded fallback after demonstrated Luna coordination
failure (no live-exercised adapter); Muse Spark 1.3 Contributor xhigh
free-first then the same model on Go included allowance only after
explicit `FREE_ALLOWANCE_EXHAUSTED` `confirmed:true`, exact vendor
`FreeUsageLimitError` (including decoded `responseBody` JSON), or retry
`free_tier_limit` + provider `opencode`; Go DeepSeek V4.1 Flash -> GLM
5.3 Flash -> MiniMax M3 then MiMo 2.5 / LongCat 2.0 as recorded capacity
(precise blocker if not operational); Kimi K2.7 Code only after the
initial worker plus one bounded correction (not operational here);
independent Luna max review; Opus 5 high combined review; planning
Fable 5.1 max / Astra max (the production fable-5.1 slug is not
live-verified; Claude help advertises `claude-fable-5`); recovery Grok
4.6 medium (Grok Build) then Astra medium then Opus 5 high (named
recovery adapters are not live-exercised); `sonnet/medium` is the
explicit `claude-sonnet-5` / `medium` live-test override only. Cross-job
capacity memory persists exhausted routes until trusted provider reset
evidence; unknown reset stays unknown and is never invented as a daily
timer. Generic 429, `RateLimitError`/`rate_limit`, timeout,
permission/consent, invalid-plan, `DataPolicyError`, `RegionError`,
`AuthError`, and `GoUsageLimitError`/`account_rate_limit` never switch
quota. No text-only 429 matching. Zen overflow and direct paid APIs are
off. `make_action`/`make_result` carry implementation, planner-question,
review, and completion envelopes with artifact and failure evidence.

## Tests

Deterministic only; no live model CLIs:

```
python -m unittest discover -s tests -v
python -m compileall -q runner tests
```

Fault injection uses real detached fake processes for duplicate submits,
workspace clashes, launch races, controller death, worker death with
same-session recovery, partial logs, question replay, cancel/timeout,
quota-vs-other-errors, unsupported routes, missing permissions, unknown
ownership, bounded recovery, live-child adopt, completion-before-recover
consume-once, NULL-pid unresolved ownership, and answer+recover
continuation. Bridge tests add default command construction, missing
planner session, adapter session persistence, planner resume/no-fork,
built-in controller lifecycle seams, exact
`FreeUsageLimitError`/`responseBody`/retry classification, fake HTTP
provider-envelope + owned-serve control-path checks, capacity blockers,
and callback failure/busy persistence. All offline; no live model CLIs.
Named recovery adapters and live OpenCode quota transfers are not
claimed from this suite.

Live check: `fixtures/LIVE_RECIPE.md` plus `fixtures/live_sequence.json`.
Real live verification is external.
