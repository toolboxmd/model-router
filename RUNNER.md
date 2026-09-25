# Durable local runner

A small local runner that accepts prepared work from a planner in any
supported harness (Claude Code, Codex, OpenCode, Grok Build), hands
it to a persistent Codex Luna dispatcher, and returns questions and
completion without depending on the planner process staying open. Python
standard library only. No additional services, runtime dependencies, or network
listeners beyond one owned loopback OpenCode server per implementation turn.

AgentsMD and the target project own workflow, authority, proof, and review.
The runner persists the handoff, owns child processes, and follows one
versioned route policy. It cannot grant new authority.

## Requirements

Use Python 3.11 or newer, Git, and authenticated harness CLIs for the routes you
intend to execute. Keep the full Model Router plugin together. Install AgentsMD
and the Skills, plugins, and MCP integrations named by the selected role kit;
missing dependencies block that dispatch. No credentials ship in this package.
The planner callback wakes the saved planner session in its own harness
(Claude resume, Codex `exec resume`, OpenCode `run --session`, Grok Build
`grok --resume` read-only in the user's own Grok home; only an explicit
recorded fallback for an unresumable Grok session answers from a fresh
read-only session seeded with the handoff summary).

## Public CLI

Set `MODEL_ROUTER_ROOT` to the installed plugin's real root. Its launcher resolves
its bundled runtime without changing the caller's working directory, so relative
task files and workspace paths keep their meaning. State belongs outside the
plugin cache. `python -m runner` remains available from a source checkout for
development.

```
"$MODEL_ROUTER_ROOT/bin/model-router" --state-dir DIR submit --request-id ID (--task JSON | --task-file F) \
  --workspace PATH --planner-session SID \
  [--planner-model M] [--planner-effort E] [--lane L | --route R] [--max-attempts N] \
  [--timeout-secs S] [--job-kind ordinary|experiment|replay] [--replay-of ID] \
  [--planner-harness claude|codex|opencode|grok] [--handoff-summary TEXT | --handoff-summary-file F] \
  [--start | --no-start]
"$MODEL_ROUTER_ROOT/bin/model-router" --state-dir DIR start --request-id ID
"$MODEL_ROUTER_ROOT/bin/model-router" --state-dir DIR status --request-id ID
"$MODEL_ROUTER_ROOT/bin/model-router" --state-dir DIR questions --request-id ID [--all | --clear QID]
"$MODEL_ROUTER_ROOT/bin/model-router" --state-dir DIR answer --request-id ID --qid Q --answer TEXT
"$MODEL_ROUTER_ROOT/bin/model-router" --state-dir DIR cancel --request-id ID
"$MODEL_ROUTER_ROOT/bin/model-router" --state-dir DIR recover (--request-id ID | --all)
"$MODEL_ROUTER_ROOT/bin/model-router" --state-dir DIR result --request-id ID
"$MODEL_ROUTER_ROOT/bin/model-router" --state-dir DIR capacity [--clear ROUTE]
```

`submit` commits the task, workspace claim, policy identity, planner session
and harness, planner model, effort, route, and attempt budget before
it acknowledges. `--timeout-secs` is still accepted for compatibility and
recorded on the job, but since toolboxmd/model-router#88 it bounds nothing:
agent turns and jobs carry no elapsed deadline. The same ID and payload return the existing job, also with
`--start`. A changed payload conflicts. A new job's workspace
must be an existing directory. A workspace that equals, contains,
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
controller after the commit.
`--job-kind` is `ordinary`, `experiment`, or `replay`; a replay names the
request it repeats with `--replay-of`. `--planner-harness` names the harness
hosting the planner session (`claude`, `codex`, `opencode`, or `grok`);
`--planner-session` carries that harness's session id (a Claude session, a
Codex thread, an OpenCode session, or the Grok session id, resumed
read-only with `grok --resume` in the user's own Grok home). Planner callbacks run in
the job workspace.
`--handoff-summary` (or `--handoff-summary-file`) stores the durable handoff
summary on the job; without it the summary is derived from the task packet
(`handoff_summary`, else Issue, decisions, proof command, and goal). The same
ID and payload return the existing job, including the stored summary; a
changed summary conflicts like any other payload field. The task JSON may
carry an optional `links` array of `{"rel": "...", "href": "..."}` objects
(references for the worker, kept verbatim in the stored task); it changes no
routing and needs no runner flag.

For Agent Observer, the task JSON may carry `observer_task_id`, a stable opaque
task identifier such as `project:issue-42`. Related jobs use the same value,
while their request IDs remain distinct. Use the existing Observer identity;
do not infer it from a shared session or an Issue URL. The existing task envelope preserves
this field with the job and its invocations; changing it under an existing
request ID conflicts like any other payload change. No extra CLI flag or
Observer runtime dependency is required. Observer consumes this explicit
ownership, keeps missing identity visible through its legacy per-job task,
and owns attribution and pricing. The field does not assign the planner's
whole session or establish acceptance. The coordinator captures that session's
genuine submissions and outcome evidence through Observer's CLI.

The task JSON may also carry `acceptance` (required acceptance evidence the
completion envelope must then carry as `acceptance_evidence`, so worker exit
zero, passing helper tests, and an open PR cannot establish completion when
the required real product interaction or independent review is missing) and
`draft_pr_allowed` (explicit authorization to complete with a draft PR;
draft status itself is not a defect). Both are preserved verbatim with the
job like any other payload field.

`capacity` lists remembered route capacity with its original provider
evidence, plus the latest usage-probe Reading per pool, model, and window.
`--clear ROUTE` is an operator action after checking the provider
allowance; the runner never invents a reset time.

## Flow

1. Dispatch: `codex exec --json --output-last-message PATH --model
   gpt-5.6-luna -c model_reasoning_effort="max" --sandbox read-only --cd WS`.
   Before the dispatch the runner runs the Codex rate-limit probe when due
   (`account/rateLimits/read` through the app-server first, the newest
   rollout record as fallback; unknown with its reason when nothing
   reads, never blocking the dispatch). A dispatch route the capacity
   memory knows as exhausted or resting is skipped in preflight before any
   child starts: later jobs go straight to Luna on OpenCode Go until the
   reset revalidates. The kit carries the user's
   `auth.json` login by symlink, so the dispatch authenticates. A 401 or
   "missing authentication" text classifies `hard` with reason `auth` and
   blocks with `codex_auth_failed: <short reason>` (no fallback: no other
   route holds the same login). A usage-limit refusal (the "You've hit
   your usage limit ... try again at ..." message, or a
   `UsageLimitExceeded` / `RateLimitExceeded` error item with `resets_at`)
   classifies `exhausted` with the carried reset time (the message's human
   date reads as UTC): the Codex pool is marked exhausted until that reset
   in the capacity ledger with the verbatim message as evidence, and the
    job dispatches on `luna-go/max` in the same step without retrying Codex,
    with the dispatch reason recorded separately from any worker route
    reason: `dispatch_route_reason` in the controller state (the shared
    `route_reason` key is left for worker moves, so the first worker
    invocation keeps reason `initial`) and
    `scope: dispatch` on the `route_switched` event (worker moves carry
    `scope: worker`), so a worker attempt is never misread with the
    dispatch cause. A dispatch that fails with no recognized
    signal still blocks, but with the last provider message in the block
    reason, never a bare "turn not completed".
    The first `thread.started` ID is saved as the Luna task. Luna's action is
    read only from its own final `item.completed` `agent_message` text, or
    the last-message file. Command output is never parsed for actions. On
    both dispatcher paths the action is the last complete JSON
    object of the turn's last assistant message (two messages arrive as
    prose, then the envelope, optionally fenced; surrounding prose is
    tolerated, and a tail missing at most three closers is completed),
    validated as an action envelope; the raw text stays in the ledger.
   A turn with no extractable envelope blocks quoting the first 200
   characters of the assistant text, never a bare "no structured envelope".
2. Luna replies with one envelope: `planner_question`, `implementation`, or
   `completion`. The envelope is saved before its effect.
3. `planner_question`: the question is saved, then the saved planner wakes
     automatically in its own harness without human action: Claude resumes
     with `claude --resume SID --model M --effort E --output-format
     json --tools "" -p PROMPT` in the job workspace, Codex resumes with
     `codex exec resume THREAD --json -c sandbox_mode="read-only" PROMPT`,
     OpenCode resumes with `opencode run --session SID --dir WS --format
     json PROMPT`, and Grok Build resumes with `grok --resume SID -p
     PROMPT --verbatim --cwd WS --tools "" --permission-mode plan
     --no-subagents --disable-web-search --output-format json` in the
     user's own Grok home (where the planner session lives), never a
     runner kit. PROMPT carries the
     stored handoff summary before the dispatcher's question, so a callback
     hours later resumes the exact saved planner session and answers from
     the ledger. The
     answer counts only when the result is a success from the same
     session ID (a Grok resume the CLI cannot run falls back once to a
     fresh read-only session, recorded explicitly as a fallback in the
     invocation metadata and a `grok_planner_fallback` ledger event,
     never as a same-session answer; a result from another session
     blocks as a mismatch without fallback). The planner callback invocation records the resumed
     context with elapsed
     time, so the resumed context is visible per job in the Observer mapping
     (`status` and `result` measurements); the Claude invocation also carries
     input, cache-read, and cache-creation tokens. A running `claude` process that
     names the session in its arguments is a busy planner. Busy, failed, or
     mismatched callbacks block the job with a reason; the question stays
     pending for `answer` plus `recover`. A missing planner session never
     forks a fresh answer: it blocks as `missing_planner_session`. The Astra fallback never resumes:
     it answers from the handoff summary in a fresh session with no resume
     (`controller.astra_fallback_prompt`), persisted the same way.
     `questions` and `answer` stay as an optional human override.
4. The answer is saved, then the same Luna task resumes with `codex exec
   resume ID --json -m gpt-5.6-luna -c model_reasoning_effort="max" -c
   sandbox_mode="read-only"`. A resume that reports another thread, or none,
   blocks.
5. `implementation`: one Muse turn runs on an owned `opencode serve
    --hostname 127.0.0.1 --port 0` on a runner-generated configuration
    directory built from the route's kit (see below). The worker commits as
    it works; at the end it pushes the branch and opens exactly one PR
    without merging it, and reports the PR URL. The worker report
    returns to the same Luna task.
6. `completion`: the terminal result is saved before acknowledgment, but
    never while the latest implementation turn's proof failed, while the
    bound proof is missing, skipped, or stale, while named acceptance
    evidence is missing, or while the PR check fails. The
    completion envelope carries the opened PR URL (`pr_url`) and, when the
    task names required acceptance evidence, its `acceptance_evidence`;
    `result` shows the PR URL, the acceptance evidence, and the candidate
    head commit it was bound to; nothing merges the PR. A completion
    envelope arriving with a non-zero `proof_exit_code` or a `failed` report
    status is refused: the controller records `completion_refused: proof
    failed rc=<n>` (or `proof timed out rc=124` for a proof that ran past
    its budget, apart from an executable-not-found rc127), hands the failed
    report's evidence back to the dispatcher once (a resume carrying the
    report, proof log, and diff paths), and blocks with that reason if the
    dispatcher insists on completion for the same failed turn; `status`
    shows the refusal and the block reason. A completion whose bound proof
    is skipped (`completion_refused: incomplete proof`), ran against an
    older workspace HEAD (`completion_refused: stale proof`), lacks the
    task's required acceptance evidence, names a second PR identity
    (`completion_refused: duplicate PR`), or whose PR does not verify live
    (missing, closed, wrong repository, or pointing at another commit; a
    draft only when the task explicitly sets `draft_pr_allowed`) refuses
    through the same once-then-block pattern, with evidence naming the
    exact gap instead of a proof failure. The PR check reads the PR live
    with `gh pr view` once per completion (stubbed in deterministic tests);
    an unavailable check refuses as unverified, never as verified. A PR URL
    is preserved as the job's single PR identity (`pr_url` in the
    controller state, `pr_identity_preserved` event) only after the live
    check passes, so an invalid first URL never poisons the job and a
    corrected URL is accepted instead of refused as a duplicate:
    correction and recovery update that PR, never open a second, and the
    worker prompt carries it. Recovery refuses the same way when it
    consumes a completion.

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
`implementation_failed` for this ladder without running the suite) or when the task's own proof
command exits non-zero. The first failure leads to a correction in the same
worker session on the same route. The second leads to a fresh correction on
the correction stage's route (Kimi K2.7 Code). The third is the single
escalation to the recovery stage (Grok 4.6 on Go, then native Grok Build on
the xAI subscription, then OpenCode's xAI provider; those pool moves are not
second escalations; recovery never reuses a rung the job already ran).
The ladder stays authoritative: an ordinary implementation envelope carries
no route and never moves the job, so a repeated default can never undo a
correction or recovery move. Every
rung move records a `recovery_decision` event (failures, rung, route
target, the failed attempt's seq and proof timestamps where known, reason;
the next attempt seq stays unknown there) so Observer can measure failure-to-restart and recovery
success; `recovery_next_attempt` links the decision to the actual next
attempt seq when that worker invocation starts, and
`recovery_attempt_result` preserves its outcome, so the next attempt's report and invocation rows join on that seq.
When every rung was used, and normal failures reach 4 or more before any
authorized directed attempt is used (the usual path is one recovery
escalation that fails), the evidence returns to the planner as a concrete
decision through a planner question (the decision required, the evidence,
attempted remedies, the genuinely eligible dispatcher routes, and the
dispatcher's recommendation), never a request for the planner to
implement; the job and Observer identities are preserved. An approach-only
answer consumes exactly one dispatcher-owned attempt on the current route
without re-posting the question. Routine mechanical
recovery stays inside the dispatch subsystem
(the runner's bounded ladder and capacity moves); missing authority or a
consequential scope or approach decision reaches the planner, which answers
with direction (an approach or an eligible route the dispatcher relays as
`directed_route`). That answer permits exactly one explicitly authorized
attempt assigned under the routing policy, never an implicit loop or
unlimited retries. The submitted candidate stays
dispatcher-owned throughout: a failure never authorizes the planner to take
over implementation, debugging, test execution, or verification. The planner
may direct a different approach or a stronger eligible agent through the
dispatcher by naming it (an implementation envelope `directed_route` or a planner
answer): the dispatcher assigns that work under the routing policy when the
route is dispatcher-assignable and has capacity (correction and recovery
stage routes are valid from any lane; other routes must sit in the job
lane), and rejects anything else
with a `planner_route_rejected` event, never a silent substitution. Two
routes stay explicitly planner-executed and are never auto-selected or
dispatcher-assigned: the planner-chosen rungs (Astra medium on Codex, Opus
5.5 high on the Claude harness), which the planner itself chooses and runs
in its own session before submission, and `critical` work, which the planner does in its own
session before submitting and never inside a submitted job. A failure after
the single authorized directed attempt ends the job as `failed` with
`ESCALATION_EXHAUSTED` and the same concrete
decision content in its error payload, which is the planner's to act on. A
failed turn does not block the job: the dispatcher receives the report and
decides; the runner enforces the ceiling. A stopped turn with a supervisor
result and every owned process group proven dead (an OpenCode rc124
startup failure or an rc143 confirmed stop, with every recorded child and
supervisor group present and confirmed dead and no live or unresolved
invocation) returns its evidence to the dispatcher as a failed turn
instead of a sticky block; a finished database row alone never proves
death, absent group identities prove nothing, and missing results, live
groups, rc125, and ambiguous ownership keep the sticky block so no second
writer starts behind a possible owner.

A stored answer is reused only when the stored prompt matches the
dispatcher's prompt; a reused `qid` with a different prompt blocks with
`planner_question_conflict`, cleared by `questions --clear QID` or by the
dispatcher using a new qid. When the live planner callback fails or reports
another session after the question was answered publicly through `answer`,
the stored public answer wins. A resume that fails before `thread.started`
records `codex_resume_failed` with its exit code, as recovery does.

Before resuming a Claude planner, the runner also waits up to 30 seconds for the
session transcript to be unchanged for 5 seconds; a planner still writing
its transcript counts as busy.

## Implementation worker

Each turn starts a fresh `opencode serve` with a random password in its
environment only. The server runs on a runner-generated configuration
directory built from the route's kit (`runner/kits.py`): `OPENCODE_CONFIG_DIR`
and `OPENCODE_CONFIG` point at
`state/kits/<request>.<invocation>.<kit>/`, which holds `kit.json`
(kit identity and hash, plus the observed `skills_loaded`), `AGENTS.md`
(AgentsMD link), `opencode.json`
(the kit's MCP subset only), `skills/` plus `plugins/` equal to the
kit, and `xdg-mirror/` (one symlink per entry of the user's `~/.config`,
or `$XDG_CONFIG_HOME` when the runner itself runs under one, except
`opencode`, rebuilt per invocation). Plugins the kit names are allowed;
nothing is inherited from the
user's own OpenCode configuration (`--pure` is gone). `XDG_CONFIG_HOME`
points at the kit's `xdg-mirror/`, so the shell inside a worker or
dispatcher session keeps the user's environment as in the user's own shell
(`gh auth status`, the global git config, and other XDG-aware tools behave
the same) while OpenCode scans no user skills;
`OPENCODE_DISABLE_EXTERNAL_SKILLS=1` disables the `~/.claude` and
`~/.agents` skill roots, so a session can invoke exactly the skills its
kit names plus OpenCode's built-in `customize-opencode`. The OpenCode
binary already resolves its global config directory as
`OPENCODE_CONFIG_DIR ?? XDG_CONFIG_HOME/opencode`, so `OPENCODE_CONFIG_DIR`
alone keeps the user's `~/.config/opencode` out, and
`policy._opencode_home` prefers `XDG_CONFIG_HOME/opencode` only when it
is a real OpenCode home (it holds `opencode.json`, `opencode.jsonc`,
`skills/`, or `plugins/`); otherwise it falls back to
`~/.config/opencode`. OpenCode writes its plugin scratch (`package.json`,
`package-lock.json`, `node_modules/`, `.gitignore`) into
`XDG_CONFIG_HOME/opencode` at startup, so the mirror gains an `opencode`
entry the resolver ignores, and this project's own proof passes inside a
worker without extra variables (`MODEL_ROUTER_OPENCODE_HOME` keeps
precedence).
The supervisor authenticates
with Basic auth (`opencode:<password>`) and scopes every call with
`directory=<workspace>`. It creates or reuses the saved session and saves
the session ID before the model request. The session's permission rules
are policy data, resolved per route from its role kit: the kit's
`permission_set` (`full` for worker, correction, recovery; `read-only`
for planner, dispatcher, reviewer) selects the base set, with a route's
own `permissions` overrides on top. Dispatch and review routes on OpenCode
run read-only in plan mode (`luna-go/max`, `luna-go-review`): `edit`,
outside-workspace writes, web fetch/search, and doom-loop prompts are denied
because the coordinator role never needs them; the plan agent is kept. Two
rules stay denied on every route: `question`, because an interactive question
stalls a headless session forever, and `task`, because spawning subagents would
bypass the policy and the runner's ownership and proof guarantees. OpenCode does not
confine Bash at the OS level; that remains a known
limit.

Role kits are policy rows (`runner/policy.py` `KITS`): one row per role
(planner, dispatcher, reviewer, worker, correction, recovery) with
instructions, skills, plugins, MCP servers, and the permission set.
Changing any of them is a policy edit with no state-machine change;
`python -m runner.policy validate` rejects a kit that names a skill,
plugin, or MCP server that is not installed, and the generated skill
table lists each role's kit. The Codex kit links the user's `auth.json`
by symlink from the Codex home (honoring `MODEL_ROUTER_CODEX_HOME` and
`CODEX_HOME` overrides), and its `sessions/` links to one directory per job
(`<state-dir>/codex-sessions/<request-id>`) shared by every Codex
invocation of the job, so `codex exec resume` finds the thread its
dispatch wrote; a rollout left in an earlier kit of the same job is copied
there before a resume. The Grok kit links `auth.json` the same way
when the Grok home keeps its login under that filename (recorded as
missing otherwise); `kit.json` records the linked
filenames without secrets, and `status`/`result` list the kit contents
(skills, plugins, MCP, auth link state) from the materialized kit without
secret values.

AgentsMD compatibility follows [AgentsMD #120](https://github.com/toolboxmd/agentsmd/issues/120).
The single `operations` skill can contain ordinary on-demand procedures under
`workflows/`. Its `SKILL.md` declares `agentsmd-layout: procedures-v1` inside
the frontmatter's block-style `metadata` map. Unsupported layouts fail clearly.
Its `workflows/project-direction/index.md` and
`workflows/project-direction/references/context.md` satisfy the worker and
correction kits' `project-direction` policy dependency. The observed invocable
skills then contain only `operations` (plus OpenCode's built-in skill).
Legacy installations still supply the separate `project-direction` skill.
A partial new bundle fails validation and materialization instead of borrowing
an older direction skill. The entire operations tree is linked or copied so
nested procedures and relative Markdown links remain available on every host.
The canonical global instructions, executable `bin/project-direction`, and
`agentsmd-project-direction` hooks remain supported installation components.
Role permissions, direction metadata, and authority boundaries remain unchanged.
Generated kits and other job artifacts stay under the runner's state directory.

Direction supply (`runner/direction.py`): every role session's input
carries the current Project Direction of the job's workspace. The
installed AgentsMD loader (`project-direction` on PATH, `project-direction
hook --host HOST` with `{"cwd": workspace, "hook_event_name":
"UserPromptSubmit", "session_id": "model-router-<request>-<invocation>-<uuid>"}`
on stdin) owns the block; the runner attaches it verbatim and never
fabricates it. Every loader call from the runner uses a unique
`session_id` per invocation, so the loader's per-session hook cache never
suppresses a block; the loader is still called exactly once per
invocation. On the owned OpenCode server the route's kit decides: a kit
whose `plugins` name `agentsmd-project-direction` is a hook host (supply
`hook`, direction hash recorded from the runner's own loader read, no
runner block prepended to the prompt); a kit without that plugin gets the
runner injection (supply `runner`, the loader's verbatim block with
status, the three files VISION.md, MISSION.md, OBJECTIVE.md with hashes,
the core instruction link, and the workspace's own AGENTS.md when present
prepended, hash recorded). Codex, Claude, and Grok stay `hook`.
Review sessions run on hosts whose own hook supplies direction (Codex,
Claude, Grok) or manually from the role's kit below; the runner records
that hook supply and does not duplicate the block. When the loader is missing or fails, the
invocation records supply `none` with the reason, the owned-server prompt
names the three files to read, `status` shows the gap in the invocation
measurements, and the job continues. The ledger records what was actually
sent: `runner` when the runner injected the block on the owned server,
`hook` when the host or kit hook supplied it, and `none` only when neither happened,
with the direction hash in every supplied case (`hook` and `runner`).
The owned-server drive path persists its actual supply back to the invocation row, so a runner-injected
block never records `none` and a kit-hook turn never records `runner`.
Per invocation the ledger records kit
identity and hash, supply (`hook`, `runner`, `none`), hash of the supplied
block, direction status, skills loaded (observed at materialization: the
kit directory's `skills/*/SKILL.md` names plus OpenCode's built-in
`customize-opencode` on the owned server, not the policy list), and tools
called (distinct tool part names in the turn, empty on text-only turns);
`status` and `result` expose them in the Agent Observer mapping.

Manual dispatch recipe (stand-in runs carry the same kit as the runner).
Resolve the route's kit with `kit_name_for_route`, generate an isolated
directory, and point the harness at it:

```
# Owned OpenCode worker on the worker kit (paid treg kept, other dropped).
python3 -c "from runner import kits, policy; kits.materialize_opencode_kit(
  policy.kit_name_for_route('muse-spark-xhigh-free'),
  '/tmp/manual-kit', route='muse-spark-xhigh-free')"
OPENCODE_CONFIG_DIR=/tmp/manual-kit \
  OPENCODE_CONFIG=/tmp/manual-kit/opencode.json \
  XDG_CONFIG_HOME=/tmp/manual-kit/xdg-mirror \
  OPENCODE_DISABLE_EXTERNAL_SKILLS=1 \
  opencode serve --hostname 127.0.0.1 --port 0
# Codex dispatcher on the dispatcher kit (nothing inherited except the
# auth.json login link, so the dispatch authenticates).
python3 -c "from runner import kits; kits.materialize_codex_kit('dispatcher', '/tmp/codex-kit')"
CODEX_HOME=/tmp/codex-kit codex exec --json --model gpt-5.6-luna --sandbox read-only --cd WS PROMPT
# Claude review equivalent (isolated); the planner keeps the user's session.
python3 -c "from runner import kits; kits.materialize_claude_kit('reviewer', '/tmp/claude-kit')"
CLAUDE_CONFIG_DIR=/tmp/claude-kit claude --resume SID -p PROMPT
# Grok recovery equivalent (kit links `auth.json` when the Grok home keeps
# its login under that filename, recorded as missing otherwise).
python3 -c "from runner import kits; kits.materialize_grok_kit('recovery', '/tmp/grok-kit')"
GROK_HOME=/tmp/grok-kit grok -p PROMPT --model grok-4.6
```

The planner keeps the user's own session and is never
isolated: `claude --resume SID -p PROMPT`, `codex exec resume THREAD --json
-c sandbox_mode="read-only" PROMPT`, `opencode run --session SID --dir WS
--format json PROMPT`, or read-only `grok --resume SID -p PROMPT
--verbatim --cwd WS --tools "" --permission-mode plan --no-subagents
--disable-web-search --output-format json` in the job workspace. Worker
turns run on generated kits, but the Grok planner callback runs in the
user's own Grok home where its session lives. Codex uses `CODEX_HOME`, Claude uses `CLAUDE_CONFIG_DIR`, and
Grok Build uses `GROK_HOME` (or `~/.grok`) for their kit equivalents.

The prompt goes through `POST /session/{id}/prompt_async` with the job
route's model, variant, and agent from the policy (free Muse is
`opencode/muse-spark-1.3-contributor-free`, variant `xhigh`, agent `build`;
GLM, Qwen, MiniMax, Kimi, and DeepSeek on Go use the provider default variant;
Grok on Go or xAI uses `medium`). An unknown route is an error, never a
substitution.
The supervisor polls `GET /session/status` for that session only, polls the
session's message parts at a one-second interval, and reads the new
assistant messages when it is idle. Every part creation or update (text,
reasoning, or tool parts) refreshes the turn's last-activity timestamp; a
tool part in running state counts as activity while its process is alive.
A turn whose status is busy with no activity past its harness silence
window is aborted, confirmed idle, and recorded with signal `stalled`
carrying the last-activity age and the last part type. The windows are
per-harness policy data in `runner/policy.py`
(`STALL_SILENCE_SECS_BY_HARNESS`): Codex dispatch and resume wait 300
seconds of stream silence (evidence: Luna at max effort reasoned silently
for 181 seconds on 2026-09-23 with no stream events), while OpenCode
worker turns and every other harness use 180 seconds (evidence: healthy
2026-09-20 sessions showed 110 to 161 second gaps, the stalled session 321
seconds then nothing). Before aborting, the detector probes the same route with
a fresh minimal request, so a stall that is exhaustion in disguise moves
pools instead of retrying the same route; the probe's answer (exhausted,
overloaded, or unknown) is recorded as the signal evidence. An unknown probe
retries the same route bounded by the overload window, then moves laterally
with the route degraded. The Codex and Claude CLI harnesses apply their own
windows to their JSON-line streams through the harness seam (last line time).
Agent turns carry no elapsed deadline (toolboxmd/model-router#88): a
productive turn stays running regardless of total elapsed time, and genuine
stream silence past the policy window is the only time-based end for an
active turn, alongside explicit cancellation and real terminal failures.
The silence window passes through unclamped: no per-turn timeout exists to
clamp below, and no deadline is checked before the stall window.
`RUNNER_STALL_SECS` and per-invocation `stall_secs` shrink the window for
deterministic drills only; they stay unset in production, where the policy
value governs. Every invocation
records its longest observed stream silence (`longest_silence_secs`) so the
window is tuned on data through Agent Observer. The server group stops after
every turn.

A turn on the native Grok Build route runs one headless
`grok -p PROMPT --verbatim --cwd WS -m grok-4.6 --effort medium
--always-approve --disable-web-search --no-subagents --output-format json`
process (plus `--resume SESSION` when a saved Grok session exists) through
the same supervisor path: the exited process is the turn boundary, so no
abort or idle confirmation applies. The JSON result carries the worker
text, the stop reason, the session id, and counters where reported; only
the JSON error object classifies signals, never worker text. The saved
session is stored as `grok_session_id` and cleared on rung changes like
the OpenCode session. Web access and subagents stay off so one worker
cannot block on approval or fork a second writer. The Grok host carries
AgentsMD through `worker-kits/grok/ensure.py`, which keeps the
configuration directory's `AGENTS.md` link and `project-direction` hook
(`--check` reports without writing).

Provider signals are classified from structured evidence only, never from
model-authored text:

- exhausted: a `retry` status with reason `free_tier_limit`, or an
  `APIError` whose provider `responseBody` names `FreeUsageLimitError`,
  `GoUsageLimitError`, `insufficient_quota`, or `UsageLimitExceeded`.
  On the Codex dispatch route a usage-limit refusal also counts: the
  provider's usage-limit message or a `UsageLimitExceeded` /
  `RateLimitExceeded` error item with `resets_at` (failed turns only; the
  message's human reset date reads as UTC and the verbatim message is the
  evidence). Zero retries: the session is
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
- stalled: no stream activity past the silence window while busy. The probe
  answer decides the move: exhausted moves pools, overloaded moves family,
  unknown retries the same route bounded by the overload window, then moves
  laterally with the route degraded 15 minutes like overload.
- context: `context_length_exceeded` is a capacity signal, not a hard
  failure. The turn moves to the next route in its lane with a larger
  `context_window` (a per-route policy hint in tokens); only when no
  larger-context route is left does the turn end as `implementation_failed`.
- hard: auth, region, and consent errors end the
  turn as `implementation_failed` for the ladder; they never move routes.
  A Codex 401 or "missing authentication" text (live: stderr 401
  Unauthorized with stdout missing bearer authentication) is `hard` with
  reason `auth`: the dispatch blocks as `codex_auth_failed: <short reason>`
  with the evidence in `status`, and never falls back to another route.

Before a turn starts, a route the capacity memory knows as exhausted or
resting is skipped the same way (`preflight_exhausted`, `preflight_degraded`),
as is a route whose `max_concurrent` is already used by running jobs
(`preflight_concurrent`), a one-turn route that already ran its turn
(`preflight_one_turn`), and one with no turn left in this job — all without a
child. Preflight consults the usage-probe Readings too: a window at or above
the policy margin (`USAGE_DEGRADED_FRACTION`, 80 percent of its limit) marks
the route degraded for that window, and an exhausted window (100 percent)
skips the route until its `reset_at`. The concurrency reservation is atomic: the target's running count is
read inside the same write transaction that records the new route, and a full
target moves to the next free route in the same transaction. The Go dispatch
fallback (`luna-go/max`) reserves the same way before any child starts. No eligible route left in the lane blocks the job with
`capacity_exhausted`. The `capacity` command lists remembered routes with
pool, model, window, state, evidence, reset time, and reset source
(`provider`, `assumed`, or `cooldown`), plus the latest Reading per pool,
model, and window (`used`, `limit`, `reset_at`, `observed_at`, `source` in
`provider_reported`, `measured`, `derived`, `assumed`); `--clear ROUTE` is an
operator action after checking the provider. A provider-named reset is used
verbatim (Codex `resets_at`, Go `Retry-After` seconds as the exact reset,
Claude's reset time in text); a limit event with none assumes the named or
default window (5-hour: now plus five hours; weekly plus seven days; monthly
the next month boundary), flagged as assumed, honored by preflight.
Readings and limit errors reconcile by fixed rules: an error always
overrides a probe for the window it names; a probe below 100 percent never
clears a provider exhaustion before its `reset_at` (the mark holds and the
probe outcome stays on the ledger); after `reset_at` the route needs one
fresh probe or one successful request to revalidate it before it is eligible
again, so an expired mark stays until `record_probe_outcome`,
a healthy fresh Reading, or `record_route_success` clears it (a successful
worker turn revalidates the same way automatically). Assumed weekly and
monthly marks are re-probed on a lengthening schedule (one hour first,
doubling, six-hour cap) and cleared on the first success; every probe
outcome is recorded. Re-probing is operator-driven via
`core.probe_due_routes`/`core.record_probe_outcome`/`core.record_reading`/`core.record_route_success`
(no sentinel process: an always-on observer is a non-goal), plus the
harness default before a Codex dispatch: when due it reads the newest
rollout record first and otherwise records unknown with its reason.
Probes never block routing: a failing or slow probe records `unknown`
(`used` None with its source semantics kept) and the route stays eligible
on error evidence alone, so the router still works with every probe
failing. `status` and `result` expose the same `capacity` and `readings`
tables Agent Observer reads. The three Go windows stay separate,
so a weekly mark does not clear when the 5-hour mark expires. Zen balance
overflow and direct paid APIs are disabled.

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
  whose action was not saved yet). A job blocked only by `runtime_missing`
  that still owes a planner answer restarts its callback in the same single
  call, like a question-pending job. While the controller lock is
  held, or within 60 seconds of a launch (acknowledged or not), recover
  never replaces the controller and never blocks it for a missing
  handshake. It still blocks for orphaned servers, and for unresolved
  children once the controller's PID is recorded. A consumed Luna turn is
  applied only when it reports the saved thread, and a consumed failed or
  mismatched Luna turn blocks the job with its reason. A consumed failed or
  mismatched planner turn blocks only while its question is still pending;
  if the question was answered meanwhile, the stored answer wins. A failed
  planner callback stays pending for `answer` plus `recover`: the answer
  unblocks the job and a following recover restarts its controller. A failed Muse turn is left to the restarted controller,
  which may switch routes on provider evidence. After that, a free lock proves no controller has
  started; one that starts later fails the lock or its acknowledgment,
  because its token no longer holds the lease, and exits without changing
  the job. Adopting a live child never interrupts it, but the compatibility
  decision gates starting its replacement controller: on an unsupported
  state no controller starts and the next recovery blocks with the specific
  incompatibility instead of executing on unreadable rows. Terminal jobs
  are never resurrected and never assessed, but rows that finished after
  the job ended are still consumed once so their measurements land on the
  ledger.
  Never-started rows are not closed while another process holds the lock. A row a live controller has just
  inserted, not yet claimed by its supervisor, is normal startup. A controller that advertised the current
  lease token with a proven identity is adopted even with an old heartbeat,
  because heartbeats refresh only between steps. Unknown ownership blocks. Results of failed
  turns, and any result after cancellation was requested, are recorded but
  never applied.
- `blocked` jobs never restart through `recover`, except for recover's own
  ownership blocks, which it re-evaluates. Genuine unknown-ownership,
  identity-mismatch, orphaned-server, unresolved-child, failed-resume, and
  insisted-completion blocks stay sticky by design: no automatic wakeup can
  safely start another writer there, and their reasons name no `recover`
  action. Every block reason that does name `run recover` as its next
  action (`runtime_missing`, the step-budget restart, answered planner
  questions) is recoverable through exactly that call. A planner-question block clears
  through `answer`. `cancel` works on any non-terminal job.
- Runtime recovery: when the installed plugin runtime disappears between
  turns (an update removes the old plugin-cache path), the next spawn
  fails before any child exists. That failure is recorded with explicit
  never-started evidence naming the missing path, and the dispatcher,
  worker, and planner-callback turns block as `runtime_missing` with
  `recover` as the next action instead of an opaque sticky failure or a
  ladder move: the cause names the missing path and the recovery action,
  the explicit route is never substituted, and a never-started repairable
  action is never counted as an ordinary provider failure. Only the
  newest attempt for an action decides: an older missing-runtime row
  never reblocks a newer completed or superseding attempt, so a repaired
  retry stays repaired. `recover` resolves the currently installed
  runtime, checks explicitly that it can read the stored job and
  invocation state (controller state parses, job policy matches, route
  known, no newer schema, invocation metas parse), and records the
  actual runtime and policy before and after (`runtime_assessed`,
  `runtime_recovered`, visible in `status` under `runtime`; terminal
  jobs are not assessed). The compatibility decision gates only
  execution handoffs (starting a replacement controller): live children
  are adopted, never interrupted, and ownership is established before
  any transfer. Compatible recovery preserves the job, task, session,
  and route identities and restarts the controller to remake only the
  never-started action. Unsupported state blocks as
  `runtime_incompatible` with its specific reason and retains the
  work: no migration, no route substitution, no replay of live,
  successful, or uncertain actions. Policy compatibility is by policy
  identity (`POLICY_ID`): the recorded `POLICY_VERSION` is provenance,
  and a minor policy revision (such as 2.7.0 to 2.7.1) stays
  recoverable by design; anything else keeps its specific
  incompatibility. Other spawn failures (a missing job workspace, a bad
  command) keep their sticky semantics.
- `cancel` signals every proven child and supervisor group and
  keeps the workspace claimed until they are confirmed dead. If ownership
  stays unresolved, the job blocks and `recover` finalizes it once the
  children are confirmed gone. (Pre-#88 timeout drains used the same stop
  path; since #88 no new timeout intent is ever written.)
- One guarded statement finishes a stop: the outcome follows the stop
  intent stored last (a cancel request overwrites a pending timeout), never
  replaces a terminal status, and the result file mirrors what was
  committed.

Agent turns and jobs carry no elapsed deadline (toolboxmd/model-router#88):
a productive turn stays running regardless of total elapsed time, and
`recover` never cancels or drains an active job because of its age.
`--timeout-secs` is a legacy compatibility slot: it is accepted and
recorded (pre-#88 rows stay readable with their positive values), but it
bounds nothing and is never enforced. Only explicit cancellation stops an
active job: `cancel` persists the intent, signals every proven child and
supervisor group, and keeps the workspace claimed until they are confirmed
dead. If ownership stays unresolved, the job blocks and `recover`
finalizes it once the children are confirmed gone. Rows that already
carried a pre-#88 timeout drain intent (`cancel_requested=2`) still
finalize with their timeout origin. A timeout finalizes as `failed` with
`timeout` only for such legacy drains, a cancellation as `cancelled`.

## State

Each worker turn writes `outputs/<request_id>/turn-<seq>/` (retries on the
same seq use `turn-<seq>-1`, ... so no turn overwrites another):
`report.json` (route, policy version, observed model and variant as separate
fields, session, changed files, the workspace HEAD the evidence was taken
against, proof command, exit code, proof class and attempt timestamps,
failure class and harness signal where one exists, tokens verbatim
with a source label including reasoning and cache read and write, native
message identities, blockers, a redacted worker summary), `proof.log` (the
task's own `proof` command run through `/bin/sh -c` in the workspace with the
runner's environment, so `&&`, pipes, and quoting behave as in the project's
own docs; the log records the exact command and its exit code plus the
redacted output, or truthfully records that the suite was skipped and why),
`diff.patch` (`git diff HEAD` plus untracked files, or a note
when the workspace is not a checkout), and `worker.txt` (the worker's full
text, redacted). Writing the failure report is separate from executing
proof: exhausted, stalled, crashed, hard-error, or otherwise incomplete
turns skip the
suite (`proof_class skipped` with its reason) instead of running the full
suite automatically against an unfinished candidate; the dispatcher requests
useful proof of a coherent candidate, and required final verification still
refuses completion without a bound passing proof. A proof that runs past its
budget has its whole process group stopped and classifies `timeout` (rc 124),
apart from an executable-not-found `not_found` (rc 127); a main process that
already exited while a background child holds the output pipe is reaped and
classifies by its actual exit, never as a false timeout; leftover group
members are stopped after a normal exit too, so no proof child races a later
writer. The proof group is recorded durably while it runs
(`proof-owner.json` with PID, PGID, and leader start identity): public
cancel drains that actual owned group with PID reuse protection, and
cancel and `recover` block on unresolved proof ownership instead of
treating the job as stopped while the proof tree keeps running. An
unreadable record or one without group identities is ambiguous
ownership, never safe death: the workspace claim is retained until
ownership resolves. The workspace claim is otherwise retained until
ownership is confirmed dead. Every executed proof is also a durable invocation row with
`stage='verification'` (kind `proof`), its start/end timestamps, exit code,
proof class, and elapsed time, so verification outcomes are observable from
the existing invocation records. A supervisor-level rc124 with no proof run
(a startup failure) classifies `infrastructure`, never `timeout`. Turn failure classes
are timeout, stall, provider (exhausted, overloaded, context), infrastructure
(hard errors, missing runtime, lost supervisor, unconfirmed stop),
implementation (the worker ran and errored without a capacity signal), and
verification (the task's own proof ran and failed); intentional cancellation
lives on the job's cancel intent, never on a turn report; missing causes and
timestamps stay unknown. Harness-reported worker questions travel in the report's
blockers (redacted), never as live questions; the implementation harness
denies question permission, so blockers are typically empty. The dispatcher's resume message carries these paths and the
structured fields (route, policy version, models, variants, status, failure and
proof classes, tokens, native identities, the candidate commit, and the
preserved PR identity) instead of the worker's prose. The dispatcher consumes
that exact-candidate evidence; it requests new proof only when the evidence is
missing, skipped, stale, or for a different candidate, and never reruns the
full suite automatically against an unfinished candidate.

Every invocation records its stage, requested route, policy version, route
reason (worker moves record `route_reason` with `scope: worker`; dispatch
moves record `dispatch_route_reason` with `scope: dispatch` and leave the
worker's reason alone, so the first worker invocation keeps `initial`),
harness version, elapsed time, terminal class (completed, failed,
crashed, cancelled only with explicit job cancellation intent, timeout,
quota, overloaded, stalled, context, hard_error, infrastructure for a
startup rc124 with no proof run or a stop without cancel intent),
longest observed stream silence (`longest_silence_secs`), usage counters
verbatim under a source label, the observed model and the observed variant as
separate fields, and native identities (Codex thread and turn ids, Claude
session and result ids with the callback's prompt digest, OpenCode session
and message ids). Jobs record their kind (`ordinary`, `experiment`, `replay`
with `--replay-of`), the planner harness (`claude`, `codex`, `opencode`,
or `grok`), the workspace
commit at submit (`base_commit`) and completion (`head_commit`), and the
optional task `links`. Events and invocations carry `schema_version` 2;
Agent Observer reads this ledger directly. `status` and `result` show the
measurements including native identities, variants, and schema versions,
plus the `capacity` marks and usage-probe `readings`;
`status` keeps the redacted error evidence (signal, evidence, retry counts
and caps) so CLI readers need not query the database directly;
`result` also lists the turn reports. Recovery links are observable from
existing records: each `recovery_decision` event carries the failed seq,
rung, route target, reason, and proof timestamps where known (its
`next_attempt_seq` stays unknown until linked); `recovery_next_attempt`
binds the decision to the actual next worker seq when that invocation
starts; `recovery_attempt_result` preserves that attempt's outcome.
Verification attempts are invocation rows with `stage='verification'`
(kind `proof`) carrying started/ended timestamps, exit code, proof class,
and elapsed time; cancellation intent lives on the job
(`cancel_requested`) with `cancelled`/`timeout` events. A stop (rc143)
reads `cancelled` on the invocation row only with explicit job
cancellation intent; without intent it reads `infrastructure` when stop
evidence is present and `unknown` when none is, never cancelled by exit
code alone. A startup rc124 carries `infrastructure` in both the turn
report and its invocation row; an actual proof rc124 timeout carries
`timeout` in both. Combined
acceptance with Agent Observer still needs its importer to read these
fields (it currently discards Router events and never reads `report.json`):
the remaining adapter additions are importing `recovery_decision`,
`recovery_next_attempt`, `recovery_attempt_result`, and
`verification_attempt` events (with sanitized failure and proof classes,
seq linkage, and timestamps), `stage='verification'` invocation rows with
kind `proof`, sequence metadata from `meta_json`, `dispatch_route_reason`
versus worker `route_reason` (event scope `dispatch` versus `worker`),
the job cancel intent (`cancel_requested`), and the class mapping
(context pressure is Router `provider`, rc143 unconfirmed stop is Router
`infrastructure`, startup rc124 is `infrastructure`, proof timeout is
`timeout`); no new Router fields are planned for this. Unknown
timestamps, causes, and ownership stay unknown throughout and are never
fabricated.

`--state-dir` (or `DURABLE_RUNNER_STATE_DIR`) is made absolute and is
`0700`. Detached controllers and supervisors start from the package
directory, so the CLI works from any working directory. `jobs.db` (SQLite,
WAL), `outputs/*`, and `workers/*.json` are `0600`. Passwords and credentials are never written to the
database, events, or logs; worker text, proof output, error evidence, and
report errors are redacted for secret keys and free-text secret shapes
before they are persisted or forwarded.

States: `pending -> running <-> question_pending -> succeeded | failed |
cancelled`, with `cancelling` while children stop; the last stored stop
intent decides between `cancelled` and a timeout (only legacy pre-#88
timeout drains still carry the timeout intent). `blocked` always has a reason. `status`
shows summaries: the lease token, the task body, Luna envelopes, worker
reports, the completion report, and child output stay in the private
database and output files. It does show Luna's planner questions and block
reasons, which can include redacted provider or exception text. `result` prints the full terminal result on
request.

## Harness seam

`runner/harnesses.py` defines the seam between the runner and the harnesses
that execute turns. A `Harness` base class declares the interface; `CodexCLI`,
`ClaudeCLI`, `OpenCodeServer`, and `GrokBuildCLI` are the concrete adapters. A registry holds
one instance per harness with lookup helpers: `HARNESSES` by name, `harness_for`
by invocation kind, `harness_named` by name, `kind_for_cmd` from a command
line, and `worker_control_kinds` for the implementation-turn kinds across
harnesses. A harness provides spawn specs, session and report parsing, provider
signal classification, stream-activity tracking (last-activity timestamps
per turn for stall detection on every harness), and usage measurement; the
core and supervisor call
these methods instead of branching on invocation kind. Each harness declares
its capabilities, and each policy stage declares the capabilities it needs.
Each harness also probes its subscription windows through the seam:
Codex `account/rateLimits/read` (every 300 seconds, or on demand before a
dispatch through the controller default, which reads the app-server first
and the newest rollout as fallback, and records unknown when nothing
reads; a response with no rate record stores unknown with the raw shape,
keys only, in the reading detail so the parser can be fixed from the
ledger; an injected probe hook never blocks the dispatch), Claude `/usage`
(every 180 seconds at most, plus the free statusline feed while a session
runs), OpenCode Go rolling cost sums from the local session database against
the policy tier windows, Zen free request counts against an assumed cap, and
Grok monthly billing (provider-reported) with weekly assumed and
error-driven cool-off. While a session runs, the harness reads the latest
quota record from the session's own file instead of probing (Codex rollouts,
Claude transcripts), treated as provider-reported with the file timestamp.
Account-level probes fan out to one Reading per pool route model (Codex to
every codex-pool model, Claude to every claude-pool model, Grok monthly to
both xai-pool models; the Go Grok route keeps its own Go-dollar readings),
so preflight sees each reading on every route that draws on the pool.
Unrecognized Codex window durations are skipped and never stored; the Claude
session window stays on the ledger for the observer but never degrades or
exhausts a route. Every probe returns Reading dicts for `core.record_reading`
to reconcile; a failure records unknown and the route stays eligible on
error evidence alone.
The harness also owns its action identity: a Grok turn resumed with
`--resume` is the same logical turn, so a restarted controller reuses the
finished attempt instead of starting a second writer.
`route_capability_blocker(route, stage)` names any capability the route's
harness lacks; a dispatch then fails with `RunnerError("route_capability_mismatch: ...")`
before any child starts. Dispatch has one fallback route: when the Codex CLI
dispatch route cannot start a task, the controller dispatches Luna on OpenCode
Go (`luna-go/max`, agent `plan`, one turn per job) through the owned OpenCode
server (`_dispatch_on_opencode` in `runner/controller.py`). Later turns resume
that OpenCode session while the saved dispatch route stays OpenCode. The
fallback extracts the envelope the same way on dispatch and on resume
(last complete JSON object of the last assistant message, fenced or bare),
keeps the raw text in the `luna_action` ledger event, and quotes the first
200 characters of the assistant text when no envelope extracts.

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
recorded fallback), the implementation
lanes default, small, and hard, planner-chosen rungs (Astra medium on Codex,
Opus 5.5 high on the Claude harness; the planner itself chooses and runs them),
critical
(planner-executed), correction (Kimi K2.7 Code after the same session), recovery
(Grok 4.6 on Go, then native Grok Build on the xAI subscription, then
OpenCode's xAI provider; pool moves inside recovery are not second
escalations; recovery skips rungs the job already used), ticket review (Luna max on Codex, then Luna on OpenCode Go in plan
mode), and final review (Opus 5.5 high on the Claude harness, then Astra high
on Codex, then Luna max). Worker pools in order: Zen free, Go, xAI;
subscription logins only. The default lane carries the full Go implementer
chain in intelligence order: Muse xhigh on Zen free, Muse xhigh on Go, GLM
5.3 Flash, Qwen 3.8 Flash, DeepSeek V4.1 Flash, Hy3, MiniMax M3, MiMo 2.5,
MiniMax M2.7, LongCat 2.0, GLM 5.2, Kimi K2.6, GLM 5.1 (Qwen 3.7 Plus and
Qwen 3.6 Plus are absent: no known tier); the small lane starts from Muse free,
then Muse Go, then GLM 5.3 Flash onward; the hard lane runs Muse free, Muse Go, GLM-5.3, DeepSeek V4 Pro, Grok
4.6 on Go, native Grok Build on the xAI subscription, Grok 4.6 on xAI; correction is Kimi K2.7 Code. Muse on Zen free
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
capped route). Recovery moves (`grok-4.6-go`, `grok-4.6-build`, `grok-4.6-xai`) are valid from any
implementation lane and reason in the recovery stage. A manual or
planner-chosen route inside an implementation lane, the recovery stage, or the
dispatch order never validates. DeepSeek
V4.1 Flash is a 15 USD route from 2026-09-20. Every route in
the policy has an adapter today; the OpenCode adapter takes model, variant,
and agent from the route, the Grok Build adapter takes the bare model id and
effort. The xAI pool names the native harness first (`grok-4.6-build`) with
OpenCode's xAI provider as its `next_pool` fallback (`grok-4.6-xai`); the
hard lane's last rung and recovery use both. Go Luna as a dispatch fallback arrives with the
adapter seam. Changing a stage's harness or model is a policy edit; adding a
role needs a policy row plus a report contract.

Signal classes are defined in the policy and applied by later work: exhaustion
(`free_tier_limit`, `FreeUsageLimitError`, `GoUsageLimitError`,
`insufficient_quota`) moves the same model to the next pool where the route
declares one (free Muse to Go Muse, Go Grok to native xAI Grok Build, Build
to OpenCode's xAI provider), otherwise to the
next family; overload
(`overloaded`, `rate_limit`, `account_rate_limit`, HTTP 503 and 529) moves to
the next family after a bounded retry window; stalled (stream silence past
the window, with a same-route probe first) retries bounded like overload,
then moves laterally; context (`context_length_exceeded`) moves to a
larger-context route in the lane; hard errors (auth, region, consent) end the
turn as `implementation_failed` for the ladder and never move routes. The
Grok Build harness classifies the CLI's JSON error objects into the same classes
(quota and credit wording as exhausted, rate-limit wording and 429/503/529
as overloaded, context and auth wording as hard). The controller
applies them on both worker paths, driven through the harness seam.

## Verification

```
python3 scripts/test.py
python3 -m compileall -q runner scripts tests
```

The test entry point creates disposable dependency homes using the suite's fake
skill, plugin and MCP inventory. It preserves the user's installation and removes
the fixtures afterward. Pass unittest arguments for a targeted run, for example
`python3 scripts/test.py tests.test_release`.

The suite uses fake `codex`, `claude`, `opencode`, and `grok` executables shaped like
the real contracts (`tests/fakes.py`) and real detached processes. The fake
OpenCode server answers plan-agent dispatcher turns the live way: two
assistant messages, the first prose, the second the envelope, optionally
fenced (`FAKE_OC_FENCE`), with the first prompt carrying implementation
under `FAKE_OC_PLAN=implement_then_complete`. The fake
`grok` covers ok, xAI exhaustion, overload, hard errors, hangs, and a held
turn for controller-death drills. It covers
duplicate and concurrent submission, workspace conflicts, launch races,
controller and worker death, killed supervisors, pending questions,
answer-plus-recover, cancellation, legacy timeout drains, trusted and untrusted quota
evidence, unsupported routes, and unknown ownership. It does not prove live
model behavior. Live verification evidence is recorded on the owning Issue.

Not verified live: the production planner default `claude-fable-5-1`, a real
free-to-Go transfer through this runner, and recovery routes. Known limits:
an interactive planner that is open but idle, without the session ID in its
arguments, is not detectable as busy; OpenCode does not confine Bash at the
OS level; any process running as the same user can read the state directory.

Project-local OpenCode configuration and Skills are disabled on owned servers
with `OPENCODE_DISABLE_PROJECT_CONFIG=1`. The kit remains the source of allowed
Skills and MCP configuration. The flag also suppresses root instruction
discovery, so the runner supplies applicable `AGENTS.md` files from the repository
root through the workspace in each OpenCode input, including when the direction
loader fails. The AgentsMD hook continues
to supply Project Direction. The `skills_loaded` materialization
inventory describes kit files and the known built-in Skill; it does not count
which Skills the model invoked. The optional local discovery regression checks
the installed OpenCode binary without sending a model request.
