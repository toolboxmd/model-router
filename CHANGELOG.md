# Changelog

## [0.36.0] - 2026-09-26

### Added

- An independent review turn from the Prism Reviewer role approves or requests changes before a job completes (#126)

## [0.35.5] - 2026-09-26

### Changed

- The dispatcher supplies the proof command so the runner checks every job (#128)

## [0.35.4] - 2026-09-25

### Changed

- An unparsable dispatcher reply gets one repair message on the same dispatcher thread; a second bad reply blocks, and recover retries the block (#105)

## [0.35.3] - 2026-09-25

### Added

- A small fixed `model-router` Skill (how to submit and follow a job through the bundled CLI), so the marketplace can build the Cursor plugin; promotion of 0.35.2 was rejected with `released Project declares no Cursor Skills`

## [0.35.2] - 2026-09-25

### Fixed

- The Project Record omits `factSources.skills` instead of listing none, so the marketplace can promote the release (0.35.0 and 0.35.1 were rejected with `factSources.skills must be a non-empty list`)

## [0.35.1] - 2026-09-25

### Fixed

- A T3 child thread's ID is saved before its create request and its turn-start state before posting, so a controller crash never loses a running thread; recovery adopts the saved child on its saved route (including fallback routes) instead of starting a second writer, and an unreadable T3 state never posts and stays recoverable (#120)
- `cancel` confirms the controller process has exited (TERM, bounded wait, KILL, PID identity check), then interrupts every active T3 child and confirms it settled; until then the job stays `cancelling` and keeps its workspace claim. Only a real HTTP 404 counts as a missing child (#120)
- The recovery deadline applies only to confirming a turn started, not to healthy adopted turns (#120)

## [0.35.0] - 2026-09-25

### Changed

- Jobs run only as T3 threads: `submit` requires `--planner-t3-thread`, and every stage (dispatch, resume, worker, correction and recovery turns, planner questions, terminal report) runs through T3. `--planner-harness`, `--planner-model` and `--planner-effort` are recorded as evidence only (#110, #116)
- Models, limits and role preferences come from T3's Prism provider snapshot (`GET /api/prism/snapshot`, toolboxmd/t3code#19), read before each controller step: a model turned off in T3 leaves routing, a usage window at 100 percent rests every route on that meter until `resetsAt` (or a later read), 80 percent degrades, and a non-empty Prism (role, lane) list replaces the policy order, with unknown entries running as `t3:<instance>:<model>@<effort>`. An unreadable snapshot filters nothing (#116)
- Error marks expire at their reset (OpenCode's `next` in epoch milliseconds, a `resetsAt`, a `retry_after`, else one assumed 5-hour window), so a stale Zen free mark no longer blocks Muse free; exhaustion rests the whole pool, so a Go limit never moves laterally to another Go model; a successful turn clears assumed marks on its pool (#111, #116)
- Routes are T3 model selections (instance, model, effort) with pool, family and caps; policy 2.9.0 (#116)
- T3 path: a job's threads form a tree. The dispatcher thread stays a child of the planner thread, and worker, correction and recovery threads are children of the dispatcher thread (`sub.<dispatcher>.<suffix>`, plus `parentThreadId`), so the planner's Agents panel shows one dispatcher per job. Questions and the terminal report still go to the planner thread, and every child's first message still links the planner thread (#113)

### Added

- Baseline proof: before the first dispatch the task's proof runs once on the base commit (in place when the workspace is clean there, else in a removed scratch worktree); a red base blocks as `baseline_proof_failed` before any thread starts. `"baseline_proof": false` opts out (#111, #116)
- A capacity signal on the correction route moves laterally into the job's lane instead of blocking (#111, #116)
- The dispatcher's evidence carries the job branch's commits since base and any open branch PR, so finished work is not mistaken for no work (#116)
- `capacity` shows the T3 snapshot view per route next to the error marks (#116)

### Fixed

- A capped fallback dispatch route is reserved before its thread starts, and an exhausted escalation asks the planner in its thread on the next step instead of waiting for `recover` (#116)

### Removed

- The direct harness path: Codex CLI dispatch and resume, the owned OpenCode server, the Grok Build CLI worker, the Claude, Codex, OpenCode and Grok planner callbacks and their busy checks, the CLI terminal-report senders, child-process supervision, role kits, direction supply, and per-harness silence windows (#110, #116)
- The model-routing Skill and its generated routing reference; the plugin is now the runtime's distribution and usage guidance lives in Chromeria's Prism tool descriptions (#115, #116)
- Go cost tables and tier derivations, usage probes and readings, prose reset parsing, and the host-only planning, review and planner-rung routes (#116)

## [0.34.0] - 2026-09-25

### Added

- T3 path: a job submitted with `--planner-t3-thread` runs its dispatcher and worker turns as T3 child threads of the planner thread (`sub.<planner>.<suffix>`, plus `parentThreadId` for the fork link), created through the T3 orchestration API on the route's provider, model and effort; each child's first message names the job and links the planner thread (#106)
- Dispatcher questions and the terminal report for a T3-hosted planner go into the planner thread as messages, and the planner's in-thread reply is read back as the answer; no headless process resumes the planner session. The direct CLI path is unchanged for jobs without a planner thread (#106)
- Activity-based liveness on the T3 path: an open tool or task call keeps a turn healthy however long it runs, silence with no running tool (including a tool held behind an unanswered approval) is flagged after about a minute and probed at once, and explicit provider errors, including a refused turn start, act at once (#106)

## [0.33.1] - 2026-09-25

### Changed

- Project Direction: the milestone is proven from a planner in a T3 thread whose agents run as T3 threads

## [0.33.0] - 2026-09-25

### Added

- The runner wakes the saved planner once per terminal state (succeeded, blocked, failed, cancelled) with an end-of-job report through the existing callback path, carrying the request id, status, PR URL or reason, and handoff summary; busy planners retry within a bounded window, delivery is recorded on the job and visible in status, and restarts send no duplicates (#94)
- Terminal-report delivery is a durable per-event send: a finished prior send is adopted by its output, so a crash between send and record neither duplicates nor loses the report (#94)
- Grok terminal reports resume the saved planner session read-only through the #96 callback path, with the same identity check and busy wait as the other harnesses (#94)

## [0.32.0] - 2026-09-25

### Added

- Grok planner callbacks resume the saved planner session read-only (`grok --resume` with no tools, plan permission mode, no subagents, no web access) in the user's own Grok home, with the answer counting only from that same session; an unresumable session falls back once to a fresh read-only session recorded explicitly as a fallback, and a result from another session blocks as a mismatch (policy 2.8.0)

## [0.31.2] - 2026-09-25

### Changed

- Project Direction: the delivery milestone now requires every job's end state, ready to merge or blocked, to reach the planner in its own thread

## [0.31.1] - 2026-09-25

### Changed

- Project Direction: planners are hosted in the T3 Code workspace and jobs run as T3 threads

## [0.31.0] - 2026-09-25

### Added

- dispatcher-owned recovery evidence and truthful acceptance

## [0.30.0] - 2026-09-24

### Added

- A failure caused by a removed plugin runtime recovers through the currently installed compatible runtime: the missing path is diagnosed with its cause and next action, only provably never-started actions are remade with identities preserved, and unsupported state is retained with its specific reason

## [0.29.2] - 2026-09-24

### Changed

- delete elapsed-time termination of agent turns and jobs (#88)

## [0.29.1] - 2026-09-24

### Changed

- Carry Observer task identity across related Router jobs

## [0.29.0] - 2026-09-24

### Added

- A planner in any harness hands off to Model Router: submit accepts Claude, Codex, OpenCode or Grok planners, the dispatcher wakes the planner automatically in its own harness, workers commit and open the job's PR, and the Codex dispatcher path repairs up to three missing closing braces

## [0.28.1] - 2026-09-24

### Changed

- Project Direction: the delivery milestone is simplified to one open PR per job, with the planner woken automatically in its own harness when the dispatcher needs judgment

## [0.28.0] - 2026-09-24

### Added

- Model Router no longer compacts the planner session after submit: callbacks resume the planner with the stored handoff summary, and policy 2.7.0 drops the compaction settings

## [0.27.1] - 2026-09-24

### Changed

- Project Direction: the next milestone delivers tasks from a planner in any harness to one review-ready final PR, with the dispatcher as the human's point of contact and the planner engaged only for escalations

## [0.27.0] - 2026-09-24

### Added

- Codex dispatch and resume record the observed model from the thread rollout and block the job on a model switch

## [0.26.2] - 2026-09-23

### Changed

- Codex turns wait 300 seconds of stream silence before a stall, and command or file output no longer reads as an auth failure (policy 2.6.1)

## [0.26.1] - 2026-09-23

### Changed

- Codex resume finds its dispatch thread through one shared sessions directory per job; planner compaction requests JSON and is recorded truthfully

## [0.26.0] - 2026-09-23

### Added

- Remove the manual-only Codex dispatch route and resume Codex on the job's active dispatch route

## [0.25.1] - 2026-09-23

### Changed

- Isolate deterministic proof from host installations and verify pull requests before release

## [0.25.0] - 2026-09-23

### Added

- Automatically release validated Model Router versions after merge

## [0.24.0] - 2026-09-23

### Added

- Support AgentsMD operations procedure bundles and legacy role kits

## [0.23.0] - 2026-09-23

### Added

- Bundle the MIT-licensed routing plugin, preserve project instructions in isolated OpenCode sessions, and update Opus routes to 5.5

## [0.22.0] - 2026-09-21

### Added

- Policy resolver ignores OpenCode's run-time scratch directory in the XDG mirror so the project's proof passes inside a worker

## [0.21.0] - 2026-09-21

### Added

- Skill isolation on the owned OpenCode server: XDG mirror keeps the user's environment, external skill roots disabled, skills_loaded observed

## [0.20.0] - 2026-09-21

### Added

- Direction supply follows the kit with a unique loader session id (#52)
- XDG shadow dropped and the stray live report removed from the owned-server environment (#53)

## [0.19.0] - 2026-09-21

### Added

- Proof runs through the shell, completion is refused while the last turn's proof failed, and worker supply is labeled truthfully

## [0.18.0] - 2026-09-21

### Added

- Parse the dispatcher envelope from the OpenCode-hosted Luna turn: last JSON object of the last assistant message, fake mirrors the real RUNNER_RESULT shape

## [0.17.0] - 2026-09-21

### Added

- Codex usage-limit message is exhaustion: classify it with its reset time and fall back to Luna on OpenCode Go for dispatch

## [0.16.0] - 2026-09-21

### Added

- Codex kit must carry the login: auth.json into the kit CODEX_HOME, 401 classified as an auth failure, usage probe before dispatch

## [0.15.0] - 2026-09-21

### Added

- Compact the planner session at handoff and store a durable handoff summary on the job

## [0.14.0] - 2026-09-20

### Added

- Direction supply and measurement through the harness seam

## [0.13.0] - 2026-09-20

### Added

- Usage probes per harness: proactive window readings with reset times, reconciled with limit errors

## [0.12.0] - 2026-09-20

### Added

- Role kits as policy data and per-role session configuration

## [0.11.0] - 2026-09-20

### Added

- Grok Build CLI harness for the xAI pool

## [0.10.0] - 2026-09-20

### Added

- Stall detection from stream activity: abort a silent worker turn within minutes, not at the timeout

## [0.9.0] - 2026-09-20

### Added

- Policy v2.1: full Go implementer chain, concurrency caps, sticky homes, planner and review fallbacks, per-route worker permissions

## [0.8.0] - 2026-09-20

### Added

- Single 0.7.0 to 0.8.0 transition covering Issues 13-16: signal classes with pool/model/window capacity (13), turn reports with measurements and ledger schema 2 (14), one escalation with recoverable step budget and #8 minors (15), harness seam with legacy lane removal and Go Luna dispatch fallback (16); harness seam: every harness call goes through runner/harnesses.py; legacy worker lane removed; dispatch falls back to Luna on OpenCode Go

## [0.7.0] - 2026-09-19

### Added

- Escalation ladder with one recovery escalation and a terminal hand-back to the planner, job-level step budget with recover-owned launch budget, question conflict clearing, resume and recovery fixes from #8

## [0.6.0] - 2026-09-19

### Added

- Turn reports, structured dispatcher evidence, per-invocation measurements with native identities, job provenance, and ledger schema version 2

## [0.5.0] - 2026-09-19

### Added

- Signal classes applied: exhaustion moves pools with zero retries, overload moves families after a bounded window, capacity per pool, model, and window with a degraded state and preflight skips

## [0.4.0] - 2026-09-19

### Added

- Policy v2 as data with lanes, subscription pools, and signal classes; skill table generated from the policy; glossary

## [0.3.0] - 2026-09-18

### Added

- Add an opt-in durable local runner that hands Claude-planned work to a persistent Codex Luna dispatcher and Muse workers with crash-safe process ownership

## [0.2.1] - 2026-09-19

### Changed

- Record the confirmed Project Direction: Claude Code host, one policy for skill and runner, subscription pools

## [0.2.0] - 2026-09-17

### Added

- Add a small Codex routing skill with one Markdown model table and bounded escalation; narrow project direction to Codex v1.

- Add the Codex-only routing skill, canonical model policy, bounded recovery, native dispatch reference, package validation, and approved project direction.

## [0.1.0] - 2026-09-03

### Added

- Establish the Model Router project and confirmed Project Direction
