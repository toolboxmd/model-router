# Glossary

Canonical terms for Model Router. One term per concept; avoid the synonyms.

| Term | Definition | Avoid |
| --- | --- | --- |
| Job | One accepted request in the runner, identified by its request id. Several jobs may contribute to one Observer task through `observer_task_id`. | task (in runner code) |
| Observer task | An explicitly owned work outcome in Agent Observer. Its identity can connect submissions and several Router jobs without replacing their native identities. | job (for the combined outcome) |
| Invocation | One supervised child process run for a job: a dispatch turn, a planner callback, or a worker turn. Agent Observer's *dispatch* plus *attempt*. | call, run |
| Report | The structured result of a worker turn written to the job directory. Agent Observer's *outcome evidence*. | output, summary |
| Stage | A step of the flow with an executor and an ordered route list: planning, dispatch, the implementation lanes, critical, correction, recovery, review. | phase, step |
| Lane | An implementation stage the planner picks by task class: default, small, hard. | tier, track |
| Critical | A load-bearing step or prose the rest of the work depends on. Planner-executed in its own session before submission, never inside a submitted job. | important |
| Route | One dispatchable choice: harness, pool, model, variant, agent. | model (alone) |
| Pool | A subscription allowance a route draws from: Zen free, Go, xAI, Codex, Claude. | provider, account |
| Window | A share of a Go model's monthly dollar limit: 5-hour, weekly, monthly. | quota period |
| Signal class | How provider evidence is classified: exhausted, overloaded, stalled, context, hard. | error type |
| Stall | A worker turn that stops streaming: busy with no new parts past the silence window. | hang |
| Silence window | Per-harness policy seconds of stream silence that end a busy turn (Codex dispatch and resume 300, OpenCode and every other harness 180), held in `runner/policy.py` as `STALL_SILENCE_SECS_BY_HARNESS`; agent turns carry no elapsed deadline since #88, so the window passes through unclamped and genuine stream silence is the only time-based end for an active turn. | timeout |
| Probe | A fresh minimal request on the same route when a turn goes silent (stall evidence), on an assumed weekly or monthly mark to learn the real boundary, or a proactive usage read of a subscription window through the harness seam. | ping |
| Reading | One proactive usage measurement per pool, model, and window: used, limit, reset_at, observed_at, and source (provider_reported, measured, derived, assumed). Unknown means the probe failed and the route stays eligible on error evidence alone. | usage snapshot |
| Usage probe | A proactive window reading through the harness seam (Codex rate limits, Claude usage, Go cost sums, Zen request counts, Grok billing), on its policy interval or on demand, never blocking routing. | meter read |
| Revalidation | The fresh probe or successful request after a reset_at that clears an exhaustion mark; provider marks hold before reset_at, assumed marks clear on the first healthy probe. | re-probe |
| Reconciliation | The fixed rules joining readings and limit errors: an error always overrides a probe for its window, a sub-100-percent probe never clears a provider exhaustion before reset_at, and eligibility after reset_at needs revalidation. | merge |
| Assumed reset | A limit reset derived from the named or default window when the provider names none, flagged as assumed rather than provider-reported. | guessed reset |
| Context window | A route's input context in tokens as policy data; a context-overflow turn moves to a larger one in its lane. | context size |
| Escalation | The single automatic move to the recovery stage after a failed implementation and one correction. | retry, fallback |
| Sticky home | Muse on Zen free takes every new job (no concurrency cap; skipped only when exhausted or degraded); the `fewest running jobs` spread applies only among capped routes, ties going to the earlier route. | default route |
| Concurrency cap | A route's `max_concurrent`: the most running jobs that may sit on it (exactly one on the 15 and 30 USD Go tiers, reserved atomically with the route record; Muse free carries no cap, so parallel jobs open parallel Muse free sessions). | slot limit |
| Planner-chosen rung | A policy-listed route (Astra medium on Codex, Opus 5.5 high on Claude) the planner itself chooses and runs in its own session; never auto-selected by the runner and never assigned as a dispatcher worker turn. | automatic recovery |
| Failure class | How a failed turn or verification attempt is classified for the ledger: timeout, stall, provider, infrastructure, implementation, verification. Intentional cancellation lives on the job, never on a turn. Unknown means the cause is missing, never a guess. | error type |
| Recovery decision | The ledger event linking a failed attempt to its correction: failures so far, the chosen rung and route target, the failed attempt's seq and timestamps where known, and the next attempt seq. | retry record |
| Acceptance evidence | The required proof that the work is accepted (for example a real product interaction or an independent review), named by the task and carried by the completion envelope, distinct from the worker's exit code, helper tests, and PR URL. | proof (alone) |
| Report contract | The fields every role's report must carry. | schema |
| Worker kit | The checked-in setup that gives a worker host its AgentsMD link and hook; the Grok kit lives in `worker-kits/grok`. | host setup |
| Direction block | The installed AgentsMD loader's verbatim output for a workspace: status, VISION.md, MISSION.md, OBJECTIVE.md with hashes, the core instruction link, plus the workspace's own AGENTS.md when present. Never fabricated. | context block |
| Supply mechanism | How a session received direction, decided from the route's kit on the owned OpenCode server: `runner` (owned server with a kit that does not name `agentsmd-project-direction`, attached verbatim), `hook` (Codex, Claude, Grok, or an owned-OpenCode kit naming `agentsmd-project-direction`, recorded from the runner's own loader read without duplication), `none` (loader missing/failed, reason recorded, job continues). Every loader call uses a unique `session_id` per invocation, still exactly once per invocation. The ledger records what was actually sent, with the direction hash in every supplied case (`hook` and `runner`). | direction source |
| Kit identity | The role kit name and its content hash (`kit_hash`) recorded per invocation, with the kit contents (skills, plugins, MCP, auth link state) listed without secrets. | role tag |
| Auth failure | A missing or rejected login (Codex 401 or missing-authentication text): `hard` with reason `auth`; the dispatch blocks as `codex_auth_failed` with no fallback. | login error |
| Usage-limit refusal | A Codex dispatch answer refusing on quota: the usage-limit message or a `UsageLimitExceeded` / `RateLimitExceeded` error item with `resets_at` (failed turns only). Classifies `exhausted` with the carried reset (the human date reads as UTC); the pool is marked and dispatch moves to Luna on Go. | limit error |
| Dispatch probe | The best-effort Codex rate-limit read before dispatch when due (`account/rateLimits/read` through the app-server first, newest rollout as fallback, else `unknown` with its reason); never blocks dispatch. | pre-dispatch probe |
| Dispatcher envelope | Luna's action object on the OpenCode-hosted dispatcher: the last complete JSON object of the turn's last assistant message (prose then envelope, optionally fenced), validated as an action; the raw text stays in the ledger, and a missing envelope blocks quoting its first 200 characters. | luna action |
| Raw shape | The keys-only outline of a probe response that carried no rate record, stored in the reading detail so the parser can be fixed from the ledger; values never travel. | payload outline |
| Skills loaded | The observed invocable skills recorded per invocation at materialization: the kit directory's `skills/*/SKILL.md` names plus OpenCode's built-in `customize-opencode` on the owned server, not the policy list. | loaded plugins |
| Tools called | Distinct tool part names observed in the turn's assistant messages (empty on text-only turns). | tool usage |
| Experiment, replay | A job run to test a policy change, or to repeat an earlier job under a new policy; never counted as ordinary work. | benchmark |
| Handoff summary | The durable summary stored on the job at submit (explicit or derived from the task packet) and carried before every callback question; the Astra fallback answers from it in a fresh session with no resume. | handoff note |
| Proof command | The task's own proof, run through `/bin/sh -c` in the workspace with the runner's environment; `proof.log` records the exact command and exit code. | proof script |
| Completion refusal | A completion envelope refused while the latest implementation turn's proof failed (`completion_refused: proof failed rc=<n>`); the evidence returns to the dispatcher once, then the job blocks with that reason. | completion block |
| Resumed context | A callback's measured input, cache-read, and cache-creation tokens on its `claude_callback` invocation, visible per job in the Observer mapping. | cached context |
| Policy | The versioned data in `runner/policy.py` that defines pools, routes, stages, windows, and signal classes. The skill table is rendered from it. | config |
| Installed runtime | The plugin directory holding `bin/model-router` and `runner/` that a controller or supervisor runs from, with its package version, policy, and schema. Recovery continues on the currently installed one. | plugin cache |
| Never-started failure | A spawn error witnessed before any child existed, persisted with explicit evidence. Only the missing-runtime cause is recoverable; other causes keep sticky semantics. | spawn error |
