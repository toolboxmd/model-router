# Glossary

Canonical terms for Model Router. One term per concept; avoid the synonyms.

| Term | Definition | Avoid |
| --- | --- | --- |
| Job | One accepted request in the runner, identified by its request id. Several jobs may contribute to one Observer task through `observer_task_id`. | task (in runner code) |
| Observer task | An explicitly owned work outcome in Agent Observer. Its identity can connect submissions and several Router jobs without replacing their native identities. | job (for the combined outcome) |
| Invocation | One recorded run for a job: a T3 thread turn (dispatch, worker, Retry, Escalation, review), a planner question or terminal post, or a verification (proof) attempt. Agent Observer's *dispatch* plus *attempt*. | call, run |
| Report | The structured result of a worker turn written to the job directory. Agent Observer's *outcome evidence*. | output, summary |
| Stage | A step of the flow with an executor and an ordered route list: dispatch, the implementation lanes, critical, correction (Retry), recovery (Escalation), review. | phase, step |
| Lane | An implementation stage the planner picks by task class: small, default, hard (Prism's easy, medium, hard). | tier, track |
| Critical | A load-bearing step or prose the rest of the work depends on. Planner-executed in its own thread before submission, never inside a submitted job. | important |
| Route | One dispatchable choice: a T3 provider instance, model, and effort, with its pool and family. A Prism preference with no policy route runs as `t3:<instance>:<model>@<effort>`. | model (alone) |
| Pool | A subscription allowance a route draws from: Zen free, Go, xAI, Codex, Claude. Its meter is one T3 provider's usage windows (Go is one OpenCode account meter); Zen free has none. An exhaustion error rests the whole pool. | provider, account |
| Signal class | How provider evidence is classified: exhausted, overloaded, stalled, context, hard. | error type |
| Stall | A T3 turn with no streaming tokens and no running tool call past about a minute (`T3_SILENCE_SECS`), confirmed by a fresh snapshot read. | hang |
| Assumed reset | The reset of a limit error that names none: one 5-hour window, flagged as assumed rather than provider-reported. | guessed reset |
| Context window | A route's input context in tokens as policy data; a context-overflow turn moves to a larger one in its lane. | context size |
| Retry | The automatic re-attempt of a failed worker turn: first in the same worker thread, then fresh on the Retry route. Its stage and Prism role keep the internal key `correction` so the snapshot contract stays stable. Prism can switch it off (`enabled: false`), which drops both rungs. | correction (in user-facing text) |
| Escalation | The single automatic move to the Escalation stage after a failed implementation and its retries. Its stage and Prism role keep the internal key `recovery` so the snapshot contract stays stable. Prism can switch it off (`enabled: false`), which drops the rung; with Retry also off, a failed worker turn goes straight to the planner question. | recovery (for this step), fallback |
| Recovery | Controller or crash recovery: a restarted controller continuing a job from its stored state (`recover`). Never the ladder step, which is Escalation. | escalation |
| Sticky home | Muse on Zen free takes every new job (no concurrency cap; skipped only when exhausted, degraded, or ineligible); the `fewest running jobs` spread applies only among capped routes, ties going to the earlier route. | default route |
| Concurrency cap | A route's `max_concurrent`: the most running jobs that may sit on it, reserved atomically with the route record (Muse free carries none, so parallel jobs open parallel Muse free threads). | slot limit |
| Failure class | How a failed turn or verification attempt is classified for the ledger: timeout, stall, provider, infrastructure, implementation, verification. Intentional cancellation lives on the job, never on a turn. Unknown means the cause is missing, never a guess. | error type |
| Recovery decision | The ledger event (internal name) linking a failed attempt to its Retry or Escalation: failures so far, the chosen rung and route target, the failed attempt's seq and timestamps where known, and the next attempt seq. | retry record |
| Acceptance evidence | The required proof that the work is accepted (for example a real product interaction or an independent review), named by the task and carried by the completion envelope, distinct from the worker's exit code, helper tests, and PR URL. | proof (alone) |
| Report contract | The fields every role's report must carry. | schema |
| Dispatcher envelope | Luna's action object: the last complete JSON object of the turn's last assistant message (prose then envelope, optionally fenced), validated as an action; the raw text stays in the ledger, and a missing envelope blocks quoting its first 200 characters. | luna action |
| Experiment, replay | A job run to test a policy change, or to repeat an earlier job under a new policy; never counted as ordinary work. | benchmark |
| Handoff summary | The durable summary stored on the job at submit (explicit or derived from the task packet) and carried before every planner question. | handoff note |
| Terminal report | The once-per-terminal-state end-of-job message posted into the planner thread (request id, status, PR URL or reason, handoff summary); recorded on the job, never changing its status or result. | notification |
| T3 path | How every job runs: dispatcher and worker turns are child threads on a T3 server, and questions and the terminal report are messages in the planner thread. `submit` requires the planner thread. | T3 mode |
| Provider snapshot | T3's `GET /api/prism/snapshot` for the planner's project: every provider instance with its enabled state, models, and usage windows, plus the Prism role kits. Unreadable means unknown, which filters nothing. | provider list |
| Eligible route | A route whose T3 instance is enabled and offers its model in the provider snapshot, and that no usage window or error mark rests. | available model |
| Prism preference | One `{instanceId, model, effort}` entry in a Prism role's model list (the Worker has one list per lane, every other role one list); a non-empty list replaces the policy order for that stage, first entry primary. | route override |
| Baseline proof | The task's proof run once on the base commit before the first dispatch; a red base blocks as `baseline_proof_failed` before any thread starts. | pre-check |
| Planner thread | The T3 thread hosting the planner that submitted a job on the T3 path; its child is the job's dispatcher thread, whose children are the worker threads, and it receives the job's questions and terminal report. | parent session |
| Child thread | A T3 thread running one job slot: the dispatcher under the planner thread, or one worker seq under the dispatcher thread, identified as `sub.<parent>.<suffix>`. | subthread |
| Proof command | The task's own proof, run through `/bin/sh -c` in the workspace with the runner's environment; `proof.log` records the exact command and exit code. | proof script |
| Review turn | One read-only turn on a candidate's exact head in a reviewer child thread of the dispatcher thread, run before a completion is accepted; routed by the review stage (the Prism Reviewer list, else policy order). A restarted controller adopts it, never starts a second one. | dispatcher review, self-review |
| Reviewer verdict | A review turn's result: `approve` or `request_changes`, the findings text, and the reviewed SHA. Approve lets the job complete and lands in the PR body and job result; request changes sends the findings to the worker as the next implementation turn and counts as one ladder failure, so the ladder bounds the rounds. | approval, review result |
| Completion refusal | A completion envelope refused while the latest implementation turn's proof failed (`completion_refused: proof failed rc=<n>`); the evidence returns to the dispatcher once, then the job blocks with that reason. | completion block |
| Policy | The versioned data in `runner/policy.py` that defines pools, routes, stages (the default role preferences), and signal classes. | config |
| Installed runtime | The plugin directory holding `bin/model-router` and `runner/` that a controller runs from, with its package version, policy, and schema. Recovery continues on the currently installed one when the stored job state is compatible. | plugin cache |
