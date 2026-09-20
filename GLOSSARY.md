# Glossary

Canonical terms for Model Router. One term per concept; avoid the synonyms.

| Term | Definition | Avoid |
| --- | --- | --- |
| Job | One accepted request in the runner, identified by its request id. Agent Observer's *task*. | task (in runner code) |
| Invocation | One supervised child process run for a job: a dispatch turn, a planner callback, or a worker turn. Agent Observer's *dispatch* plus *attempt*. | call, run |
| Report | The structured result of a worker turn written to the job directory. Agent Observer's *outcome evidence*. | output, summary |
| Stage | A step of the flow with an executor and an ordered route list: planning, dispatch, the implementation lanes, critical, correction, recovery, review. | phase, step |
| Lane | An implementation stage the planner picks by task class: default, small, hard. | tier, track |
| Critical | A load-bearing step or prose the rest of the work depends on. Planner-executed, never dispatched. | important |
| Route | One dispatchable choice: harness, pool, model, variant, agent. | model (alone) |
| Pool | A subscription allowance a route draws from: Zen free, Go, xAI, Codex, Claude. | provider, account |
| Window | A share of a Go model's monthly dollar limit: 5-hour, weekly, monthly. | quota period |
| Signal class | How provider evidence is classified: exhausted, overloaded, stalled, context, hard. | error type |
| Stall | A worker turn that stops streaming: busy with no new parts past the silence window. | hang |
| Silence window | Policy seconds of stream silence that end a busy turn (default 180, with session-database evidence); the per-turn timeout stays the outer budget for active turns. | timeout |
| Probe | A fresh minimal request on the same route when a turn goes silent (stall evidence), or on an assumed weekly or monthly mark to learn the real boundary. | ping |
| Assumed reset | A limit reset derived from the named or default window when the provider names none, flagged as assumed rather than provider-reported. | guessed reset |
| Context window | A route's input context in tokens as policy data; a context-overflow turn moves to a larger one in its lane. | context size |
| Escalation | The single automatic move to the recovery stage after a failed implementation and one correction. | retry, fallback |
| Sticky home | Muse on Zen free takes every new job (no concurrency cap; skipped only when exhausted or degraded); the `fewest running jobs` spread applies only among capped routes, ties going to the earlier route. | default route |
| Concurrency cap | A route's `max_concurrent`: the most running jobs that may sit on it (exactly one on the 15 and 30 USD Go tiers, reserved atomically with the route record; Muse free carries no cap, so parallel jobs open parallel Muse free sessions). | slot limit |
| Planner-chosen rung | A policy-listed route (Astra medium on Codex, Opus 5 high on Claude) the planner itself chooses and runs; never selected automatically by the runner. | automatic recovery |
| Report contract | The fields every role's report must carry. | schema |
| Experiment, replay | A job run to test a policy change, or to repeat an earlier job under a new policy; never counted as ordinary work. | benchmark |
| Policy | The versioned data in `runner/policy.py` that defines pools, routes, stages, windows, and signal classes. The skill table is rendered from it. | config |
