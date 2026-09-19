---
name: model-routing
description: Select the route for delegated software work. Implementation, correction, and recovery go through the Model Router runner on subscription pools; native Codex subagents handle ticket review. Use when dispatching delegated work; keep project workflow and authority unchanged.
---

# Model routing

The coordinator selects routes; it never chooses a model for the human. Project
instructions own delegation, authority, proof, review, and delivery. Keep their
small-direct-work exception; do not change the human-facing model or override
explicit user choices.

Read [the routing policy](references/codex.md), relative to this Skill. It is
generated from the runner's policy data and lists every stage, its routes in
order, and the rules. Reuse it while unchanged.

Implementation, correction, and recovery are submitted to the runner
(`python -m runner submit --lane default|small|hard ...`), which owns the
routes, capacity windows, fallback, and the terminal report. A step or prose
that the rest of the work depends on is critical: the planner does it itself
and submits the remainder. Use a native Codex subagent only for the
`review_ticket` stage, passing the listed model and effort explicitly with
fresh context. If a route is unavailable, stop that dispatch with the reason;
never substitute silently.

In the existing handoff, identify the policy version, the requested and
observed route, and any override or escalation reason. Distinguish requested
from observed execution. Reuse the existing proof, elapsed time, and usage
evidence; leave unavailable measurements unknown.
