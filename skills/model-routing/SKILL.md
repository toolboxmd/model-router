---
name: model-routing
description: Route delegated software work through one durable, capacity-aware subscription policy. Use when coordinating implementation, correction, recovery, or review across supported harnesses. Preserve the project's workflow, authority, and proof requirements.
---

# Model routing

Project instructions own delegation, authority, proof, review, and delivery.
Keep their small-direct-work exception and explicit user choices. Do not change
the human-facing model.

Resolve this `SKILL.md` to its real filesystem path, including symlinks. The
plugin root is two directories above its containing `model-routing` directory.
Read [the routing policy](references/codex.md) relative to this Skill. That
shared reference is generated from `runner/policy.py`; reuse it while unchanged.
The filename remains stable for existing consumers.

Use the bundled `<plugin-root>/bin/model-router` from the target workspace.
Do not depend on the caller's Python path or a separate source checkout.
The package must contain `bin/`, `runner/`, and `skills/` together. Stop the
affected dispatch if any required component is missing.

Submit prepared implementation, correction, and recovery work through this
launcher using the policy's lane. The runner owns capacity, fallback, durable
state, and the terminal report. Read [the runner contract](../../RUNNER.md)
for task fields, proof, and commands. A critical step stays with the planner:
do it in the planner session before submitting, then submit the remainder.
Once submitted, the candidate stays dispatcher-owned: the dispatcher assigns
implementation, debugging, test execution, and mechanical recovery, and a
failure never authorizes the planner to take over. The planner returns
direction (an approach or an eligible route) through the dispatcher, which
assigns that work under the routing policy without silent substitution.
Completion needs proof bound to the current candidate, one open PR pointing
at it (the job's single PR identity; correction updates it, never a second),
and required acceptance evidence.

When Agent Observer is available, reuse its task identity in the prepared task
JSON as `observer_task_id`, including related correction and review jobs. Keep
each Router request ID distinct. Use Observer's installed Skill to record the
owning submission and any work outside Router; a shared planner session is not
exclusive to a job. Preserve both identities in the existing handoff. Missing
capture remains a measurement gap and does not block other authorized work.

A planner in any supported harness (Claude Code, Codex, OpenCode, Grok Build,
or a T3 thread) can submit a job: pass that harness's session id as
--planner-session with --planner-harness, with no planner working
directory. A planner working in T3 adds --planner-t3-thread with its T3
thread id: dispatcher and worker turns then run as that thread's child
threads on the route's provider, model and effort, dispatcher questions and
the terminal state land in the planner thread as messages, and the planner's
in-thread reply is the answer. A planner hosted in T3 must pass
--planner-t3-thread; without it the runner resumes the session headlessly
and the exchange never appears in the open thread. The dispatcher wakes the
saved planner automatically in its own harness whenever its judgment is needed;
`questions` and `answer` stay as an optional human override. Never invent a
session id or substitute another session's id. On Codex, also read [native ticket review](references/codex-host.md)
before using a Codex subagent. Do not emulate that host-specific operation on
another harness.

In the existing handoff, identify the policy version, requested and observed
route, and any override or escalation reason. Reuse proof, elapsed time, and
usage evidence. Leave unavailable measurements unknown. Stop an unavailable
route with its reason; never substitute silently.
