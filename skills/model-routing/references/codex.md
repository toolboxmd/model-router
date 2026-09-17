# Codex routing policy

| Work | Model | Reasoning effort |
| --- | --- | --- |
| Implementation or debugging | `gpt-5.6-sol` | `high` |
| Clear, bounded prose edits | `gpt-5.6-luna` | `max` |
| Independent review | `gpt-5.6-luna` | `max` |
| Implementation that cannot recover | `gpt-6-astra` | `medium` |

Classify by consequences, not file type. Changes to agent instructions,
security rules, specifications, or uncertain behavior use implementation.

Use the active host's native subagent tool. Pass the selected `model` and
`reasoning_effort` explicitly with fresh context (`fork_turns: "none"` where
supported). Check its schema first. If the selected route is unavailable,
stop only that dispatch; do not silently substitute another route.

Escalate from Sol to Astra only after a concrete implementation failure and
a bounded correction still leave progress stalled. Carry the failure, attempted
correction, and current proof forward. Stop the previous writer and verify it
is no longer running before transferring work. Allow at most one escalation
per task; replacing a worker does not reset that limit. If Astra cannot recover,
report the unresolved failure instead of starting another attempt.

Normal coding and test iterations are not escalation triggers. Missing access,
credentials, infrastructure, or user decisions are blockers, not model failures.

Follow the host's child lifecycle: interrupt a child after its final result,
completion, error, or idle state. Before transferring write ownership, verify
its current status; an interrupt response can describe the previous status.
