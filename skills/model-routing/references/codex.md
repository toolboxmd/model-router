# Codex routing policy

Generated from `runner/policy.py` (`durable-runner-policy-v2` 2.0.0); do not edit by hand.
Source: human decision, toolboxmd/model-router#10 (amended 2026-09-19) and #12. Evidence: https://github.com/toolboxmd/model-router/issues/10.

| Stage | Routes in order | Notes |
| --- | --- | --- |
| planning | `fable-5.1/max` (override: `sonnet/medium`) | host; Fable 5.1 in Claude Code; Sonnet medium only as the explicit live-test override |
| dispatch | `luna/max`, `luna-go/max` | runner; read-only Codex sandbox first; the same model on Go in OpenCode plan mode when Codex cannot start the task |
| implementation_default | `muse-spark-xhigh-free`, `muse-spark-xhigh-go`, `glm-5.3-go` | runner; policy declares exhaustion moves the same model to the next pool where declared (free Muse to Go Muse, Go Grok to xAI Grok), otherwise to the next family; overload moves to the next model family within one minute |
| implementation_small | `glm-5.3-flash-go`, `qwen3.8-flash-go`, `minimax-m3-go` | runner; small bounded edits; $60 and $30 models |
| implementation_hard | `muse-spark-xhigh-free`, `muse-spark-xhigh-go`, `kimi-k3-go`, `deepseek-v4-pro-go`, `grok-4.6-xai` | runner; $15 models get one turn per job |
| critical | planner itself | planner; a load-bearing step or prose the rest depends on is done by the planner itself in its own host session; the runner never dispatches it |
| correction | `kimi-k2.7-code-go` | runner; once per job; the same worker session is tried first |
| recovery | `grok-4.6-go`, `grok-4.6-xai` | runner; one escalation per job, same model across two pools, then the planner |
| review_ticket | `luna-max-review` | host; native Codex subagent |
| review_final | `opus-5/high-review` | host; only when the planner asks |

Route models, in policy order: `fable-5.1/max` = `claude-fable-5-1` max; `sonnet/medium` = `claude-sonnet-5` medium; `luna/max` = `gpt-5.6-luna` max; `luna-go/max` = `opencode-go/gpt-5.6-luna`; `luna-max-review` = `gpt-5.6-luna` max; `opus-5/high-review` = `claude-opus-5` high; `muse-spark-xhigh-free` = `opencode/muse-spark-1.3-contributor-free` xhigh; `muse-spark-xhigh-go` = `opencode-go/muse-spark-1.3-contributor` xhigh; `glm-5.3-go` = `opencode-go/glm-5.3`; `glm-5.3-flash-go` = `opencode-go/glm-5.3-flash`; `qwen3.8-flash-go` = `opencode-go/qwen3.8-flash`; `minimax-m3-go` = `opencode-go/minimax-m3`; `kimi-k3-go` = `opencode-go/kimi-k3`; `deepseek-v4-pro-go` = `opencode-go/deepseek-v4-pro`; `kimi-k2.7-code-go` = `opencode-go/kimi-k2.7-code`; `grok-4.6-go` = `opencode-go/grok-4.6` medium; `grok-4.6-xai` = `xai/grok-4.6` medium.

Rules:

- Implementation, correction, and recovery use the routes declared for the runner (`python -m runner submit`). Use a native Codex subagent only for `review_ticket`.
- Classify by consequences, not file type. A step or prose that the rest of the work depends on is `critical`: do it in the planner session yourself, then submit the remainder. Changes to agent instructions, security rules, and specifications are at least `implementation_hard`.
- Pools in order: zen-free, go, xai. Subscription logins only; no pay-per-token API keys and no paid balance overflow.
- The policy declares that exhaustion (free_tier_limit, FreeUsageLimitError, GoUsageLimitError, insufficient_quota) moves the same model to the next pool with no retries where the route declares one (free Muse to Go Muse, Go Grok to xAI Grok); without a next pool it moves to the next model family in its lane. It declares that overload (overloaded, rate_limit, account_rate_limit, overloaded_error, rate_limit_exceeded, RateLimitError, HTTP 503, 529) allows 2 retries inside 45 seconds with `next` capped at 20 seconds (a `next` larger than the cap aborts instead of waiting), then moves to the next model family within one minute. Hard errors end the turn as `implementation_failed` for the ladder; they never move routes.
- Go models with a $15 monthly limit get one turn per job. Windows: 5-hour 20 percent, weekly 50 percent, monthly 100 percent of the model's limit.
- One escalation per job; afterwards evidence returns to the planner. No duplicate attempts, no retry loops. Never substitute a route silently; if the selected route is unavailable, stop that dispatch with the reason.
- Record the policy version, the requested and observed route, and any override or escalation in the existing handoff. Instructions describe the policy and its required evidence.
