"""Versioned routing policy for the durable local runner (policy v2).

Policy is data. This module names the pools, the routes on them, the stages
(lanes) that order those routes, each Go model's monthly dollar limit and its
usage windows, the provider signal classes, and the policy's provenance.
Behavior lives in the controller and supervisor; project workflow,
authority, proof, and review stay with AgentsMD and the target project.

The Codex skill table is rendered from this module (``policy.main(["render-skill"])``)
and a test keeps the two equal.
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path

POLICY_ID = "durable-runner-policy-v2"
POLICY_VERSION = "2.4.0"
# Provenance: who decided this policy and where the evidence lives.
POLICY_SOURCE = ("human decision, toolboxmd/model-router#10 (amended 2026-09-19), "
                 "#12, #26 (Go plan, 2026-09-20), #28/#29 (role kits, 2026-09-20), "
                 "#34 (usage probes, 2026-09-20), and #38 (planner handoff, 2026-09-20)")
POLICY_EVIDENCE = "https://github.com/toolboxmd/model-router/issues/29"

# Subscription pools only. No Zen balance overflow, no pay-per-token APIs.
ALLOW_ZEN_OVERFLOW = False
ALLOW_DIRECT_PAID_API = False

# Go usage windows as a share of each model's monthly dollar limit.
WINDOWS = {"5h": 0.20, "weekly": 0.50, "monthly": 1.00}

# Stall detection (toolboxmd/model-router#33). Evidence from the 2026-09-20
# OpenCode session database: healthy worker sessions (330 to 463 parts each)
# showed a longest gap between consecutive parts of 110 to 161 seconds, all
# explained by tool calls such as the 100-second test suite; the stalled
# session showed gaps of 321 seconds and then nothing. A 180-second silence
# window catches today's stalls within three minutes with zero false
# positives on today's healthy sessions. The per-turn timeout stays as the
# outer budget for runaway but active turns.
STALL_SILENCE_SECS = 180
STALL_EVIDENCE = ("2026-09-20 OpenCode session database: healthy worker sessions "
                  "(330-463 parts) longest inter-part gap 110-161s; stalled session "
                  "gaps of 321s then nothing (toolboxmd/model-router#33)")
# The supervisor polls stream activity this often; a stall ends within the
# silence window plus one poll interval.
STALL_POLL_SECS = 1.0
# A fresh minimal request on the same route when a turn goes silent, so a
# stall that is exhaustion in disguise moves pools instead of retrying.
STALL_PROBE_TIMEOUT_SECS = 30
STALL_PROBE_PROMPT = "stall probe: reply with the single word ok"
# Assumed limit windows when a limit event carries no provider reset time.
# The named window wins; otherwise the 5-hour default applies. Weekly and
# monthly assumed marks are re-probed on a lengthening schedule (first probe
# after one hour, doubling, capped at six) and cleared on the first success.
WINDOW_SECS = {"5h": 5 * 3600, "weekly": 7 * 24 * 3600}
ASSUMED_WINDOW_DEFAULT = "5h"
PROBE_FIRST_DELAY_SECS = 3600
PROBE_BACKOFF_FACTOR = 2
PROBE_MAX_DELAY_SECS = 6 * 3600
# Where a capacity reset time came from: verbatim provider evidence, a
# derived window assumption, or the documented overload cooldown.
RESET_SOURCES = ("provider", "assumed", "cooldown")

# Usage-probe readings (toolboxmd/model-router#34). Before choosing a route
# the router knows each subscription window's usage and exact reset time
# wherever the provider exposes it. A Reading carries used, limit,
# reset_at, observed_at, and source in READING_SOURCES, keyed by pool,
# model, and window in the capacity ledger:
# provider_reported (Codex rateLimits/read, Claude /usage and OAuth usage,
# Grok monthly billing, session-file quota records), measured (OpenCode Go
# rolling cost sums against the tier windows, Zen free request counts),
# derived (a limit computed from policy data, such as a Go window share of
# the monthly tier), assumed (a window with no provider signal, such as
# Grok weekly, reconciled by error evidence). A failed or slow probe
# records used None (unknown) and never marks a route.
READING_SOURCES = ("provider_reported", "measured", "derived", "assumed")
# A window at or above this share of its limit marks the route degraded
# for that window; 100 percent skips it until its reset_at. Below the
# margin the reading is healthy.
USAGE_DEGRADED_FRACTION = 0.8
# Minimum seconds between proactive probes per harness. Codex
# account/rateLimits/read runs every 300 seconds or on demand before a
# dispatch; Claude /usage runs every 180 seconds at most, plus the free
# statusline feed while a session runs; the OpenCode Go cost sums are a
# cheap local-database read; the Grok monthly billing call is cached for
# an hour. Research evidence lives on toolboxmd/model-router#34.
CODEX_PROBE_INTERVAL_SECS = 300
CLAUDE_PROBE_INTERVAL_SECS = 180
OPENCODE_PROBE_INTERVAL_SECS = 60
GROK_PROBE_INTERVAL_SECS = 3600
PROBE_INTERVAL_SECS = {"codex": CODEX_PROBE_INTERVAL_SECS,
                       "claude": CLAUDE_PROBE_INTERVAL_SECS,
                       "opencode": OPENCODE_PROBE_INTERVAL_SECS,
                       "grok": GROK_PROBE_INTERVAL_SECS}
# Zen free names no allowance, so measured request counts are compared
# against this assumed per-window cap (flagged in the reading detail).
# Grok names no 5-hour or weekly allowance either: its weekly window is
# assumed with error-driven cool-off, while the monthly billing call
# reports provider data.
ZEN_FREE_ASSUMED_REQUESTS = 50

# Planner handoff (toolboxmd/model-router#38). After the planner submits a
# job, its session is compacted around that job, so a callback hours later
# resumes a small context instead of re-reading the whole planning
# conversation at cold-cache prices. The job also carries a durable handoff
# summary, so a callback can be answered from the ledger alone, including
# by the Astra fallback planner in a fresh session with no resume.
# Verified 2026-09-20: `claude -p --resume <sid> "/compact <focus>"`
# returns local_command compact and persists the summary in the transcript.
COMPACT_FOCUS_TEMPLATE = (
    "job {request_id} (Issue {issue}): keep the decisions and the "
    "proof command, drop the planning noise. "
    "Decisions: {decisions}. Proof: {proof}. Summary: {summary}"
)
# One flag per planner harness: the Claude planner session compacts
# headlessly after submit with --start; the Astra fallback on the Codex
# harness never compacts and instead answers from the stored handoff
# summary in a fresh session with no resume.
COMPACT_ON_SUBMIT = {"claude": True, "codex": False}

# Worker pools in preference order, then the two host pools.
POOLS = {
    "zen-free": {"order": 0, "provider": "opencode", "allowance": "free",
                 "credential": "subscription-issued key"},
    "go": {"order": 1, "provider": "opencode-go", "allowance": "go-included",
           "credential": "subscription-issued key", "windows": WINDOWS},
    "xai": {"order": 2, "provider": "xai", "allowance": "xai-subscription",
            "credential": "oauth"},
    "codex": {"order": 3, "provider": None, "allowance": "codex-subscription",
              "credential": "login"},
    "claude": {"order": 4, "provider": None, "allowance": "claude-subscription",
               "credential": "login"},
}
WORKER_POOL_ORDER = ["zen-free", "go", "xai"]

# Monthly dollar limits per Go model (opencode.ai/docs/go Go plan, 2026-09-20).
GO_MONTHLY_LIMIT_USD = {
    "muse-spark-1.3-contributor": 60, "glm-5.3-flash": 60, "glm-5.3": 15,
    "kimi-k3": 15, "kimi-k2.7-code": 60, "minimax-m3": 60, "hy3": 60,
    "minimax-m2.7": 60, "mimo-v2.5": 60, "longcat-2.0": 60, "glm-5.2": 60,
    "kimi-k2.6": 60, "glm-5.1": 60, "qwen3.8-flash": 30,
    "deepseek-v4-flash": 30, "hy4-preview": 30, "deepseek-v4.1-flash": 15,
    "deepseek-v4-pro": 15, "mimo-v2.5-pro": 15, "grok-4.6": 15,
    "gpt-5.6-luna": 15,
}
# DeepSeek V4.1 Flash drops to the $15 tier from this date (human decision,
# 2026-09-20). Before it the tier was unknown and the route added only now.
DEEPSEEK_V4_1_FLASH_TIER_FROM = "2026-09-20"
SCARCE_MONTHLY_LIMIT_USD = 15  # such models get one turn per job
# 15 and 30 USD tiers allow at most one running job at a time.
CONCURRENT_CAP_TIERS_USD = (15, 30)

# Older generations of pooled models are never routes.
EXCLUDED_MODELS = (
    "muse-spark-1.2-contributor",
)

# The 2026-09-20 Go plan: never implementers on Go. grok-4.6 is allowed only
# as the hard lane's last Go rung and as a recovery rung.
GO_IMPLEMENTER_EXCLUDED = ("gpt-5.6-luna", "kimi-k3", "qwen3.8-max", "qwen3.7-max")
GO_IMPLEMENTER_GROK_ROUTES = ("grok-4.6-go", "grok-4.6-xai")

# Route fields: harness (codex|claude|opencode|grok), pool, model
# (provider/model for opencode, bare model id for the native grok harness),
# variant (None = provider default; the grok harness sends it as
# --effort), agent (opencode agent; grok runs headless without one),
# role, family, next_pool (same model on the next pool), one_turn_per_job,
# max_concurrent (running jobs allowed on the route), override_only,
# planner_requested, planner_chosen (the planner runs it itself, never a
# dispatch route), manual (never selected automatically), context_window
# (input context in tokens, a routing hint for context-overflow moves).
# Context windows are rounded vendor-documented sizes as of 2026-09-20, kept
# here so a context_length_exceeded turn can move to a larger-context route
# in its lane; exact vendor limits vary and the observer tunes from evidence.
ROUTES = {
    "fable-5.1/max": {"harness": "claude", "pool": "claude", "model": "claude-fable-5-1",
                      "variant": "max", "role": "planning", "family": "claude",
                      "context_window": 200_000},
    "astra/max": {"harness": "codex", "pool": "codex", "model": "gpt-6-astra",
                  "variant": "max", "role": "planning", "family": "gpt",
                  "context_window": 400_000,
                  "note": "planning fallback on the Codex subscription"},
    "sonnet/medium": {"harness": "claude", "pool": "claude", "model": "claude-sonnet-5",
                      "variant": "medium", "role": "planning", "family": "claude",
                      "override_only": True, "context_window": 200_000,
                      "note": "explicit live-test override only, never a default"},
    "terra/max": {"harness": "codex", "pool": "codex", "model": "gpt-5.6-terra",
                  "variant": "max", "role": "dispatch", "family": "gpt",
                  "manual": True, "context_window": 400_000,
                  "note": "manual-only option; never selected automatically"},
    "luna/max": {"harness": "codex", "pool": "codex", "model": "gpt-5.6-luna",
                 "variant": "max", "role": "dispatch", "family": "gpt",
                 "sandbox": "read-only", "context_window": 400_000},
    "luna-go/max": {"harness": "opencode", "pool": "go", "model": "opencode-go/gpt-5.6-luna",
                    "variant": None, "agent": "plan", "role": "dispatch", "family": "gpt",
                    "one_turn_per_job": True, "max_concurrent": 1,
                    "context_window": 400_000,
                    "note": "dispatch fallback in OpenCode plan mode when Codex is unavailable"},
    "luna-max-review": {"harness": "codex", "pool": "codex", "model": "gpt-5.6-luna",
                        "variant": "max", "role": "review", "family": "gpt",
                        "context_window": 400_000},
    "luna-go-review": {"harness": "opencode", "pool": "go", "model": "opencode-go/gpt-5.6-luna",
                       "variant": None, "agent": "plan", "role": "review", "family": "gpt",
                       "one_turn_per_job": True, "max_concurrent": 1,
                       "context_window": 400_000,
                       "note": "ticket review fallback in OpenCode plan mode"},
    "astra/high-review": {"harness": "codex", "pool": "codex", "model": "gpt-6-astra",
                          "variant": "high", "role": "review", "family": "gpt",
                          "context_window": 400_000},
    "opus-5/high-review": {"harness": "claude", "pool": "claude", "model": "claude-opus-5",
                           "variant": "high", "role": "review", "family": "claude",
                           "planner_requested": True, "context_window": 200_000},
    "muse-spark-xhigh-free": {"harness": "opencode", "pool": "zen-free",
                              "model": "opencode/muse-spark-1.3-contributor-free",
                              "variant": "xhigh", "agent": "build", "role": "implementation",
                              "family": "muse", "next_pool": "muse-spark-xhigh-go",
                              "context_window": 200_000},
    "muse-spark-xhigh-go": {"harness": "opencode", "pool": "go",
                            "model": "opencode-go/muse-spark-1.3-contributor",
                            "variant": "xhigh", "agent": "build", "role": "implementation",
                            "family": "muse", "context_window": 200_000},
    "glm-5.3-go": {"harness": "opencode", "pool": "go", "model": "opencode-go/glm-5.3",
                   "variant": None, "agent": "build", "role": "implementation",
                   "family": "glm", "one_turn_per_job": True, "max_concurrent": 1,
                   "context_window": 128_000},
    "deepseek-v4-pro-go": {"harness": "opencode", "pool": "go", "model": "opencode-go/deepseek-v4-pro",
                           "variant": None, "agent": "build", "role": "implementation",
                           "family": "deepseek", "one_turn_per_job": True,
                           "max_concurrent": 1, "context_window": 128_000},
    "kimi-k2.7-code-go": {"harness": "opencode", "pool": "go", "model": "opencode-go/kimi-k2.7-code",
                          "variant": None, "agent": "build", "role": "correction",
                          "family": "kimi", "context_window": 256_000},
    "glm-5.3-flash-go": {"harness": "opencode", "pool": "go", "model": "opencode-go/glm-5.3-flash",
                         "variant": None, "agent": "build", "role": "implementation",
                         "family": "glm", "context_window": 256_000},
    "qwen3.8-flash-go": {"harness": "opencode", "pool": "go", "model": "opencode-go/qwen3.8-flash",
                         "variant": None, "agent": "build", "role": "implementation",
                       "family": "qwen", "max_concurrent": 1, "context_window": 256_000},
    "deepseek-v4.1-flash-go": {"harness": "opencode", "pool": "go",
                               "model": "opencode-go/deepseek-v4.1-flash",
                               "variant": None, "agent": "build", "role": "implementation",
                               "family": "deepseek",
                               "one_turn_per_job": True, "max_concurrent": 1,
                               "context_window": 128_000,
                               "note": f"${GO_MONTHLY_LIMIT_USD['deepseek-v4.1-flash']} USD tier from "
                                       f"{DEEPSEEK_V4_1_FLASH_TIER_FROM}"},
    "hy3-go": {"harness": "opencode", "pool": "go", "model": "opencode-go/hy3",
               "variant": None, "agent": "build", "role": "implementation",
               "family": "hy", "context_window": 200_000},
    "minimax-m3-go": {"harness": "opencode", "pool": "go", "model": "opencode-go/minimax-m3",
                      "variant": None, "agent": "build", "role": "implementation",
                      "family": "minimax", "context_window": 200_000},
    "mimo-v2.5-go": {"harness": "opencode", "pool": "go", "model": "opencode-go/mimo-v2.5",
                     "variant": None, "agent": "build", "role": "implementation",
                     "family": "mimo", "context_window": 200_000},
    "minimax-m2.7-go": {"harness": "opencode", "pool": "go", "model": "opencode-go/minimax-m2.7",
                        "variant": None, "agent": "build", "role": "implementation",
                        "family": "minimax", "context_window": 256_000},
    "longcat-2.0-go": {"harness": "opencode", "pool": "go", "model": "opencode-go/longcat-2.0",
                       "variant": None, "agent": "build", "role": "implementation",
                       "family": "longcat", "context_window": 256_000},
    "glm-5.2-go": {"harness": "opencode", "pool": "go", "model": "opencode-go/glm-5.2",
                   "variant": None, "agent": "build", "role": "implementation",
                   "family": "glm", "context_window": 200_000},
    "kimi-k2.6-go": {"harness": "opencode", "pool": "go", "model": "opencode-go/kimi-k2.6",
                     "variant": None, "agent": "build", "role": "implementation",
                     "family": "kimi", "context_window": 256_000},
    "glm-5.1-go": {"harness": "opencode", "pool": "go", "model": "opencode-go/glm-5.1",
                   "variant": None, "agent": "build", "role": "implementation",
                   "family": "glm", "context_window": 200_000},
    "astra/medium": {"harness": "codex", "pool": "codex", "model": "gpt-6-astra",
                     "variant": "medium", "role": "implementation", "family": "gpt",
                     "planner_chosen": True, "context_window": 400_000,
                     "note": "planner-chosen rung; runs in the planner's own session"},
    "opus-5/high": {"harness": "claude", "pool": "claude", "model": "claude-opus-5",
                    "variant": "high", "role": "implementation", "family": "claude",
                    "planner_chosen": True, "context_window": 200_000,
                    "note": "planner-chosen rung; runs in the planner's Claude session"},
    "grok-4.6-go": {"harness": "opencode", "pool": "go", "model": "opencode-go/grok-4.6",
                    "variant": "medium", "agent": "build", "role": "recovery",
                    "family": "grok", "next_pool": "grok-4.6-build", "one_turn_per_job": True,
                    "max_concurrent": 1, "context_window": 2_000_000},
    "grok-4.6-build": {"harness": "grok", "pool": "xai", "model": "grok-4.6",
                       "variant": "medium", "role": "recovery",
                       "family": "grok", "next_pool": "grok-4.6-xai",
                       "context_window": 2_000_000,
                       "note": "native Grok Build CLI on the xAI subscription; "
                               "OpenCode's xAI provider is the next_pool fallback"},
    "grok-4.6-xai": {"harness": "opencode", "pool": "xai", "model": "xai/grok-4.6",
                     "variant": "medium", "agent": "build", "role": "recovery",
                     "family": "grok", "context_window": 2_000_000,
                     "note": "OpenCode xAI provider fallback for the native Grok Build route"},
}

# Stages in flow order. executor: host (the human-facing session or its
# native subagent), runner (dispatched by the runner), planner (done by the
# planner itself, never dispatched).
STAGES = {
    "planning": {"executor": "host",
                 "routes": ["fable-5.1/max", "astra/max"],
                 "overrides": ["sonnet/medium"],
                 "capabilities": ["session_resume"],
                 "note": "Fable 5.1 in Claude Code, then Astra max on Codex; "
                         "Sonnet medium only as the explicit live-test override"},
    "dispatch": {"executor": "runner", "routes": ["luna/max", "luna-go/max"],
                 "manual": ["terra/max"],
                 "capabilities": ["read_only", "session_resume", "structured_output"],
                 "note": "read-only Codex sandbox first; the same model on Go in OpenCode plan mode "
                         "when Codex cannot start the task; Terra max is a manual-only option"},
    "implementation_default": {"executor": "runner",
                               "routes": ["muse-spark-xhigh-free", "muse-spark-xhigh-go",
                                          "glm-5.3-flash-go", "qwen3.8-flash-go",
                                          "deepseek-v4.1-flash-go", "hy3-go", "minimax-m3-go",
                                          "mimo-v2.5-go", "minimax-m2.7-go", "longcat-2.0-go",
                                          "glm-5.2-go", "kimi-k2.6-go", "glm-5.1-go"],
                               "capabilities": ["workspace_write", "session_resume"],
                               "note": "the full Go implementer chain in intelligence order, every "
                                        "new job starting on Muse free (no concurrency cap, "
                                        "parallel sessions by design); "
                                        "Qwen 3.7 Plus and 3.6 Plus are absent: no known tier. "
                                        "Policy declares exhaustion moves the same model to the next pool "
                                        "where declared (free Muse to Go Muse, Go Grok to xAI Grok), "
                                        "otherwise to the next family; overload moves to the next model "
                                        "family within one minute"},
    "implementation_small": {"executor": "runner",
                              "routes": ["muse-spark-xhigh-free", "muse-spark-xhigh-go",
                                         "glm-5.3-flash-go", "qwen3.8-flash-go",
                                         "deepseek-v4.1-flash-go", "hy3-go", "minimax-m3-go",
                                         "mimo-v2.5-go", "minimax-m2.7-go", "longcat-2.0-go",
                                         "glm-5.2-go", "kimi-k2.6-go", "glm-5.1-go"],
                              "capabilities": ["workspace_write", "session_resume"],
                              "note": "small bounded edits; every new job starts on Muse free "
                                      "(no concurrency cap, parallel sessions by design), then "
                                      "Muse Go, then from GLM 5.3 Flash onward"},
    "implementation_hard": {"executor": "runner",
                            "routes": ["muse-spark-xhigh-free", "muse-spark-xhigh-go",
                                       "glm-5.3-go", "deepseek-v4-pro-go", "grok-4.6-go",
                                       "grok-4.6-build", "grok-4.6-xai"],
                             "capabilities": ["workspace_write", "session_resume"],
                             "note": "$15 models get one turn per job; every new job starts on "
                                     "Muse free (no cap); Grok 4.6 sits here as the "
                                     "hard lane's last Go rung, then native Grok Build on "
                                     "the xAI subscription, then OpenCode's xAI provider"},
    "planner_rungs": {"executor": "planner",
                      "routes": ["astra/medium", "opus-5/high"],
                      "capabilities": [],
                      "planner_selects": True,
                      "note": "the planner itself chooses a rung and runs it in its own session; "
                              "the runner never dispatches these"},
    "critical": {"executor": "planner", "routes": [], "capabilities": [],
                 "note": "a load-bearing step or prose the rest depends on is done by the planner "
                         "itself in its own host session; the runner never dispatches it"},
    "correction": {"executor": "runner", "routes": ["kimi-k2.7-code-go"],
                   "capabilities": ["workspace_write", "session_resume"],
                   "note": "once per job; the same worker session is tried first"},
    "recovery": {"executor": "runner", "routes": ["grok-4.6-go", "grok-4.6-build", "grok-4.6-xai"],
                 "capabilities": ["workspace_write"],
                 "note": "one escalation per job, Grok 4.6 on Go, then native Grok Build on "
                         "the xAI subscription, then OpenCode's xAI provider; pool moves "
                         "are not second escalations, then the planner; "
                         "skips rungs the job already used"},
    "review_ticket": {"executor": "host", "routes": ["luna-max-review", "luna-go-review"],
                      "capabilities": ["read_only"],
                      "note": "native Codex subagent first, then Luna on OpenCode Go in plan mode"},
    "review_final": {"executor": "host",
                     "routes": ["opus-5/high-review", "astra/high-review", "luna-max-review"],
                     "capabilities": ["read_only"],
                     "note": "the planner chooses; Opus 5 high on Claude, then Astra high on "
                             "Codex, then Luna max"},
}
IMPLEMENTATION_LANES = ["implementation_default", "implementation_small", "implementation_hard"]
LANE_ALIASES = {"default": "implementation_default", "small": "implementation_small",
                "hard": "implementation_hard", "critical": "critical"}
DEFAULT_LANE = "implementation_default"

# Provider signal classes. Definitions only; the controller applies them.
# ``stalled`` is detected from stream silence, never from provider text, so
# it carries no matchers; the controller still treats it like overload
# (bounded same-route retry inside the overload window, then a lateral move
# with the route degraded). ``context`` is a capacity signal, not a hard
# failure: the turn moves to a larger-context route in its lane.
SIGNAL_CLASSES = {
    "exhausted": {"action": "next_pool", "retries": 0,
                  "retry_reasons": ["free_tier_limit"],
                  "error_names": ["FreeUsageLimitError", "GoUsageLimitError", "insufficient_quota",
                                  "UsageLimitExceeded"]},
    "overloaded": {"action": "next_family", "retries": 2, "window_secs": 45,
                   "next_cap_secs": 20, "degraded_secs": 900,
                   "retry_reasons": ["overloaded", "rate_limit", "account_rate_limit"],
                   "error_names": ["overloaded_error", "rate_limit_exceeded", "RateLimitError"],
                   "status_codes": [503, 529]},
    "stalled": {"action": "next_family", "retries": 2, "window_secs": 45,
                "degraded_secs": 900,
                "retry_reasons": [], "error_names": []},
    "context": {"action": "next_larger_context", "retries": 0,
                "retry_reasons": [],
                "error_names": ["context_length_exceeded"]},
    "hard": {"action": "implementation_failed", "retries": 0,
             "retry_reasons": ["auth", "region", "consent"],
             "error_names": ["AuthError", "RegionError", "DataPolicyError",
                             "Unauthorized", "AuthenticationError",
                             "MissingAuthentication"],
             "status_codes": [401, 403]},
}

# Vendor-shaped free-exhaustion evidence (OpenCode 1.18.31). Free exhaustion
# is proven only by the exact vendor class or by a session retry action with
# reason free_tier_limit and provider opencode. Nothing else moves a task
# off the free route.
VENDOR_FREE_ERROR_CLASS = "FreeUsageLimitError"
VENDOR_FREE_RETRY_REASON = "free_tier_limit"
VENDOR_FREE_PROVIDER = "opencode"


# ---------------------------------------------------------------------------
# Lookups
# ---------------------------------------------------------------------------

def route_spec(route: str) -> dict:
    if route not in ROUTES:
        raise ValueError(f"unsupported route: {route!r}")
    return ROUTES[route]


def validate_route(route: str) -> dict:
    """The derived compatibility view of a route (model, effort, role,
    allowance, harness, pool, operational)."""
    route_spec(route)
    return SUPPORTED_ROUTES[route]


def is_supported(route: str) -> bool:
    return route in ROUTES


def is_operational(route: str) -> bool:
    """Every policy route has an adapter; unknown routes are not operational."""
    return route in ROUTES


def route_blocker(route: str) -> str | None:
    return None if route in ROUTES else f"unsupported route: {route!r}"


def route_allowance(route: str) -> str:
    return POOLS[route_spec(route)["pool"]]["allowance"]


def route_pool(route: str) -> str:
    return route_spec(route)["pool"]


def stage_routes(stage: str) -> list[str]:
    if stage not in STAGES:
        raise ValueError(f"unsupported stage: {stage!r}")
    return list(STAGES[stage]["routes"])


def implementation_routes() -> list[str]:
    seen: list[str] = []
    for lane in IMPLEMENTATION_LANES:
        for r in STAGES[lane]["routes"]:
            if r not in seen:
                seen.append(r)
    return seen


def resolve_lane(lane: str) -> str:
    stage = LANE_ALIASES.get(lane, lane)
    if stage not in STAGES:
        raise ValueError(f"unsupported lane: {lane!r}")
    return stage


def lane_default_route(lane: str) -> str:
    """First route of an implementation lane. The critical lane has none:
    the planner does that work itself."""
    stage = resolve_lane(lane)
    if STAGES[stage]["executor"] == "planner":
        raise ValueError(
            f"lane {lane!r} is planner-executed: do the load-bearing step in the planner "
            "session, then submit the remaining work")
    if stage not in IMPLEMENTATION_LANES:
        raise ValueError(f"lane {lane!r} is not an implementation lane")
    return STAGES[stage]["routes"][0]


def lane_of_route(route: str, lane: str | None = None) -> str | None:
    """The lane to reason in. A route shared by lanes (Muse sits in the
    default and hard lanes) is ambiguous, so callers pass the job's stored
    lane; without one, the first lane listing the route is used. Recovery
    moves are valid from any implementation lane: a job already on a recovery
    route reasons in the recovery stage, never the original lane, and never
    raises."""
    first = next((c for c in IMPLEMENTATION_LANES if route in STAGES[c]["routes"]), None)
    if lane:
        stage = resolve_lane(lane)
        if stage == "recovery" and route in STAGES["recovery"]["routes"]:
            return "recovery"
        # A job already on a recovery route stays in recovery, from any lane.
        if route in STAGES["recovery"]["routes"]:
            return "recovery"
        if stage in IMPLEMENTATION_LANES and route in STAGES[stage]["routes"]:
            return stage
        if first is not None:
            # An implementation route outside the stated lane is a mismatch,
            # never a silent move to another lane.
            raise ValueError(f"route {route!r} is not in lane {lane!r}")
    return first


def one_turn_routes_used(route: str, turns_by_route: dict | None = None) -> bool:
    """True when ``route`` is a one-turn-per-job route that already ran once."""
    if not route_spec(route).get("one_turn_per_job"):
        return False
    return int((turns_by_route or {}).get(route, 0)) >= 1


def validate_implementation_route(route: str) -> dict:
    """A job route must belong to an implementation lane."""
    info = route_spec(route)
    if route not in implementation_routes():
        raise ValueError(f"route {route!r} is a {info.get('role')} route, not an implementation "
                         f"route; supported: {', '.join(implementation_routes())}")
    return info


def opencode_route_params(route: str | None) -> tuple[str, str | None, str]:
    """(model, variant, agent) the OpenCode adapter sends for a route.
    Unknown routes and routes on other harnesses raise: the runner never
    substitutes a model silently."""
    spec = route_spec(route or "")
    if spec.get("harness") != "opencode":
        raise ValueError(f"route {route!r} runs on {spec.get('harness')}, not on the OpenCode adapter")
    return spec["model"], spec.get("variant"), spec.get("agent") or "build"


def grok_route_params(route: str | None) -> tuple[str, str | None]:
    """(model, effort) the native Grok Build CLI sends for a route.
    Unknown routes and routes on other harnesses raise: the runner never
    substitutes a model silently."""
    spec = route_spec(route or "")
    if spec.get("harness") != "grok":
        raise ValueError(f"route {route!r} runs on {spec.get('harness')}, not on the Grok Build CLI")
    return spec["model"], spec.get("variant")


def worker_model_variant(route: str | None) -> tuple[str, str | None]:
    """(model, variant) a worker route carries, on either worker harness.
    Dispatch and review routes raise: only implementation, correction, and
    recovery routes run worker turns."""
    spec = route_spec(route or "")
    if spec.get("harness") not in ("opencode", "grok"):
        raise ValueError(f"route {route!r} runs on {spec.get('harness')}, not on a worker harness")
    return spec["model"], spec.get("variant")


def monthly_limit_usd(route: str) -> int | None:
    spec = route_spec(route)
    if spec["pool"] != "go":
        return None
    return GO_MONTHLY_LIMIT_USD.get(spec["model"].split("/", 1)[1])


def one_turn_per_job(route: str) -> bool:
    """The explicit route field; ``validate_policy`` keeps it equal to the
    $15 limit rule so the data cannot drift from the table."""
    return bool(route_spec(route).get("one_turn_per_job"))


def route_max_concurrent(route: str) -> int | None:
    """At most this many running jobs may sit on the route (exactly int 1
    on the $15 and $30 Go tiers); None means the route has no concurrency
    cap. Non-int caps (including bool True) return None so a type error
    cannot masquerade as a unit cap."""
    cap = route_spec(route).get("max_concurrent")
    if cap is None:
        return None
    if type(cap) is int:
        return cap
    return None


# Worker-session permission defaults, resolved per route by role.
# Implementation, correction, and recovery routes run with full access;
# ``question`` and ``task`` stay denied (headless stall; policy bypass).
# Dispatch and review routes on OpenCode run read-only in plan mode: edit,
# outside-workspace writes, web fetch/search, and doom-loop prompts are
# denied because the coordinator/verifier role never needs them; ``question``
# and ``task`` stay denied as well. The plan agent is kept.
DEFAULT_SESSION_PERMISSIONS = (
    {"permission": "external_directory", "pattern": "*", "action": "allow"},
    {"permission": "webfetch", "pattern": "*", "action": "allow"},
    {"permission": "websearch", "pattern": "*", "action": "allow"},
    {"permission": "doom_loop", "pattern": "*", "action": "allow"},
    {"permission": "question", "pattern": "*", "action": "deny"},
    {"permission": "task", "pattern": "*", "action": "deny"},
)

READ_ONLY_SESSION_PERMISSIONS = (
    {"permission": "edit", "pattern": "*", "action": "deny"},
    {"permission": "external_directory", "pattern": "*", "action": "deny"},
    {"permission": "webfetch", "pattern": "*", "action": "deny"},
    {"permission": "websearch", "pattern": "*", "action": "deny"},
    {"permission": "doom_loop", "pattern": "*", "action": "deny"},
    {"permission": "question", "pattern": "*", "action": "deny"},
    {"permission": "task", "pattern": "*", "action": "deny"},
)


def session_permissions(route: str | None = None) -> tuple[dict, ...]:
    """The resolved permission rules for a route from its role kit.

    The kit's ``permission_set`` is the policy edit point: ``full`` resolves
    to the full-access set (implementation, correction, recovery), anything
    else to the read-only set (planning, dispatch, review). Unknown or
    missing routes get the read-only set (least privilege). A route's own
    ``permissions`` overrides apply on top of its kit base, in base order."""
    base = READ_ONLY_SESSION_PERMISSIONS
    if route in ROUTES:
        try:
            kit = kit_for_route(route)
        except ValueError:
            kit = None
        if isinstance(kit, dict) and kit.get("permission_set") == "full":
            base = DEFAULT_SESSION_PERMISSIONS
    spec: dict = route_spec(route) if route in ROUTES else {}
    overrides = {o["permission"]: o["action"]
                 for o in spec.get("permissions", ()) if isinstance(o, dict)}
    out = tuple(dict(r, action=overrides.get(r["permission"], r["action"]))
                for r in base)
    # A per-route override may add a permission absent from the base.
    base_names = {r["permission"] for r in base}
    extra = tuple({"permission": k, "pattern": "*", "action": v}
                  for k, v in overrides.items() if k not in base_names)
    return out + extra


# ---------------------------------------------------------------------------
# Role kits: one row per role. A role's tools change by editing this table;
# no state-machine change is needed. The harness seam materializes the kit
# at spawn time: the owned OpenCode server runs on a runner-generated
# configuration directory built from the kit (AgentsMD link, plugins,
# skills, MCP servers named by the kit, permission set from the route);
# Codex uses CODEX_HOME, Claude uses CLAUDE_CONFIG_DIR, Grok uses its
# config directory; the planner keeps the user's own session.
# First cut (tuning follows measurement): the worker and correction kits
# hold the paid ``treg`` MCP gateway; every other kit names no MCP server.
# ---------------------------------------------------------------------------

KIT_ROLES = ("planner", "dispatcher", "reviewer", "worker", "correction", "recovery")

# Policy role (on routes) -> kit role.
KIT_FOR_POLICY_ROLE = {
    "planning": "planner",
    "dispatch": "dispatcher",
    "review": "reviewer",
    "implementation": "worker",
    "correction": "correction",
    "recovery": "recovery",
}

KITS = {
    "planner": {
        "instructions": ("You are the planner. Decide scope and acceptance criteria. "
                         "You keep your own session and tools; the runner never isolates it."),
        "skills": [],
        "plugins": [],
        "mcp": [],
        "permission_set": "read-only",
        "agentsmd": True,
    },
    "dispatcher": {
        "instructions": ("You are the read-only dispatcher. Coordinate and verify; never edit. "
                         "Ask the planner for decisions and request implementation with "
                         "complete worker instructions."),
        "skills": ["operations"],
        "plugins": ["agentsmd-project-direction"],
        "mcp": [],
        "permission_set": "read-only",
        "agentsmd": True,
    },
    "reviewer": {
        "instructions": ("You are the reviewer. Verify the work against acceptance; "
                         "read-only, never edit."),
        "skills": ["operations"],
        "plugins": ["agentsmd-project-direction"],
        "mcp": [],
        "permission_set": "read-only",
        "agentsmd": True,
    },
    "worker": {
        "instructions": ("You are the implementation worker. Edit only what the task allows "
                         "inside the workspace. Run the task's proof command when it has one. "
                         "Finish with changed files and proof results."),
        "skills": ["operations", "project-direction"],
        "plugins": ["agentsmd-project-direction"],
        "mcp": ["treg"],
        "permission_set": "full",
        "agentsmd": True,
    },
    "correction": {
        "instructions": ("You are the correction worker. Fix the failed turn with the smallest "
                         "change that satisfies the task and its proof."),
        "skills": ["operations", "project-direction"],
        "plugins": ["agentsmd-project-direction"],
        "mcp": ["treg"],
        "permission_set": "full",
        "agentsmd": True,
    },
    "recovery": {
        "instructions": ("You are the recovery worker. Make one bounded attempt to rescue the "
                         "job, then report evidence for the planner."),
        "skills": ["operations"],
        "plugins": ["agentsmd-project-direction"],
        "mcp": [],
        "permission_set": "full",
        "agentsmd": True,
    },
}


def kit_for_role(role: str) -> dict:
    """The kit row for a kit role. Unknown roles raise: never substitute."""
    if role not in KITS:
        raise ValueError(f"unsupported kit role: {role!r}")
    return KITS[role]


def kit_name_for_route(route: str) -> str:
    """The kit role for a route, via the route's policy role."""
    spec = route_spec(route)
    kit = KIT_FOR_POLICY_ROLE.get(spec.get("role") or "")
    if kit is None:
        raise ValueError(f"route {route!r} has no kit role")
    return kit


def kit_for_route(route: str) -> dict:
    """The kit row for a route. Unknown routes raise: never substitute."""
    return kit_for_role(kit_name_for_route(route))


def kit_hash(kit: dict) -> str:
    """Short stable identity of a kit's content (for the ledger and probes)."""
    blob = json.dumps(kit, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


# -- installed-kit checks (local directories; fakes in tests) -------------

def _opencode_home() -> Path:
    override = os.environ.get("MODEL_ROUTER_OPENCODE_HOME")
    if override:
        return Path(override).expanduser()
    xdg = os.environ.get("XDG_CONFIG_HOME")
    if xdg:
        cand = Path(xdg).expanduser() / "opencode"
        try:
            if cand.exists():
                return cand
        except OSError:
            pass
        # Inside an owned-server worker XDG points at the per-kit mirror,
        # which carries every user config entry except opencode: fall back
        # to the user's real opencode home so this project's own proof
        # resolves its installed skills without extra variables.
        return Path.home() / ".config" / "opencode"
    return Path.home() / ".config" / "opencode"


def _codex_home() -> Path:
    override = os.environ.get("MODEL_ROUTER_CODEX_HOME") or os.environ.get("CODEX_HOME")
    if override:
        return Path(override).expanduser()
    return Path.home() / ".codex"


def _claude_home() -> Path:
    override = os.environ.get("MODEL_ROUTER_CLAUDE_HOME") or os.environ.get("CLAUDE_CONFIG_DIR")
    if override:
        return Path(override).expanduser()
    return Path.home() / ".claude"


def _grok_home() -> Path:
    override = os.environ.get("MODEL_ROUTER_GROK_HOME") or os.environ.get("GROK_HOME")
    if override:
        return Path(override).expanduser()
    return Path.home() / ".grok"


def _agents_home() -> Path:
    override = os.environ.get("MODEL_ROUTER_AGENTS_HOME")
    if override:
        return Path(override).expanduser()
    return Path.home() / ".agents"


def _skill_search_roots() -> list[Path]:
    return [_opencode_home() / "skills", _codex_home() / "skills",
            _claude_home() / "skills", _grok_home() / "skills",
            _agents_home() / "skills"]


def is_skill_installed(name: str) -> bool:
    """True when a skill directory or file with this name exists locally."""
    if not isinstance(name, str) or not name or "/" in name or name in (".", ".."):
        return False
    for root in _skill_search_roots():
        try:
            if (root / name).is_dir() or (root / name).is_file():
                return True
            if (root / f"{name}.md").is_file():
                return True
        except OSError:
            continue
    return False


def is_plugin_installed(name: str) -> bool:
    """True when a plugin module with this name exists locally."""
    if not isinstance(name, str) or not name or "/" in name or name in (".", ".."):
        return False
    candidates = [_opencode_home() / "plugins", _codex_home() / "plugins",
                  _claude_home() / "plugins", _grok_home() / "plugins"]
    for root in candidates:
        try:
            if (root / name).exists() or (root / f"{name}.js").is_file():
                return True
        except OSError:
            continue
    return False


def _opencode_mcp_names() -> set[str]:
    names: set[str] = set()
    for candidate in (_opencode_home() / "opencode.json",
                      _opencode_home() / "opencode.jsonc"):
        try:
            raw = candidate.read_text(encoding="utf-8")
        except OSError:
            continue
        try:
            obj = json.loads(raw)
        except ValueError:
            continue
        mcp = obj.get("mcp") if isinstance(obj, dict) else None
        if isinstance(mcp, dict):
            names.update(str(k) for k in mcp.keys())
    return names


def _codex_mcp_names() -> set[str]:
    """MCP servers configured in the Codex ``config.toml`` only.

    Skills are not MCP servers: a skill directory with the same name
    never counts, so ``validate`` still rejects a kit that names an
    MCP server that is not configured.
    """
    names: set[str] = set()
    try:
        raw = (_codex_home() / "config.toml").read_text(encoding="utf-8")
    except OSError:
        return names
    for line in raw.splitlines():
        line = line.strip()
        if line.startswith("[mcp_servers."):
            rest = line[len("[mcp_servers."):]
            name = rest.split("]")[0].strip().strip('"').strip("'")
            if name:
                names.add(name)
    return names


def is_mcp_installed(name: str) -> bool:
    """True when an MCP server with this name is configured locally."""
    if not isinstance(name, str) or not name:
        return False
    return name in _opencode_mcp_names() or name in _codex_mcp_names()


def kit_problems(role: str | None = None) -> list[str]:
    """Problems in kit rows (empty when consistent and installed)."""
    problems: list[str] = []
    roles = [role] if role else list(KITS)
    for name in roles:
        kit = KITS.get(name)
        if not isinstance(kit, dict):
            problems.append(f"kit {name}: missing row")
            continue
        if not isinstance(kit.get("instructions"), str) or not kit["instructions"].strip():
            problems.append(f"kit {name}: missing instructions")
        if kit.get("permission_set") not in ("full", "read-only"):
            problems.append(f"kit {name}: permission_set must be full or read-only")
        for field, check in (("skills", is_skill_installed),
                             ("plugins", is_plugin_installed),
                             ("mcp", is_mcp_installed)):
            items = kit.get(field)
            if not isinstance(items, list) or any(not isinstance(i, str) for i in items):
                problems.append(f"kit {name}: {field} must be a list of names")
                continue
            for item in items:
                if not check(item):
                    problems.append(f"kit {name}: {field} {item!r} is not installed")
    return problems


def next_pool_route(route: str) -> str | None:
    """Same model on the next pool (or the same pool through another
    harness for a compat fallback), or None."""
    return route_spec(route).get("next_pool")


def next_family_route(route: str, exhausted=None, degraded=None, lane: str | None = None,
                      turns_by_route: dict | None = None) -> str | None:
    """Next route in the job's lane from a different model family that is
    neither exhausted, degraded, nor a one-turn route already used. Recovery
    is same-model across pools, so the recovery stage ignores the family
    filter and skips any rung the job already used."""
    stage = lane_of_route(route, lane)
    if stage is None:
        return None
    skip = set(exhausted or ()) | set(degraded or ())
    order = STAGES[stage]["routes"]
    if stage == "recovery":
        used = turns_by_route or {}
        for cand in order[order.index(route) + 1:]:
            if cand in skip:
                continue
            if int(used.get(cand, 0)) >= 1:
                continue
            if one_turn_routes_used(cand, turns_by_route):
                continue
            return cand
        return None
    family = route_spec(route)["family"]
    for cand in order[order.index(route) + 1:]:
        if cand in skip or ROUTES[cand]["family"] == family:
            continue
        if one_turn_routes_used(cand, turns_by_route):
            continue
        return cand
    return None


def route_context_window(route: str) -> int:
    """Input context in tokens for a route (a routing hint, not a vendor
    guarantee). Unknown routes raise: the runner never substitutes."""
    size = route_spec(route).get("context_window")
    if type(size) is not int or size <= 0:
        raise ValueError(f"route {route!r} has no positive context_window")
    return size


def next_larger_context_route(route: str, exhausted=None, degraded=None, lane: str | None = None,
                              turns_by_route: dict | None = None) -> str | None:
    """Next route in the job's lane with a strictly larger context window
    that is neither exhausted, degraded, nor a one-turn route already used.
    None when no larger-context route is left: the turn then ends as
    implementation_failed."""
    stage = lane_of_route(route, lane)
    if stage is None:
        return None
    skip = set(exhausted or ()) | set(degraded or ())
    order = STAGES[stage]["routes"]
    try:
        size = route_context_window(route)
    except ValueError:
        return None
    for cand in order[order.index(route) + 1:]:
        if cand in skip:
            continue
        if one_turn_routes_used(cand, turns_by_route):
            continue
        try:
            cand_size = route_context_window(cand)
        except ValueError:
            continue
        if cand_size > size:
            return cand
    return None


def next_capacity_route(current: str | None, exhausted: set[str] | None = None,
                        lane: str | None = None) -> tuple[str | None, str | None]:
    """Next eligible route in the job's lane, skipping exhausted ones.
    Returns (route, blocker); blocker is always None because every policy
    route has an adapter, and (None, None) ends the lane."""
    exhausted = exhausted or set()
    stage = lane_of_route(current, lane) if current else resolve_lane(lane or DEFAULT_LANE)
    if stage is None:
        return None, None
    order = STAGES[stage]["routes"]
    start = order.index(current) + 1 if current in order else 0
    for route in order[start:]:
        if route in exhausted:
            continue
        return route, None
    return None, None


def next_recovery_route(current: str | None, turns_by_route: dict | None = None) -> str | None:
    """Recovery order: one escalation to Grok 4.6, same model across the Go
    and xAI pools, then the planner (None). Rungs the job already ran
    (present in ``turns_by_route``) are skipped."""
    order = STAGES["recovery"]["routes"]
    used = turns_by_route or {}
    if current is None:
        for i, cand in enumerate(order):
            if int(used.get(cand, 0)) >= 1:
                continue
            return cand
        return None
    if current in order:
        idx = order.index(current)
        for cand in order[idx + 1:]:
            if int(used.get(cand, 0)) >= 1:
                continue
            return cand
        return None
    if current in implementation_routes() or current in STAGES["correction"]["routes"] \
            or current in ("luna/max", "sonnet/medium"):
        first = next((cand for cand in order if int(used.get(cand, 0)) == 0), None)
        return first
    return None


# ---------------------------------------------------------------------------
# Signal classification
# ---------------------------------------------------------------------------

def is_free_tier_retry_status(status) -> bool:
    """OpenCode session status proving free-allowance exhaustion."""
    if not isinstance(status, dict) or status.get("type") != "retry":
        return False
    action = status.get("action")
    return (isinstance(action, dict)
            and action.get("reason") == VENDOR_FREE_RETRY_REASON
            and action.get("provider") == VENDOR_FREE_PROVIDER)


def is_free_usage_api_error(error) -> bool:
    """Provider ``APIError`` whose response body names the free limit."""
    if not isinstance(error, dict) or error.get("name") != "APIError":
        return False
    data = error.get("data")
    body = data.get("responseBody") if isinstance(data, dict) else None
    return isinstance(body, str) and VENDOR_FREE_ERROR_CLASS in body


def classify_quota_exhaustion(evidence) -> bool:
    """True only for exact provider-shaped free-exhaustion evidence."""
    if not isinstance(evidence, dict):
        return False
    if is_free_tier_retry_status(evidence) or is_free_usage_api_error(evidence):
        return True
    return evidence.get("type") == "error" and is_free_usage_api_error(evidence.get("error"))


classify_provider_free_exhaustion = classify_quota_exhaustion


def _signal_facts(evidence) -> tuple[str | None, list[str], int | None]:
    """(retry reason, error class names, status code) from provider evidence.
    Model-authored text is never inspected: only structured fields count."""
    if not isinstance(evidence, dict):
        return None, [], None
    reason = None
    action = evidence.get("action")
    if evidence.get("type") == "retry" and isinstance(action, dict):
        reason = action.get("reason")
    names: list[str] = []
    code = None
    err = evidence.get("error") if evidence.get("type") == "error" else evidence
    if isinstance(err, dict):
        for key in ("name", "type", "code", "class"):
            val = err.get(key)
            if isinstance(val, str):
                names.append(val)
        data = err.get("data") if isinstance(err.get("data"), dict) else {}
        if isinstance(data.get("statusCode"), int):
            code = data["statusCode"]
        body = data.get("responseBody")
        if isinstance(body, str):
            for cls in SIGNAL_CLASSES.values():
                for name in cls["error_names"]:
                    if name in body:
                        names.append(name)
    return reason, names, code


def classify_signal(evidence) -> str | None:
    """``exhausted``, ``overloaded``, ``context``, ``hard``, or None for
    provider evidence. ``stalled`` never matches provider text: it is
    detected from stream silence, not classified."""
    reason, names, code = _signal_facts(evidence)
    for cls in ("exhausted", "overloaded", "context", "hard"):
        spec = SIGNAL_CLASSES[cls]
        if reason in spec.get("retry_reasons", ()):
            return cls
        if any(n in spec["error_names"] for n in names):
            return cls
        if code is not None and code in spec.get("status_codes", ()):
            return cls
    return None


def _coerce_reset_moment(value, now_ts: float) -> str | None:
    """An ISO reset timestamp from a provider reset value: an ISO string
    verbatim, or epoch seconds (int/float or digit string) as UTC."""
    import datetime
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            moment = datetime.datetime.fromtimestamp(float(value), datetime.timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
        if moment.timestamp() < now_ts - 86400:
            return None
        return moment.isoformat()
    if isinstance(value, str) and value.strip():
        text = value.strip()
        if text.isdigit() and len(text) >= 9:
            return _coerce_reset_moment(float(text), now_ts)
        try:
            moment = datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=datetime.timezone.utc)
        return moment.isoformat()
    return None


def _reset_in_text(text, now_ts: float) -> str | None:
    """A reset timestamp carried in free text (Claude names the reset time
    in its message). Only an ISO-8601 moment counts; anything else is None
    so model-authored prose can never invent a reset."""
    import re
    if not isinstance(text, str) or not text:
        return None
    for match in re.finditer(r"20\d\d-\d\d-\d\d[T ]\d\d:\d\d(?::\d\d)?"
                             r"(?:Z|[+-]\d\d:?\d\d)?", text):
        moment = _coerce_reset_moment(match.group(0), now_ts)
        if moment is not None:
            return moment
    return None


_HUMAN_MONTHS = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5,
                 "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10,
                 "nov": 11, "dec": 12}

# Codex names the usage-limit reset in prose ("try again at Sep 22nd, 2026
# 9:51 AM", live 2026-09-21): month, day with an optional ordinal suffix,
# year, and 12-hour time. No timezone travels with it, so it is read as
# UTC like every other provider reset on this ledger.
_HUMAN_RESET_RE = None


def _human_reset_re():
    global _HUMAN_RESET_RE
    if _HUMAN_RESET_RE is None:
        import re
        _HUMAN_RESET_RE = re.compile(
            r"(?i)\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|"
            r"jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|"
            r"oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
            r"\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})\s+"
            r"(\d{1,2}):(\d{2})(?::(\d{2}))?\s*([ap])\.?\s*m\.?\b")
    return _HUMAN_RESET_RE


def _human_reset_in_text(text, now_ts: float) -> str | None:
    """A provider reset written as a human date in free text.

    Only the full month-day-year-time shape counts ("Sep 22nd, 2026 9:51
    AM"); a bare date or time without the rest is None, so prose can never
    invent a reset. A moment more than a day in the past is stale and also
    None. Read as UTC.
    """
    import datetime
    if not isinstance(text, str) or not text:
        return None
    for match in _human_reset_re().finditer(text):
        try:
            month = _HUMAN_MONTHS[match.group(1).lower()[:3]]
            day = int(match.group(2))
            year = int(match.group(3))
            hour = int(match.group(4))
            minute = int(match.group(5))
            second = int(match.group(6) or 0)
            meridiem = match.group(7).lower()
            if not (1 <= day <= 31 and 1 <= hour <= 12
                    and minute < 60 and second < 60):
                continue
            hour = hour % 12 + (12 if meridiem == "p" else 0)
            moment = datetime.datetime(year, month, day, hour, minute,
                                       second, tzinfo=datetime.timezone.utc)
        except (ValueError, OverflowError):
            continue
        if moment.timestamp() < now_ts - 86400:
            continue
        return moment.isoformat()
    return None


def _walk_dicts(evidence) -> list[dict]:
    """Every nested dict in provider evidence (stall probes nest the probe
    answer under ``probe``/``transport_evidence``/``status``/
    ``message_error_detail``). ``responseBody`` strings are never descended
    into as dicts; stray dates there must never invent a reset."""
    out: list[dict] = []
    seen: set[int] = set()
    stack: list = [evidence]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            if id(cur) in seen:
                continue
            seen.add(id(cur))
            out.append(cur)
            for key, val in cur.items():
                if key == "responseBody":
                    continue
                if isinstance(val, (dict, list)):
                    stack.append(val)
        elif isinstance(cur, list):
            for val in cur:
                if isinstance(val, (dict, list)):
                    stack.append(val)
    return out


def _evidence_strings(evidence) -> list[str]:
    """Candidate strings that may carry a provider reset in prose (Claude
    names the reset time in its message). Only message, reason, and text
    fields are scanned: response bodies are structured payloads where a
    stray date must never invent a reset. Structured reset keys are read
    separately; this only feeds the free-text scan. Stall-probe nesting
    (``probe``/``transport_evidence``/``status``/``message_error_detail``)
    is scanned too, so a probe-discovered reset counts."""
    out: list[str] = []
    for blob in _walk_dicts(evidence):
        for key in ("message", "reason", "text"):
            val = blob.get(key)
            if isinstance(val, str) and val and val not in out:
                out.append(val)
    return out


def parse_provider_reset(evidence, now_ts: float | None = None) -> str | None:
    """A provider-named reset timestamp (ISO string), or None.

    Reads explicit reset fields first (Codex ``resets_at`` in epoch or ISO;
    OpenCode Go ``Retry-After`` in seconds as now plus the delay, which is
    the exact reset), then a reset moment named in message text (Claude's
    ISO moment, Codex's human "Sep 22nd, 2026 9:51 AM" date).
    Zen free and Grok name no reset and yield None: the assumed-window rule
    applies only then. Model-authored text without a full reset moment never
    counts. Stall-probe nesting is scanned, so a probe-discovered reset on
    the same route counts without re-shaping the evidence.
    """
    import time as _time
    now = now_ts if now_ts is not None else _time.time()
    if not isinstance(evidence, dict):
        return None
    dicts: list[dict] = _walk_dicts(evidence)
    for blob in dicts:
        for key in ("resets_at", "reset_at", "resetAt", "provider_reset_at",
                    "quota_reset_at", "resetsAt"):
            if blob.get(key) is not None:
                moment = _coerce_reset_moment(blob.get(key), now)
                if moment is not None:
                    return moment
        for key in ("retry_after", "retryAfter", "Retry-After",
                    "retry_after_secs", "retryAfterSeconds"):
            raw = blob.get(key)
            if raw is None or isinstance(raw, bool):
                continue
            try:
                delay = float(raw)
            except (TypeError, ValueError):
                continue
            if delay > 0:
                import datetime
                return (datetime.datetime.fromtimestamp(now, datetime.timezone.utc)
                        + datetime.timedelta(seconds=delay)).isoformat()
    for text in _evidence_strings(evidence):
        moment = _reset_in_text(text, now)
        if moment is not None:
            return moment
    for text in _evidence_strings(evidence):
        moment = _human_reset_in_text(text, now)
        if moment is not None:
            return moment
    return None


def assumed_reset_at(window: str | None = None, now_ts: float | None = None) -> str:
    """An assumed reset timestamp for a limit event with no provider reset:
    the named window from the event, else the 5-hour default (now plus five
    hours; weekly plus seven days; monthly the next calendar month boundary
    at 00:00 UTC). Stored flagged as assumed, never as provider-reported."""
    import datetime
    import time as _time
    now = now_ts if now_ts is not None else _time.time()
    base = datetime.datetime.fromtimestamp(now, datetime.timezone.utc)
    win = window if window in WINDOW_SECS or window == "monthly" else ASSUMED_WINDOW_DEFAULT
    if win == "weekly":
        return (base + datetime.timedelta(seconds=WINDOW_SECS["weekly"])).isoformat()
    if win == "monthly":
        first = base.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if base.month == 12:
            nxt = first.replace(year=base.year + 1, month=1)
        else:
            nxt = first.replace(month=base.month + 1)
        return nxt.isoformat()
    return (base + datetime.timedelta(seconds=WINDOW_SECS["5h"])).isoformat()


def probe_delay_secs(failures: int) -> float:
    """Delay before the next probe of an assumed weekly or monthly mark:
    starts at one hour, lengthens with each failure, caps at six hours."""
    try:
        n = max(0, int(failures))
    except (TypeError, ValueError):
        n = 0
    return float(min(PROBE_FIRST_DELAY_SECS * (PROBE_BACKOFF_FACTOR ** n),
                     PROBE_MAX_DELAY_SECS))


def compact_enabled(harness: str | None) -> bool:
    """True when this planner harness compacts after submit with --start.

    The Claude planner session compacts headlessly; the Astra fallback on
    the Codex harness never compacts and answers from the stored handoff
    summary in a fresh session with no resume. Unknown harnesses never
    compact: the job still proceeds, only without a compacted session.
    """
    return bool(COMPACT_ON_SUBMIT.get(harness or ""))


def compact_focus(request_id: str, issue: str = "", decisions: str = "",
                  proof: str = "", summary: str = "") -> str:
    """Focus instruction for the headless post-submit compaction.

    Names the job (request id, Issue, decisions, proof command) so the
    resumed session keeps the decisions and drops the planning noise.
    Missing fields render as `-` so the template never fails to format.
    """
    def _clean(value) -> str:
        text = str(value or "").strip()
        return text if text else "-"
    return COMPACT_FOCUS_TEMPLATE.format(
        request_id=_clean(request_id), issue=_clean(issue),
        decisions=_clean(decisions), proof=_clean(proof),
        summary=_clean(summary))


def next_implementation_route(current: str, error) -> str | None:
    """Same model on the next pool, only on exact exhaustion evidence."""
    validate_route(current)
    if classify_signal(error) == "exhausted" and (
            current != "muse-spark-xhigh-free" or classify_quota_exhaustion(error)):
        return next_pool_route(current)
    return None


# ---------------------------------------------------------------------------
# Envelope
# ---------------------------------------------------------------------------

# ``research`` is reserved for a later recon stage: the controller blocks it.
VALID_ACTIONS = ("implementation", "planner_question", "review", "completion", "research")


def make_action(action: str, route: str, payload: dict | None = None,
                artifact: str | None = None, evidence: str | None = None) -> dict:
    if action not in VALID_ACTIONS:
        raise ValueError(f"unsupported action: {action!r}")
    validate_route(route)
    return {"policy": POLICY_ID, "action": action, "route": route,
            "payload": payload or {}, "artifact": artifact, "evidence": evidence}


def make_result(action: str, ok: bool, output: str | None = None,
                error: dict | str | None = None, artifact: str | None = None) -> dict:
    if action not in VALID_ACTIONS:
        raise ValueError(f"unsupported action: {action!r}")
    return {"policy": POLICY_ID, "action": action, "ok": bool(ok),
            "output": output, "error": error, "artifact": artifact}


# ---------------------------------------------------------------------------
# Derived compatibility views
# ---------------------------------------------------------------------------

def _derived_routes() -> dict:
    out = {}
    for name, spec in ROUTES.items():
        out[name] = {"model": spec["model"], "effort": spec.get("variant"),
                     "role": spec["role"], "allowance": POOLS[spec["pool"]]["allowance"],
                     "harness": spec["harness"], "pool": spec["pool"], "operational": True}
    return out


SUPPORTED_ROUTES = _derived_routes()
OPERATIONAL_ROUTES = set(ROUTES)
IMPLEMENTATION_ORDER = implementation_routes()
RECOVERY_ORDER = list(STAGES["recovery"]["routes"])


# ---------------------------------------------------------------------------
# Consistency and rendering
# ---------------------------------------------------------------------------

def validate_policy() -> list[str]:
    """Problems in the policy data, empty when consistent."""
    problems: list[str] = []
    for cls in ("exhausted", "overloaded", "stalled", "context", "hard"):
        spec = SIGNAL_CLASSES.get(cls)
        if not isinstance(spec, dict) or spec.get("action") not in (
                "next_pool", "next_family", "next_larger_context", "implementation_failed"):
            problems.append(f"signal class {cls}: unknown action")
    if not isinstance(STALL_SILENCE_SECS, (int, float)) or STALL_SILENCE_SECS <= 0:
        problems.append("STALL_SILENCE_SECS must be a positive number of seconds")
    if not STALL_EVIDENCE:
        problems.append("STALL_SILENCE_SECS needs its session-database evidence beside it")
    if ASSUMED_WINDOW_DEFAULT not in WINDOW_SECS:
        problems.append("ASSUMED_WINDOW_DEFAULT must name a window in WINDOW_SECS")
    if not (0 < PROBE_FIRST_DELAY_SECS <= PROBE_MAX_DELAY_SECS):
        problems.append("probe schedule must start positive and cap at or above the start")
    if not isinstance(COMPACT_FOCUS_TEMPLATE, str) or "{request_id}" not in COMPACT_FOCUS_TEMPLATE:
        problems.append("COMPACT_FOCUS_TEMPLATE must name the job request id")
    for _token in ("{issue}", "{decisions}", "{proof}", "{summary}"):
        if _token not in COMPACT_FOCUS_TEMPLATE:
            problems.append(f"COMPACT_FOCUS_TEMPLATE must carry {_token}")
    try:
        _probe_focus = compact_focus("r-probe", issue="i", decisions="d", proof="p", summary="s")
    except Exception as _e:  # noqa: BLE001
        problems.append(f"COMPACT_FOCUS_TEMPLATE does not format: {_e}")
    else:
        for _need in ("r-probe", "i", "d", "p", "s"):
            if _need not in _probe_focus:
                problems.append("compact_focus must carry request id, Issue, decisions, proof, and summary")
                break
    if not isinstance(COMPACT_ON_SUBMIT, dict):
        problems.append("COMPACT_ON_SUBMIT must map each planner harness to a flag")
    else:
        if COMPACT_ON_SUBMIT.get("claude") is not True:
            problems.append("COMPACT_ON_SUBMIT must compact the claude planner harness")
        if COMPACT_ON_SUBMIT.get("codex") is not False:
            problems.append("COMPACT_ON_SUBMIT must leave the codex (Astra fallback) harness uncompacted")
    if ROUTES.get("muse-spark-xhigh-free", {}).get("max_concurrent") is not None:
        problems.append("muse-spark-xhigh-free: must carry no concurrency cap "
                        "(parallel Muse free sessions by design)")
    for lane in IMPLEMENTATION_LANES:
        routes = STAGES[lane]["routes"]
        if not routes or routes[0] != "muse-spark-xhigh-free":
            problems.append(f"stage {lane}: first route must be muse-spark-xhigh-free "
                            "(Muse free takes every new job)")
    for name, spec in ROUTES.items():
        if spec["harness"] not in ("codex", "claude", "opencode", "grok"):
            problems.append(f"{name}: unknown harness {spec['harness']}")
        if spec["pool"] not in POOLS:
            problems.append(f"{name}: unknown pool {spec['pool']}")
        size = spec.get("context_window")
        if type(size) is not int or size <= 0:
            problems.append(f"{name}: context_window must be a positive int of tokens")
        if spec["harness"] == "grok":
            if spec["pool"] != "xai":
                problems.append(f"{name}: the native Grok Build harness serves the xai pool, "
                                f"not {spec['pool']}")
            if not spec.get("model") or "/" in spec["model"]:
                problems.append(f"{name}: grok harness model must be a bare model id, "
                                f"got {spec.get('model')!r}")
            if spec.get("agent"):
                problems.append(f"{name}: the grok harness runs headless without an agent")
        if spec["harness"] == "opencode":
            provider, sep, model_id = spec["model"].partition("/")
            if not sep or provider != POOLS[spec["pool"]]["provider"]:
                problems.append(f"{name}: model {spec['model']} is not on pool {spec['pool']}")
            if spec["pool"] == "zen-free" and not model_id.endswith("-free"):
                problems.append(f"{name}: zen-free route must use a -free model")
            if spec["pool"] == "go" and model_id not in GO_MONTHLY_LIMIT_USD:
                problems.append(f"{name}: no monthly limit recorded for {model_id}")
            if model_id.removesuffix("-free") in EXCLUDED_MODELS:
                problems.append(f"{name}: excluded older generation {model_id}")
            if spec["pool"] == "go":
                limit = GO_MONTHLY_LIMIT_USD.get(model_id)
                scarce = (limit or 0) <= SCARCE_MONTHLY_LIMIT_USD
                if scarce != bool(spec.get("one_turn_per_job")):
                    problems.append(f"{name}: one_turn_per_job must be {scarce} for a "
                                    f"${GO_MONTHLY_LIMIT_USD.get(model_id)} model")
                allowed_cap = 1 if limit is not None and limit <= max(CONCURRENT_CAP_TIERS_USD) else None
                cap = spec.get("max_concurrent")
                if allowed_cap == 1:
                    if type(cap) is not int or cap != 1:
                        problems.append(f"{name}: max_concurrent must be 1 for a "
                                        f"${GO_MONTHLY_LIMIT_USD.get(model_id)} model")
                elif cap is not None:
                    problems.append(f"{name}: max_concurrent must be None for a "
                                    f"${GO_MONTHLY_LIMIT_USD.get(model_id)} model")
        nxt = spec.get("next_pool")
        if nxt is not None:
            if nxt not in ROUTES:
                problems.append(f"{name}: next_pool {nxt} unknown")
            elif ROUTES[nxt]["family"] != spec["family"] or not (
                    POOLS[ROUTES[nxt]["pool"]]["order"] > POOLS[spec["pool"]]["order"]
                    # Same model on the same pool through another harness is a
                    # compat fallback (native Grok Build to OpenCode's xAI
                    # provider), not a pool advance.
                    or (ROUTES[nxt]["pool"] == spec["pool"]
                        and ROUTES[nxt]["harness"] != spec["harness"])):
                problems.append(f"{name}: next_pool {nxt} is not the same model on a later pool "
                                "or the same pool through another harness")
    # The 2026-09-20 Go plan: excluded implementers never sit in a lane.
    implementer_routes = set(implementation_routes()) | set(STAGES["correction"]["routes"])
    for route in sorted(implementer_routes):
        spec = ROUTES.get(route)
        if not spec or spec["pool"] != "go":
            continue
        model_id = spec["model"].split("/", 1)[1]
        if model_id in GO_IMPLEMENTER_EXCLUDED:
            problems.append(f"lane route {route}: {model_id} is a never-implementer on Go")
        if model_id == "grok-4.6" and route not in GO_IMPLEMENTER_GROK_ROUTES:
            problems.append(f"lane route {route}: grok-4.6 is allowed only on "
                            f"{', '.join(GO_IMPLEMENTER_GROK_ROUTES)}")
    for stage, spec in STAGES.items():
        if not spec.get("planner_selects"):
            listed = set(spec["routes"])
            guarded = [r for r in ROUTES if ROUTES[r].get("planner_chosen") or ROUTES[r].get("manual")]
            overlap = listed & set(guarded)
            if overlap:
                problems.append(f"stage {stage}: {', '.join(sorted(overlap))} is never "
                                "selected automatically")
        for r in spec.get("manual", ()):
            ms = ROUTES.get(r)
            if ms is None:
                problems.append(f"stage {stage}: unknown manual route {r}")
            elif not ms.get("manual"):
                problems.append(f"stage {stage}: {r} is not marked manual")
        for r in spec["routes"]:
            if r not in ROUTES:
                problems.append(f"stage {stage}: unknown route {r}")
        if spec["executor"] == "planner" and spec["routes"] and not spec.get("planner_selects"):
            problems.append(f"stage {stage}: planner-executed stage lists routes")
        if stage in IMPLEMENTATION_LANES and not spec["routes"]:
            problems.append(f"stage {stage}: empty implementation lane")
    # Role kits: one row per role; installed entries only.
    if set(KITS) != set(KIT_ROLES):
        problems.append(f"kits: must carry one row per role {', '.join(KIT_ROLES)}")
    for _role, _kit in KITS.items():
        if not isinstance(_kit, dict):
            problems.append(f"kit {_role}: missing row")
            continue
        if not isinstance(_kit.get("instructions"), str) or not _kit["instructions"].strip():
            problems.append(f"kit {_role}: missing instructions")
        if _kit.get("permission_set") not in ("full", "read-only"):
            problems.append(f"kit {_role}: permission_set must be full or read-only")
    problems.extend(kit_problems())
    return problems


def render_skill_table() -> str:
    """The Codex skill reference, generated from this policy."""
    lines = [
        "# Codex routing policy",
        "",
        f"Generated from `runner/policy.py` (`{POLICY_ID}` {POLICY_VERSION}); do not edit by hand.",
        f"Source: {POLICY_SOURCE}. Evidence: {POLICY_EVIDENCE}.",
        "",
        "| Stage | Routes in order | Notes |",
        "| --- | --- | --- |",
    ]
    for stage, spec in STAGES.items():
        routes = ", ".join(f"`{r}`" for r in spec["routes"]) or "planner itself"
        if spec.get("planner_selects"):
            routes += " (the planner chooses)"
        if spec.get("overrides"):
            routes += " (override: " + ", ".join(f"`{r}`" for r in spec["overrides"]) + ")"
        if spec.get("manual"):
            routes += " (manual-only: " + ", ".join(f"`{r}`" for r in spec["manual"]) + ")"
        lines.append(f"| {stage} | {routes} | {spec['executor']}; {spec['note']} |")
    lines += [
        "",
        "## Planner-chosen rungs",
        "",
        "The planner itself chooses one rung and runs it in its own session; "
        "the runner never selects these automatically: "
        + "; ".join(f"`{r}` = `{ROUTES[r]['model']}` {ROUTES[r]['variant']}"
                    for r in STAGES["planner_rungs"]["routes"]) + ".",
        "",
        "Route models, in policy order: "
        + "; ".join(f"`{r}` = `{s['model']}`" + (f" {s['variant']}" if s.get("variant") else "")
                    for r, s in ROUTES.items())
        + ".",
        "",
        "Rules:",
        "",
        "- Implementation, correction, and recovery use the routes declared for "
        "the runner (`python -m runner submit`). Use a native Codex subagent only "
        "for `review_ticket`.",
        "- Classify by consequences, not file type. A step or prose that the rest "
        "of the work depends on is `critical`: do it in the planner session yourself, "
        "then submit the remainder. Changes to agent instructions, security rules, "
        "and specifications are at least `implementation_hard`.",
        f"- Pools in order: {', '.join(WORKER_POOL_ORDER)}. Subscription logins only; "
        "no pay-per-token API keys and no paid balance overflow.",
        "- The policy declares that exhaustion (" + ", ".join(SIGNAL_CLASSES["exhausted"]["retry_reasons"]
                                      + SIGNAL_CLASSES["exhausted"]["error_names"])
        + ") moves the same model to the next pool with no retries where the route declares one (free Muse to Go Muse, Go Grok to xAI Grok); without a next pool it moves to the next model family in its lane. "
        "It declares that overload ("
        + ", ".join(SIGNAL_CLASSES["overloaded"]["retry_reasons"]
                     + SIGNAL_CLASSES["overloaded"]["error_names"]) + ", HTTP "
        + ", ".join(str(c) for c in SIGNAL_CLASSES["overloaded"]["status_codes"])
        + f") allows {SIGNAL_CLASSES['overloaded']['retries']} retries inside "
        f"{SIGNAL_CLASSES['overloaded']['window_secs']} seconds with `next` capped at "
        f"{SIGNAL_CLASSES['overloaded']['next_cap_secs']} seconds (a `next` larger than the cap "
        "aborts instead of waiting), then moves to the next "
        "model family within one minute. "
        "Hard errors end the turn as `implementation_failed` for the ladder; they never move routes.",
        f"- A worker turn that stops streaming is stalled: no new part for {STALL_SILENCE_SECS} "
        "seconds while busy ends the turn within the window plus one poll "
        f"(evidence: {STALL_EVIDENCE}). The detector probes the same route with a minimal request "
        "first, so a stall that is exhaustion in disguise moves pools instead of retrying the same "
        "route. Stalled is treated like overload: a bounded same-route retry inside "
        f"{SIGNAL_CLASSES['stalled']['window_secs']} seconds, then a lateral move with the route "
        f"degraded {SIGNAL_CLASSES['stalled']['degraded_secs'] // 60} minutes. "
        "The per-turn timeout stays as the outer budget for active turns.",
        "- A `context_length_exceeded` turn is a capacity signal: it moves to the next route in "
        "its lane with a larger `context_window`, and only ends as `implementation_failed` when no "
        "larger-context route is left.",
        "- A limit event with no provider reset time assumes one from the named or default window "
        "(5-hour: now plus five hours; weekly plus seven days; monthly the next month boundary), "
        "flagged as assumed rather than provider-reported, honored by preflight, and retried once "
        "after it passes. Assumed weekly and monthly marks are re-probed on a lengthening schedule "
        "(one hour first, doubling, six-hour cap) and cleared on the first success; every probe "
        "outcome is recorded for the observer.",
        "- Each invocation records its longest observed stream silence "
        "(`longest_silence_secs`), so the silence window is tuned on data through Agent Observer.",
        "- Before choosing a route the router reads each subscription window's "
        "usage and exact reset time wherever the provider exposes it: Codex "
        "`account/rateLimits/read` (every 300 seconds, or on demand before a "
        "dispatch), Claude `/usage` (every 180 seconds at most, plus the free "
        "statusline feed), OpenCode Go rolling cost sums from the local "
        "database against the tier windows, Zen free request counts against "
        "an assumed cap, Grok monthly billing (provider-reported) with weekly "
        "assumed and error-driven cool-off. Every reading carries its source "
        f"({', '.join(READING_SOURCES)}) with `used`, `limit`, `reset_at`, "
        "and `observed_at` per pool, model, and window, stored in the "
        "capacity ledger and exposed by `capacity` and `status`.",
        "- Readings and limit errors reconcile by fixed rules: an error "
        "always overrides a probe for the window it names; a probe below 100 "
        "percent never clears a provider exhaustion before its `reset_at`; "
        "after `reset_at` the route needs one fresh probe or one successful "
        "request to revalidate it before it is eligible again. A window at or above "
        f"{int(USAGE_DEGRADED_FRACTION * 100)} percent marks the route degraded; "
        "an exhausted window skips it until `reset_at`. A failing or slow "
        "probe records `unknown` and never blocks routing: the route stays "
        "eligible on error evidence alone.",
        f"- Go models with a ${SCARCE_MONTHLY_LIMIT_USD} monthly limit get one turn per job. "
        "Go routes on the "
        f"${' and $'.join(str(t) for t in CONCURRENT_CAP_TIERS_USD)} tiers allow at most one "
        "running job at a time (`max_concurrent`)."
        " Muse on Zen free (`muse-spark-xhigh-free`) is the first route of every "
        "implementation lane, carries no concurrency cap, and takes every new job "
        "(parallel jobs open parallel Muse free sessions by design). The "
        "`fewest running jobs` spread applies only among routes that carry a "
        "`max_concurrent` cap; a job leaves Muse free only on evidence "
        "(exhausted moves the same model to the next pool, overload moves to the "
        "next family with the route degraded 15 minutes)."
        " A job walks the lane only on evidence."
        f" Windows: 5-hour {int(WINDOWS['5h'] * 100)} percent, weekly {int(WINDOWS['weekly'] * 100)} "
        "percent, monthly 100 percent of the model's limit.",
        "- Implementation, correction, and recovery worker sessions run with full access: "
        "outside-workspace writes, web fetch, web search, and doom-loop prompts are allowed "
        "as a per-route policy flag; `question` and `task` stay denied (headless stall; "
        "policy bypass). Dispatch and review routes on OpenCode run read-only in plan mode: "
        "`edit`, outside-workspace writes, web fetch/search, doom-loop, `question` and `task` "
        "stay denied (the coordinator role never needs them).",
        "- One escalation per job; afterwards evidence returns to the planner. No "
        "duplicate attempts, no retry loops. Never substitute a route silently; if "
        "the selected route is unavailable, stop that dispatch with the reason.",
        "- After a successful submit with `--start`, the Claude planner session "
        "compacts headlessly around the job (`claude -p --resume <sid> "
        "\"/compact <focus>\"`, focus from `COMPACT_FOCUS_TEMPLATE` naming the "
        "request id, Issue, decisions, and proof command), recorded as a "
        "`claude_compact` invocation with elapsed time and usage; a compact "
        "failure is recorded and never blocks the job, and `COMPACT_ON_SUBMIT` "
        "leaves the Codex (Astra fallback) harness uncompacted. Every callback "
        "prompt carries the stored handoff summary before the question, and its "
        "`claude_callback` invocation records input, cache-read, and "
        "cache-creation tokens so the saving is visible per job; the Astra "
        "fallback answers from the handoff summary in a fresh session with no "
        "resume.",
        "- Record the policy version, the requested and observed route, and any "
        "override or escalation in the existing handoff. Instructions describe "
        "the policy and its required evidence.",
        "",
        "## Role kits",
        "",
        "One kit row per role; a role's tools change by editing policy. "
        "The owned OpenCode server runs on a runner-generated configuration "
        "directory built from the route's kit (AgentsMD link, plugins, skills, "
        "MCP servers named by the kit, permission set from the route); "
        "Codex uses `CODEX_HOME`, Claude uses `CLAUDE_CONFIG_DIR`, Grok uses "
        "its config directory; the planner keeps the user's own session.",
        "",
        "| Role | Skills | Plugins | MCP servers | Permissions | Instructions |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for _role in KIT_ROLES:
        _kit = KITS[_role]
        _skills = ", ".join(f"`{s}`" for s in _kit.get("skills", [])) or "none"
        _plugins = ", ".join(f"`{p}`" for p in _kit.get("plugins", [])) or "none"
        _mcp = ", ".join(f"`{m}`" for m in _kit.get("mcp", [])) or "none"
        _perm = str(_kit.get("permission_set"))
        _instr = str(_kit.get("instructions", "")).split(".")[0].strip()
        lines.append(f"| {_role} | {_skills} | {_plugins} | {_mcp} | {_perm} | {_instr} |")
    lines += [
        "",
        "Kit hashes (`kit_hash`): "
        + "; ".join(f"`{r}` = {kit_hash(k)}" for r, k in KITS.items())
        + ".",
        "",
    ]
    return "\n".join(lines)


def main(argv=None) -> int:
    """``render-skill`` prints the skill reference; ``validate`` exits 1 on
    problems. Run as ``python -m runner.policy validate`` or
    ``python -c "from runner import policy; policy.main([...])"``."""
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv == ["render-skill"]:
        sys.stdout.write(render_skill_table())
        return 0
    if argv == ["validate"]:
        problems = validate_policy()
        for problem in problems:
            print(problem)
        return 1 if problems else 0
    print("usage: policy.main(['render-skill'] | ['validate'])", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
