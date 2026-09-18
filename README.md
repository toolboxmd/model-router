# Model Router

A small Codex routing contract. AgentsMD owns workflow, authority, and proof;
Model Router owns the model choices in [one policy table](skills/model-routing/references/codex.md).
The [routing Skill](skills/model-routing/SKILL.md) loads only the active host's reference.

## Install from a release

This package has not been released yet. Once released, ask Codex's
`$skill-installer` to install `skills/model-routing` from `toolboxmd/model-router`
at that exact release tag. Start a fresh session and confirm the Skill is available
before activating the AgentsMD integration.

The plugin manifest also supports future marketplace distribution; no marketplace
entry or installation is included here. Validate with the existing Skill Creator
and Plugin Creator validators. Actual routing behavior is verified through ordinary work.

## Project Direction

[Vision](VISION.md) · [Mission](MISSION.md) · [Objective](OBJECTIVE.md).
Active work belongs in GitHub Issues.

## Durable runner bridge (stdlib only)

Small production-shaped bridge. No services, dependencies, credentials,
or network brokers. See [RUNNER.md](RUNNER.md) for state, ownership,
and policy details.

Public submit works with built-in defaults. Provide only a prepared
JSON task, workspace, stable request ID, and original planner session ID:

```
python -m runner --state-dir DIR submit --request-id ID --task JSON|--task-file F \
  --workspace PATH --planner-session SID [--route R] [--planner-model M] [--planner-effort E] \
  [--start|--no-start]
python -m runner --state-dir DIR start --request-id ID
python -m runner --state-dir DIR status --request-id ID
python -m runner --state-dir DIR questions --request-id ID [--all]
python -m runner --state-dir DIR answer --request-id ID --qid Q --answer TEXT
python -m runner --state-dir DIR cancel --request-id ID
python -m runner --state-dir DIR recover (--request-id ID | --all)
```

Defaults: Codex `codex exec --json --output-last-message PATH --model
gpt-5.6-luna -c model_reasoning_effort="max" --sandbox workspace-write
--cd WS` (resume is `codex exec resume ID --json` with the saved
workspace as cwd, never `--cd`/`--reasoning`); Claude `claude --resume
SID --model fable-5.1 --effort max` production default (explicit
`claude-sonnet-5`/`medium` is only the bounded live-test override,
overrides allowed, never forks a new session); OpenCode
`opencode run --format json --pure --dir WS --model
opencode/muse-spark-1.3-contributor-free --variant xhigh --agent build`
free-first (`opencode-go/muse-spark-1.3-contributor` only after exact
free exhaustion, `--session` resumes the saved session).
`--no-start` keeps deterministic tests offline; `--start`/`start`
uses the built-ins with no injection required. OpenCode control uses
an owned ephemeral per-job `opencode serve --pure --hostname
127.0.0.1 --port 0` process (fresh in-memory password, never logged)
or the injectable equivalent.

Controller flow: Luna returns `planner_question`, `implementation`, or
`completion`. Questions persist before Claude `--resume`; answers persist
before resuming the same Luna task. Implementation carries the saved
artifact. Busy planner or callback failure becomes durable `blocked`
with a reason. `recover` resumes saved IDs and never forks.

Quota: free to Go only on the exact vendor class `FreeUsageLimitError`
(including decoded `responseBody` JSON) or a retry with reason
`free_tier_limit` and provider `opencode`. The old free session aborts
before transfer, ownership/idle confirms, artifacts preserve. Generic
`RateLimitError`/`rate_limit`, 429, timeout, `DataPolicyError`,
`RegionError`, `AuthError`, consent/permission, and `GoUsageLimitError`/
`account_rate_limit` never select Go. Zen overflow and direct paid APIs
stay disabled.

Bounded live fixture (external only, never in deterministic tests):
Sonnet medium probe, then Luna max dispatch, then Muse xhigh free
implementation, then one planner question, then completion. Fixture:
`fixtures/live_sequence.json`; recipe: `fixtures/LIVE_RECIPE.md`.
Real live verification burns real quota and stays outside the suite:

```
python -m unittest discover -s tests -v
python -m compileall -q runner tests
```
